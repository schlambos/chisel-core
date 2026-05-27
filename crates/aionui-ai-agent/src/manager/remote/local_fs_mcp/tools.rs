//! Filesystem tool implementations exposed over MCP.
//!
//! Every operation resolves user-supplied paths *relative to* a fixed
//! project root and rejects anything that escapes it. The model never
//! sees absolute client paths; only `src/foo.ts`-style relative paths.

use std::path::{Component, Path, PathBuf};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use ignore::WalkBuilder;
use regex::RegexBuilder;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::fs;
use tracing::warn;

const MAX_READ_BYTES: u64 = 4 * 1024 * 1024; // 4 MiB cap per read
const MAX_GREP_FILES: usize = 5000;
const MAX_GREP_MATCHES: usize = 500;
const MAX_LIST_ENTRIES: usize = 1000;

/// One MCP tool descriptor (advertised in `tools/list`).
pub struct ToolDescriptor {
    pub name: &'static str,
    pub description: &'static str,
    pub input_schema: Value,
}

pub fn all_tool_descriptors() -> Vec<ToolDescriptor> {
    vec![
        ToolDescriptor {
            name: "read_file",
            description: "Read a UTF-8 text file from the user's local project. \
Path must be RELATIVE to the project root (e.g. \"src/main.rs\"). \
Use this instead of any built-in read tools — the project lives on the user's machine, not yours.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Relative path inside the project root." }
                },
                "required": ["path"]
            }),
        },
        ToolDescriptor {
            name: "write_file",
            description: "Create or overwrite a UTF-8 text file in the user's local project. \
Path must be RELATIVE to the project root. \
Use this instead of any built-in write tools — the project lives on the user's machine, not yours.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "content": { "type": "string" }
                },
                "required": ["path", "content"]
            }),
        },
        ToolDescriptor {
            name: "list_dir",
            description: "List directory entries in the user's local project. \
Path must be RELATIVE to the project root; pass \"\" or \".\" for the root.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "default": "" }
                }
            }),
        },
        ToolDescriptor {
            name: "grep_dir",
            description: "Search for a regular expression across files in the user's local project. \
Respects .gitignore. `path` is RELATIVE to project root (default: project root).",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "pattern": { "type": "string", "description": "Regex pattern." },
                    "path": { "type": "string", "default": "" },
                    "case_insensitive": { "type": "boolean", "default": false }
                },
                "required": ["pattern"]
            }),
        },
        ToolDescriptor {
            name: "delete_file",
            description: "Delete a file in the user's local project. Path must be RELATIVE to project root.",
            input_schema: json!({
                "type": "object",
                "properties": { "path": { "type": "string" } },
                "required": ["path"]
            }),
        },
        ToolDescriptor {
            name: "rename",
            description: "Rename or move a file within the user's local project. Both paths RELATIVE to project root.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "from": { "type": "string" },
                    "to": { "type": "string" }
                },
                "required": ["from", "to"]
            }),
        },
    ]
}

/// Resolve a model-supplied relative path against `root`, refusing anything
/// that escapes it (via `..`, absolute paths, or symlink chains pointing
/// outside). Returns the canonicalized absolute path on success.
pub fn resolve_under_root(root: &Path, rel: &str) -> Result<PathBuf, String> {
    let trimmed = rel.trim().trim_start_matches('/');
    let rel_path = Path::new(if trimmed.is_empty() { "." } else { trimmed });

    for component in rel_path.components() {
        match component {
            Component::ParentDir => return Err(format!("path escapes project root: {rel}")),
            Component::Prefix(_) | Component::RootDir => {
                return Err(format!("path escapes project root: {rel}"));
            }
            _ => {}
        }
    }

    let joined = root.join(rel_path);

    // We don't require the target to exist (for write_file / rename targets),
    // so canonicalize the parent and re-attach the file name when needed.
    let canon = if joined.exists() {
        joined.canonicalize().map_err(|e| format!("canonicalize failed: {e}"))?
    } else if let Some(parent) = joined.parent() {
        let parent_canon = parent
            .canonicalize()
            .map_err(|e| format!("canonicalize parent failed: {e}"))?;
        match joined.file_name() {
            Some(name) => parent_canon.join(name),
            None => parent_canon,
        }
    } else {
        return Err(format!("invalid path: {rel}"));
    };

    let root_canon = root
        .canonicalize()
        .map_err(|e| format!("project root canonicalize failed: {e}"))?;
    if !canon.starts_with(&root_canon) {
        return Err(format!("path escapes project root: {rel}"));
    }
    Ok(canon)
}

#[derive(Debug, Deserialize)]
struct ReadInput {
    path: String,
}

#[derive(Debug, Deserialize)]
struct WriteInput {
    path: String,
    content: String,
}

#[derive(Debug, Deserialize)]
struct ListInput {
    #[serde(default)]
    path: String,
}

#[derive(Debug, Deserialize)]
struct GrepInput {
    pattern: String,
    #[serde(default)]
    path: String,
    #[serde(default)]
    case_insensitive: bool,
}

#[derive(Debug, Deserialize)]
struct DeleteInput {
    path: String,
}

#[derive(Debug, Deserialize)]
struct RenameInput {
    from: String,
    to: String,
}

#[derive(Debug, Serialize)]
struct ListEntry {
    name: String,
    #[serde(rename = "type")]
    kind: &'static str,
    size: Option<u64>,
}

/// Dispatch a `tools/call` to the appropriate fs operation.
/// Returns `(content_text, is_error)`.
pub async fn dispatch(root: &Path, tool: &str, args: &Value) -> (String, bool) {
    match tool {
        "read_file" => match serde_json::from_value::<ReadInput>(args.clone()) {
            Ok(input) => match read_file(root, &input.path).await {
                Ok(text) => (text, false),
                Err(e) => (e, true),
            },
            Err(e) => (format!("invalid params: {e}"), true),
        },
        "write_file" => match serde_json::from_value::<WriteInput>(args.clone()) {
            Ok(input) => match write_file(root, &input.path, &input.content).await {
                Ok(msg) => (msg, false),
                Err(e) => (e, true),
            },
            Err(e) => (format!("invalid params: {e}"), true),
        },
        "list_dir" => match serde_json::from_value::<ListInput>(args.clone()) {
            Ok(input) => match list_dir(root, &input.path).await {
                Ok(json_text) => (json_text, false),
                Err(e) => (e, true),
            },
            Err(e) => (format!("invalid params: {e}"), true),
        },
        "grep_dir" => match serde_json::from_value::<GrepInput>(args.clone()) {
            Ok(input) => match grep_dir(root, &input.pattern, &input.path, input.case_insensitive) {
                Ok(json_text) => (json_text, false),
                Err(e) => (e, true),
            },
            Err(e) => (format!("invalid params: {e}"), true),
        },
        "delete_file" => match serde_json::from_value::<DeleteInput>(args.clone()) {
            Ok(input) => match delete_file(root, &input.path).await {
                Ok(msg) => (msg, false),
                Err(e) => (e, true),
            },
            Err(e) => (format!("invalid params: {e}"), true),
        },
        "rename" => match serde_json::from_value::<RenameInput>(args.clone()) {
            Ok(input) => match rename(root, &input.from, &input.to).await {
                Ok(msg) => (msg, false),
                Err(e) => (e, true),
            },
            Err(e) => (format!("invalid params: {e}"), true),
        },
        _ => (format!("unknown tool: {tool}"), true),
    }
}

async fn read_file(root: &Path, rel: &str) -> Result<String, String> {
    let abs = resolve_under_root(root, rel)?;
    let meta = fs::metadata(&abs).await.map_err(|e| format!("stat failed: {e}"))?;
    if !meta.is_file() {
        return Err(format!("not a regular file: {rel}"));
    }
    if meta.len() > MAX_READ_BYTES {
        return Err(format!(
            "file too large ({} bytes; cap is {})",
            meta.len(),
            MAX_READ_BYTES
        ));
    }
    let bytes = fs::read(&abs).await.map_err(|e| format!("read failed: {e}"))?;
    match String::from_utf8(bytes.clone()) {
        Ok(text) => Ok(text),
        Err(_) => {
            // Surface binary content as base64 so the agent can still operate.
            Ok(json!({
                "encoding": "base64",
                "content": BASE64.encode(&bytes),
            })
            .to_string())
        }
    }
}

async fn write_file(root: &Path, rel: &str, content: &str) -> Result<String, String> {
    let abs = resolve_under_root(root, rel)?;
    if let Some(parent) = abs.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|e| format!("mkdir failed: {e}"))?;
    }
    fs::write(&abs, content)
        .await
        .map_err(|e| format!("write failed: {e}"))?;
    Ok(format!("wrote {} bytes to {rel}", content.len()))
}

async fn list_dir(root: &Path, rel: &str) -> Result<String, String> {
    let abs = resolve_under_root(root, rel)?;
    let meta = fs::metadata(&abs).await.map_err(|e| format!("stat failed: {e}"))?;
    if !meta.is_dir() {
        return Err(format!("not a directory: {rel}"));
    }
    let mut entries = Vec::with_capacity(64);
    let mut reader = fs::read_dir(&abs).await.map_err(|e| format!("readdir failed: {e}"))?;
    let mut truncated = false;
    while let Some(entry) = reader.next_entry().await.map_err(|e| format!("readdir failed: {e}"))? {
        if entries.len() >= MAX_LIST_ENTRIES {
            truncated = true;
            break;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        let file_type = entry.file_type().await.map_err(|e| format!("file_type failed: {e}"))?;
        let (kind, size) = if file_type.is_dir() {
            ("dir", None)
        } else if file_type.is_file() {
            let size = entry.metadata().await.ok().map(|m| m.len());
            ("file", size)
        } else if file_type.is_symlink() {
            ("symlink", None)
        } else {
            ("other", None)
        };
        entries.push(ListEntry { name, kind, size });
    }
    let payload = json!({
        "path": if rel.trim().is_empty() { "." } else { rel },
        "truncated": truncated,
        "entries": entries,
    });
    serde_json::to_string(&payload).map_err(|e| format!("serialize failed: {e}"))
}

fn grep_dir(root: &Path, pattern: &str, rel: &str, case_insensitive: bool) -> Result<String, String> {
    let base = resolve_under_root(root, rel)?;
    let re = RegexBuilder::new(pattern)
        .case_insensitive(case_insensitive)
        .build()
        .map_err(|e| format!("bad regex: {e}"))?;

    let mut matches: Vec<Value> = Vec::new();
    let mut files_scanned = 0usize;
    let mut truncated = false;

    let walker = WalkBuilder::new(&base).hidden(false).follow_links(false).build();
    for dent in walker {
        let entry = match dent {
            Ok(e) => e,
            Err(e) => {
                warn!(error = %e, "grep walker error");
                continue;
            }
        };
        if !entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
            continue;
        }
        if files_scanned >= MAX_GREP_FILES {
            truncated = true;
            break;
        }
        files_scanned += 1;

        let path = entry.path();
        let Ok(contents) = std::fs::read_to_string(path) else {
            continue;
        };
        for (idx, line) in contents.lines().enumerate() {
            if re.is_match(line) {
                if matches.len() >= MAX_GREP_MATCHES {
                    truncated = true;
                    break;
                }
                let rel_path = path.strip_prefix(root).unwrap_or(path).to_string_lossy().into_owned();
                matches.push(json!({
                    "path": rel_path,
                    "line": idx + 1,
                    "text": line,
                }));
            }
        }
        if matches.len() >= MAX_GREP_MATCHES {
            break;
        }
    }

    serde_json::to_string(&json!({
        "pattern": pattern,
        "files_scanned": files_scanned,
        "truncated": truncated,
        "matches": matches,
    }))
    .map_err(|e| format!("serialize failed: {e}"))
}

async fn delete_file(root: &Path, rel: &str) -> Result<String, String> {
    let abs = resolve_under_root(root, rel)?;
    let meta = fs::metadata(&abs).await.map_err(|e| format!("stat failed: {e}"))?;
    if !meta.is_file() {
        return Err(format!("not a regular file: {rel}"));
    }
    fs::remove_file(&abs).await.map_err(|e| format!("remove failed: {e}"))?;
    Ok(format!("deleted {rel}"))
}

async fn rename(root: &Path, from: &str, to: &str) -> Result<String, String> {
    let from_abs = resolve_under_root(root, from)?;
    let to_abs = resolve_under_root(root, to)?;
    if let Some(parent) = to_abs.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|e| format!("mkdir failed: {e}"))?;
    }
    fs::rename(&from_abs, &to_abs)
        .await
        .map_err(|e| format!("rename failed: {e}"))?;
    Ok(format!("renamed {from} -> {to}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn tmp() -> (TempDir, PathBuf) {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().canonicalize().expect("canonicalize root");
        (dir, root)
    }

    #[test]
    fn resolve_rejects_parent_dir() {
        let (_g, root) = tmp();
        assert!(resolve_under_root(&root, "../etc/passwd").is_err());
        assert!(resolve_under_root(&root, "a/../../etc").is_err());
    }

    #[test]
    fn resolve_rejects_absolute() {
        let (_g, root) = tmp();
        assert!(resolve_under_root(&root, "/etc/passwd").is_err());
    }

    #[test]
    fn resolve_accepts_empty_and_dot() {
        let (_g, root) = tmp();
        let a = resolve_under_root(&root, "").unwrap();
        let b = resolve_under_root(&root, ".").unwrap();
        assert_eq!(a, root);
        assert_eq!(b, root);
    }

    #[tokio::test]
    async fn write_then_read_roundtrip() {
        let (_g, root) = tmp();
        let (out, err) = dispatch(&root, "write_file", &json!({"path": "hello.txt", "content": "hi"})).await;
        assert!(!err, "write_file should succeed: {out}");

        let (out, err) = dispatch(&root, "read_file", &json!({"path": "hello.txt"})).await;
        assert!(!err, "read_file should succeed: {out}");
        assert_eq!(out, "hi");
    }

    #[tokio::test]
    async fn list_dir_returns_entries() {
        let (_g, root) = tmp();
        std::fs::create_dir(root.join("sub")).unwrap();
        std::fs::write(root.join("a.txt"), "x").unwrap();
        let (out, err) = dispatch(&root, "list_dir", &json!({"path": ""})).await;
        assert!(!err);
        let v: Value = serde_json::from_str(&out).unwrap();
        let entries = v["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn read_rejects_escape() {
        let (_g, root) = tmp();
        let (out, err) = dispatch(&root, "read_file", &json!({"path": "../etc/passwd"})).await;
        assert!(err);
        assert!(out.contains("escapes project root"));
    }

    #[tokio::test]
    async fn grep_finds_matches() {
        let (_g, root) = tmp();
        std::fs::write(root.join("a.txt"), "hello\nworld\nHelloAgain\n").unwrap();
        let (out, err) = dispatch(
            &root,
            "grep_dir",
            &json!({"pattern": "hello", "case_insensitive": true}),
        )
        .await;
        assert!(!err, "grep failed: {out}");
        let v: Value = serde_json::from_str(&out).unwrap();
        let matches = v["matches"].as_array().unwrap();
        assert_eq!(matches.len(), 2);
    }
}
