//! Filesystem tool implementations exposed over MCP.
//!
//! Every operation resolves user-supplied paths *relative to* a fixed
//! project root and rejects anything that escapes it. The model never
//! sees absolute client paths; only `src/foo.ts`-style relative paths.

use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use ignore::WalkBuilder;
use regex::RegexBuilder;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::fs;
use tracing::warn;

use super::shell::{self, ShellApproval, ShellApprover};

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
Paths are RELATIVE to the project root (e.g. \"src/main.rs\"). \
Do NOT prepend the workspace's absolute path that appears in session context. \
Use this instead of any built-in read tools — the project lives on the user's machine, not yours. \
If you're unsure whether a path exists, call list_dir first — never tell the user a file is missing without verifying.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Relative path inside the project root (e.g. \"src/main.rs\"). Do not prepend the workspace's absolute path." }
                },
                "required": ["path"]
            }),
        },
        ToolDescriptor {
            name: "write_file",
            description: "Create or overwrite a UTF-8 text file in the user's local project. \
Paths are RELATIVE to the project root (e.g. \"src/main.rs\"). \
Do NOT prepend the workspace's absolute path that appears in session context. \
Use this instead of any built-in write tools — the project lives on the user's machine, not yours.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Relative path inside the project root. Do not prepend the workspace's absolute path." },
                    "content": { "type": "string" }
                },
                "required": ["path", "content"]
            }),
        },
        ToolDescriptor {
            name: "list_dir",
            description: "List directory entries in the user's local project. \
Pass \"\" or \".\" for the project root. Subdirectories are RELATIVE (e.g. \"src\" or \"docs/api\"). \
Do NOT prepend the workspace's absolute path that appears in session context. \
Call this whenever you need to confirm what is or isn't present — do not assume from prior turns. \
Re-list after any write/delete/rename if subsequent decisions depend on the new layout.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "default": "", "description": "\"\" or \".\" for the project root, otherwise a relative subdirectory (e.g. \"src\"). Do not prepend the workspace's absolute path." }
                }
            }),
        },
        ToolDescriptor {
            name: "grep_dir",
            description: "Search for a regular expression across files in the user's local project. \
Respects .gitignore. `path` is RELATIVE to project root (default: project root). \
Do NOT prepend the workspace's absolute path that appears in session context.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "pattern": { "type": "string", "description": "Regex pattern." },
                    "path": { "type": "string", "default": "", "description": "Relative subdirectory; \"\" for the project root. Do not prepend the workspace's absolute path." },
                    "case_insensitive": { "type": "boolean", "default": false }
                },
                "required": ["pattern"]
            }),
        },
        ToolDescriptor {
            name: "delete_file",
            description: "Delete a file in the user's local project. Paths are RELATIVE to project root. \
Do NOT prepend the workspace's absolute path that appears in session context.",
            input_schema: json!({
                "type": "object",
                "properties": { "path": { "type": "string", "description": "Relative path inside the project root. Do not prepend the workspace's absolute path." } },
                "required": ["path"]
            }),
        },
        ToolDescriptor {
            name: "rename",
            description: "Rename or move a file within the user's local project. Both paths RELATIVE to project root. \
Do NOT prepend the workspace's absolute path that appears in session context.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "from": { "type": "string", "description": "Relative source path. Do not prepend the workspace's absolute path." },
                    "to":   { "type": "string", "description": "Relative destination path. Do not prepend the workspace's absolute path." }
                },
                "required": ["from", "to"]
            }),
        },
        ToolDescriptor {
            name: "run_shell",
            description: "Run a shell command on the USER'S LOCAL machine — where their project actually lives — and return its stdout, stderr, and exit code. \
Use this to verify your work: build, run tests, run linters/formatters, git, or any terminal command. \
Your own built-in/remote shell cannot see this project, so this is the ONLY way to execute commands against it. \
The command runs in the project root using the user's native shell (the session context states which OS/shell — write the command in that syntax). \
IMPORTANT: every call requires the user to approve the exact command before it runs, so combine related steps into one command rather than issuing many small ones.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "command": { "type": "string", "description": "The exact command line to run in the user's native shell, in that shell's syntax." }
                },
                "required": ["command"]
            }),
        },
    ]
}

/// Resolve a model-supplied path against `root`, refusing anything that
/// escapes it (via `..` traversal or absolute paths pointing outside).
/// Returns the canonicalized absolute path on success.
///
/// Tolerates absolute paths that happen to live under the project root —
/// models often paste the workspace's absolute path from session context
/// despite the "relative" instruction; rejecting those produces the
/// intermittent "is_error=true" failures we were seeing in production.
pub fn resolve_under_root(root: &Path, rel: &str) -> Result<PathBuf, String> {
    let trimmed = rel.trim();
    let input = Path::new(trimmed);

    let root_canon = root
        .canonicalize()
        .map_err(|e| format!("project root canonicalize failed: {e}"))?;

    let working: String = if input.is_absolute() {
        let abs_canon = if input.exists() {
            input.canonicalize().map_err(|e| format!("canonicalize failed: {e}"))?
        } else {
            input.to_path_buf()
        };
        match abs_canon.strip_prefix(&root_canon) {
            Ok(stripped) if stripped.as_os_str().is_empty() => String::from("."),
            Ok(stripped) => stripped.to_string_lossy().into_owned(),
            Err(_) => return Err(format!("path escapes project root: {rel}")),
        }
    } else if trimmed.is_empty() {
        String::from(".")
    } else {
        trimmed.to_string()
    };

    let rel_path = Path::new(if working.is_empty() { "." } else { &working });

    for component in rel_path.components() {
        if matches!(component, Component::ParentDir) {
            return Err(format!("path escapes project root: {rel}"));
        }
    }

    let joined = root.join(rel_path);

    // We don't require the target — or its parent chain — to exist. Write
    // operations (write_file, rename) often target paths several levels
    // deep into directories that haven't been created yet; the operation
    // itself does the mkdir. Walk up to the nearest existing ancestor,
    // canonicalize that, and re-attach the missing tail.
    let canon = if joined.exists() {
        joined.canonicalize().map_err(|e| format!("canonicalize failed: {e}"))?
    } else {
        let existing = joined
            .ancestors()
            .skip(1)
            .find(|a| a.exists())
            .ok_or_else(|| format!("no existing ancestor for: {rel}"))?;
        let existing_canon = existing
            .canonicalize()
            .map_err(|e| format!("canonicalize ancestor failed: {e}"))?;
        let tail = joined
            .strip_prefix(existing)
            .map_err(|e| format!("strip_prefix failed: {e}"))?;
        existing_canon.join(tail)
    };

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

#[derive(Debug, Deserialize)]
struct ShellInput {
    command: String,
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
///
/// `approver` gates the `run_shell` tool: the command is only executed once
/// the user approves it through the host agent's confirmation UI. It is
/// `None` for the filesystem tools (which need no approval) and when no
/// approval channel is wired up — in which case `run_shell` fails closed.
pub async fn dispatch(
    root: &Path,
    tool: &str,
    args: &Value,
    approver: Option<&Arc<dyn ShellApprover>>,
) -> (String, bool) {
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
        "run_shell" => match serde_json::from_value::<ShellInput>(args.clone()) {
            Ok(input) => run_shell(root, &input.command, approver).await,
            Err(e) => (format!("invalid params: {e}"), true),
        },
        _ => (format!("unknown tool: {tool}"), true),
    }
}

/// Gate a shell command on user approval, then run it locally. Fails closed:
/// an empty command or a missing approval channel never executes anything.
async fn run_shell(root: &Path, command: &str, approver: Option<&Arc<dyn ShellApprover>>) -> (String, bool) {
    let command = command.trim();
    if command.is_empty() {
        return ("empty command".to_string(), true);
    }
    let Some(approver) = approver else {
        warn!("run_shell invoked with no approval channel; refusing to execute");
        return (
            "shell execution is unavailable: no approval channel is configured for this session".to_string(),
            true,
        );
    };
    let cwd = root.to_string_lossy();
    match approver.approve_shell(command, &cwd).await {
        ShellApproval::Allow => shell::run_shell(root, command).await,
        ShellApproval::Reject => ("command was rejected by the user".to_string(), true),
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

    #[test]
    fn resolve_accepts_absolute_path_inside_root() {
        let (_g, root) = tmp();
        std::fs::create_dir(root.join("src")).unwrap();
        std::fs::write(root.join("src/main.rs"), "fn main() {}").unwrap();

        let abs_root = root.to_string_lossy().into_owned();
        let resolved = resolve_under_root(&root, &abs_root).unwrap();
        assert_eq!(resolved, root);

        let abs_sub = root.join("src").to_string_lossy().into_owned();
        let resolved = resolve_under_root(&root, &abs_sub).unwrap();
        assert_eq!(resolved, root.join("src"));

        let abs_file = root.join("src/main.rs").to_string_lossy().into_owned();
        let resolved = resolve_under_root(&root, &abs_file).unwrap();
        assert_eq!(resolved, root.join("src/main.rs"));
    }

    #[test]
    fn resolve_accepts_nonexistent_multilevel_path_for_write() {
        let (_g, root) = tmp();
        // Relative path whose parent dirs don't exist yet — write_file
        // would create them. Resolver must not bail before that.
        let resolved = resolve_under_root(&root, "new_dir/sub/new_file.txt").unwrap();
        assert_eq!(resolved, root.join("new_dir/sub/new_file.txt"));

        // Same case via absolute path from session context.
        let abs_new = root.join("other/deeper/file.txt").to_string_lossy().into_owned();
        let resolved = resolve_under_root(&root, &abs_new).unwrap();
        assert_eq!(resolved, root.join("other/deeper/file.txt"));
    }

    #[tokio::test]
    async fn write_file_creates_missing_parent_dirs() {
        let (_g, root) = tmp();
        let (out, err) = dispatch(
            &root,
            "write_file",
            &json!({"path": "newly/nested/dir/hello.txt", "content": "hi"}),
            None,
        )
        .await;
        assert!(!err, "write_file should succeed: {out}");
        assert!(root.join("newly/nested/dir/hello.txt").exists());
    }

    #[tokio::test]
    async fn write_then_read_roundtrip() {
        let (_g, root) = tmp();
        let (out, err) = dispatch(
            &root,
            "write_file",
            &json!({"path": "hello.txt", "content": "hi"}),
            None,
        )
        .await;
        assert!(!err, "write_file should succeed: {out}");

        let (out, err) = dispatch(&root, "read_file", &json!({"path": "hello.txt"}), None).await;
        assert!(!err, "read_file should succeed: {out}");
        assert_eq!(out, "hi");
    }

    #[tokio::test]
    async fn list_dir_returns_entries() {
        let (_g, root) = tmp();
        std::fs::create_dir(root.join("sub")).unwrap();
        std::fs::write(root.join("a.txt"), "x").unwrap();
        let (out, err) = dispatch(&root, "list_dir", &json!({"path": ""}), None).await;
        assert!(!err);
        let v: Value = serde_json::from_str(&out).unwrap();
        let entries = v["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn read_rejects_escape() {
        let (_g, root) = tmp();
        let (out, err) = dispatch(&root, "read_file", &json!({"path": "../etc/passwd"}), None).await;
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
            None,
        )
        .await;
        assert!(!err, "grep failed: {out}");
        let v: Value = serde_json::from_str(&out).unwrap();
        let matches = v["matches"].as_array().unwrap();
        assert_eq!(matches.len(), 2);
    }

    /// Test approver that records the command it was asked about and returns
    /// a fixed decision.
    struct FixedApprover {
        decision: ShellApproval,
        seen: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl ShellApprover for FixedApprover {
        async fn approve_shell(&self, command: &str, _cwd: &str) -> ShellApproval {
            self.seen.lock().unwrap().push(command.to_string());
            self.decision
        }
    }

    #[tokio::test]
    async fn run_shell_executes_when_approved() {
        let (_g, root) = tmp();
        let approver: Arc<dyn ShellApprover> = Arc::new(FixedApprover {
            decision: ShellApproval::Allow,
            seen: std::sync::Mutex::new(Vec::new()),
        });
        let (out, err) = dispatch(
            &root,
            "run_shell",
            &json!({"command": "echo gated_ok"}),
            Some(&approver),
        )
        .await;
        assert!(!err, "approved command should run: {out}");
        assert!(out.contains("gated_ok"), "missing command output: {out}");
    }

    #[tokio::test]
    async fn run_shell_refuses_when_rejected() {
        let (_g, root) = tmp();
        let seen = std::sync::Mutex::new(Vec::new());
        let approver: Arc<dyn ShellApprover> = Arc::new(FixedApprover {
            decision: ShellApproval::Reject,
            seen,
        });
        let (out, err) = dispatch(
            &root,
            "run_shell",
            &json!({"command": "echo should_not_run"}),
            Some(&approver),
        )
        .await;
        assert!(err, "rejected command must report an error");
        assert!(out.contains("rejected"), "unexpected message: {out}");
        assert!(
            !out.contains("should_not_run"),
            "rejected command must not execute: {out}"
        );
    }

    #[tokio::test]
    async fn run_shell_fails_closed_without_approver() {
        let (_g, root) = tmp();
        let (out, err) = dispatch(&root, "run_shell", &json!({"command": "echo nope"}), None).await;
        assert!(err, "must fail closed with no approver");
        assert!(out.contains("no approval channel"), "unexpected message: {out}");
    }

    #[tokio::test]
    async fn run_shell_rejects_empty_command() {
        let (_g, root) = tmp();
        let approver: Arc<dyn ShellApprover> = Arc::new(FixedApprover {
            decision: ShellApproval::Allow,
            seen: std::sync::Mutex::new(Vec::new()),
        });
        let (out, err) = dispatch(&root, "run_shell", &json!({"command": "   "}), Some(&approver)).await;
        assert!(err, "empty command must error");
        assert!(out.contains("empty command"), "unexpected message: {out}");
    }
}
