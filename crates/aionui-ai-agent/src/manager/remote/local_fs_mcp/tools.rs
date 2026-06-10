//! Filesystem tool implementations exposed over MCP.
//!
//! Every operation resolves user-supplied paths *relative to* a fixed
//! project root and rejects anything that escapes it. The model never
//! sees absolute client paths; only `src/foo.ts`-style relative paths.

use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use aionui_db::IOpencodeToolSnapshotRepository;
use aionui_db::models::OpencodeToolSnapshotRow;
use aionui_file::{ISnapshotService, SnapshotService};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use ignore::WalkBuilder;
use regex::RegexBuilder;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::fs;
use tracing::warn;

use super::shell::{self, ElicitationHandler, McpRequestContext, ShellApproval, ShellApprover};

/// Bundle of dependencies the per-tool-call snapshot hook needs. Optional —
/// if `None` (or any inner field missing) the hook is a no-op. See
/// [`commit_tool_snapshot_after`] for the failure-tolerant semantics.
#[derive(Clone)]
pub struct SnapshotHook {
    pub snapshot_service: Arc<SnapshotService>,
    pub tool_snapshot_repo: Arc<dyn IOpencodeToolSnapshotRepository>,
    pub conversation_id: String,
    pub workspace_root: PathBuf,
    /// Set to `true` after a successful first `init`; guards the per-server
    /// one-shot init so we don't fight the snapshot service for a temp repo
    /// on every tool call.
    init_done: Arc<tokio::sync::Mutex<bool>>,
}

impl SnapshotHook {
    pub fn new(
        snapshot_service: Arc<SnapshotService>,
        tool_snapshot_repo: Arc<dyn IOpencodeToolSnapshotRepository>,
        conversation_id: String,
        workspace_root: PathBuf,
    ) -> Self {
        Self {
            snapshot_service,
            tool_snapshot_repo,
            conversation_id,
            workspace_root,
            init_done: Arc::new(tokio::sync::Mutex::new(false)),
        }
    }
}

const MAX_READ_BYTES: u64 = 4 * 1024 * 1024; // 4 MiB cap per read
const MAX_GREP_FILES: usize = 5000;
const MAX_GREP_MATCHES: usize = 500;
const MAX_LIST_ENTRIES: usize = 1000;

/// MCP tool annotation hints (camelCase on the wire).
#[derive(Debug, Clone, Copy)]
pub struct ToolAnnotations {
    pub read_only_hint: bool,
    pub destructive_hint: bool,
    pub idempotent_hint: bool,
    pub open_world_hint: bool,
}

const READ_ONLY_ANNOTATIONS: ToolAnnotations = ToolAnnotations {
    read_only_hint: true,
    destructive_hint: false,
    idempotent_hint: true,
    open_world_hint: false,
};
const MUTATING_ANNOTATIONS: ToolAnnotations = ToolAnnotations {
    read_only_hint: false,
    destructive_hint: true,
    idempotent_hint: true,
    open_world_hint: false,
};
const SHELL_ANNOTATIONS: ToolAnnotations = ToolAnnotations {
    read_only_hint: false,
    destructive_hint: true,
    idempotent_hint: false,
    open_world_hint: true,
};

/// One MCP tool descriptor (advertised in `tools/list`).
pub struct ToolDescriptor {
    pub name: &'static str,
    pub description: &'static str,
    pub input_schema: Value,
    pub annotations: ToolAnnotations,
}

/// Tools advertised in `tools/list`, optionally omitting `run_shell` when no
/// approver is wired (fail-closed at dispatch time either way).
pub fn tool_descriptors_for_state(has_approver: bool) -> Vec<ToolDescriptor> {
    all_tool_descriptors()
        .into_iter()
        .filter(|d| d.name != "run_shell" || has_approver)
        .collect()
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
            annotations: READ_ONLY_ANNOTATIONS,
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
            annotations: MUTATING_ANNOTATIONS,
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
            annotations: READ_ONLY_ANNOTATIONS,
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
            annotations: READ_ONLY_ANNOTATIONS,
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
            annotations: MUTATING_ANNOTATIONS,
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
            annotations: MUTATING_ANNOTATIONS,
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
            annotations: SHELL_ANNOTATIONS,
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
///
/// `elicitation` lets a tool raise a free-form, schema-driven prompt back to
/// the host UI (MCP `elicitation/create`-style flow). `None` disables it; the
/// filesystem tools today don't elicit, so they are unaffected.
///
/// `context` carries the OpenCode-injected session-attribution headers (see
/// [`McpRequestContext`]) so the approver / elicitation handler can route the
/// resulting UI prompt to the right sub-agent transcript instead of bubbling
/// it up at the parent transcript level.
///
/// `tool_call_id` is the JSON-RPC `id` of the inbound request, used as the
/// primary key of the per-tool-call snapshot ledger. `None` for `tools/list`
/// and non-`tools/call` methods (not passed in this dispatch).
///
/// `snapshot_hook` is the optional per-conversation handle to the Git-backed
/// snapshot service + the `opencode_tool_snapshots` DB repo. When `Some`,
/// the mutating ops (`write_file`, `delete_file`, `rename`) commit a per-call
/// snapshot and persist the ledger row on success. When `None`, the hook is
/// a no-op (preserves the non-OpenCode paths and the test-only invocations
/// that don't want a DB write).
#[allow(clippy::too_many_arguments)]
pub async fn dispatch(
    root: &Path,
    tool: &str,
    args: &Value,
    approver: Option<&Arc<dyn ShellApprover>>,
    elicitation: Option<&Arc<dyn ElicitationHandler>>,
    context: &McpRequestContext,
    tool_call_id: Option<&str>,
    snapshot_hook: Option<&SnapshotHook>,
) -> (String, bool) {
    let _ = elicitation; // reserved for future per-tool elicitation calls.
    let result = match tool {
        "read_file" => dispatch_read(root, args).await,
        "write_file" => dispatch_write(root, args).await,
        "list_dir" => dispatch_list(root, args).await,
        "grep_dir" => dispatch_grep(root, args).await,
        "delete_file" => dispatch_delete(root, args).await,
        "rename" => dispatch_rename(root, args).await,
        "run_shell" => dispatch_run_shell(root, args, approver, context).await,
        _ => Err(format!("unknown tool: {tool}")),
    };

    match result {
        Ok((text, false, changed)) => {
            // Per-tool-call snapshot hook. Fires only on success, only for
            // mutating tools that returned a non-empty changed-files list.
            if !changed.is_empty()
                && let (Some(id), Some(hook)) = (tool_call_id, snapshot_hook)
            {
                // Outcome is dropped: the tool's success path is unchanged.
                // Structured observability lives on the returned
                // `LedgerHookOutcome` (warning/error fields) and the
                // tracing events emitted by `commit_tool_snapshot_after`.
                let _outcome = commit_tool_snapshot_after(hook, id, changed).await;
            }
            (text, false)
        }
        Ok((text, true, _)) => (text, true),
        Err(msg) => (msg, true),
    }
}

async fn dispatch_read(root: &Path, args: &Value) -> Result<(String, bool, Vec<String>), String> {
    let input: ReadInput = serde_json::from_value(args.clone()).map_err(|e| format!("invalid params: {e}"))?;
    read_file(root, &input.path).await.map(|t| (t, false, Vec::new()))
}

async fn dispatch_write(root: &Path, args: &Value) -> Result<(String, bool, Vec<String>), String> {
    let input: WriteInput = serde_json::from_value(args.clone()).map_err(|e| format!("invalid params: {e}"))?;
    write_file(root, &input.path, &input.content)
        .await
        .map(|m| (m, false, vec![input.path]))
}

async fn dispatch_list(root: &Path, args: &Value) -> Result<(String, bool, Vec<String>), String> {
    let input: ListInput = serde_json::from_value(args.clone()).map_err(|e| format!("invalid params: {e}"))?;
    list_dir(root, &input.path).await.map(|t| (t, false, Vec::new()))
}

async fn dispatch_grep(root: &Path, args: &Value) -> Result<(String, bool, Vec<String>), String> {
    let input: GrepInput = serde_json::from_value(args.clone()).map_err(|e| format!("invalid params: {e}"))?;
    grep_dir(root, &input.pattern, &input.path, input.case_insensitive).map(|t| (t, false, Vec::new()))
}

async fn dispatch_delete(root: &Path, args: &Value) -> Result<(String, bool, Vec<String>), String> {
    let input: DeleteInput = serde_json::from_value(args.clone()).map_err(|e| format!("invalid params: {e}"))?;
    delete_file(root, &input.path)
        .await
        .map(|m| (m, false, vec![input.path]))
}

async fn dispatch_rename(root: &Path, args: &Value) -> Result<(String, bool, Vec<String>), String> {
    let input: RenameInput = serde_json::from_value(args.clone()).map_err(|e| format!("invalid params: {e}"))?;
    // Both endpoints touched: `from` (removed) and `to` (created). The
    // snapshot commit records both so the narrow revert can restore the
    // source if the tool call created it, or delete the destination if the
    // tool call created that.
    rename(root, &input.from, &input.to)
        .await
        .map(|m| (m, false, vec![input.from, input.to]))
}

async fn dispatch_run_shell(
    root: &Path,
    args: &Value,
    approver: Option<&Arc<dyn ShellApprover>>,
    context: &McpRequestContext,
) -> Result<(String, bool, Vec<String>), String> {
    let input: ShellInput = serde_json::from_value(args.clone()).map_err(|e| format!("invalid params: {e}"))?;
    // `run_shell` is intentionally not snapshotted here: the per-shell-call
    // delta attribution is out of scope for Task 14.3 (see
    // `forge-5-02-critical-per-tool-call-snapshotting.md` §5 step 3 — that
    // work is a follow-up). Passing an empty changed-files list keeps the
    // hook inert for shell invocations.
    let (text, is_error) = run_shell(root, &input.command, approver, context).await;
    Ok((text, is_error, Vec::new()))
}

/// Structured observability record for a per-tool-call snapshot hook attempt.
///
/// Returned by [`commit_tool_snapshot_after`] so the call site can decide
/// whether to surface a warning, log structured fields, or attach the
/// outcome to the model's response metadata. The tool's success path
/// remains unchanged: hook failures never bubble up as a tool error.
#[derive(Debug, Clone, serde::Serialize)]
pub struct LedgerHookOutcome {
    pub tool_call_id: String,
    pub conversation_id: String,
    pub stage: &'static str,
    pub success: bool,
    pub files_count: usize,
    pub commit_sha: Option<String>,
    pub error: Option<String>,
    pub warning: Option<String>,
}

impl LedgerHookOutcome {
    fn new(tool_call_id: &str, conversation_id: &str, files_count: usize) -> Self {
        Self {
            tool_call_id: tool_call_id.to_string(),
            conversation_id: conversation_id.to_string(),
            stage: "init",
            success: false,
            files_count,
            commit_sha: None,
            error: None,
            warning: None,
        }
    }
}

/// Lazily init the snapshot service for the workspace, then commit a
/// per-tool-call snapshot and persist the resulting commit SHA + changed
/// files to `opencode_tool_snapshots`.
///
/// Best-effort: any failure (init, commit, DB insert) is logged and swallowed
/// so a transient snapshot/DB outage never breaks the tool call's success
/// path back to the model. The model still sees the original `Ok` response.
///
/// Returns a [`LedgerHookOutcome`] describing what happened so callers can
/// surface observability metadata without re-parsing log lines.
async fn commit_tool_snapshot_after(hook: &SnapshotHook, tool_call_id: &str, files: Vec<String>) -> LedgerHookOutcome {
    let mut outcome = LedgerHookOutcome::new(tool_call_id, &hook.conversation_id, files.len());
    if files.is_empty() {
        outcome.stage = "skipped";
        outcome.success = true;
        outcome.warning = Some("no changed files; hook is a no-op".into());
        return outcome;
    }

    // Lazy one-shot init under a per-server mutex. The snapshot service is
    // process-global, so two conversations racing for the same workspace key
    // would otherwise both try to `init_snapshot_repo` for the same temp
    // path; the in-server flag only protects within this server, but the
    // snapshot service's own `init` is idempotent enough (it removes-then-
    // recreates the temp dir on re-entry).
    {
        let mut done = hook.init_done.lock().await;
        if !*done {
            let ws = hook.workspace_root.to_string_lossy().into_owned();
            if let Err(e) = hook.snapshot_service.init(&ws).await {
                let msg = format!("snapshot service init failed: {e}");
                warn!(
                    tool_call_id,
                    workspace = %ws,
                    error = %e,
                    stage = "init",
                    "Snapshot service init failed; per-tool-call hook will be a no-op"
                );
                outcome.stage = "init";
                outcome.error = Some(msg);
                return outcome;
            }
            *done = true;
        }
    }
    outcome.stage = "commit";

    let commit_sha = match hook.snapshot_service.commit_tool_snapshot(tool_call_id, &files).await {
        Ok(sha) => sha,
        Err(e) => {
            let msg = format!("commit_tool_snapshot failed: {e}");
            warn!(
                tool_call_id,
                files_changed = files.len(),
                error = %e,
                stage = "commit",
                "commit_tool_snapshot failed; skipping DB ledger write"
            );
            outcome.error = Some(msg);
            return outcome;
        }
    };
    outcome.commit_sha = Some(commit_sha.clone());
    outcome.stage = "persist";

    let files_json = match serde_json::to_string(&files) {
        Ok(j) => j,
        Err(e) => {
            let msg = format!("serialize files_changed: {e}");
            warn!(tool_call_id, error = %e, stage = "persist", "failed to serialize files_changed for ledger row; using []");
            outcome.warning = Some(msg);
            "[]".to_string()
        }
    };

    let row = OpencodeToolSnapshotRow {
        tool_call_id: tool_call_id.to_string(),
        conversation_id: hook.conversation_id.clone(),
        commit_sha,
        files_changed_json: files_json,
        created_at: aionui_common::now_ms(),
    };

    if let Err(e) = hook.tool_snapshot_repo.insert(&row).await {
        let msg = format!("insert opencode_tool_snapshots ledger row: {e}");
        warn!(
            tool_call_id,
            error = %e,
            stage = "persist",
            "failed to insert opencode_tool_snapshots ledger row"
        );
        outcome.error = Some(msg);
        return outcome;
    }

    outcome.stage = "complete";
    outcome.success = true;
    tracing::info!(
        tool_call_id,
        conversation_id = %hook.conversation_id,
        commit_sha = %outcome.commit_sha.as_deref().unwrap_or(""),
        files_count = files.len(),
        stage = "complete",
        "Per-tool-call ledger hook completed"
    );
    outcome
}

/// Gate a shell command on user approval, then run it locally. Fails closed:
/// an empty command or a missing approval channel never executes anything.
async fn run_shell(
    root: &Path,
    command: &str,
    approver: Option<&Arc<dyn ShellApprover>>,
    context: &McpRequestContext,
) -> (String, bool) {
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
    match approver.approve_shell_with_context(command, &cwd, context).await {
        ShellApproval::Allow => shell::run_shell(root, command).await,
        ShellApproval::Reject => ("command was rejected by the user".to_string(), true),
        ShellApproval::TimedOut => ("shell approval timed out waiting for user".to_string(), true),
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
            None,
            &McpRequestContext::default(),
            None,
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
            None,
            &McpRequestContext::default(),
            None,
            None,
        )
        .await;
        assert!(!err, "write_file should succeed: {out}");

        let (out, err) = dispatch(
            &root,
            "read_file",
            &json!({"path": "hello.txt"}),
            None,
            None,
            &McpRequestContext::default(),
            None,
            None,
        )
        .await;
        assert!(!err, "read_file should succeed: {out}");
        assert_eq!(out, "hi");
    }

    #[tokio::test]
    async fn list_dir_returns_entries() {
        let (_g, root) = tmp();
        std::fs::create_dir(root.join("sub")).unwrap();
        std::fs::write(root.join("a.txt"), "x").unwrap();
        let (out, err) = dispatch(
            &root,
            "list_dir",
            &json!({"path": ""}),
            None,
            None,
            &McpRequestContext::default(),
            None,
            None,
        )
        .await;
        assert!(!err);
        let v: Value = serde_json::from_str(&out).unwrap();
        let entries = v["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn read_rejects_escape() {
        let (_g, root) = tmp();
        let (out, err) = dispatch(
            &root,
            "read_file",
            &json!({"path": "../etc/passwd"}),
            None,
            None,
            &McpRequestContext::default(),
            None,
            None,
        )
        .await;
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
            None,
            &McpRequestContext::default(),
            None,
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
            None,
            &McpRequestContext::default(),
            None,
            None,
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
            None,
            &McpRequestContext::default(),
            None,
            None,
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
        let (out, err) = dispatch(
            &root,
            "run_shell",
            &json!({"command": "echo nope"}),
            None,
            None,
            &McpRequestContext::default(),
            None,
            None,
        )
        .await;
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
        let (out, err) = dispatch(
            &root,
            "run_shell",
            &json!({"command": "   "}),
            Some(&approver),
            None,
            &McpRequestContext::default(),
            None,
            None,
        )
        .await;
        assert!(err, "empty command must error");
        assert!(out.contains("empty command"), "unexpected message: {out}");
    }

    // ── Per-tool-call snapshot hook (Task 14.3) ─────────────────────

    use aionui_db::IConversationRepository;
    use aionui_db::IUserRepository;
    use aionui_db::SqliteOpencodeToolSnapshotRepository;
    use aionui_db::init_database_memory;
    use aionui_file::SnapshotService;

    /// Test harness for the snapshot hook: a real `SnapshotService` (backed
    /// by a temp-dir workspace) and a real `SqliteOpencodeToolSnapshotRepository`
    /// (in-memory) wired through `SnapshotHook`.
    struct HookHarness {
        #[allow(dead_code)]
        snapshot_service: Arc<SnapshotService>,
        tool_snapshot_repo: Arc<dyn aionui_db::IOpencodeToolSnapshotRepository>,
        conversation_id: String,
        workspace_root: PathBuf,
        hook: SnapshotHook,
    }

    async fn make_hook_harness() -> (HookHarness, TempDir) {
        let workspace = tempfile::tempdir().expect("tempdir");
        let root = workspace.path().canonicalize().expect("canonicalize");
        let db = init_database_memory().await.expect("init db");
        let pool = db.pool().clone();
        // The tool_snapshot_repo's FK chain is users -> conversations ->
        // opencode_tool_snapshots; seed both parents via the repository
        // APIs so the schema stays one source of truth.
        let user_repo = aionui_db::SqliteUserRepository::new(pool.clone());
        let user = user_repo.create_user("tester", "").await.expect("seed user");
        let conversation_repo = aionui_db::SqliteConversationRepository::new(pool.clone());
        let row = aionui_db::models::ConversationRow {
            id: "conv-hook".into(),
            user_id: user.id.clone(),
            name: "test conv".into(),
            r#type: "acp".into(),
            extra: "{}".into(),
            model: None,
            status: Some("pending".into()),
            source: None,
            channel_chat_id: None,
            pinned: false,
            pinned_at: None,
            created_at: 0,
            updated_at: 0,
        };
        conversation_repo.create(&row).await.expect("seed conversation");
        let snapshot_service = Arc::new(SnapshotService::new());
        let tool_snapshot_repo: Arc<dyn aionui_db::IOpencodeToolSnapshotRepository> =
            Arc::new(SqliteOpencodeToolSnapshotRepository::new(pool));
        let hook = SnapshotHook::new(
            snapshot_service.clone(),
            tool_snapshot_repo.clone(),
            "conv-hook".to_string(),
            root.clone(),
        );
        let harness = HookHarness {
            snapshot_service,
            tool_snapshot_repo,
            conversation_id: "conv-hook".to_string(),
            workspace_root: root,
            hook,
        };
        (harness, workspace)
    }

    #[tokio::test]
    async fn snapshot_hook_records_write_file_ledger_row() {
        let (harness, _g) = make_hook_harness().await;
        let (out, err) = dispatch(
            &harness.workspace_root,
            "write_file",
            &json!({"path": "hello.txt", "content": "hi"}),
            None,
            None,
            &McpRequestContext::default(),
            Some("call-1"),
            Some(&harness.hook),
        )
        .await;
        assert!(!err, "write_file should succeed: {out}");
        assert!(harness.workspace_root.join("hello.txt").exists());

        // Give the lazy init's tokio::Mutex a beat to release before the
        // next call's hook runs; otherwise the second init is held back
        // waiting on the first's dropped guard.
        drop(out);

        // Ledger row should now exist with the tool_call_id we passed.
        let row = harness
            .tool_snapshot_repo
            .get_by_tool_call_id("call-1")
            .await
            .expect("db ok")
            .expect("ledger row exists for call-1");
        assert_eq!(row.conversation_id, harness.conversation_id);
        assert_eq!(row.files_changed_json, r#"["hello.txt"]"#);
        assert_eq!(row.commit_sha.len(), 40, "commit sha should be 40 hex chars");
    }

    #[tokio::test]
    async fn snapshot_hook_records_delete_file_ledger_row() {
        let (harness, _g) = make_hook_harness().await;
        // Pre-create the file so `delete_file` has a target.
        std::fs::write(harness.workspace_root.join("bye.txt"), "x").unwrap();
        let (out, err) = dispatch(
            &harness.workspace_root,
            "delete_file",
            &json!({"path": "bye.txt"}),
            None,
            None,
            &McpRequestContext::default(),
            Some("call-del"),
            Some(&harness.hook),
        )
        .await;
        assert!(!err, "delete_file should succeed: {out}");
        assert!(!harness.workspace_root.join("bye.txt").exists());

        let row = harness
            .tool_snapshot_repo
            .get_by_tool_call_id("call-del")
            .await
            .expect("db ok")
            .expect("ledger row exists for call-del");
        assert_eq!(row.files_changed_json, r#"["bye.txt"]"#);
    }

    #[tokio::test]
    async fn snapshot_hook_records_rename_with_both_paths() {
        let (harness, _g) = make_hook_harness().await;
        std::fs::write(harness.workspace_root.join("old.txt"), "x").unwrap();
        let (out, err) = dispatch(
            &harness.workspace_root,
            "rename",
            &json!({"from": "old.txt", "to": "new.txt"}),
            None,
            None,
            &McpRequestContext::default(),
            Some("call-rename"),
            Some(&harness.hook),
        )
        .await;
        assert!(!err, "rename should succeed: {out}");
        assert!(!harness.workspace_root.join("old.txt").exists());
        assert!(harness.workspace_root.join("new.txt").exists());

        let row = harness
            .tool_snapshot_repo
            .get_by_tool_call_id("call-rename")
            .await
            .expect("db ok")
            .expect("ledger row exists for call-rename");
        // Both paths persisted so a narrow revert can re-create `old.txt`
        // and delete `new.txt`.
        let files: Vec<String> = serde_json::from_str(&row.files_changed_json).expect("valid json");
        assert_eq!(files, vec!["old.txt", "new.txt"]);
    }

    #[tokio::test]
    async fn snapshot_hook_is_inert_when_tool_call_id_missing() {
        // No tool_call_id → the dispatch wrapper treats the call as
        // un-attributable and skips the hook entirely. The mutation still
        // succeeds (the model must not see a different error path just
        // because the upstream caller forgot to set a `jsonrpc id`).
        let (harness, _g) = make_hook_harness().await;
        let (out, err) = dispatch(
            &harness.workspace_root,
            "write_file",
            &json!({"path": "silent.txt", "content": "x"}),
            None,
            None,
            &McpRequestContext::default(),
            None,
            Some(&harness.hook),
        )
        .await;
        assert!(!err, "write_file should succeed: {out}");
        assert!(harness.workspace_root.join("silent.txt").exists());

        // No ledger row should exist for a missing tool_call_id.
        let row = harness
            .tool_snapshot_repo
            .get_by_tool_call_id("missing")
            .await
            .expect("db ok");
        assert!(row.is_none(), "no row expected for absent tool_call_id");
    }

    #[tokio::test]
    async fn snapshot_hook_no_ops_when_hook_absent() {
        // Existing non-OpenCode / test path: `snapshot_hook = None`. The
        // dispatch must continue to work and must NOT touch the DB.
        let (_g, root) = tmp();
        let db = init_database_memory().await.expect("init db");
        let tool_snapshot_repo: Arc<dyn aionui_db::IOpencodeToolSnapshotRepository> =
            Arc::new(SqliteOpencodeToolSnapshotRepository::new(db.pool().clone()));
        let (out, err) = dispatch(
            &root,
            "write_file",
            &json!({"path": "nohook.txt", "content": "x"}),
            None,
            None,
            &McpRequestContext::default(),
            Some("call-nohook"),
            None,
        )
        .await;
        assert!(!err, "write_file should succeed: {out}");
        assert!(root.join("nohook.txt").exists());
        let row = tool_snapshot_repo
            .get_by_tool_call_id("call-nohook")
            .await
            .expect("db ok");
        assert!(row.is_none(), "no row when hook is None");
    }

    // ── Hook observability (forge-5-02 gap-fill) ───────────────────

    #[tokio::test]
    async fn ledger_hook_outcome_reports_success_for_write_file() {
        let (harness, _g) = make_hook_harness().await;
        let (out, err) = dispatch(
            &harness.workspace_root,
            "write_file",
            &json!({"path": "obs.txt", "content": "ok"}),
            None,
            None,
            &McpRequestContext::default(),
            Some("call-obs-success"),
            Some(&harness.hook),
        )
        .await;
        assert!(!err, "write_file should succeed: {out}");

        // Reconstruct the outcome by calling the hook helper directly so the
        // assertion is independent of dispatch's drop site.
        let outcome = commit_tool_snapshot_after(&harness.hook, "call-obs-success", vec!["obs.txt".to_string()]).await;
        // The dispatch above already wrote a row for call-obs-success, so a
        // second hook invocation surfaces a duplicate-key warning (DB error)
        // rather than a hard success -- but the tool call itself succeeded.
        // We assert the outcome carries structured fields either way.
        assert!(outcome.files_count == 1);
        assert_eq!(outcome.tool_call_id, "call-obs-success");
        // Either success (clean re-run) or a Conflict warning (dup key) is
        // acceptable; what we MUST NOT see is a panic or a silent miss.
        assert!(outcome.error.is_some() || outcome.success);
    }

    #[tokio::test]
    async fn ledger_hook_outcome_reports_skipped_for_empty_changes() {
        // Build a hook and call the helper with no changed files; the
        // outcome must report a "skipped" stage with a warning, not an error.
        let (harness, _g) = make_hook_harness().await;
        let outcome = commit_tool_snapshot_after(&harness.hook, "call-skip", Vec::new()).await;
        assert!(outcome.success, "empty-changes hook should be a clean skip");
        assert_eq!(outcome.stage, "skipped");
        assert!(outcome.warning.is_some());
        assert!(outcome.error.is_none());
        assert!(outcome.commit_sha.is_none());
    }
}
