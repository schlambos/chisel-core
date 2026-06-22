//! Verify-hook runner (T1 Unit 5).
//!
//! After an edit-type tool completes, reads `.chisl/verify.json` from the
//! workspace root and optionally runs the configured shell command. The
//! result is returned as a [`VerifyResultEventData`] for the caller to
//! emit as a stream event.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use tokio::sync::Mutex;
use tracing::{debug, warn};

use aionui_common::VerifyConfig;

use crate::protocol::events::VerifyResultEventData;

/// Maximum combined stdout+stderr captured, in bytes. Keeps the event
/// payload bounded so a chatty build doesn't blow up the SSE frame.
const MAX_OUTPUT_BYTES: usize = 8 * 1024; // 8 KiB

/// Marker appended when output is truncated mid-stream.
const TRUNCATION_MARKER: &str = "\n…[output truncated]";

// ---------------------------------------------------------------------------
// Per-workspace concurrency control (Fix 3)
// ---------------------------------------------------------------------------

/// Per-workspace locks ensuring at most one verify runs at a time per
/// workspace root. Rapid edits coalesce via `try_lock`: if a verify is
/// already in progress for the same workspace, the new invocation skips
/// rather than queuing (queueing would still run N sequential builds,
/// wasting resources). Chosen over a semaphore because the lock is
/// keyed by workspace path, and try-lock-skip gives the coalescing
/// behaviour we want without unbounded queue growth.
static VERIFY_LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();

/// Read `.chisl/verify.json` from the workspace root.
///
/// Returns `None` if the file is missing (normal — no verification
/// configured) or if it cannot be parsed (malformed — logged as a
/// warning per Fail Fast / Early Exit philosophy).
pub async fn load_verify_config(workspace_root: &Path) -> Option<VerifyConfig> {
    let config_path = workspace_root.join(".chisl").join("verify.json");
    let raw = match tokio::fs::read_to_string(&config_path).await {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(e) => {
            warn!(path = %config_path.display(), error = %e, "Failed to read verify config");
            return None;
        }
    };
    match serde_json::from_str::<VerifyConfig>(&raw) {
        Ok(cfg) => Some(cfg),
        Err(e) => {
            warn!(path = %config_path.display(), error = %e, "Malformed verify config");
            None
        }
    }
}

/// Run the verification command and return the result data.
///
/// Enforces per-workspace concurrency: if a verify is already running
/// for `workspace_root`, this invocation is skipped (coalesced) and
/// returns a `VerifyResultEventData` with `success: true` and a
/// descriptive `output` noting the skip.
///
/// Never panics. On spawn failure or internal error, returns a
/// `VerifyResultEventData` with `success: false` and a descriptive
/// `output` message.
pub async fn run_verification(
    config: &VerifyConfig,
    workspace_root: &Path,
    conversation_id: &str,
    tool_call_id: Option<&str>,
) -> VerifyResultEventData {
    let start = Instant::now();

    // Acquire (or create) the per-workspace lock, then try-lock it.
    // If another verify holds it, skip — coalesce rather than queue.
    let workspace_key = workspace_root.to_path_buf();
    let lock = {
        let map = VERIFY_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
        let mut guard = map.lock().await;
        guard.entry(workspace_key.clone()).or_default().clone()
    };

    match lock.try_lock() {
        Ok(_guard) => {
            // We hold the per-workspace lock — run the verify.
            let result = run_shell_with_timeout(workspace_root, &config.command, config.timeout_secs).await;
            let duration_ms = start.elapsed().as_millis() as u64;

            VerifyResultEventData {
                success: result.success,
                command: config.command.clone(),
                exit_code: result.exit_code,
                output: result.output,
                duration_ms,
                conversation_id: conversation_id.to_string(),
                tool_call_id: tool_call_id.map(|s| s.to_string()),
            }
        }
        Err(_) => {
            // tokio::sync::Mutex::try_lock returns Err for any contention
            // (no poisoned variant — tokio mutexes don't poison).
            debug!(
                workspace = %workspace_root.display(),
                "Verify already running for workspace, skipping (coalesced)"
            );
            let duration_ms = start.elapsed().as_millis() as u64;
            VerifyResultEventData {
                success: true,
                command: config.command.clone(),
                exit_code: None,
                output: "skipped: verify already running for this workspace".to_string(),
                duration_ms,
                conversation_id: conversation_id.to_string(),
                tool_call_id: tool_call_id.map(|s| s.to_string()),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Internal shell runner
// ---------------------------------------------------------------------------

/// Outcome of a shell invocation.
struct ShellOutcome {
    success: bool,
    exit_code: Option<i32>,
    output: String,
}

/// Run `command` in the user's native shell, rooted at `root`, with a
/// configurable timeout. Captures combined stdout+stderr, bounded to
/// [`MAX_OUTPUT_BYTES`] during reading so a noisy child cannot balloon
/// memory before the cap applies.
///
/// Uses `aionui_runtime::Builder::clean_cli` for env hygiene (NO_COLOR,
/// TERM=dumb, stripped debug env) per repo subprocess-spawning rules.
async fn run_shell_with_timeout(root: &Path, command: &str, timeout_secs: u64) -> ShellOutcome {
    let spec = super::local_fs_mcp::shell::resolve_shell();
    let (program, arg) = spec.parts();

    let mut builder = aionui_runtime::Builder::clean_cli(program);
    builder.arg(arg).arg(command).current_dir(root);

    let mut child = match builder.spawn() {
        Ok(c) => c,
        Err(e) => {
            return ShellOutcome {
                success: false,
                exit_code: None,
                output: format!("failed to launch shell: {e}"),
            };
        }
    };

    let stdout_pipe = child.stdout.take();
    let stderr_pipe = child.stderr.take();

    // Bounded reader: each stream reads up to MAX_OUTPUT_BYTES, stopping
    // early once the budget is reached. Remaining bytes are drained and
    // discarded so the pipe doesn't deadlock.
    let stdout_task = tokio::spawn(async move { bounded_read(stdout_pipe, MAX_OUTPUT_BYTES).await });
    let stderr_task = tokio::spawn(async move { bounded_read(stderr_pipe, MAX_OUTPUT_BYTES).await });

    let timeout = std::time::Duration::from_secs(timeout_secs);
    let status = match tokio::time::timeout(timeout, child.wait()).await {
        Err(_) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            let _ = stdout_task.await;
            let _ = stderr_task.await;
            return ShellOutcome {
                success: false,
                exit_code: None,
                output: format!("command timed out after {timeout_secs}s"),
            };
        }
        Ok(Err(e)) => {
            return ShellOutcome {
                success: false,
                exit_code: None,
                output: format!("failed to wait on shell: {e}"),
            };
        }
        Ok(Ok(status)) => status,
    };

    let stdout_result = stdout_task.await.unwrap_or_default();
    let stderr_result = stderr_task.await.unwrap_or_default();

    // Combine stdout + stderr into a single string, respecting the
    // MAX_OUTPUT_BYTES budget. If either stream was truncated during
    // reading, or the combined total exceeds the budget, truncate
    // and append the marker.
    let mut output = String::new();
    let stdout_str = String::from_utf8_lossy(&stdout_result.bytes);
    let stderr_str = String::from_utf8_lossy(&stderr_result.bytes);

    if !stdout_str.trim().is_empty() {
        output.push_str(&stdout_str);
        if !output.ends_with('\n') {
            output.push('\n');
        }
    }
    if !stderr_str.trim().is_empty() {
        output.push_str(&stderr_str);
        if !output.ends_with('\n') {
            output.push('\n');
        }
    }

    // If either stream hit the per-stream budget, or the combined output
    // exceeds the total budget, truncate and mark.
    let truncated = stdout_result.truncated || stderr_result.truncated;
    let output = if truncated || output.len() > MAX_OUTPUT_BYTES {
        truncate_output(output)
    } else {
        output
    };

    debug!(
        command = %command,
        exit_code = ?status.code(),
        success = status.success(),
        "Verify command completed"
    );

    ShellOutcome {
        success: status.success(),
        exit_code: status.code(),
        output,
    }
}

// ---------------------------------------------------------------------------
// Bounded async reader
// ---------------------------------------------------------------------------

/// Result of a bounded read: captured bytes and whether the stream
/// exceeded the budget.
#[derive(Default)]
struct BoundedReadResult {
    bytes: Vec<u8>,
    truncated: bool,
}

/// Read from `pipe` up to `budget` bytes. Once the budget is reached,
/// stop accumulating but keep draining the pipe so the child doesn't
/// deadlock on a full pipe buffer. Returns the captured bytes and
/// whether truncation occurred.
async fn bounded_read<R: tokio::io::AsyncRead + Unpin>(pipe: Option<R>, budget: usize) -> BoundedReadResult {
    use tokio::io::AsyncReadExt;

    let mut pipe = match pipe {
        Some(p) => p,
        None => return BoundedReadResult::default(),
    };

    let mut buf = Vec::with_capacity(budget.min(4096));
    let mut tmp = [0u8; 4096];
    let mut truncated = false;

    loop {
        match pipe.read(&mut tmp).await {
            Ok(0) => break, // EOF
            Ok(n) => {
                if buf.len() < budget {
                    let remaining = budget - buf.len();
                    let take = n.min(remaining);
                    buf.extend_from_slice(&tmp[..take]);
                    if take < n {
                        // Budget reached — mark truncated, keep draining.
                        truncated = true;
                    }
                }
                // If budget already exceeded, bytes are silently discarded
                // but we keep reading to avoid pipe deadlock.
            }
            Err(_) => break, // Read error — stop.
        }
    }

    BoundedReadResult { bytes: buf, truncated }
}

/// Truncate output to [`MAX_OUTPUT_BYTES`] on a char boundary, appending
/// a truncation marker if shortened.
fn truncate_output(mut output: String) -> String {
    if output.len() <= MAX_OUTPUT_BYTES {
        return output;
    }
    let mut cut = MAX_OUTPUT_BYTES;
    while cut > 0 && !output.is_char_boundary(cut) {
        cut -= 1;
    }
    output.truncate(cut);
    output.push_str(TRUNCATION_MARKER);
    output
}
