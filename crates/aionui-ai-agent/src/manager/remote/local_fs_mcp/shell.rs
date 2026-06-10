//! Local shell execution for the client-side MCP server.
//!
//! Restores the remote agent's ability to run terminal commands to verify
//! its work — build, test, lint, git — by executing them on the *user's*
//! machine (where the project actually lives), through the user's native
//! shell. The remote OpenCode has no access to the client filesystem, so a
//! server-side shell would run in the wrong place; this runs locally and
//! streams the result back over MCP.
//!
//! Every command is gated by a [`ShellApprover`] callback into the host
//! agent's confirmation UI, so the user approves the exact command before
//! it runs. Execution fails closed if no approver is wired up.

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use async_trait::async_trait;

/// Wall-clock cap on a single command. Long enough for a typical build or
/// test run, short enough that a hung command doesn't wedge the session.
const SHELL_TIMEOUT: Duration = Duration::from_secs(120);

/// Env var to override the shell execution timeout in seconds (tests only).
const SHELL_TIMEOUT_ENV: &str = "AIONUI_LOCAL_SHELL_TIMEOUT_SECS";

fn shell_timeout() -> Duration {
    std::env::var(SHELL_TIMEOUT_ENV)
        .ok()
        .and_then(|s| s.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or(SHELL_TIMEOUT)
}

/// Cap on the combined stdout+stderr returned to the model, so a chatty
/// command can't blow up the context window or the MCP response.
const MAX_SHELL_OUTPUT: usize = 1024 * 1024; // 1 MiB

/// Env var to force a specific shell program, overriding OS auto-detection
/// (e.g. set to `pwsh` on Windows, or `bash` on a zsh-default mac).
pub const SHELL_OVERRIDE_ENV: &str = "AIONUI_LOCAL_SHELL";

/// The user's decision for a single shell command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShellApproval {
    Allow,
    Reject,
    /// User did not respond before the approver wait deadline.
    TimedOut,
}

/// Per-request session context captured from incoming MCP headers.
///
/// OpenCode 1.15.x does **not** forward per-call session headers to remote MCP
/// servers today — only static `headers` from connect-time config are sent
/// (`packages/opencode/src/mcp/index.ts`). Chisl therefore falls back to
/// conversation-level session attribution in [`super::agent::RemoteShellApprover`]
/// when these fields are `None`.
#[derive(Debug, Clone, Default)]
pub struct McpRequestContext {
    pub session_id: Option<String>,
    pub parent_session_id: Option<String>,
}

/// Bridges a shell-tool call back to the host agent's confirmation flow.
///
/// Implemented by the remote agent; the MCP server holds it as a trait
/// object and consults it before running any command locally. The agent's
/// implementation surfaces the exact command to the user and blocks until
/// they answer.
#[async_trait]
pub trait ShellApprover: Send + Sync {
    /// Ask the user to approve running `command` in `cwd` (the project
    /// root). Returns whether to proceed. Implementations should fail
    /// closed — if the request can't be presented or is cancelled, return
    /// [`ShellApproval::Reject`].
    ///
    /// `context` carries per-request session attribution from the inbound MCP
    /// headers (see [`McpRequestContext`]) so the host can route the
    /// confirmation prompt to the right sub-agent UI surface. A default
    /// implementation forwards to [`Self::approve_shell`] for back-compat with
    /// older approvers that didn't take the context.
    async fn approve_shell_with_context(&self, command: &str, cwd: &str, context: &McpRequestContext) -> ShellApproval {
        let _ = context;
        self.approve_shell(command, cwd).await
    }

    async fn approve_shell(&self, command: &str, cwd: &str) -> ShellApproval;
}

/// Bridges a tool's elicitation request (free-form user input mid-tool-call,
/// MCP-spec `elicitation/create`) back to the host agent's confirmation flow.
///
/// MCP's HTTP-only transport in this codebase cannot do server-initiated
/// reverse calls, so we fold elicitation into the same approver pattern: the
/// tool calls [`Self::request_elicitation`] and parks until the user answers.
/// The response is forwarded back to the tool, which can either embed it in
/// the tool's return value or use it to decide whether to proceed.
///
/// Like [`ShellApprover`], implementations should fail closed — return
/// [`ElicitationOutcome::Declined`] when the prompt can't be raised or the
/// user dismisses it.
#[async_trait]
pub trait ElicitationHandler: Send + Sync {
    async fn request_elicitation(
        &self,
        request: ElicitationRequest<'_>,
        context: &McpRequestContext,
    ) -> ElicitationOutcome;
}

/// One free-form prompt the tool wants to surface to the user.
#[derive(Debug, Clone)]
pub struct ElicitationRequest<'a> {
    /// Tool that raised the request — surfaced in the prompt header.
    pub tool_name: &'a str,
    /// Short human-readable explanation, e.g. "Confirm overwrite of foo.ts".
    pub message: &'a str,
    /// JSON Schema describing the response shape. The renderer uses it to
    /// build a form (text input by default). Free-form `{ raw: <text> }`
    /// fallback is used when the schema can't be rendered.
    pub requested_schema: Option<serde_json::Value>,
}

/// Outcome the tool gets back from the user.
#[derive(Debug, Clone)]
pub enum ElicitationOutcome {
    /// User supplied a response payload matching `requested_schema`.
    Accepted(serde_json::Value),
    /// User explicitly cancelled.
    Declined,
}

/// A resolved native shell invocation: the program plus the flag that makes
/// it execute a command string. `pub(crate)` so the plugin webserver's
/// streaming shell tool can spawn the same native shell the local fs
/// MCP server uses, without duplicating the OS detection.
pub(crate) struct ShellSpec {
    program: String,
    arg: &'static str,
    /// Human-readable `os/shell` label for the system hint and the
    /// confirmation dialog (e.g. `macos/zsh`, `windows/cmd.exe`).
    label: String,
}

impl ShellSpec {
    /// Program path + the flag that takes a command string (e.g. `-c` for
    /// `sh`/`zsh`/`bash`, `/C` for `cmd.exe`). Exposed so the plugin
    /// webserver's streaming shell tool can spawn the same native shell the
    /// local fs MCP server uses, without duplicating the OS detection.
    pub(crate) fn parts(&self) -> (&str, &str) {
        (self.program.as_str(), self.arg)
    }
}

/// Strip a path down to its final component, handling both separators so
/// the label reads `zsh` rather than `/bin/zsh` and `cmd.exe` rather than
/// `C:\Windows\System32\cmd.exe`.
fn basename(path: &str) -> &str {
    path.rsplit(['/', '\\']).next().unwrap_or(path)
}

#[cfg(windows)]
pub(crate) fn resolve_shell() -> ShellSpec {
    // Explicit override wins; PowerShell takes `-Command`, cmd.exe takes /C.
    if let Ok(custom) = std::env::var(SHELL_OVERRIDE_ENV).map(|v| v.trim().to_string())
        && !custom.is_empty()
    {
        let lower = custom.to_ascii_lowercase();
        let arg = if lower.contains("powershell") || lower.contains("pwsh") {
            "-Command"
        } else {
            "/C"
        };
        let label = format!("windows/{}", basename(&custom));
        return ShellSpec {
            program: custom,
            arg,
            label,
        };
    }
    let comspec = std::env::var("COMSPEC")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "cmd.exe".to_string());
    let label = format!("windows/{}", basename(&comspec));
    ShellSpec {
        program: comspec,
        arg: "/C",
        label,
    }
}

#[cfg(not(windows))]
pub(crate) fn resolve_shell() -> ShellSpec {
    let shell = std::env::var(SHELL_OVERRIDE_ENV)
        .ok()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| std::env::var("SHELL").ok().filter(|s| !s.trim().is_empty()))
        .unwrap_or_else(|| "/bin/sh".to_string());
    let label = format!("{}/{}", std::env::consts::OS, basename(&shell));
    ShellSpec {
        program: shell,
        arg: "-c",
        label,
    }
}

/// `os/shell` label for the current machine, injected into the agent's
/// system hint so the model writes commands in the right syntax.
pub fn shell_hint() -> String {
    resolve_shell().label
}

/// Run `command` in the user's native shell, rooted at `root`, capturing
/// stdout and stderr. Returns `(report, is_error)` where `is_error` is true
/// on a non-zero exit, spawn failure, or timeout — matching the MCP
/// dispatch contract used by the filesystem tools.
pub async fn run_shell(root: &Path, command: &str) -> (String, bool) {
    let spec = resolve_shell();

    let timeout = shell_timeout();
    let mut cmd = tokio::process::Command::new(&spec.program);
    cmd.arg(spec.arg)
        .arg(command)
        .current_dir(root)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => return (format!("failed to launch shell ({}): {e}", spec.program), true),
    };

    let stdout_pipe = child.stdout.take();
    let stderr_pipe = child.stderr.take();
    let stdout_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        if let Some(mut pipe) = stdout_pipe {
            let _ = tokio::io::AsyncReadExt::read_to_end(&mut pipe, &mut buf).await;
        }
        buf
    });
    let stderr_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        if let Some(mut pipe) = stderr_pipe {
            let _ = tokio::io::AsyncReadExt::read_to_end(&mut pipe, &mut buf).await;
        }
        buf
    });

    let status = match tokio::time::timeout(timeout, child.wait()).await {
        Err(_) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            let _ = stdout_task.await;
            let _ = stderr_task.await;
            return (format!("command timed out after {}s", timeout.as_secs()), true);
        }
        Ok(Err(e)) => return (format!("failed to wait on shell: {e}"), true),
        Ok(Ok(status)) => status,
    };

    let stdout_bytes = stdout_task.await.unwrap_or_default();
    let stderr_bytes = stderr_task.await.unwrap_or_default();
    let stdout = String::from_utf8_lossy(&stdout_bytes);
    let stderr = String::from_utf8_lossy(&stderr_bytes);
    let exit = status
        .code()
        .map(|c| c.to_string())
        .unwrap_or_else(|| "terminated by signal".to_string());

    let mut report = format!("exit code: {exit}\n");
    if !stdout.trim().is_empty() {
        report.push_str("--- stdout ---\n");
        report.push_str(&stdout);
        if !stdout.ends_with('\n') {
            report.push('\n');
        }
    }
    if !stderr.trim().is_empty() {
        report.push_str("--- stderr ---\n");
        report.push_str(&stderr);
        if !stderr.ends_with('\n') {
            report.push('\n');
        }
    }
    if report.len() > MAX_SHELL_OUTPUT {
        // Truncate on a char boundary to keep the string valid UTF-8.
        let mut cut = MAX_SHELL_OUTPUT;
        while cut > 0 && !report.is_char_boundary(cut) {
            cut -= 1;
        }
        report.truncate(cut);
        report.push_str("\n…[output truncated]");
    }

    (report, !status.success())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn run_shell_captures_stdout_and_exit_zero() {
        let dir = TempDir::new().unwrap();
        let (out, err) = run_shell(dir.path(), "echo hello_from_shell").await;
        assert!(!err, "expected success, got: {out}");
        assert!(out.contains("hello_from_shell"), "missing stdout: {out}");
        assert!(out.contains("exit code: 0"), "missing exit code: {out}");
    }

    #[tokio::test]
    async fn run_shell_reports_nonzero_exit_as_error() {
        let dir = TempDir::new().unwrap();
        let (out, err) = run_shell(dir.path(), "exit 3").await;
        assert!(err, "expected error flag for non-zero exit: {out}");
        assert!(out.contains("exit code: 3"), "missing exit code: {out}");
    }

    #[tokio::test]
    async fn run_shell_runs_in_root() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("marker.txt"), "x").unwrap();
        // `ls`/`dir` differ across shells; `pwd`-style check via a file the
        // command can only see if cwd is the project root.
        #[cfg(not(windows))]
        let probe = "cat marker.txt";
        #[cfg(windows)]
        let probe = "type marker.txt";
        let (out, err) = run_shell(dir.path(), probe).await;
        assert!(!err, "probe failed: {out}");
        assert!(out.contains('x'), "did not run in root: {out}");
    }

    #[test]
    fn shell_hint_has_os_prefix() {
        let hint = shell_hint();
        assert!(hint.contains('/'), "hint should be os/shell: {hint}");
    }

    #[test]
    fn basename_handles_both_separators() {
        assert_eq!(basename("/bin/zsh"), "zsh");
        assert_eq!(basename(r"C:\Windows\System32\cmd.exe"), "cmd.exe");
        assert_eq!(basename("bash"), "bash");
    }

    #[tokio::test]
    async fn run_shell_kills_hung_command_on_timeout() {
        // Override to 1s so the test stays fast (see SHELL_TIMEOUT_ENV).
        unsafe { std::env::set_var(SHELL_TIMEOUT_ENV, "1") };
        let dir = TempDir::new().unwrap();
        #[cfg(not(windows))]
        let hang_cmd = "sleep 30";
        #[cfg(windows)]
        let hang_cmd = "timeout /t 30 /nobreak";
        let (out, err) = run_shell(dir.path(), hang_cmd).await;
        assert!(err, "hung command must time out: {out}");
        assert!(out.contains("timed out"), "expected timeout message: {out}");
        unsafe { std::env::remove_var(SHELL_TIMEOUT_ENV) };
    }
}
