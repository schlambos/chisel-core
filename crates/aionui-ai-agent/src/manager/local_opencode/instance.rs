//! Single OpenCode instance lifecycle: spawn, port capture,
//! health check, stop.
//!
//! Each [`OpenCodeInstance`] owns one `opencode serve` child
//! process. The flow is:
//!
//! 1. `spawn` runs the child via
//!    [`aionui_runtime::Builder`] (which sets `kill_on_drop(true)`
//!    and strips debug env pollution) with the auto-injected
//!    `OPENCODE_CONFIG_CONTENT` and a per-instance `OPENCODE_DATA_DIR`.
//! 2. The child's stdout is read line-by-line until we see a
//!    "listening on http://…:PORT" line. Port capture has a
//!    30 s budget; if opencode takes longer to bind we surface
//!    a timeout error to the route handler.
//! 3. A background tokio task pings
//!    `http://127.0.0.1:{port}/global/health` every 30 s. A
//!    failed health check is logged at `warn`; the child
//!    process itself is monitored separately so we can flip the
//!    status to [`LocalOpenCodeStatus::Crashed`] promptly.
//! 4. `stop` sends SIGKILL via the child's own `kill` (the
//!    `kill_on_drop` on the Builder is a backstop if `stop`
//!    itself is bypassed — e.g. by `Drop`).
//!
//! ## Bounded restart
//!
//! [`OpenCodeInstance::can_restart`] enforces a soft restart
//! policy: at most [`MAX_RESTARTS`] restarts inside a rolling
//! [`RESTART_WINDOW`]. Past that we refuse the restart so a
//! misconfigured instance can't burn CPU in a tight crash loop.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use aionui_api_types::LocalOpenCodeStatus;
use aionui_runtime::Builder;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Child;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Maximum restart attempts within the restart window.
const MAX_RESTARTS: u32 = 3;
/// Window for counting restarts (5 minutes).
const RESTART_WINDOW: Duration = Duration::from_secs(300);
/// Health check interval.
const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(30);
/// Timeout for port capture from stdout (30 seconds).
const PORT_CAPTURE_TIMEOUT: Duration = Duration::from_secs(30);

/// A single managed OpenCode instance.
///
/// Owns the child process, the health-check task, and the
/// restart-window bookkeeping. `Drop` signals both to stop so
/// a leaked instance can never orphan a running `opencode
/// serve` (the `kill_on_drop` on the underlying Builder is the
/// final backstop).
pub struct OpenCodeInstance {
    pub id: String,
    pub name: String,
    pub working_dir: PathBuf,
    pub data_dir: PathBuf,
    pub port: Option<u16>,
    pub status: LocalOpenCodeStatus,
    pub pid: Option<u32>,
    pub agent_id: String,
    pub plugin_token: String,
    pub created_at: u64,
    child: Option<Child>,
    health_handle: Option<JoinHandle<()>>,
    shutdown_tx: Option<watch::Sender<bool>>,
    restart_count: u32,
    restart_window_start: std::time::Instant,
}

impl OpenCodeInstance {
    /// Construct a new (not-yet-spawned) instance.
    ///
    /// `agent_id` and `plugin_token` are the values that will be
    /// embedded in the `OPENCODE_CONFIG_CONTENT` env var so the
    /// plugin can dial back to the matching AionCore agent row.
    pub fn new(name: String, working_dir: PathBuf, data_dir: PathBuf, agent_id: String, plugin_token: String) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            name,
            working_dir,
            data_dir,
            port: None,
            status: LocalOpenCodeStatus::Stopped,
            pid: None,
            agent_id,
            plugin_token,
            created_at: aionui_common::now_ms() as u64,
            child: None,
            health_handle: None,
            shutdown_tx: None,
            restart_count: 0,
            restart_window_start: std::time::Instant::now(),
        }
    }

    /// Spawn the `opencode serve` process with auto-injected
    /// config.
    ///
    /// Returns the bound port once the process prints its
    /// listening line. On any error along the way the partially
    /// constructed child is killed before bubbling the error up
    /// so the caller never sees a half-started instance.
    pub async fn spawn(&mut self, opencode_config_content: &str) -> Result<u16, String> {
        // Create data directory; opencode won't start cleanly if
        // its data dir doesn't exist.
        tokio::fs::create_dir_all(&self.data_dir)
            .await
            .map_err(|e| format!("failed to create data dir: {e}"))?;

        let mut builder = Builder::new("opencode");
        builder
            .arg("serve")
            .current_dir(&self.working_dir)
            .env("OPENCODE_CONFIG_CONTENT", opencode_config_content)
            .env("OPENCODE_DATA_DIR", self.data_dir.to_string_lossy().as_ref())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = builder.spawn().map_err(|e| format!("failed to spawn opencode: {e}"))?;
        self.pid = child.id();
        self.status = LocalOpenCodeStatus::Starting;

        // Capture port from stdout. We have to move `child` into
        // the timeout so we can kill it on failure without
        // holding two mutable borrows.
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "child stdout was not piped".to_string())?;
        let id = self.id.clone();
        let port_result = tokio::time::timeout(PORT_CAPTURE_TIMEOUT, async move {
            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();
            while let Ok(Some(line)) = lines.next_line().await {
                debug!(instance_id = %id, line = %line, "opencode stdout");
                if let Some(port) = parse_port_from_line(&line) {
                    return Some(port);
                }
            }
            None
        })
        .await;

        let port = match port_result {
            Ok(Some(port)) => port,
            Ok(None) => {
                // We never saw a port; kill the child so we
                // don't leak a process that's bound to a
                // unknown port.
                let _ = child.kill().await;
                return Err("could not parse port from opencode output".to_string());
            }
            Err(_) => {
                let _ = child.kill().await;
                return Err("timeout waiting for opencode to print its listening port".to_string());
            }
        };

        self.port = Some(port);
        self.status = LocalOpenCodeStatus::Running;
        self.child = Some(child);

        // Start health check loop. The watch channel doubles as
        // a cancellation signal — sending `true` makes the loop
        // exit on its next tick.
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        self.shutdown_tx = Some(shutdown_tx);
        let port_for_health = port;
        let id_for_health = self.id.clone();
        self.health_handle = Some(tokio::spawn(async move {
            health_check_loop(port_for_health, &id_for_health, shutdown_rx).await;
        }));

        info!(
            instance_id = %self.id,
            port = port,
            pid = ?self.pid,
            "local OpenCode instance started"
        );

        Ok(port)
    }

    /// Stop the instance gracefully.
    ///
    /// Idempotent — calling on an already-stopped instance is a
    /// no-op. Sends SIGKILL via the child's own `kill` (which
    /// waits for the OS to actually reap it) and aborts the
    /// health-check task.
    pub async fn stop(&mut self) {
        // Signal health check to stop
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }

        // Cancel health check task
        if let Some(handle) = self.health_handle.take() {
            handle.abort();
        }

        // Kill the child process (kill_on_drop is also set by
        // Builder, so even if this future is dropped between
        // take() and kill().await, the runtime will still
        // SIGKILL the child).
        if let Some(mut child) = self.child.take() {
            let _ = child.kill().await;
            info!(instance_id = %self.id, "local OpenCode instance stopped");
        }

        self.status = LocalOpenCodeStatus::Stopped;
        self.pid = None;
    }

    /// Check if the process has exited unexpectedly.
    ///
    /// Called periodically by the manager (e.g. on each list
    /// request). Returns `true` if the child has exited and the
    /// exit status was not a clean success — in that case the
    /// status has been flipped to [`LocalOpenCodeStatus::Crashed`]
    /// and the caller may want to surface that in the UI.
    pub fn check_crash(&mut self) -> bool {
        if let Some(child) = &mut self.child {
            match child.try_wait() {
                Ok(Some(status)) => {
                    if !status.success() {
                        warn!(
                            instance_id = %self.id,
                            status = %status,
                            "local OpenCode instance crashed"
                        );
                        self.status = LocalOpenCodeStatus::Crashed;
                        self.pid = None;
                        return true;
                    }
                    // Clean exit — treat as stopped, not crashed.
                    self.status = LocalOpenCodeStatus::Stopped;
                    self.pid = None;
                }
                Ok(None) => {} // still running
                Err(e) => {
                    warn!(instance_id = %self.id, error = %e, "failed to check child status");
                }
            }
        }
        false
    }

    /// Whether the instance can be restarted (bounded restart
    /// policy).
    ///
    /// Sliding 5-minute window: if the window has elapsed we
    /// reset the counter. Otherwise the count is compared to
    /// [`MAX_RESTARTS`]. This keeps a misconfigured instance
    /// from eating CPU in a tight crash loop while still
    /// allowing recovery from transient blips.
    pub fn can_restart(&mut self) -> bool {
        let now = std::time::Instant::now();
        if now.duration_since(self.restart_window_start) > RESTART_WINDOW {
            self.restart_count = 0;
            self.restart_window_start = now;
        }
        self.restart_count < MAX_RESTARTS
    }

    /// Record a successful restart. Increments the rolling
    /// counter that `can_restart` checks.
    pub fn record_restart(&mut self) {
        self.restart_count += 1;
    }
}

impl Drop for OpenCodeInstance {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }
        if let Some(handle) = self.health_handle.take() {
            handle.abort();
        }
        // Child is killed by kill_on_drop when dropped.
    }
}

/// Parse the port number from an `opencode serve` stdout line.
///
/// Recognises the shapes opencode actually prints:
///
/// - `Server listening on http://127.0.0.1:4096`
/// - `Listening on http://0.0.0.0:12345`
/// - Bare URLs: `http://localhost:8080/`
///
/// We deliberately accept any `http://…:NNN` line so a future
/// opencode version that rewords the prefix doesn't break us.
fn parse_port_from_line(line: &str) -> Option<u16> {
    let url_start = line.find("http://")?;
    let url_part = &line[url_start..];
    let colon_pos = url_part.rfind(':')?;
    let after_colon = &url_part[colon_pos + 1..];
    let port_str: String = after_colon.chars().take_while(|c| c.is_ascii_digit()).collect();
    port_str.parse().ok()
}

/// Periodic health-check loop.
///
/// Hits `GET /global/health` on the spawned `opencode serve`
/// every [`HEALTH_CHECK_INTERVAL`]. Non-2xx and network errors
/// are logged at `warn` but do not flip the status — that's
/// `check_crash`'s job, which is driven by the actual child
/// exit code. The loop exits when the watch channel flips to
/// `true` (i.e. on `stop`).
async fn health_check_loop(port: u16, instance_id: &str, mut shutdown_rx: watch::Receiver<bool>) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap_or_default();
    let url = format!("http://127.0.0.1:{port}/global/health");

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    break;
                }
            }
            _ = tokio::time::sleep(HEALTH_CHECK_INTERVAL) => {
                match client.get(&url).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        debug!(instance_id = %instance_id, "health check passed");
                    }
                    Ok(resp) => {
                        warn!(instance_id = %instance_id, status = %resp.status(), "health check returned non-success");
                    }
                    Err(e) => {
                        warn!(instance_id = %instance_id, error = %e, "health check failed");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_port_from_listening_line() {
        assert_eq!(
            parse_port_from_line("Server listening on http://127.0.0.1:4096"),
            Some(4096)
        );
        assert_eq!(parse_port_from_line("Listening on http://0.0.0.0:12345"), Some(12345));
        assert_eq!(parse_port_from_line("  http://localhost:8080/"), Some(8080));
    }

    #[test]
    fn parse_port_returns_none_for_non_port_lines() {
        assert_eq!(parse_port_from_line("Starting up..."), None);
        assert_eq!(parse_port_from_line("Loading configuration"), None);
        assert_eq!(parse_port_from_line(""), None);
        // No http:// prefix → None even if a colon is present.
        assert_eq!(parse_port_from_line("Loaded plugin foo:1.2.3"), None);
    }

    #[test]
    fn restart_window_resets_after_elapsed() {
        let mut inst = OpenCodeInstance::new(
            "t".into(),
            PathBuf::from("."),
            PathBuf::from("."),
            "a".into(),
            "t".into(),
        );
        // We can't easily fast-forward Instant, so just verify
        // the can_restart / record_restart bookkeeping works
        // for a fresh instance.
        assert!(inst.can_restart());
        inst.record_restart();
        inst.record_restart();
        inst.record_restart();
        // 3 restarts consumed the budget.
        assert!(!inst.can_restart());
    }
}
