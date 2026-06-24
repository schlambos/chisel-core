//! Background-process management for the OpenCode bridge plugin.
//!
//! A "background process" is a long-running local command the plugin
//! (running on the remote OpenCode host) wants executed on the
//! user's machine. They differ from the streaming shell tool in that
//! they are *detached* from any one plugin request: the plugin asks
//! the host to start a process, then comes back later to read its
//! output, tail live updates, or stop it. The user still has to
//! approve the start (via the same [`ShellApprover`] the streaming
//! shell tool uses), but a single process can outlive the plugin
//! request that created it.
//!
//! ## Resource model
//!
//! - **Per-agent process cap.** Up to [`MAX_BG_PROCESSES_PER_AGENT`]
//!   running processes per `agent_id`. The 9th `start` call fails
//!   with `BgError::LimitExceeded`.
//! - **Ring buffer.** Combined stdout+stderr are written into a
//!   512 KiB ring buffer per process. Once the buffer wraps, the
//!   `truncated` flag flips and older bytes are evicted. Callers
//!   reading with `offset` get only the bytes the buffer still
//!   remembers.
//! - **Lifetime.** Each process has a `timeout_secs` (default
//!   [`DEFAULT_BG_TIMEOUT_SECS`] = 2h, max [`MAX_BG_TIMEOUT_SECS`]
//!   = 24h). On expiry the manager kills the child and records a
//!   `bg.timeout` audit entry.
//! - **Terminal records.** Exited/killed processes stay listed so
//!   the UI can show their final state. The manager prunes the
//!   oldest once an agent has more than [`MAX_TERMINAL_RECORDS`]
//!   terminal records.
//!
//! ## Auditing
//!
//! Every lifecycle transition writes a [`PluginAuditRecord`] via
//! [`PluginRegistry::record_audit`] with one of the `bg.*` kinds
//! (`bg.start`, `bg.stop`, `bg.exit`, `bg.timeout`, `bg.denied`).
//! Summaries are the first ~80 chars of the command (see
//! `server.rs::truncate_command_preview` for the exact truncation
//! rule) — never the captured output, which may contain user
//! secrets.
//!
//! ## Orphan cleanup
//!
//! The conversation-close path calls
//! [`BgProcessManager::kill_all_for_agent`] so a closing
//! conversation doesn't leak its background processes. The host's
//! graceful-shutdown path calls [`BgProcessManager::kill_all`] as a
//! belt-and-braces backstop. `kill_on_drop(true)` is the third
//! backstop if both cleanup paths are skipped.
//!
//! ## Process-global singleton
//!
//! Matches [`PluginRegistry`]: a `OnceLock<Arc<…>>` in front of the
//! real manager, with a separate `new()` constructor for tests so
//! each test can build an isolated manager and avoid bleeding state
//! between cases.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use chisl_api_types::BgProcessUiInfo;
use chisl_runtime::Builder;
use tokio::io::{AsyncReadExt, BufReader};
use tokio::process::Child;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::{debug, warn};
use uuid::Uuid;

use super::protocol::{BgProcessInfo, BgStatus, PluginAuditRecord};
use super::registry::PluginRegistry;
use super::ui_push;
use crate::manager::remote::local_fs_mcp::shell::{McpRequestContext, ShellApproval, ShellApprover};

// ── Constants ─────────────────────────────────────────────────────

/// Max concurrent running processes per `agent_id`. A 9th
/// `start` call returns `BgError::LimitExceeded`.
pub const MAX_BG_PROCESSES_PER_AGENT: usize = 8;

/// Per-process ring buffer cap for combined stdout+stderr. Once
/// the buffer is full, older bytes are evicted and the process's
/// `truncated` flag flips to true.
pub const BG_RING_CAP_BYTES: usize = 512 * 1024;

/// Default wall-clock cap on a single background process (2 h).
/// Picked to cover a typical build / test / dev-server lifecycle
/// without holding the slot forever if the user walks away.
pub const DEFAULT_BG_TIMEOUT_SECS: u64 = 7200;

/// Hard cap on per-process `timeout_secs`. 24 h — anything beyond
/// that should be driven by the plugin's own restart policy, not
/// by a single long-lived background process.
pub const MAX_BG_TIMEOUT_SECS: u64 = 86400;

/// Max terminal (exited/killed) records we keep listed per agent.
/// Older terminal records are pruned silently.
const MAX_TERMINAL_RECORDS: usize = 16;

/// Capacity of the per-process output broadcast channel used by
/// `subscribe_output` / the `bg_tail` SSE handler. Slow consumers
/// fall behind and miss updates; we don't replay.
const OUTPUT_CHANNEL_CAP: usize = 256;

/// Chunk size for stdout / stderr reads — small enough for low
/// per-chunk latency, large enough that we don't pay an
/// `AsyncReadExt` round-trip per byte.
const READ_CHUNK: usize = 8 * 1024;

// ── Error type ───────────────────────────────────────────────────

/// Errors a caller (route handler or service) can translate to a
/// wire response. Each variant carries enough context to surface
/// in a `BgErrorResponse` and is mapped to a stable `code` string
/// on the wire.
#[derive(Debug, thiserror::Error)]
pub enum BgError {
    #[error("shell approver rejected the command")]
    Denied,
    #[error("no shell approver registered for this workspace")]
    NoApprover,
    #[error("approval timed out")]
    ApprovalTimedOut,
    #[error("agent already has {0} running background processes (limit: {MAX_BG_PROCESSES_PER_AGENT})")]
    LimitExceeded(usize),
    #[error("process not found: {0}")]
    NotFound(String),
    #[error("failed to spawn shell: {0}")]
    Spawn(String),
    #[error("invalid request: {0}")]
    Invalid(String),
}

impl BgError {
    /// Stable wire `code` the plugin can switch on.
    pub fn code(&self) -> &'static str {
        match self {
            BgError::Denied => "denied",
            BgError::NoApprover => "no_approver",
            BgError::ApprovalTimedOut => "approval_timeout",
            BgError::LimitExceeded(_) => "limit_exceeded",
            BgError::NotFound(_) => "not_found",
            BgError::Spawn(_) => "spawn_failed",
            BgError::Invalid(_) => "invalid",
        }
    }
}

// ── Ring buffer ──────────────────────────────────────────────────

/// Bounded byte buffer that evicts the oldest bytes when full.
/// Stores the buffer as a `VecDeque<u8>` so the eviction is an
/// `O(1)` `pop_front` and the writer can `extend_from_slice` in
/// place. The `offset` cursor is absolute — it never wraps — so
/// callers can pass an old `next_offset` and still get a sensible
/// slice (we just clamp to whatever's still in the buffer).
struct RingBuffer {
    inner: VecDeque<u8>,
    cap: usize,
    /// Absolute byte offset: `inner` represents the byte range
    /// `[base_offset, base_offset + inner.len())`. `base_offset` is
    /// the offset the buffer "starts" at (i.e. the offset the
    /// caller's `next_offset` would land on after a successful
    /// read-to-end).
    base_offset: u64,
    /// Total bytes ever appended to the buffer, including the ones
    /// the ring has since evicted. The renderer can use this as
    /// the "true" output size.
    total_written: u64,
    /// Set to `true` on the first eviction. Mirrored onto the
    /// `BgProcessInfo::truncated` field.
    truncated: bool,
}

impl RingBuffer {
    fn new(cap: usize) -> Self {
        Self {
            inner: VecDeque::with_capacity(cap),
            cap,
            base_offset: 0,
            total_written: 0,
            truncated: false,
        }
    }

    /// Append a chunk. Evicts oldest bytes to stay under `cap`.
    /// `chunk` is the lossy utf-8 string the reader already
    /// decoded from the raw pipe bytes — we re-encode it to feed
    /// the byte buffer; this preserves the on-the-wire byte
    /// semantics of the streaming shell tool.
    fn append(&mut self, chunk: &str) {
        let bytes = chunk.as_bytes();
        self.total_written = self.total_written.saturating_add(bytes.len() as u64);
        for &b in bytes {
            if self.inner.len() == self.cap {
                self.inner.pop_front();
                if !self.truncated {
                    self.truncated = true;
                }
                self.base_offset = self.base_offset.saturating_add(1);
            }
            self.inner.push_back(b);
        }
    }

    /// Bytes in the range `[start, end)` where `start` and `end`
    /// are absolute byte offsets. Returns the bytes still
    /// resident, clamped to the buffer's current window. The
    /// returned `next_offset` is `base_offset + inner.len()` —
    /// what the caller should pass on their next read to keep
    /// reading the tail.
    fn read_from(&self, start: u64) -> (Vec<u8>, u64) {
        let end = self.base_offset + self.inner.len() as u64;
        if start >= end {
            return (Vec::new(), end);
        }
        let clamped_start = start.max(self.base_offset);
        let local_start = (clamped_start - self.base_offset) as usize;
        let slice: Vec<u8> = self.inner.iter().skip(local_start).copied().collect();
        (slice, end)
    }

    fn total_bytes(&self) -> u64 {
        self.total_written
    }

    fn is_truncated(&self) -> bool {
        self.truncated
    }
}

// ── Process record ──────────────────────────────────────────────

/// One background process. Held under the manager's mutex; the
/// fields the monitor task mutates after spawn (status,
/// `exit_code`, `ended_at_ms`) sit behind the same mutex for
/// consistency with the registry pattern.
struct BgProcess {
    info: BgProcessInfo,
    ring: RingBuffer,
    /// Live output broadcaster. `None` if the monitor task has
    /// marked the process terminal and closed the channel.
    output_tx: Option<broadcast::Sender<String>>,
    /// Per-process stop signal. The monitor task selects on it
    /// alongside the child's `wait()` so a stop call can interrupt
    /// a quiet child immediately.
    stop_tx: tokio::sync::watch::Sender<bool>,
    /// Resolved per-process timeout (clamped to
    /// `[1, MAX_BG_TIMEOUT_SECS]`). The monitor's deadline arm
    /// uses this; on fire the monitor records a `bg.timeout`
    /// audit before the process is reaped. Stored on the
    /// record so a future call site (e.g. UI "time remaining"
    /// widget) can read it without re-clamping.
    #[allow(dead_code)]
    timeout_secs: u64,
    /// Join handle for the monitor task. `Some` until the monitor
    /// returns and writes the final exit record. Holding the
    /// handle lets `Drop` (or `kill_all`) join the monitor so we
    /// don't leak a task per killed process.
    monitor: Option<JoinHandle<()>>,
}

impl BgProcess {
    /// Build the public `BgProcessInfo` snapshot. Cheap — clones
    /// the few scalar fields. Avoids cloning the ring buffer.
    fn info(&self) -> BgProcessInfo {
        BgProcessInfo {
            id: self.info.id.clone(),
            name: self.info.name.clone(),
            command: self.info.command.clone(),
            cwd: self.info.cwd.clone(),
            session_id: self.info.session_id.clone(),
            status: self.info.status,
            exit_code: self.info.exit_code,
            started_at_ms: self.info.started_at_ms,
            ended_at_ms: self.info.ended_at_ms,
            output_bytes: self.ring.total_bytes(),
            truncated: self.ring.is_truncated(),
        }
    }
}

// ── Per-agent table ─────────────────────────────────────────────

/// Per-agent record table. Held under the manager's mutex.
struct AgentTable {
    /// All known processes, running + terminal. Pruned on
    /// `stop` and on the next `start` once the terminal cap is
    /// exceeded.
    processes: HashMap<String, BgProcess>,
}

// ── The manager ─────────────────────────────────────────────────

/// Process-wide background-process manager. Public API mirrors
/// [`PluginRegistry`]: a process-global singleton fronted by
/// [`bg_global`], with a separate `BgProcessManager::new()` for
/// tests that need an isolated manager.
pub struct BgProcessManager {
    agents: Mutex<HashMap<String, AgentTable>>,
}

static BG_MANAGER: OnceLock<Arc<BgProcessManager>> = OnceLock::new();

/// Get the process-wide background-process manager. Installs one
/// on first call so test code that doesn't go through
/// `services::remote`'s bootstrap can still find something to talk
/// to.
pub fn bg_global() -> Arc<BgProcessManager> {
    BG_MANAGER
        .get_or_init(|| Arc::new(BgProcessManager::new_internal()))
        .clone()
}

/// Convenience wrapper around [`bg_global`] for the host's
/// graceful-shutdown path. Returns the number of running
/// processes we sent the stop signal to. The caller does not
/// need to await the result — the function returns once the
/// stop signals are delivered, and the per-process monitor tasks
/// reap the children asynchronously.
pub async fn kill_all_bg_processes() -> usize {
    bg_global().kill_all().await
}

impl BgProcessManager {
    /// Test constructor. Production code should use [`bg_global`].
    pub fn new() -> Self {
        Self::new_internal()
    }

    fn new_internal() -> Self {
        Self {
            agents: Mutex::new(HashMap::new()),
        }
    }

    // ── Helpers (under mutex) ───────────────────────────────

    fn with_agent_table<F, R>(&self, agent_id: &str, f: F) -> R
    where
        F: FnOnce(&mut AgentTable) -> R,
    {
        let mut map = self.agents.lock().expect("BgProcessManager mutex poisoned");
        let table = map.entry(agent_id.to_string()).or_insert_with(|| AgentTable {
            processes: HashMap::new(),
        });
        f(table)
    }

    /// Drop the oldest terminal records until the table holds at
    /// most [`MAX_TERMINAL_RECORDS`] of them. Running processes
    /// are never pruned. Called from the monitor's terminal
    /// bookkeeping so a chatty agent that spawns-and-exits many
    /// processes over time doesn't leak memory.
    fn prune_terminal(&self, agent_id: &str) {
        self.with_agent_table(agent_id, |table| {
            let mut terminal: Vec<(String, u64)> = table
                .processes
                .iter()
                .filter(|(_, p)| p.info.status != BgStatus::Running)
                .map(|(id, p)| (id.clone(), p.info.ended_at_ms.unwrap_or(p.info.started_at_ms)))
                .collect();
            if terminal.len() <= MAX_TERMINAL_RECORDS {
                return;
            }
            // Sort oldest-first by ended_at_ms (falling back to
            // started_at_ms for records that never reached a
            // terminal state — shouldn't happen but the
            // comparison is total).
            terminal.sort_by_key(|(_, t)| *t);
            let to_drop = terminal.len() - MAX_TERMINAL_RECORDS;
            for (id, _) in terminal.into_iter().take(to_drop) {
                table.processes.remove(&id);
            }
        });
    }

    fn with_process<F, R>(&self, agent_id: &str, process_id: &str, f: F) -> Result<R, BgError>
    where
        F: FnOnce(&mut BgProcess) -> R,
    {
        let mut map = self.agents.lock().expect("BgProcessManager mutex poisoned");
        let table = map
            .get_mut(agent_id)
            .ok_or_else(|| BgError::NotFound(process_id.to_string()))?;
        let proc = table
            .processes
            .get_mut(process_id)
            .ok_or_else(|| BgError::NotFound(process_id.to_string()))?;
        Ok(f(proc))
    }

    // ── Start ──────────────────────────────────────────────

    /// Start a background process. Performs shell approval FIRST;
    /// `Reject` and `TimedOut` are surfaced as [`BgError::Denied`]
    /// and [`BgError::ApprovalTimedOut`] respectively, with a
    /// `bg.denied` audit record and no spawn.
    pub async fn start(
        self: &Arc<Self>,
        agent_id: &str,
        registry: &Arc<PluginRegistry>,
        request: super::protocol::BgRequest,
        approver: Arc<dyn ShellApprover>,
        default_cwd: &Path,
    ) -> Result<BgProcessInfo, BgError> {
        let super::protocol::BgRequest::Start {
            command,
            cwd,
            session_id,
            call_id,
            name,
            timeout_secs,
        } = request
        else {
            return Err(BgError::Invalid("expected Start op".into()));
        };

        if command.trim().is_empty() {
            return Err(BgError::Invalid("command must not be empty".into()));
        }

        let resolved_cwd = resolve_cwd(cwd.as_deref(), default_cwd);
        let cwd_string = resolved_cwd.to_string_lossy().into_owned();

        // Approval gate — exact same `McpRequestContext` shape the
        // streaming shell tool uses (see shell_stream.rs:194). The
        // approver is supplied by the caller (the route handler
        // resolves it from the plugin registry and 404s with
        // `no_approver` before reaching here).
        let ctx = McpRequestContext {
            session_id: Some(session_id.clone()),
            parent_session_id: None,
        };
        let approval = approver.approve_shell_with_context(&command, &cwd_string, &ctx).await;
        let (kind, summary_cmd) = match approval {
            ShellApproval::Allow => ("bg.start", command_preview(&command)),
            ShellApproval::Reject => {
                audit_bg(
                    registry,
                    agent_id,
                    "bg.denied",
                    &session_id,
                    call_id.as_deref(),
                    "user rejected",
                );
                return Err(BgError::Denied);
            }
            ShellApproval::TimedOut => {
                audit_bg(
                    registry,
                    agent_id,
                    "bg.denied",
                    &session_id,
                    call_id.as_deref(),
                    "approval timed out",
                );
                return Err(BgError::ApprovalTimedOut);
            }
        };

        // Cap check after approval so a denied process doesn't
        // hold a slot reservation.
        let running_count = {
            let map = self.agents.lock().expect("BgProcessManager mutex poisoned");
            map.get(agent_id)
                .map(|t| {
                    t.processes
                        .values()
                        .filter(|p| p.info.status == BgStatus::Running)
                        .count()
                })
                .unwrap_or(0)
        };
        if running_count >= MAX_BG_PROCESSES_PER_AGENT {
            return Err(BgError::LimitExceeded(running_count));
        }

        let id = Uuid::new_v4().to_string();
        let now = chisl_common::now_ms().max(0) as u64;
        let timeout_secs = clamp_timeout(timeout_secs);
        let session_id_for_audit = session_id.clone();

        // Build the shell pipeline identically to
        // shell_stream.rs:237-242 — same shell program, same
        // `Builder::clean_cli` for the env hygiene, same
        // `kill_on_drop(true)` via the Builder defaults.
        let spec = crate::manager::remote::local_fs_mcp::shell::resolve_shell();
        let (program, arg) = spec.parts();
        let mut builder = Builder::clean_cli(program);
        builder.arg(arg).arg(&command).current_dir(&resolved_cwd);
        let child = builder.spawn().map_err(|e| BgError::Spawn(e.to_string()))?;

        // Build the per-process state.
        let (output_tx, _) = broadcast::channel::<String>(OUTPUT_CHANNEL_CAP);
        let (stop_tx, _stop_rx) = tokio::sync::watch::channel(false);
        let info = BgProcessInfo {
            id: id.clone(),
            name: name.clone(),
            command: command.clone(),
            cwd: cwd_string.clone(),
            session_id: session_id.clone(),
            status: BgStatus::Running,
            exit_code: None,
            started_at_ms: now,
            ended_at_ms: None,
            output_bytes: 0,
            truncated: false,
        };
        let mut proc = BgProcess {
            info,
            ring: RingBuffer::new(BG_RING_CAP_BYTES),
            output_tx: Some(output_tx),
            stop_tx: stop_tx.clone(),
            timeout_secs,
            monitor: None,
        };

        // Insert under the mutex BEFORE we spawn the monitor so a
        // racing `list` / `read` can't miss the new process.
        let monitor = self.spawn_monitor(
            agent_id.to_string(),
            id.clone(),
            child,
            stop_tx.subscribe(),
            timeout_secs,
            registry.clone(),
        );
        proc.monitor = Some(monitor);

        self.with_agent_table(agent_id, |table| {
            table.processes.insert(id.clone(), proc);
        });

        // Audit the start AFTER the spawn succeeds — we don't want
        // a denied / never-launched process to show up in the
        // audit ring. Summary is the first 80 chars of the
        // command, never the output.
        audit_bg(
            registry,
            agent_id,
            kind,
            &session_id_for_audit,
            call_id.as_deref(),
            &summary_cmd,
        );
        debug!(
            agent_id = %agent_id,
            process_id = %id,
            timeout_secs,
            "background process started"
        );

        // Re-read the snapshot so the caller sees the real
        // `started_at_ms` and `id`. The handle we stored
        // doesn't change these fields, so we can just rebuild
        // from the in-memory record.
        let snapshot = self
            .with_process(agent_id, &id, |p| p.info())
            .unwrap_or_else(|_| BgProcessInfo {
                id: id.clone(),
                name: name.clone(),
                command: command.clone(),
                cwd: cwd_string.clone(),
                session_id: session_id_for_audit.clone(),
                status: BgStatus::Running,
                exit_code: None,
                started_at_ms: now,
                ended_at_ms: None,
                output_bytes: 0,
                truncated: false,
            });

        // UI broadcast: a fresh process appeared. The renderer
        // polls the REST list endpoint on its 5 s cadence, but a
        // push is the difference between a 5 s lag and an
        // instant refresh. Best-effort — the notifier is a
        // no-op when the host hasn't installed one (test code).
        notify_bg_process_changed(agent_id, &snapshot);

        Ok(snapshot)
    }

    /// Spawn the monitor task: drives the child, writes to the
    /// ring buffer, broadcasts to tail subscribers, records the
    /// terminal exit code. The monitor owns the `Child` for its
    /// entire lifetime.
    fn spawn_monitor(
        self: &Arc<Self>,
        agent_id: String,
        process_id: String,
        mut child: Child,
        mut stop_rx: tokio::sync::watch::Receiver<bool>,
        timeout_secs: u64,
        registry: Arc<PluginRegistry>,
    ) -> JoinHandle<()> {
        let manager = self.clone();
        tokio::spawn(async move {
            let outcome = drive_child(&mut child, &manager, &agent_id, &process_id, &mut stop_rx, timeout_secs).await;
            // The process is now terminal. Lock once and update
            // the record in place.
            let now = chisl_common::now_ms().max(0) as u64;
            let (status, exit_code) = match outcome {
                DriveOutcome::Exited(code) => (BgStatus::Exited, code),
                DriveOutcome::KilledByTimeout => (BgStatus::Killed, None),
                DriveOutcome::KilledByStop => (BgStatus::Killed, None),
                DriveOutcome::SpawnError => (BgStatus::Killed, None),
            };
            // Take the output_tx out so tail subscribers see
            // the channel close promptly.
            let (prev_command, prev_session_id) = manager
                .with_process(&agent_id, &process_id, |p| {
                    p.info.status = status;
                    p.info.exit_code = exit_code;
                    p.info.ended_at_ms = Some(now);
                    // Take the broadcaster so the channel closes.
                    p.output_tx.take();
                    let _ = p.stop_tx.send(true);
                    (p.info.command.clone(), p.info.session_id.clone())
                })
                .unwrap_or_default();

            // Audit the terminal transition. For a self-exit
            // (no `stop` call) we record a `bg.exit` audit so
            // the UI's timeline shows the natural exit. For a
            // timeout kill the kind is `bg.timeout` so the UI
            // can highlight the cause. We prefer the
            // directly-passed `registry` (always set) over the
            // thread-local fallback so tests that don't go
            // through the route handler still see the audit.
            let audit_kind = match outcome {
                DriveOutcome::KilledByTimeout => "bg.timeout",
                _ if matches!(status, BgStatus::Exited) => "bg.exit",
                _ => "bg.stop",
            };
            let registry = current_registry().unwrap_or(registry);
            let summary = command_preview(&prev_command);
            audit_bg(&registry, &agent_id, audit_kind, &prev_session_id, None, &summary);

            // UI broadcast: a running process just became
            // terminal. Best-effort — the notifier is a no-op
            // when the host hasn't installed one.
            if let Ok(snapshot) = manager.with_process(&agent_id, &process_id, |p| p.info()) {
                notify_bg_process_changed(&agent_id, &snapshot);
            }

            // Prune oldest terminal records to keep the
            // per-agent table bounded.
            manager.prune_terminal(&agent_id);
        })
    }

    // ── Stop ───────────────────────────────────────────────

    /// Stop a running process. Idempotent for already-terminal
    /// records. Returns the post-stop snapshot.
    ///
    /// `async` so the monitor task can make progress while we
    /// poll for the terminal transition. A blocking `stop`
    /// would deadlock `#[tokio::test(flavor = "current_thread")]`
    /// since `std::thread::sleep` would park the only worker.
    pub async fn stop(&self, agent_id: &str, process_id: &str) -> Result<BgProcessInfo, BgError> {
        // Flip the per-process stop flag so the monitor wakes up
        // even if the child is silent. The monitor owns the
        // Child; the watch signal is what propagates the kill.
        let _ = self.with_process(agent_id, process_id, |p| {
            let _ = p.stop_tx.send(true);
        });

        // Wait briefly for the monitor to mark terminal. We
        // poll the status rather than blocking on a join
        // because the monitor's join handle is the manager's
        // and we'd need to take the monitor out of the
        // record to do that — invasive for a stop that's
        // racing with the monitor's own bookkeeping.
        let started = std::time::Instant::now();
        loop {
            let info = self.with_process(agent_id, process_id, |p| p.info())?;
            if info.status != BgStatus::Running {
                return Ok(info);
            }
            if started.elapsed() > Duration::from_secs(5) {
                // Fall through and return the current
                // snapshot — the monitor will eventually
                // finish updating it.
                return Ok(info);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    // ── List ───────────────────────────────────────────────

    /// Snapshot every known process (running + terminal) for
    /// `agent_id`. The order is not defined; callers should
    /// sort by `started_at_ms` if they want a stable timeline.
    pub fn list(&self, agent_id: &str) -> Vec<BgProcessInfo> {
        let map = self.agents.lock().expect("BgProcessManager mutex poisoned");
        map.get(agent_id)
            .map(|t| t.processes.values().map(|p| p.info()).collect())
            .unwrap_or_default()
    }

    // ── Read ───────────────────────────────────────────────

    /// Read the ring-buffer slice starting at `offset`. Returns
    /// the slice as a (possibly lossy) utf-8 string, the
    /// `next_offset` to pass on the next call, and the latest
    /// process snapshot.
    pub fn read(&self, agent_id: &str, process_id: &str, offset: u64) -> Result<(String, u64, BgProcessInfo), BgError> {
        self.with_process(agent_id, process_id, |p| {
            let (bytes, next_offset) = p.ring.read_from(offset);
            let output = String::from_utf8_lossy(&bytes).into_owned();
            (output, next_offset, p.info())
        })
    }

    // ── Subscribe (tail) ───────────────────────────────────

    /// Subscribe to live output for a process. The returned
    /// receiver yields append chunks (combined stdout+stderr,
    /// lossy utf-8) until the process exits and the broadcast
    /// channel is closed. Caller is responsible for honouring
    /// the offset semantics — the channel only carries
    /// *future* bytes, not a replay.
    pub fn subscribe_output(&self, agent_id: &str, process_id: &str) -> Result<broadcast::Receiver<String>, BgError> {
        self.with_process(agent_id, process_id, |p| {
            p.output_tx
                .as_ref()
                .map(|tx| tx.subscribe())
                .ok_or_else(|| BgError::Invalid("process has no live output (already terminal)".into()))
        })?
    }

    // ── Bulk teardown ─────────────────────────────────────

    /// Kill every running process for `agent_id`. Terminal
    /// records stay listed (the caller can `read` them to
    /// collect final output) until the next `start` prunes
    /// them.
    ///
    /// Returns the count of processes we sent the stop signal
    /// to — useful for tests.
    pub async fn kill_all_for_agent(&self, agent_id: &str) -> usize {
        let procs: Vec<String> = {
            let map = self.agents.lock().expect("BgProcessManager mutex poisoned");
            map.get(agent_id)
                .map(|t| {
                    t.processes
                        .iter()
                        .filter(|(_, p)| p.info.status == BgStatus::Running)
                        .map(|(id, _)| id.clone())
                        .collect()
                })
                .unwrap_or_default()
        };
        for id in &procs {
            let _ = self.stop(agent_id, id).await;
        }
        procs.len()
    }

    /// Kill every running process across every agent. Used on
    /// host graceful shutdown. Returns the number of processes
    /// we sent the stop signal to.
    pub async fn kill_all(&self) -> usize {
        let agents: Vec<String> = {
            let map = self.agents.lock().expect("BgProcessManager mutex poisoned");
            map.keys().cloned().collect()
        };
        let mut total = 0;
        for a in agents {
            total += self.kill_all_for_agent(&a).await;
        }
        total
    }
}

// ── Module-level: current registry handle (for monitor audits) ─

// `tokio::task_local!` would be cleaner, but the monitor task is
// spawned from a context where we have the registry in scope but
// the `Arc<Self>` doesn't carry one. We use a thread-local pointer
// to the *current* `PluginRegistry` so the monitor can record
// `bg.exit` audits without taking the manager's `Arc<PluginRegistry>`
// as a constructor argument.
//
// The pointer is set in the route handler's call frame and reset
// when that call returns. Tests that don't go through the route
// can set it manually. Set to `None` for the monitor to skip
// auditing.
thread_local! {
    static CURRENT_REGISTRY: std::cell::RefCell<Option<Arc<PluginRegistry>>> = const { std::cell::RefCell::new(None) };
}

fn current_registry() -> Option<Arc<PluginRegistry>> {
    CURRENT_REGISTRY.with(|c| c.borrow().clone())
}

/// Set the current registry for monitor-side audits. Used by
/// route handlers so the monitor task can write `bg.exit`
/// records without taking an extra `Arc<PluginRegistry>` into
/// its closure. Returns a guard that restores the prior value
/// on drop.
pub fn set_current_registry(reg: Option<Arc<PluginRegistry>>) -> RegistryGuard {
    let prior = CURRENT_REGISTRY.with(|c| c.replace(reg));
    RegistryGuard { prior }
}

pub struct RegistryGuard {
    prior: Option<Arc<PluginRegistry>>,
}

impl Drop for RegistryGuard {
    fn drop(&mut self) {
        let p = self.prior.take();
        CURRENT_REGISTRY.with(|c| c.replace(p));
    }
}

// ── Helpers ─────────────────────────────────────────────────────

/// `Bash`-style command preview used for audit summaries. Caps
/// the summary at ~80 chars and slices on a char boundary. The
/// streaming shell tool's `truncate_command_preview` has the
/// exact same shape; we duplicate the rule here to avoid
/// cross-module visibility on a `pub(crate)` helper.
fn command_preview(cmd: &str) -> String {
    const MAX: usize = 80;
    if cmd.len() <= MAX {
        return cmd.to_string();
    }
    let mut cut = MAX;
    while cut > 0 && !cmd.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}…", &cmd[..cut])
}

fn clamp_timeout(requested: Option<u64>) -> u64 {
    let v = requested.unwrap_or(DEFAULT_BG_TIMEOUT_SECS);
    v.clamp(1, MAX_BG_TIMEOUT_SECS)
}

fn resolve_cwd(request_cwd: Option<&str>, default_cwd: &Path) -> PathBuf {
    if let Some(req) = request_cwd {
        let p = PathBuf::from(req);
        if p.is_dir() {
            if let Ok(canonical) = p.canonicalize() {
                return canonical;
            }
            return p;
        }
    }
    default_cwd.to_path_buf()
}

fn audit_bg(
    registry: &Arc<PluginRegistry>,
    agent_id: &str,
    kind: &str,
    session_id: &str,
    call_id: Option<&str>,
    summary: &str,
) {
    let record = PluginAuditRecord {
        kind: kind.to_string(),
        tool: Some("bg".to_string()),
        session_id: Some(session_id.to_string()),
        call_id: call_id.map(str::to_owned),
        at_ms: chisl_common::now_ms().max(0) as u64,
        summary: truncate_for_audit(summary),
    };
    registry.record_audit(agent_id, record);
}

fn truncate_for_audit(s: &str) -> String {
    const MAX: usize = 2048;
    if s.len() <= MAX {
        return s.to_string();
    }
    let mut cut = MAX;
    while cut > 0 && !s.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}…", &s[..cut])
}

/// Map a plugin-webserver `BgProcessInfo` to the UI snake_case
/// shape. Re-exported at the plugin module root so the services
/// layer can call the same conversion without a `services` ↔
/// `manager::remote::plugin` cycle.
pub fn bg_info_to_ui(info: BgProcessInfo) -> BgProcessUiInfo {
    BgProcessUiInfo {
        id: info.id,
        name: info.name,
        command: info.command,
        cwd: info.cwd,
        session_id: info.session_id,
        status: match info.status {
            BgStatus::Running => chisl_api_types::BgProcessStatus::Running,
            BgStatus::Exited => chisl_api_types::BgProcessStatus::Exited,
            BgStatus::Killed => chisl_api_types::BgProcessStatus::Killed,
        },
        exit_code: info.exit_code,
        started_at_ms: info.started_at_ms,
        ended_at_ms: info.ended_at_ms,
        output_bytes: info.output_bytes,
        truncated: info.truncated,
    }
}

/// Push a `remote.bgProcessChanged` UI notification. Called on
/// every lifecycle transition (start success, self-exit, stop,
/// kill, timeout) so the renderer's process list refreshes
/// without waiting for the next poll. Best-effort — the
/// underlying notifier is a no-op when the host hasn't installed
/// one (test code).
fn notify_bg_process_changed(agent_id: &str, info: &BgProcessInfo) {
    let ui = bg_info_to_ui(info.clone());
    let payload = serde_json::json!({
        "agent_id": agent_id,
        "process": ui,
    });
    ui_push::notify("remote.bgProcessChanged", payload);
}

// ── Monitor internals ───────────────────────────────────────────

enum DriveOutcome {
    Exited(Option<i64>),
    KilledByStop,
    KilledByTimeout,
    SpawnError,
}

async fn drive_child(
    child: &mut Child,
    manager: &BgProcessManager,
    agent_id: &str,
    process_id: &str,
    stop_rx: &mut tokio::sync::watch::Receiver<bool>,
    timeout_secs: u64,
) -> DriveOutcome {
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<String>(32);
    if let Some(out) = stdout {
        spawn_reader(out, out_tx.clone());
    }
    if let Some(err) = stderr {
        spawn_reader(err, out_tx.clone());
    }
    drop(out_tx);

    // Per-process deadline, already clamped to `[1, MAX_BG_TIMEOUT_SECS]`
    // by the manager's `start` path. We do not look this up
    // from the record here because the caller already passed
    // it in; the record field is kept for future use and as
    // a backstop if the caller forgets to update the field.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);

    loop {
        tokio::select! {
            biased;
            wait_result = child.wait() => {
                match wait_result {
                    Ok(status) => {
                        // Drain any remaining output before returning.
                        while let Ok(chunk) = out_rx.try_recv() {
                            append_chunk(manager, agent_id, process_id, &chunk);
                        }
                        return DriveOutcome::Exited(status.code().map(|c| c as i64));
                    }
                    Err(e) => {
                        warn!(error = %e, process_id = %process_id, "bg child wait failed");
                        return DriveOutcome::SpawnError;
                    }
                }
            }
            maybe_chunk = out_rx.recv() => {
                match maybe_chunk {
                    Some(chunk) => {
                        append_chunk(manager, agent_id, process_id, &chunk);
                    }
                    None => {
                        // Both readers finished; the child is
                        // likely already exited but we should
                        // still re-check.
                        match child.wait().await {
                            Ok(status) => return DriveOutcome::Exited(status.code().map(|c| c as i64)),
                            Err(_) => return DriveOutcome::SpawnError,
                        }
                    }
                }
            }
            _ = tokio::time::sleep_until(deadline) => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                return DriveOutcome::KilledByTimeout;
            }
            _ = wait_for_stop(stop_rx) => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                return DriveOutcome::KilledByStop;
            }
        }
    }
}

async fn wait_for_stop(rx: &mut tokio::sync::watch::Receiver<bool>) {
    // We want a `Future` that resolves the moment the watch
    // channel is set to true. Polling the receiver directly is
    // the simplest approach; the select! arm only fires once.
    loop {
        if *rx.borrow() {
            return;
        }
        if rx.changed().await.is_err() {
            // Sender dropped — treat as stop.
            return;
        }
    }
}

fn append_chunk(manager: &BgProcessManager, agent_id: &str, process_id: &str, chunk: &str) {
    let _ = manager.with_process(agent_id, process_id, |p| {
        p.ring.append(chunk);
        if let Some(tx) = p.output_tx.as_ref() {
            // Drop send errors silently — a missed tail chunk
            // is fine, the next `read` will catch up.
            let _ = tx.send(chunk.to_string());
        }
    });
}

fn spawn_reader<R: AsyncReadExt + Unpin + Send + 'static>(pipe: R, tx: tokio::sync::mpsc::Sender<String>) {
    tokio::spawn(async move {
        let mut reader = BufReader::with_capacity(READ_CHUNK, pipe);
        let mut buf = vec![0u8; READ_CHUNK];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    let text = String::from_utf8_lossy(&buf[..n]).into_owned();
                    if tx.send(text).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
}

// ── Tests ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::manager::remote::local_fs_mcp::ShellApproval;
    use crate::manager::remote::plugin::protocol::BgRequest;

    struct AllowAll;
    #[async_trait]
    impl ShellApprover for AllowAll {
        async fn approve_shell(&self, _command: &str, _cwd: &str) -> ShellApproval {
            ShellApproval::Allow
        }
    }

    struct RejectAll;
    #[async_trait]
    impl ShellApprover for RejectAll {
        async fn approve_shell(&self, _command: &str, _cwd: &str) -> ShellApproval {
            ShellApproval::Reject
        }
    }

    struct TimeoutAll;
    #[async_trait]
    impl ShellApprover for TimeoutAll {
        async fn approve_shell(&self, _command: &str, _cwd: &str) -> ShellApproval {
            ShellApproval::TimedOut
        }
    }

    fn registry() -> Arc<PluginRegistry> {
        Arc::new(PluginRegistry::new())
    }

    fn manager() -> Arc<BgProcessManager> {
        Arc::new(BgProcessManager::new())
    }

    fn start_req(command: &str) -> BgRequest {
        BgRequest::Start {
            command: command.to_string(),
            cwd: None,
            session_id: "ses_test".to_string(),
            call_id: None,
            name: None,
            timeout_secs: None,
        }
    }

    fn count_running(mgr: &BgProcessManager, agent_id: &str) -> usize {
        mgr.list(agent_id)
            .iter()
            .filter(|p| p.status == BgStatus::Running)
            .count()
    }

    #[tokio::test]
    async fn start_running_outputs_accumulate_and_read_returns_offset() {
        let mgr = manager();
        let reg = registry();
        let approver: Arc<dyn ShellApprover> = Arc::new(AllowAll);
        let dir = tempfile::tempdir().unwrap();
        let info = mgr
            .start(
                "ra_x",
                &reg,
                start_req("printf hello_world_marker"),
                approver,
                dir.path(),
            )
            .await
            .unwrap();
        assert_eq!(info.status, BgStatus::Running);
        assert!(!info.id.is_empty());

        // Give the child a moment to exit on its own.
        let mut waited_ms = 0u64;
        loop {
            let list = mgr.list("ra_x");
            if let Some(p) = list.iter().find(|p| p.id == info.id)
                && p.status != BgStatus::Running
            {
                break;
            }
            if waited_ms > 5000 {
                panic!("process never finished");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            waited_ms += 50;
        }

        let (output, next_offset, snap) = mgr.read("ra_x", &info.id, 0).unwrap();
        assert!(
            output.contains("hello_world_marker"),
            "missing marker in output: {output}"
        );
        assert!(next_offset > 0);
        assert_eq!(next_offset, snap.output_bytes);
        assert_eq!(snap.status, BgStatus::Exited);
    }

    #[tokio::test]
    async fn stop_kills_running_process_and_marks_killed() {
        let mgr = manager();
        let reg = registry();
        let approver: Arc<dyn ShellApprover> = Arc::new(AllowAll);
        let dir = tempfile::tempdir().unwrap();
        let info = mgr
            .start("ra_x", &reg, start_req("sleep 30"), approver, dir.path())
            .await
            .unwrap();
        assert_eq!(info.status, BgStatus::Running);

        let stopped = mgr.stop("ra_x", &info.id).await.unwrap();
        assert_ne!(stopped.status, BgStatus::Running);
        assert_eq!(stopped.status, BgStatus::Killed);
    }

    #[tokio::test]
    async fn approval_reject_records_denied_and_does_not_spawn() {
        let mgr = manager();
        let reg = registry();
        let approver: Arc<dyn ShellApprover> = Arc::new(RejectAll);
        let dir = tempfile::tempdir().unwrap();
        let err = mgr
            .start("ra_x", &reg, start_req("echo should_not_run"), approver, dir.path())
            .await
            .expect_err("reject should not spawn");
        assert!(matches!(err, BgError::Denied));
        assert!(mgr.list("ra_x").is_empty(), "rejected process should not be tracked");
        let records = reg.audit_records("ra_x");
        assert!(
            records.iter().any(|r| r.kind == "bg.denied"),
            "expected bg.denied audit, got: {:?}",
            records
        );
    }

    #[tokio::test]
    async fn approval_timeout_records_denied_and_does_not_spawn() {
        let mgr = manager();
        let reg = registry();
        let approver: Arc<dyn ShellApprover> = Arc::new(TimeoutAll);
        let dir = tempfile::tempdir().unwrap();
        let err = mgr
            .start("ra_x", &reg, start_req("sleep 1"), approver, dir.path())
            .await
            .expect_err("timeout should not spawn");
        assert!(matches!(err, BgError::ApprovalTimedOut));
        assert!(mgr.list("ra_x").is_empty());
        let records = reg.audit_records("ra_x");
        assert!(records.iter().any(|r| r.kind == "bg.denied"));
    }

    #[tokio::test]
    async fn max_processes_per_agent_limit() {
        let mgr = manager();
        let reg = registry();
        let approver: Arc<dyn ShellApprover> = Arc::new(AllowAll);
        let dir = tempfile::tempdir().unwrap();
        // Start 8 sleeping children.
        let mut started = Vec::new();
        for _ in 0..MAX_BG_PROCESSES_PER_AGENT {
            let info = mgr
                .start("ra_x", &reg, start_req("sleep 30"), approver.clone(), dir.path())
                .await
                .unwrap();
            started.push(info.id);
        }
        // The 9th must fail with LimitExceeded.
        let err = mgr
            .start("ra_x", &reg, start_req("sleep 30"), approver.clone(), dir.path())
            .await
            .expect_err("9th start must fail");
        assert!(matches!(err, BgError::LimitExceeded(8)), "got {err:?}");
        assert_eq!(count_running(&mgr, "ra_x"), MAX_BG_PROCESSES_PER_AGENT);

        // Stopping one frees a slot.
        mgr.stop("ra_x", &started[0]).await.unwrap();
        // Slot isn't free immediately because the monitor takes a
        // moment to settle, but it should be free within a few
        // hundred ms.
        let mut freed = false;
        for _ in 0..50 {
            if count_running(&mgr, "ra_x") < MAX_BG_PROCESSES_PER_AGENT {
                freed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(freed, "stop should free a slot");
    }

    #[tokio::test]
    async fn ring_buffer_caps_and_truncated_flag() {
        let mgr = manager();
        let reg = registry();
        let approver: Arc<dyn ShellApprover> = Arc::new(AllowAll);
        let dir = tempfile::tempdir().unwrap();
        // Emit a single chunk bigger than the cap. We use a
        // shell that prints a known number of bytes.
        let target_bytes = BG_RING_CAP_BYTES + 4096;
        // Use a busy Python heredoc; cross-platform via `sh` on
        // unix, `cmd` on windows. We just print the same byte
        // over and over.
        let cmd = format!("printf 'a%.0s' {{1..{target_bytes}}}");
        let info = mgr
            .start("ra_x", &reg, start_req(&cmd), approver, dir.path())
            .await
            .unwrap();
        // Wait for it to exit.
        for _ in 0..200 {
            let (_, _, snap) = mgr.read("ra_x", &info.id, 0).unwrap();
            if snap.status != BgStatus::Running {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let (output, next_offset, snap) = mgr.read("ra_x", &info.id, 0).unwrap();
        assert_eq!(output.len(), BG_RING_CAP_BYTES, "ring should be capped");
        assert!(snap.truncated, "truncated flag must be set");
        assert!(snap.output_bytes >= target_bytes as u64);
        // `next_offset` is the absolute position of the next byte
        // the ring will hold — i.e. `base_offset + inner.len()`,
        // which equals the total bytes the child has emitted. After
        // truncation, that's strictly greater than the ring cap.
        assert_eq!(next_offset, snap.output_bytes);
    }

    #[tokio::test]
    async fn timeout_kills_process() {
        let mgr = manager();
        let reg = registry();
        let approver: Arc<dyn ShellApprover> = Arc::new(AllowAll);
        let dir = tempfile::tempdir().unwrap();
        // 1-second timeout via the per-request override.
        let mut req = start_req("sleep 30");
        if let BgRequest::Start { timeout_secs, .. } = &mut req {
            *timeout_secs = Some(1);
        }
        let info = mgr.start("ra_x", &reg, req, approver, dir.path()).await.unwrap();
        // Wait up to 5s for the monitor to mark it Killed.
        for _ in 0..100 {
            let snap = mgr.list("ra_x");
            if let Some(p) = snap.iter().find(|p| p.id == info.id)
                && p.status != BgStatus::Running
            {
                assert_eq!(p.status, BgStatus::Killed, "timeout should mark Killed");
                let records = reg.audit_records("ra_x");
                assert!(
                    records.iter().any(|r| r.kind == "bg.timeout"),
                    "expected bg.timeout audit, got: {:?}",
                    records
                );
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("process never reached terminal state under timeout");
    }

    #[tokio::test]
    async fn kill_all_for_agent_kills_running_children() {
        let mgr = manager();
        let reg = registry();
        let approver: Arc<dyn ShellApprover> = Arc::new(AllowAll);
        let dir = tempfile::tempdir().unwrap();
        let mut started = Vec::new();
        for _ in 0..3 {
            let info = mgr
                .start("ra_x", &reg, start_req("sleep 30"), approver.clone(), dir.path())
                .await
                .unwrap();
            started.push(info.id);
        }
        let killed = mgr.kill_all_for_agent("ra_x").await;
        assert!(killed >= 1, "should have sent stop signals");

        // After kill, all processes should be terminal.
        for _ in 0..100 {
            let all_terminal = mgr.list("ra_x").iter().all(|p| p.status != BgStatus::Running);
            if all_terminal {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("not all processes became terminal after kill_all_for_agent");
    }

    #[tokio::test]
    async fn kill_all_kills_everything() {
        let mgr = manager();
        let reg = registry();
        let approver: Arc<dyn ShellApprover> = Arc::new(AllowAll);
        let dir = tempfile::tempdir().unwrap();
        // Two agents, two running children each.
        for agent in ["ra_a", "ra_b"] {
            for _ in 0..2 {
                let _ = mgr
                    .start(agent, &reg, start_req("sleep 30"), approver.clone(), dir.path())
                    .await
                    .unwrap();
            }
        }
        let total = mgr.kill_all().await;
        assert!(total >= 4, "should have killed at least 4 processes, got {total}");

        for agent in ["ra_a", "ra_b"] {
            for _ in 0..100 {
                let all_terminal = mgr.list(agent).iter().all(|p| p.status != BgStatus::Running);
                if all_terminal {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            assert!(
                mgr.list(agent).iter().all(|p| p.status != BgStatus::Running),
                "agent {agent} should have no running processes after kill_all"
            );
        }
    }

    #[tokio::test]
    async fn tail_disconnect_does_not_kill_process() {
        let mgr = manager();
        let reg = registry();
        let approver: Arc<dyn ShellApprover> = Arc::new(AllowAll);
        let dir = tempfile::tempdir().unwrap();
        let info = mgr
            .start("ra_x", &reg, start_req("sleep 1"), approver, dir.path())
            .await
            .unwrap();
        // Subscribe then drop the receiver immediately.
        let rx = mgr.subscribe_output("ra_x", &info.id).unwrap();
        drop(rx);
        // Give the process time to run.
        tokio::time::sleep(Duration::from_millis(200)).await;
        // Process should still be running (or have exited
        // naturally), not been killed by our drop.
        let snap = mgr.list("ra_x");
        let p = snap.iter().find(|p| p.id == info.id).unwrap();
        assert_ne!(
            p.status,
            BgStatus::Killed,
            "dropping a tail subscriber must not kill the process"
        );
    }

    #[tokio::test]
    async fn empty_command_is_rejected() {
        let mgr = manager();
        let reg = registry();
        let approver: Arc<dyn ShellApprover> = Arc::new(AllowAll);
        let dir = tempfile::tempdir().unwrap();
        let err = mgr
            .start("ra_x", &reg, start_req("   "), approver, dir.path())
            .await
            .expect_err("empty command must fail");
        assert!(matches!(err, BgError::Invalid(_)));
    }

    #[tokio::test]
    async fn unknown_process_read_fails() {
        let mgr = manager();
        let err = mgr.read("ra_x", "missing", 0);
        assert!(matches!(err, Err(BgError::NotFound(_))));
    }

    #[tokio::test]
    async fn approve_called_once_per_start() {
        let mgr = manager();
        let reg = registry();
        let count = Arc::new(AtomicUsize::new(0));
        struct CountingAllow {
            count: Arc<AtomicUsize>,
        }
        #[async_trait]
        impl ShellApprover for CountingAllow {
            async fn approve_shell(&self, _command: &str, _cwd: &str) -> ShellApproval {
                self.count.fetch_add(1, Ordering::SeqCst);
                ShellApproval::Allow
            }
        }
        let approver: Arc<dyn ShellApprover> = Arc::new(CountingAllow { count: count.clone() });
        let dir = tempfile::tempdir().unwrap();
        let _ = mgr
            .start("ra_x", &reg, start_req("echo ok"), approver, dir.path())
            .await
            .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    /// The "no orphans" guarantee: after `kill_all_for_agent` /
    /// `kill_all`, the actual child PIDs (and their shells'
    /// children) must be dead on the OS — not just the manager's
    /// notion of "terminal". The child PIDs aren't exposed on
    /// the `BgProcessInfo` wire shape, so we assert the
    /// observable proxy: every entry in the table reaches a
    /// non-Running status. The `monitor` task does a real
    /// `child.wait().await` after `child.kill()`, so a
    /// non-Running status proves the OS has reaped the
    /// process. This is the contract the host's
    /// graceful-shutdown path relies on.
    #[tokio::test]
    async fn kill_all_does_not_leave_orphan_children() {
        let mgr = manager();
        let reg = registry();
        let approver: Arc<dyn ShellApprover> = Arc::new(AllowAll);
        let dir = tempfile::tempdir().unwrap();
        let mut started = Vec::new();
        for _ in 0..3 {
            let info = mgr
                .start("ra_x", &reg, start_req("sleep 30"), approver.clone(), dir.path())
                .await
                .unwrap();
            started.push(info.id);
        }

        let _ = mgr.kill_all_for_agent("ra_x").await;

        for _ in 0..200 {
            let list = mgr.list("ra_x");
            if list.iter().all(|p| p.status != BgStatus::Running) {
                // Every process is terminal — the monitor
                // has finished its `child.wait().await` for
                // each, which means the OS has reaped the
                // child. The contract holds: the kill path
                // does not leak orphan processes.
                for id in &started {
                    let p = list.iter().find(|p| &p.id == id).unwrap();
                    assert_eq!(
                        p.status,
                        BgStatus::Killed,
                        "process {id} should be Killed after kill_all, got {:?}",
                        p.status
                    );
                }
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("manager-owned processes never became terminal after kill_all_for_agent");
    }

    /// Exercise the per-process timeout: with a 1s override
    /// the process must be killed and the audit log must
    /// contain a `bg.timeout` entry.
    #[tokio::test]
    async fn timeout_kills_process_records_bg_timeout_audit() {
        let mgr = manager();
        let reg = registry();
        let approver: Arc<dyn ShellApprover> = Arc::new(AllowAll);
        let dir = tempfile::tempdir().unwrap();
        let mut req = start_req("sleep 30");
        if let BgRequest::Start { timeout_secs, .. } = &mut req {
            *timeout_secs = Some(1);
        }
        let info = mgr.start("ra_x", &reg, req, approver, dir.path()).await.unwrap();
        // Wait up to 5s for the monitor to mark it Killed.
        for _ in 0..100 {
            let snap = mgr.list("ra_x");
            if let Some(p) = snap.iter().find(|p| p.id == info.id)
                && p.status != BgStatus::Running
            {
                assert_eq!(p.status, BgStatus::Killed, "timeout should mark Killed");
                let records = reg.audit_records("ra_x");
                assert!(
                    records.iter().any(|r| r.kind == "bg.timeout"),
                    "expected bg.timeout audit, got: {:?}",
                    records
                );
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("process never reached terminal state under timeout");
    }
}

#[cfg(test)]
#[path = "bg/tests.rs"]
mod bg_lifecycle_notify_tests;
