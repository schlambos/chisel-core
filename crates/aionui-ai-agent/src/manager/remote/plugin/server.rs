//! HTTP server for the OpenCode bridge plugin channel.
//!
//! Hosts the four routes the first-party OpenCode plugin
//! (`@chisl/chisl-opencode-plugin`) dials back to. All routes share a
//! single bearer-token middleware that maps the plugin's `Authorization`
//! header back to a `remote_agent_id` via the [`PluginTokenValidator`]
//! the composition root wired in. Everything else (hello bookkeeping,
//! SSE event stream, hook telemetry, shell streaming) is local state
//! driven by the [`PluginRegistry`].
//!
//! Routes:
//!
//! | Method | Path                                | Purpose                                |
//! |--------|-------------------------------------|----------------------------------------|
//! | POST   | `/plugin/hello`                     | handshake + version/hook bookkeeping  |
//! | GET    | `/plugin/events`                    | SSE stream for host → plugin pushes    |
//! | POST   | `/plugin/result`                    | fire-and-forget hook telemetry         |
//! | POST   | `/tools/run_shell_streaming`        | SSE stream of an approved shell command|
//!
//! The server is a process-wide singleton — see
//! [`ensure_plugin_server`]. Multiple agents share the same listener
//! (and therefore the same port); routing is by token, not by URL.
//!
//! ## Logging policy
//!
//! Production logs deliberately do **not** carry:
//!
//! - The bearer token (we log only that auth passed / failed).
//! - Plugin tool `args` / `output` payloads (the registry's audit ring
//!   buffer holds the redacted summary; `info!` lines are limited to
//!   `{tool, session_id, call_id, kind, at_ms}`).
//! - Shell command bodies (the audit record's `summary` is the first
//!   80 chars; the full string never reaches a log line).
//! - SSE event bodies pushed from host to plugin (we log the count, not
//!   the payload).
//!
//! See `services::remote::plugin_install_info` for the only call site
//! that surfaces the token in a response — and only over a
//! CSRF-protected, auth-gated renderer endpoint.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use futures_util::Stream;
use futures_util::stream::{self, StreamExt};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use super::protocol::{
    PROTOCOL_VERSION, PluginAuditRecord, PluginHelloRequest, PluginHelloResponse, PluginResultRequest,
    PluginResultResponse,
};
use super::registry::{PluginRegistry, PluginTokenValidator};
use super::shell_stream::run_shell_streaming;

// ── App state ───────────────────────────────────────────────────

/// App state shared across every route. Cheap to clone (Arc fields).
#[derive(Clone)]
struct PluginAppState {
    validator: Arc<dyn PluginTokenValidator>,
    /// Pre-resolved registry; the global singleton from
    /// [`super::registry::global`]. Wrapped in `Arc` so each
    /// `PluginServer` (including test-only isolated ones) can hand out
    /// a registry that the handler can reach.
    registry: Arc<PluginRegistry>,
}

// ── Singleton holder ───────────────────────────────────────────

/// Process-wide singleton. The plugin webserver binds on first call to
/// [`ensure_plugin_server`]; subsequent calls return the existing
/// address without rebinding.
///
/// `tokio::sync::Mutex` (not `std::sync::Mutex`) so we can hold the
/// guard across the `start()` await — `std::sync::MutexGuard` is
/// `!Send`, and the handler futures the caller runs us in need the
/// resulting future to be `Send`. The critical section is the "check
/// then start" race window; a `std::sync::Mutex` would deadlock or
/// trigger `clippy::await_holding_lock`.
static PLUGIN_SERVER: OnceLock<tokio::sync::Mutex<Option<PluginServerHandle>>> = OnceLock::new();

/// Subset of `PluginServer` we hand to callers of the singleton — the
/// bind address and a one-shot shutdown channel for tests. Drop
/// semantics intentionally mirror the per-conversation
/// `LocalFsMcpServer`.
struct PluginServerHandle {
    bind_addr: SocketAddr,
    /// Held so the `watch::Sender` lives as long as the singleton
    /// (and the underlying listener task can observe a shutdown
    /// signal if the OS process ever sends one). Not currently read
    /// — kept for symmetry with `LocalFsMcpServer`.
    #[allow(dead_code)]
    shutdown_tx: watch::Sender<bool>,
}

// ── `PluginServer` (public type) ───────────────────────────────

/// Running plugin webserver handle. Dropping it (or calling
/// `shutdown`) requests graceful shutdown of the listener.
pub struct PluginServer {
    bind_addr: SocketAddr,
    shutdown_tx: watch::Sender<bool>,
    join: Option<JoinHandle<()>>,
}

impl PluginServer {
    /// Bind a fresh server on `bind`. The reachability plan's default
    /// bind is `0.0.0.0:0` (all interfaces) so any routable candidate IP
    /// resolves to the same listener; the loopback `127.0.0.1:0` bind
    /// is only used when the `AIONUI_LOCAL_FS_MCP_PUBLIC_URL` env
    /// override is in effect (the public URL is then a tunnel proxying
    /// in, so the listener only needs to be reachable locally).
    /// **The bearer token is the sole authentication gate** — there is
    /// no network-level access control, so the token must be
    /// unguessable.
    ///
    /// `validator` resolves bearer tokens to `remote_agent_id`s. The
    /// caller (composition root) builds one backed by
    /// `IRemoteAgentRepository::find_by_plugin_token`.
    pub async fn start(bind: SocketAddr, validator: Arc<dyn PluginTokenValidator>) -> std::io::Result<Self> {
        Self::start_with_registry(bind, validator, super::registry::global()).await
    }

    /// Same as [`start`] but with an explicit registry. Tests use this
    /// to keep their state isolated from the process-wide singleton.
    pub async fn start_with_registry(
        bind: SocketAddr,
        validator: Arc<dyn PluginTokenValidator>,
        registry: Arc<PluginRegistry>,
    ) -> std::io::Result<Self> {
        let state = PluginAppState { validator, registry };
        let app = Router::new()
            .route("/plugin/hello", post(handle_hello))
            .route("/plugin/events", get(handle_events))
            .route("/plugin/result", post(handle_result))
            .route("/tools/run_shell_streaming", post(handle_run_shell_streaming))
            .with_state(state);

        let listener = TcpListener::bind(bind).await?;
        let bind_addr = listener.local_addr()?;

        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let join = tokio::spawn(async move {
            let server = axum::serve(listener, app);
            let result = server
                .with_graceful_shutdown(async move {
                    while shutdown_rx.changed().await.is_ok() {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                })
                .await;
            if let Err(e) = result {
                warn!(error = %e, "plugin webserver exited with error");
            }
        });

        info!(bind = %bind_addr, "plugin webserver started");

        Ok(Self {
            bind_addr,
            shutdown_tx,
            join: Some(join),
        })
    }

    pub fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }

    pub async fn shutdown(mut self) {
        let _ = self.shutdown_tx.send(true);
        if let Some(handle) = self.join.take() {
            let _ = handle.await;
        }
        debug!(addr = %self.bind_addr, "plugin webserver shut down");
    }
}

impl Drop for PluginServer {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
    }
}

/// Get-or-create the process-wide plugin webserver. First call binds;
/// later calls return the already-bound address. Pass the same
/// `validator` on every call — the first call's validator is the one
/// that's actually used.
///
/// Returns the bound address. The `Ok(addr)` is stable for the
/// lifetime of the process.
pub async fn ensure_plugin_server(
    bind: SocketAddr,
    validator: Arc<dyn PluginTokenValidator>,
) -> std::io::Result<SocketAddr> {
    let cell = PLUGIN_SERVER.get_or_init(|| tokio::sync::Mutex::new(None));
    // Serialise the start decision across racing callers. The first
    // caller proceeds to `start()`; everyone else blocks on the
    // `lock().await` and reads the resulting addr. `tokio::sync::Mutex`
    // is required here — `std::sync::MutexGuard` is `!Send` and the
    // caller's handler future needs to be `Send` to be polled across
    // threads by axum.
    let mut guard = cell.lock().await;
    if let Some(handle) = guard.as_ref() {
        return Ok(handle.bind_addr);
    }

    let server = PluginServer::start(bind, validator).await?;
    let addr = server.bind_addr();
    // Take the shutdown channel out of the server so we can flip
    // shutdown from tests / SIGINT handling, but keep the server
    // itself alive by `forget`-ing it. The `JoinHandle` stays
    // inside the task; we never need to await it (the OS process
    // exit is the natural cancellation point).
    let shutdown_tx = server.shutdown_tx.clone();
    std::mem::forget(server);
    *guard = Some(PluginServerHandle {
        bind_addr: addr,
        shutdown_tx,
    });
    Ok(addr)
}

// ── Auth helper ────────────────────────────────────────────────

/// Constant-time byte comparison. Kept as a test-only helper — the
/// `FixedValidator` in `plugin/server/tests.rs` and the
/// `constant_time_eq_works` test both rely on it, and future
/// in-memory validator implementations may want it too. **Not**
/// used by the request middleware: the request path resolves the
/// token via `PluginTokenValidator::resolve`, which is a SQL
/// equality lookup (see the security-posture comment on
/// [`resolve_agent_or_unauth`]).
#[cfg(test)]
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[derive(Serialize)]
struct AuthFailureBody {
    success: bool,
    error: &'static str,
    code: &'static str,
}

const AUTH_FAILURE_BODY: AuthFailureBody = AuthFailureBody {
    success: false,
    error: "missing or invalid bearer token",
    code: "UNAUTHORIZED",
};

/// Look up the agent id for an incoming request, returning a 401
/// response on any failure. The caller must `return` the response
/// (use `?` on a `Result<Response, ...>` is not convenient here).
///
/// **Security posture.** Authentication is the validator's
/// `IRemoteAgentRepository::find_by_plugin_token` call, which performs
/// a SQL equality lookup against the `plugin_token` column. Tokens are
/// locally generated random UUIDv4 strings (~122 bits of entropy),
/// never user-supplied; the column is plaintext precisely so the
/// equality lookup is possible. We deliberately do **not** claim
/// constant-time comparison for the token check itself — extracting
/// a ~122-bit secret via timing side-channel is infeasible at this
/// entropy, and an additional in-process constant-time compare after
/// the SQL lookup would not add meaningful protection (the SQL layer
/// is the timing oracle regardless). The earlier draft's
/// `auth_ok(headers, token)` self-compare was a tautology (the
/// `expected` argument was the very token we just extracted from the
/// same header) and has been removed.
async fn resolve_agent_or_unauth(state: &PluginAppState, headers: &HeaderMap) -> Result<String, Response> {
    let header_value = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok());
    let Some(value) = header_value else {
        return Err(unauth_response());
    };
    let Some(token) = value.strip_prefix("Bearer ") else {
        return Err(unauth_response());
    };
    if token.is_empty() {
        return Err(unauth_response());
    }
    // Reject obviously-malformed tokens before paying for a DB round
    // trip. This is a cheap pre-filter on length only — it does not
    // leak whether a token of a particular length is registered.
    if token.len() < 8 {
        return Err(unauth_response());
    }
    match state.validator.resolve(token).await {
        Some(agent_id) => Ok(agent_id),
        _ => Err(unauth_response()),
    }
}

fn unauth_response() -> Response {
    (StatusCode::UNAUTHORIZED, Json(AUTH_FAILURE_BODY)).into_response()
}

// ── /plugin/hello ──────────────────────────────────────────────

async fn handle_hello(
    State(state): State<PluginAppState>,
    headers: HeaderMap,
    Json(req): Json<PluginHelloRequest>,
) -> Result<Json<PluginHelloResponse>, Response> {
    let agent_id = resolve_agent_or_unauth(&state, &headers).await?;

    let count = state.registry.record_hello(
        &agent_id,
        req.plugin_version.clone(),
        req.opencode_version.clone(),
        req.hooks.clone(),
    );

    info!(
        agent_id = %agent_id,
        plugin_version = %req.plugin_version,
        hook_count = req.hooks.len(),
        hello_count = count,
        "plugin hello"
    );

    // Protocol-version negotiation. Today there's only one supported
    // version on each side, so a mismatch is logged but still
    // accepted — the plugin's published schema is additive. A future
    // bump will switch this to a hard reject.
    if req.protocol_version != PROTOCOL_VERSION {
        warn!(
            agent_id = %agent_id,
            client = req.protocol_version,
            server = PROTOCOL_VERSION,
            "plugin protocol version mismatch — continuing with best-effort",
        );
    }

    Ok(Json(PluginHelloResponse {
        ok: true,
        protocol_version: PROTOCOL_VERSION,
    }))
}

// ── /plugin/events ────────────────────────────────────────────

/// The actual SSE stream the `/plugin/events` handler returns. Owns
/// the `EventsStreamGuard` so the registry's `events_stream_open` flag
/// stays true for exactly the lifetime of the SSE response.
struct PluginEventsStream {
    /// `None` once we've yielded the initial ping; from then on we
    /// pull from the broadcast receiver.
    inner: PluginEventsState,
}

enum PluginEventsState {
    Guard(
        EventsStreamGuard,
        std::pin::Pin<Box<dyn Stream<Item = Result<Event, Infallible>> + Send>>,
    ),
    Done,
}

impl PluginEventsStream {
    fn new(agent_id: String, registry: Arc<PluginRegistry>) -> Self {
        let guard = EventsStreamGuard::new(registry.clone(), agent_id.clone());
        let rx = registry.subscribe(&agent_id);
        // Compose: a single `ping` event first, then the broadcast
        // receiver. The guard is held in this struct so its Drop
        // fires only when the SSE response is dropped. Both halves
        // produce `Result<Event, Infallible>` so the `chain` types
        // line up.
        let ping = stream::once(async move {
            Ok::<_, Infallible>(
                Event::default()
                    .event("ping")
                    .data(json!({"at": aionui_common::now_ms()}).to_string()),
            )
        });
        let rest = stream::unfold(rx, |mut rx| async move {
            match rx.recv().await {
                Ok(ev) => {
                    let sse_event = Event::default().event(ev.event).data(ev.data.to_string());
                    Some((Ok::<_, Infallible>(sse_event), rx))
                }
                // The receiver fell behind the broadcast ring. We do
                // NOT want to end the stream here — that would force
                // the plugin to re-handshake just because it was slow
                // for a moment. Emit a `lagged` sentinel event with an
                // empty JSON object as the data payload; the plugin
                // can use it to resync (e.g. fetch a snapshot) if it
                // cares, and otherwise ignore it.
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    debug!(skipped = skipped, "plugin events stream lagged; emitting sentinel");
                    let lagged = Event::default().event("lagged").data(json!({}).to_string());
                    Some((Ok::<_, Infallible>(lagged), rx))
                }
                // The channel is closed (registry dropped the
                // per-agent broadcast sender). End the stream so
                // axum tears down the SSE response.
                Err(tokio::sync::broadcast::error::RecvError::Closed) => None,
            }
        });
        let combined = Box::pin(ping.chain(rest));
        Self {
            inner: PluginEventsState::Guard(guard, combined),
        }
    }
}

impl Stream for PluginEventsStream {
    type Item = Result<Event, Infallible>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // SAFETY: we never move out of `inner`.
        let this = self.get_mut();
        match &mut this.inner {
            PluginEventsState::Guard(_guard, stream) => match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => Poll::Ready(Some(item)),
                Poll::Ready(None) => {
                    this.inner = PluginEventsState::Done;
                    Poll::Ready(None)
                }
                Poll::Pending => Poll::Pending,
            },
            PluginEventsState::Done => Poll::Ready(None),
        }
    }
}

/// One filter step on the broadcast stream: drop the receive errors
/// from a closed channel (the plugin might disconnect mid-stream)
/// and let the next event through.
fn plugin_event_stream(
    agent_id: String,
    registry: Arc<PluginRegistry>,
) -> Pin<Box<dyn Stream<Item = Result<Event, Infallible>> + Send>> {
    Box::pin(PluginEventsStream::new(agent_id, registry))
}

struct EventsStreamGuard {
    registry: Arc<PluginRegistry>,
    agent_id: String,
    armed: bool,
}

impl EventsStreamGuard {
    fn new(registry: Arc<PluginRegistry>, agent_id: String) -> Self {
        registry.set_events_stream_open(&agent_id, true);
        Self {
            registry,
            agent_id,
            armed: true,
        }
    }
}

impl Drop for EventsStreamGuard {
    fn drop(&mut self) {
        if self.armed {
            self.registry.set_events_stream_open(&self.agent_id, false);
        }
    }
}

async fn handle_events(State(state): State<PluginAppState>, headers: HeaderMap) -> Result<Response, Response> {
    let agent_id = resolve_agent_or_unauth(&state, &headers).await?;
    debug!(agent_id = %agent_id, "plugin events stream attached");
    let stream = plugin_event_stream(agent_id, state.registry.clone());
    Ok(Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response())
}

// ── /plugin/result ────────────────────────────────────────────

async fn handle_result(
    State(state): State<PluginAppState>,
    headers: HeaderMap,
    Json(req): Json<PluginResultRequest>,
) -> Result<Json<PluginResultResponse>, Response> {
    let agent_id = resolve_agent_or_unauth(&state, &headers).await?;
    let now = aionui_common::now_ms();
    let (kind_label, tool, session_id, call_id, summary, status) = match &req {
        PluginResultRequest::ToolBefore {
            tool,
            session_id,
            call_id,
            ..
        } => (
            "tool.before",
            Some(tool.clone()),
            Some(session_id.clone()),
            Some(call_id.clone()),
            format!("tool.before {tool}"),
            None,
        ),
        PluginResultRequest::ToolAfter {
            tool,
            session_id,
            call_id,
            output_len,
            ..
        } => {
            let len_note = output_len.map(|n| format!(" {n}B")).unwrap_or_default();
            (
                "tool.after",
                Some(tool.clone()),
                Some(session_id.clone()),
                Some(call_id.clone()),
                format!("tool.after {tool}{len_note}"),
                None,
            )
        }
        PluginResultRequest::Event { event } => {
            let event_type = event.get("type").and_then(Value::as_str).unwrap_or("unknown");
            (
                "event",
                None,
                event
                    .get("properties")
                    .and_then(|p| p.get("sessionID"))
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                None,
                format!("event {event_type}"),
                None,
            )
        }
        PluginResultRequest::PermissionAsk { permission } => {
            let tool = permission
                .get("tool")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .unwrap_or_else(|| "unknown".to_string());
            (
                "permission.ask",
                Some(tool),
                permission.get("sessionID").and_then(Value::as_str).map(str::to_owned),
                permission.get("callID").and_then(Value::as_str).map(str::to_owned),
                "permission.ask (passthrough)".to_string(),
                Some("ask".to_string()),
            )
        }
    };

    let record = PluginAuditRecord {
        kind: kind_label.to_string(),
        tool: tool.clone(),
        session_id: session_id.clone(),
        call_id: call_id.clone(),
        at_ms: now.max(0) as u64,
        summary: truncate_summary(&summary),
    };
    state.registry.record_audit(&agent_id, record);

    debug!(
        agent_id = %agent_id,
        kind = kind_label,
        tool = tool.as_deref().unwrap_or("-"),
        session_id = session_id.as_deref().unwrap_or("-"),
        call_id = call_id.as_deref().unwrap_or("-"),
        "plugin result recorded",
    );

    Ok(Json(PluginResultResponse { ok: true, status }))
}

fn truncate_summary(s: &str) -> String {
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

// ── /tools/run_shell_streaming ────────────────────────────────

async fn handle_run_shell_streaming(
    State(state): State<PluginAppState>,
    headers: HeaderMap,
    Json(req): Json<super::protocol::RunShellStreamingRequest>,
) -> Result<Response, Response> {
    let agent_id = resolve_agent_or_unauth(&state, &headers).await?;
    let approver = state.registry.shell_approver(&agent_id);
    let Some(approver) = approver else {
        // No approver registered for this workspace. Emit a single
        // SSE `error` event then a `done` so the plugin's stream
        // consumer always sees a terminal event.
        return Ok(synthetic_shell_error("no approver registered for this workspace"));
    };

    // Use the agent's session_id as the "default" the streaming tool
    // falls back to if the request omits one. The plugin should
    // always send a session_id, but matching the synchronous shell
    // tool's behaviour is the safe fallback.
    let session_id = req.session_id.clone();
    let default_cwd = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));

    // Audit the start only. In the MVP the shell completion is NOT
    // audited — the streaming path lives inside an SSE stream task
    // that does not have a clean hook for the handler to observe the
    // final `done` event (the stream is consumed by axum after this
    // function returns), and we chose request-only auditing over
    // plumbing an audit closure through `run_shell_streaming`.
    // Completion metadata (exit code, is_error, truncated) is still
    // surfaced to the plugin on the wire — it's just not in the
    // per-agent audit ring buffer.
    let now = aionui_common::now_ms();
    let cmd_preview = truncate_command_preview(&req.command);
    state.registry.record_audit(
        &agent_id,
        PluginAuditRecord {
            kind: "shell.request".to_string(),
            tool: Some("run_shell_streaming".to_string()),
            session_id: Some(session_id.clone()),
            call_id: req.call_id.clone(),
            at_ms: now.max(0) as u64,
            summary: cmd_preview.clone(),
        },
    );
    debug!(
        agent_id = %agent_id,
        session_id = %session_id,
        call_id = req.call_id.as_deref().unwrap_or("-"),
        command_bytes = req.command.len(),
        "plugin shell streaming request received",
    );

    let stream = run_shell_streaming(req, approver, &default_cwd, &session_id);
    Ok(Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response())
}

fn truncate_command_preview(cmd: &str) -> String {
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

fn synthetic_shell_error(message: &str) -> Response {
    use futures_util::stream;
    let body: Vec<Result<Event, Infallible>> = vec![
        Ok(Event::default()
            .event("error")
            .data(json!({"message": message}).to_string())),
        Ok(Event::default()
            .event("done")
            .data(json!({"exitCode": null, "isError": true, "truncated": false}).to_string())),
    ];
    Sse::new(stream::iter(body))
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}

#[cfg(test)]
#[path = "server/tests.rs"]
mod tests;
