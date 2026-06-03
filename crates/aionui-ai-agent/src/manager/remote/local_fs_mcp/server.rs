//! HTTP MCP endpoint backed by axum. Speaks JSON-RPC over POST.
//!
//! Authentication: every non-`initialize` request must carry the bearer
//! token configured at server start. `initialize` and `notifications/*`
//! are allowed unauthenticated so a client can probe protocol-level
//! capability before authenticating; tool calls always require the token.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::post;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use super::protocol::{
    INTERNAL_ERROR, INVALID_PARAMS, JsonRpcRequest, JsonRpcResponse, METHOD_NOT_FOUND, PROTOCOL_VERSION, SERVER_NAME,
    SERVER_VERSION,
};
use super::shell::{ElicitationHandler, McpRequestContext, ShellApprover};
use super::tools::{SnapshotHook, all_tool_descriptors, dispatch};

/// Header names OpenCode injects on tool-call forwarding so the MCP server can
/// attribute the call back to the originating session. Names match the
/// canonical case the OpenCode runtime uses; HTTP header lookup is
/// case-insensitive so both flavours work.
const HEADER_SESSION_ID: &str = "x-opencode-session-id";
const HEADER_PARENT_SESSION_ID: &str = "x-opencode-parent-session-id";

/// Read the session-attribution headers OpenCode injects on tool-call
/// forwarding. Both header reads are best-effort: a missing or invalid header
/// yields `None` (the host falls back to a conversation-level prompt).
fn extract_request_context(headers: &HeaderMap) -> McpRequestContext {
    let session_id = headers
        .get(HEADER_SESSION_ID)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .filter(|s| !s.is_empty());
    let parent_session_id = headers
        .get(HEADER_PARENT_SESSION_ID)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .filter(|s| !s.is_empty());
    McpRequestContext {
        session_id,
        parent_session_id,
    }
}

/// Cloneable handle over the server's "has a remote client reached us yet"
/// signal. Handed to the registration/guardian code so it can verify
/// reachability and re-arm the probe without owning the server.
#[derive(Clone)]
pub struct ContactProbe {
    /// Flipped true on the first inbound request of any kind — proof that
    /// a remote client (OpenCode) reached this server at the advertised
    /// address.
    contacted: Arc<AtomicBool>,
    /// Woken whenever `contacted` transitions to true, so a waiter can be
    /// notified without polling.
    notify: Arc<Notify>,
}

impl ContactProbe {
    fn new() -> Self {
        Self {
            contacted: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Whether any remote client has reached the server since the last
    /// `reset`.
    pub fn was_contacted(&self) -> bool {
        self.contacted.load(Ordering::SeqCst)
    }

    /// Clear the flag before a fresh reachability probe, so a hit recorded
    /// for a previous candidate isn't mistaken for the current one.
    pub fn reset(&self) {
        self.contacted.store(false, Ordering::SeqCst);
    }

    /// Wait up to `timeout` for the first inbound request. Returns true if
    /// the server has been (or is) contacted within the window. Only
    /// OpenCode dialing the advertised URL can trip it.
    pub async fn wait_for_first_contact(&self, timeout: Duration) -> bool {
        // Arm the notification *before* the load so a hit racing in between
        // can't be missed.
        let notified = self.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.contacted.load(Ordering::SeqCst) {
            return true;
        }
        tokio::time::timeout(timeout, notified).await.is_ok()
    }

    /// Record an inbound contact. Returns true if this was the first one
    /// (i.e. a transition), so the caller can log it once.
    fn record(&self) -> bool {
        if !self.contacted.swap(true, Ordering::SeqCst) {
            self.notify.notify_waiters();
            true
        } else {
            false
        }
    }
}

#[derive(Clone)]
struct McpAppState {
    project_root: PathBuf,
    auth_token: Arc<str>,
    probe: ContactProbe,
    /// Gates the `run_shell` tool through the host agent's confirmation UI.
    /// `None` leaves shell execution disabled (it fails closed); the fs
    /// tools are unaffected either way.
    approver: Option<Arc<dyn ShellApprover>>,
    /// Raises a free-form, schema-driven prompt back to the host agent's UI
    /// for MCP `elicitation/create`-style flows. `None` leaves elicitation
    /// disabled (tools that need it fail closed); shell + filesystem tools
    /// are unaffected.
    elicitation: Option<Arc<dyn ElicitationHandler>>,
    /// Per-conversation handle to the Git-backed snapshot service + the
    /// `opencode_tool_snapshots` DB repo. When `Some`, mutating fs tool
    /// calls commit a per-tool-call snapshot and persist the ledger row.
    /// `None` for non-OpenCode backends, tests, and any composition root
    /// that has not yet wired the deps.
    snapshot_hook: Option<SnapshotHook>,
}

/// Running MCP server handle. Drops trigger graceful shutdown.
pub struct LocalFsMcpServer {
    bind_addr: SocketAddr,
    auth_token: String,
    shutdown_tx: watch::Sender<bool>,
    join: Option<JoinHandle<()>>,
    probe: ContactProbe,
}

impl LocalFsMcpServer {
    /// Bind a server scoped to `project_root` (must be a canonicalized
    /// absolute path on the local filesystem). `bind` is the interface to
    /// listen on — typically `127.0.0.1:0` for loopback or `0.0.0.0:0` for
    /// all interfaces; the OS picks an ephemeral port.
    ///
    /// `approver`, when `Some`, gates the `run_shell` tool through the host
    /// agent's confirmation flow; `None` disables shell execution (the
    /// filesystem tools still work).
    ///
    /// `elicitation`, when `Some`, wires the host agent's elicitation flow so
    /// tools can prompt the user mid-call. `None` disables elicitation
    /// (tools requiring it fail closed).
    ///
    /// `snapshot_hook`, when `Some`, arms the per-tool-call snapshot hook on
    /// the mutating fs tools (`write_file`, `delete_file`, `rename`). When
    /// `None`, those tools still run but no snapshot is committed and no
    /// ledger row is written (preserves non-OpenCode backends, tests, and
    /// composition roots that have not yet wired the deps).
    pub async fn start(
        project_root: PathBuf,
        bind: SocketAddr,
        auth_token: String,
        approver: Option<Arc<dyn ShellApprover>>,
        elicitation: Option<Arc<dyn ElicitationHandler>>,
        snapshot_hook: Option<SnapshotHook>,
    ) -> std::io::Result<Self> {
        if !project_root.is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("project root is not a directory: {}", project_root.display()),
            ));
        }
        let canonical = project_root.canonicalize()?;
        let probe = ContactProbe::new();
        let state = McpAppState {
            project_root: canonical.clone(),
            auth_token: Arc::from(auth_token.as_str()),
            probe: probe.clone(),
            approver,
            elicitation,
            snapshot_hook,
        };
        let app = Router::new().route("/", post(handle_rpc)).with_state(state);

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
                error!(error = %e, "local fs MCP server exited with error");
            }
        });

        info!(
            project_root = %canonical.display(),
            bind = %bind_addr,
            "local fs MCP server started"
        );

        Ok(Self {
            bind_addr,
            auth_token,
            shutdown_tx,
            join: Some(join),
            probe,
        })
    }

    pub fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }

    /// Cloneable handle to this server's reachability signal, for the
    /// registration/guardian code.
    pub fn contact_probe(&self) -> ContactProbe {
        self.probe.clone()
    }

    /// Whether any remote client has reached this server yet.
    pub fn was_contacted(&self) -> bool {
        self.probe.was_contacted()
    }

    /// Clear the contact flag before a fresh reachability probe.
    pub fn reset_contact(&self) {
        self.probe.reset();
    }

    /// Wait up to `timeout` for the first inbound request. See
    /// [`ContactProbe::wait_for_first_contact`].
    pub async fn wait_for_first_contact(&self, timeout: Duration) -> bool {
        self.probe.wait_for_first_contact(timeout).await
    }

    /// Local URL the server listens on. Reachability from a remote
    /// OpenCode is the responsibility of the caller (e.g. a tunnel).
    pub fn local_url(&self) -> String {
        format!("http://{}/", self.bind_addr)
    }

    pub fn auth_token(&self) -> &str {
        &self.auth_token
    }

    pub async fn shutdown(mut self) {
        let _ = self.shutdown_tx.send(true);
        if let Some(handle) = self.join.take() {
            let _ = handle.await;
        }
        debug!(addr = %self.bind_addr, "local fs MCP server shut down");
    }
}

impl Drop for LocalFsMcpServer {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
    }
}

async fn handle_rpc(
    State(state): State<McpAppState>,
    headers: HeaderMap,
    Json(req): Json<JsonRpcRequest>,
) -> impl IntoResponse {
    let id = req.id.clone();
    let method = req.method.as_str();

    // First inbound request of any kind proves the advertised address is
    // reachable from the remote. Record it and wake any reachability probe.
    if state.probe.record() {
        debug!(method, "local fs MCP received first inbound contact");
    }

    let needs_auth = !matches!(method, "initialize" | "notifications/initialized" | "ping");
    if needs_auth && !auth_ok(&headers, &state.auth_token) {
        return (
            StatusCode::UNAUTHORIZED,
            json_response(JsonRpcResponse::error(
                id,
                INTERNAL_ERROR,
                "missing or invalid bearer token",
            )),
        );
    }

    let response = match method {
        "initialize" => JsonRpcResponse::success(
            id,
            json!({
                "capabilities": { "tools": {} },
                "protocolVersion": PROTOCOL_VERSION,
                "serverInfo": { "name": SERVER_NAME, "version": SERVER_VERSION }
            }),
        ),
        "notifications/initialized" | "notifications/cancelled" => {
            return (
                StatusCode::NO_CONTENT,
                json_response(JsonRpcResponse::success(id, Value::Null)),
            );
        }
        "tools/list" => {
            let tools: Vec<Value> = all_tool_descriptors()
                .iter()
                .map(|d| {
                    json!({
                        "name": d.name,
                        "description": d.description,
                        "inputSchema": d.input_schema,
                    })
                })
                .collect();
            JsonRpcResponse::success(id, json!({ "tools": tools }))
        }
        "tools/call" => {
            let params = req.params.clone().unwrap_or_else(|| json!({}));
            let tool_name = params.get("name").and_then(Value::as_str).unwrap_or("");
            if tool_name.is_empty() {
                JsonRpcResponse::error(id, INVALID_PARAMS, "missing tool name")
            } else {
                let arguments = params.get("arguments").cloned().unwrap_or_else(|| json!({}));
                // Capture per-request session attribution from OpenCode's
                // forwarded headers. Threaded through dispatch so the approver
                // / elicitation handler can stamp the resulting UI prompt
                // with the originating (parent or child) session id.
                let request_context = extract_request_context(&headers);
                let has_session_attribution = request_context.session_id.is_some();
                // The JSON-RPC `id` is unique per `tools/call` request and
                // stable across retries (OpenCode client contract). Use it as
                // the snapshot ledger's `tool_call_id` — the API revert route
                // will receive the same string back from the UI.
                let tool_call_id = req.id.as_ref().map(jsonrpc_id_to_string).filter(|s| !s.is_empty());
                let (text, is_error) = dispatch(
                    &state.project_root,
                    tool_name,
                    &arguments,
                    state.approver.as_ref(),
                    state.elicitation.as_ref(),
                    &request_context,
                    tool_call_id.as_deref(),
                    state.snapshot_hook.as_ref(),
                )
                .await;
                if is_error {
                    warn!(
                        tool = tool_name,
                        attributed = has_session_attribution,
                        error = %text,
                        "fs MCP tool returned error"
                    );
                } else {
                    debug!(
                        tool = tool_name,
                        attributed = has_session_attribution,
                        "fs MCP tool dispatch"
                    );
                }
                JsonRpcResponse::success(
                    id,
                    json!({
                        "content": [ { "type": "text", "text": text } ],
                        "isError": is_error,
                    }),
                )
            }
        }
        "ping" => JsonRpcResponse::success(id, json!({})),
        other => {
            warn!(method = other, "unknown MCP method");
            JsonRpcResponse::error(id, METHOD_NOT_FOUND, format!("method not found: {other}"))
        }
    };

    (StatusCode::OK, json_response(response))
}

fn auth_ok(headers: &HeaderMap, expected: &str) -> bool {
    let Some(value) = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()) else {
        return false;
    };
    let Some(token) = value.strip_prefix("Bearer ") else {
        return false;
    };
    constant_time_eq(token.as_bytes(), expected.as_bytes())
}

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

fn json_response(resp: JsonRpcResponse) -> Json<Value> {
    Json(serde_json::to_value(resp).unwrap_or(Value::Null))
}

/// Stringify a JSON-RPC `id` for use as a stable per-call identifier (the
/// snapshot ledger's `tool_call_id` PK). JSON-RPC allows string, number, or
/// null; we collapse to a string and pass through null/empty as `None` (the
/// caller filters with `filter(|s| !s.is_empty())`). The string form keeps
/// numeric ids loss-less and matches the wire contract the OpenCode client
/// uses for its own `callID` keys.
fn jsonrpc_id_to_string(id: &Value) -> String {
    match id {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Null => String::new(),
        // Per spec, ids are never objects/arrays — fall back to a stable
        // JSON serialization so we still get a deterministic key.
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::Client;
    use std::net::{IpAddr, Ipv4Addr};
    use tempfile::TempDir;

    async fn boot() -> (TempDir, LocalFsMcpServer, String) {
        let dir = tempfile::tempdir().unwrap();
        let token = "test-token-xyz".to_string();
        let server = LocalFsMcpServer::start(
            dir.path().to_path_buf(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            token.clone(),
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let url = server.local_url();
        (dir, server, url)
    }

    #[tokio::test]
    async fn initialize_works_without_auth() {
        let (_dir, _server, url) = boot().await;
        let client = Client::new();
        let resp = client
            .post(&url)
            .json(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize"}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let v: Value = resp.json().await.unwrap();
        assert_eq!(v["result"]["serverInfo"]["name"], SERVER_NAME);
    }

    #[tokio::test]
    async fn tools_call_requires_auth() {
        let (_dir, _server, url) = boot().await;
        let client = Client::new();
        let resp = client
            .post(&url)
            .json(&json!({
                "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                "params": {"name": "list_dir", "arguments": {}}
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 401);
    }

    #[tokio::test]
    async fn tools_call_with_auth_succeeds() {
        let (dir, _server, url) = boot().await;
        std::fs::write(dir.path().join("a.txt"), "hi").unwrap();
        let client = Client::new();
        let resp = client
            .post(&url)
            .header("Authorization", "Bearer test-token-xyz")
            .json(&json!({
                "jsonrpc": "2.0", "id": 3, "method": "tools/call",
                "params": {"name": "list_dir", "arguments": {"path": ""}}
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let v: Value = resp.json().await.unwrap();
        let text = v["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("a.txt"));
    }

    #[tokio::test]
    async fn first_contact_is_recorded_on_any_request() {
        let (_dir, server, url) = boot().await;
        // Nothing has hit the server yet.
        assert!(!server.was_contacted());
        assert!(!server.wait_for_first_contact(Duration::from_millis(50)).await);

        let client = Client::new();
        // An unauthenticated `initialize` is enough — it proves reachability.
        client
            .post(&url)
            .json(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize"}))
            .send()
            .await
            .unwrap();

        assert!(server.was_contacted());
        assert!(server.wait_for_first_contact(Duration::from_secs(1)).await);

        // reset_contact clears it for the next probe.
        server.reset_contact();
        assert!(!server.was_contacted());
    }

    #[tokio::test]
    async fn read_with_wrong_token_is_rejected() {
        let (dir, _server, url) = boot().await;
        std::fs::write(dir.path().join("a.txt"), "hi").unwrap();
        let client = Client::new();
        let resp = client
            .post(&url)
            .header("Authorization", "Bearer wrong")
            .json(&json!({
                "jsonrpc": "2.0", "id": 4, "method": "tools/call",
                "params": {"name": "read_file", "arguments": {"path": "a.txt"}}
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 401);
    }
}
