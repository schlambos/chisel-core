//! HTTP MCP endpoint backed by axum. Speaks JSON-RPC over POST.
//!
//! Authentication: every non-`initialize` request must carry the bearer
//! token configured at server start. `initialize` and `notifications/*`
//! are allowed unauthenticated so a client can probe protocol-level
//! capability before authenticating; tool calls always require the token.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::post;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use super::protocol::{
    INTERNAL_ERROR, INVALID_PARAMS, JsonRpcRequest, JsonRpcResponse, METHOD_NOT_FOUND, PROTOCOL_VERSION, SERVER_NAME,
    SERVER_VERSION,
};
use super::tools::{all_tool_descriptors, dispatch};

#[derive(Clone)]
struct McpAppState {
    project_root: PathBuf,
    auth_token: Arc<str>,
}

/// Running MCP server handle. Drops trigger graceful shutdown.
pub struct LocalFsMcpServer {
    bind_addr: SocketAddr,
    auth_token: String,
    shutdown_tx: watch::Sender<bool>,
    join: Option<JoinHandle<()>>,
}

impl LocalFsMcpServer {
    /// Bind a server scoped to `project_root` (must be a canonicalized
    /// absolute path on the local filesystem). `bind` is the interface to
    /// listen on — typically `127.0.0.1:0` for loopback or `0.0.0.0:0` for
    /// all interfaces; the OS picks an ephemeral port.
    pub async fn start(project_root: PathBuf, bind: SocketAddr, auth_token: String) -> std::io::Result<Self> {
        if !project_root.is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("project root is not a directory: {}", project_root.display()),
            ));
        }
        let canonical = project_root.canonicalize()?;
        let state = McpAppState {
            project_root: canonical.clone(),
            auth_token: Arc::from(auth_token.as_str()),
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
        })
    }

    pub fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
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
                let (text, is_error) = dispatch(&state.project_root, tool_name, &arguments).await;
                if is_error {
                    warn!(tool = tool_name, error = %text, "fs MCP tool returned error");
                } else {
                    debug!(tool = tool_name, "fs MCP tool dispatch");
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
