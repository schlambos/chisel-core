use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use aionui_common::{
    AgentKillReason, AgentType, AppError, Confirmation, ConversationStatus, ErrorChain, RemoteAgentStatus, TimestampMs,
};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures_util::{SinkExt, StreamExt};
use reqwest::header::AUTHORIZATION;
use serde_json::{Value, json};
use tokio::sync::{Mutex, RwLock, broadcast};
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use crate::agent_runtime::AgentRuntime;
use crate::manager::remote::local_fs_mcp::LocalFsMcpServer;
use crate::manager::remote::opencode_mcp;
use crate::protocol::events::{
    AcpPermissionEventData, AgentStreamEvent, FinishEventData, StartEventData, TextEventData, ThinkingEventData,
};
use crate::types::SendMessageData;
use aionui_common::ConfirmationOption;

/// Internal mutable state for the Remote agent.
struct RemoteState {
    session_key: Option<String>,
    confirmations: Vec<Confirmation>,
    has_messages: bool,
    approval_memory: HashMap<String, bool>,
    connection_status: RemoteAgentStatus,
    opencode_session_id: Option<String>,
    /// Track which part IDs are reasoning (thinking) parts.
    reasoning_parts: HashSet<String>,
    /// The desired model for the next prompt (opencode format: `{"providerID":"...","id":"...","variant":"..."}`).
    desired_model: Option<Value>,
}

/// Configuration for connecting to a remote agent.
#[derive(Debug, Clone)]
pub struct RemoteAgentConfig {
    pub remote_agent_id: String,
    pub protocol: String,
    pub url: String,
    pub auth_type: String,
    pub auth_token: Option<String>,
    pub allow_insecure: bool,
}

fn is_opencode_protocol(protocol: &str) -> bool {
    protocol == "opencode"
}

fn normalize_base_url(url: &str) -> String {
    url.trim().trim_end_matches('/').to_string()
}

fn build_auth_header(auth_type: &str, auth_token: Option<&str>) -> Option<String> {
    let token = auth_token.filter(|t| !t.is_empty())?;
    let value = match auth_type {
        "bearer" | "Bearer" => format!("Bearer {token}"),
        "password" | "Password" => format!("Basic {}", BASE64.encode(format!("opencode:{token}"))),
        _ => return None,
    };
    Some(value)
}

/// Manages a Remote Agent via WebSocket or HTTP/SSE transport.
///
/// OpenClaw / ACP protocols use WebSocket. OpenCode uses HTTP POST + SSE.
pub struct RemoteAgentManager {
    runtime: AgentRuntime,
    remote_config: RemoteAgentConfig,
    state: RwLock<RemoteState>,
    /// WebSocket sink for sending messages, wrapped in Mutex for concurrency.
    ws_sink: Mutex<
        Option<
            futures_util::stream::SplitSink<
                tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
                Message,
            >,
        >,
    >,
    /// Handle to the WebSocket reader task.
    _reader_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// HTTP client for OpenCode transport.
    http_client: reqwest::Client,
    /// Client-side MCP server vending fs tools scoped to
    /// `runtime.workspace()`, bound to the LAN-routable interface so
    /// the remote OpenCode can dial in. Some after a successful session
    /// create + mcp.add. None before session create or after teardown.
    /// Per-session — never shared across conversations.
    local_fs_mcp: Mutex<Option<LocalFsMcpServer>>,
}

impl RemoteAgentManager {
    /// Create a new Remote agent.
    pub async fn new(
        conversation_id: String,
        workspace: String,
        remote_config: RemoteAgentConfig,
    ) -> Result<Self, AppError> {
        let runtime = AgentRuntime::new(conversation_id, workspace, 256);

        let http_client = reqwest::Client::builder()
            .danger_accept_invalid_certs(remote_config.allow_insecure)
            .build()
            .map_err(|e| AppError::Internal(format!("Failed to build HTTP client: {e}")))?;

        Ok(Self {
            runtime,
            remote_config,
            state: RwLock::new(RemoteState {
                session_key: None,
                confirmations: Vec::new(),
                has_messages: false,
                approval_memory: HashMap::new(),
                connection_status: RemoteAgentStatus::Unknown,
                opencode_session_id: None,
                reasoning_parts: HashSet::new(),
                desired_model: None,
            }),
            ws_sink: Mutex::new(None),
            _reader_handle: Mutex::new(None),
            http_client,
            local_fs_mcp: Mutex::new(None),
        })
    }

    /// Connect to the remote endpoint.
    /// OpenCode uses HTTP health check + SSE reader; other protocols use WebSocket.
    pub async fn connect(self: &Arc<Self>) -> Result<(), AppError> {
        if is_opencode_protocol(&self.remote_config.protocol) {
            self.connect_opencode().await
        } else {
            self.connect_ws().await
        }
    }

    async fn connect_opencode(self: &Arc<Self>) -> Result<(), AppError> {
        let base_url = normalize_base_url(&self.remote_config.url);

        let auth_header = build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());

        let mut req = self
            .http_client
            .get(format!("{base_url}/global/health"))
            .timeout(Duration::from_secs(10));
        if let Some(ref h) = auth_header {
            req = req.header(AUTHORIZATION, h.as_str());
        }

        let resp = req
            .send()
            .await
            .map_err(|e| AppError::Internal(format!("OpenCode health check failed: {e}")))?;

        if !resp.status().is_success() {
            return Err(AppError::Internal(format!(
                "OpenCode health check returned {}",
                resp.status()
            )));
        }

        {
            let mut state = self.state.write().await;
            state.connection_status = RemoteAgentStatus::Connected;
        }

        info!(
            conversation_id = %self.runtime.conversation_id(),
            base_url = %base_url,
            "Connected to OpenCode server"
        );

        let this = Arc::clone(self);
        let event_url = format!("{base_url}/event");
        let client = self.http_client.clone();
        let auth = auth_header.clone();
        let conversation_id = self.runtime.conversation_id().to_string();
        let workspace = self.runtime.workspace().to_string();

        // The remote OpenCode server has no access to the client's local
        // filesystem, so do NOT advertise `workspace` as a `?directory=`
        // query param — that path would be interpreted as a server-local
        // path. Client filesystem access is routed through the local-fs
        // MCP server registered at session-create time. `workspace` stays
        // available as the local MCP project root via `self.runtime`.
        let _ = workspace;
        let reader_handle = tokio::spawn(async move {
            let mut req_builder = client.get(&event_url).header("Accept", "text/event-stream");
            if let Some(ref h) = auth {
                req_builder = req_builder.header(AUTHORIZATION, h.as_str());
            }

            let resp = match req_builder.send().await {
                Ok(r) => r,
                Err(e) => {
                    warn!(
                        conversation_id = %conversation_id,
                        error = %ErrorChain(&e),
                        "OpenCode SSE connection failed"
                    );
                    return;
                }
            };

            let mut stream = resp.bytes_stream();
            let mut buffer = String::new();

            while let Some(chunk_result) = stream.next().await {
                match chunk_result {
                    Ok(chunk) => {
                        let text = String::from_utf8_lossy(&chunk);
                        buffer.push_str(&text);

                        while let Some(pos) = buffer.find("\n\n") {
                            let event_text = buffer[..pos].to_string();
                            buffer = buffer[pos + 2..].to_string();

                            for line in event_text.lines() {
                                if let Some(data) = line.strip_prefix("data: ") {
                                    this.handle_opencode_sse_event(data).await;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            conversation_id = %conversation_id,
                            error = %ErrorChain(&e),
                            "OpenCode SSE stream error"
                        );
                        break;
                    }
                }
            }

            let mut state = this.state.write().await;
            state.connection_status = RemoteAgentStatus::Error;
            if this.runtime.status() == Some(ConversationStatus::Running) {
                this.runtime.transition_to(ConversationStatus::Finished);
            }
        });

        *self._reader_handle.lock().await = Some(reader_handle);

        Ok(())
    }

    async fn handle_opencode_sse_event(&self, data: &str) {
        let raw: Value = match serde_json::from_str(data) {
            Ok(v) => v,
            Err(_) => return,
        };

        let event_type = match raw.get("type").and_then(|v| v.as_str()) {
            Some(t) => t,
            None => return,
        };

        let props = match raw.get("properties") {
            Some(p) => p,
            None => return,
        };

        let session_id = props.get("sessionID").and_then(|v| v.as_str()).map(String::from);

        match event_type {
            "session.status" => {
                let status_type = props.get("status").and_then(|v| v.get("type")).and_then(|v| v.as_str());
                match status_type {
                    Some("busy") => {
                        self.runtime.bump_activity();
                        self.runtime.emit(AgentStreamEvent::Start(StartEventData {
                            session_id: session_id.clone(),
                        }));
                        {
                            let mut state = self.state.write().await;
                            if let Some(ref sid) = session_id {
                                state.session_key = Some(sid.clone());
                            }
                            state.connection_status = RemoteAgentStatus::Connected;
                        }
                    }
                    Some("idle") => {
                        self.runtime.emit(AgentStreamEvent::Finish(FinishEventData {
                            session_id: session_id.clone(),
                        }));
                        self.runtime.transition_to(ConversationStatus::Finished);
                    }
                    _ => {}
                }
            }
            "session.idle" => {
                self.runtime.emit(AgentStreamEvent::Finish(FinishEventData {
                    session_id: session_id.clone(),
                }));
                self.runtime.transition_to(ConversationStatus::Finished);
            }
            "session.error" => {
                // OpenCode sends errors as { name: "...", data: { message: "..." } }
                // in the "error" field of properties.
                let message = props
                    .get("error")
                    .and_then(|e| {
                        e.get("data")
                            .and_then(|d| d.get("message"))
                            .or_else(|| e.get("message"))
                    })
                    .and_then(|v| v.as_str())
                    .or_else(|| {
                        // Last resort: the error may be a plain string
                        props.get("error").and_then(|v| v.as_str())
                    })
                    .unwrap_or("OpenCode session error");
                warn!(
                    conversation_id = %self.runtime.conversation_id(),
                    error = message,
                    "OpenCode session error"
                );
                self.runtime
                    .emit(AgentStreamEvent::Error(crate::protocol::events::ErrorEventData {
                        message: message.to_string(),
                        code: None,
                    }));
                self.runtime.transition_to(ConversationStatus::Finished);
            }
            "session.next.model.switched" => {
                let provider_id = props
                    .get("model")
                    .and_then(|m| m.get("providerID"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("opencode-go");
                let model_id = props
                    .get("model")
                    .and_then(|m| m.get("id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let variant = props
                    .get("model")
                    .and_then(|m| m.get("variant"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("default");
                let display_label = format!("{provider_id}/{model_id}");
                let normalized = json!({
                    "modelID": model_id,
                    "providerID": provider_id,
                    "variant": variant,
                });
                {
                    let mut state = self.state.write().await;
                    state.desired_model = Some(normalized);
                }
                self.runtime.emit(AgentStreamEvent::AcpModelInfo(json!({
                    "current_model_id": model_id,
                    "current_model_label": display_label,
                })));
            }
            "session.next.agent.switched" => {
                let agent = props.get("agent").and_then(|v| v.as_str()).unwrap_or("build");
                self.runtime.emit(AgentStreamEvent::AcpModeInfo(json!({"mode": agent})));
            }
            "message.part.delta" => {
                let field = match props.get("field").and_then(|v| v.as_str()) {
                    Some(f) => f,
                    None => return,
                };
                let delta = match props.get("delta").and_then(|v| v.as_str()) {
                    Some(d) => d,
                    None => return,
                };
                if field != "text" {
                    return;
                }
                let part_id = props.get("partID").and_then(|v| v.as_str()).unwrap_or("");
                let is_reasoning = self.state.read().await.reasoning_parts.contains(part_id);
                if is_reasoning {
                    self.runtime.emit(AgentStreamEvent::Thinking(ThinkingEventData {
                        content: delta.to_string(),
                        subject: None,
                        duration: None,
                        status: None,
                    }));
                } else {
                    self.runtime.emit(AgentStreamEvent::Text(TextEventData {
                        content: delta.to_string(),
                    }));
                }
            }
            "message.part.updated" => {
                if let Some(part) = props.get("part") {
                    if let Some(part_type) = part.get("type").and_then(|v| v.as_str()) {
                        if part_type == "reasoning" {
                            if let Some(part_id) = part.get("id").and_then(|v| v.as_str()) {
                                self.state.write().await.reasoning_parts.insert(part_id.to_string());
                            }
                        }
                    }
                }
            }
            "message.updated" => {
                if let Some(info) = props.get("info") {
                    if info.get("finish").and_then(|v| v.as_str()) == Some("stop")
                        && info.get("role").and_then(|v| v.as_str()) == Some("assistant")
                    {
                        self.runtime.emit(AgentStreamEvent::Finish(FinishEventData {
                            session_id: session_id.clone(),
                        }));
                    }
                }
            }
            "permission.asked" => {
                // Map OpenCode's permission request to AionUi's Confirmation
                // queue and emit the event the UI listens for. The user's
                // reply flows back through `confirm()` → POST
                // `/permission/{id}/reply` (see the IAgentTask impl below).
                let request_id = match props.get("id").and_then(|v| v.as_str()) {
                    Some(id) => id.to_string(),
                    None => {
                        warn!(
                            conversation_id = %self.runtime.conversation_id(),
                            "permission.asked missing id; cannot prompt user"
                        );
                        return;
                    }
                };
                let permission = props
                    .get("permission")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let metadata = props.get("metadata").cloned().unwrap_or_else(|| json!({}));
                let patterns: Vec<String> = props
                    .get("patterns")
                    .and_then(|v| v.as_array())
                    .map(|arr| arr.iter().filter_map(|p| p.as_str().map(String::from)).collect())
                    .unwrap_or_default();

                let title = if permission.is_empty() {
                    "OpenCode permission request".to_string()
                } else {
                    format!("OpenCode wants to: {permission}")
                };

                // Prefer the most user-readable field from metadata if present
                // (e.g. shell command body, edit description); otherwise dump
                // metadata JSON, otherwise fall back to the patterns list.
                let description = metadata
                    .get("command")
                    .and_then(|v| v.as_str())
                    .map(String::from)
                    .or_else(|| metadata.get("description").and_then(|v| v.as_str()).map(String::from))
                    .or_else(|| metadata.get("filePath").and_then(|v| v.as_str()).map(String::from))
                    .unwrap_or_else(|| {
                        if metadata.as_object().map(|m| !m.is_empty()).unwrap_or(false) {
                            metadata.to_string()
                        } else if patterns.is_empty() {
                            String::new()
                        } else {
                            patterns.join(", ")
                        }
                    });

                let confirmation = Confirmation {
                    id: request_id.clone(),
                    call_id: request_id.clone(),
                    title: Some(title),
                    action: Some(permission.clone()),
                    description,
                    command_type: Some(permission.clone()),
                    options: vec![
                        ConfirmationOption {
                            label: "Allow once".to_string(),
                            value: Value::String("once".to_string()),
                            params: None,
                        },
                        ConfirmationOption {
                            label: "Allow always".to_string(),
                            value: Value::String("always".to_string()),
                            params: None,
                        },
                        ConfirmationOption {
                            label: "Reject".to_string(),
                            value: Value::String("reject".to_string()),
                            params: None,
                        },
                    ],
                };

                {
                    let mut state = self.state.write().await;
                    // Replace any prior entry with the same id so duplicate
                    // events (OpenCode re-emits on reconnect) don't pile up.
                    state.confirmations.retain(|c| c.call_id != confirmation.call_id);
                    state.confirmations.push(confirmation.clone());
                }

                info!(
                    conversation_id = %self.runtime.conversation_id(),
                    request_id = %request_id,
                    permission = %permission,
                    "queued OpenCode permission request for UI prompt"
                );

                self.runtime
                    .emit(AgentStreamEvent::AcpPermission(AcpPermissionEventData::Confirmation(
                        confirmation,
                    )));
            }
            _ => {
                debug!(
                    conversation_id = %self.runtime.conversation_id(),
                    event_type = event_type,
                    "Unhandled OpenCode event"
                );
            }
        }
    }

    async fn connect_ws(self: &Arc<Self>) -> Result<(), AppError> {
        let url = &self.remote_config.url;

        let (ws_stream, _response) = tokio_tungstenite::connect_async(url).await.map_err(|e| {
            error!(url = url, error = %ErrorChain(&e), "Failed to connect to remote agent");
            AppError::Internal(format!("WebSocket connection failed: {e}"))
        })?;

        info!(
            conversation_id = %self.runtime.conversation_id(),
            url = url,
            "Connected to remote agent via WebSocket"
        );

        let (sink, stream) = ws_stream.split();

        *self.ws_sink.lock().await = Some(sink);

        {
            let mut state = self.state.write().await;
            state.connection_status = RemoteAgentStatus::Connected;
        }

        let this = Arc::clone(self);
        let reader_handle = tokio::spawn(async move {
            this.run_ws_reader(stream).await;
        });

        *self._reader_handle.lock().await = Some(reader_handle);

        Ok(())
    }

    /// Fetch available models from OpenCode and emit them to the frontend.
    async fn emit_model_info(&self) {
        let models = self.fetch_opencode_models().await.unwrap_or_default();
        info!(
            conversation_id = %self.runtime.conversation_id(),
            model_count = models.len(),
            "Emitting OpenCode model info"
        );
        if models.is_empty() {
            return;
        }
        let info = json!({
            "current_model_id": null,
            "current_model_label": null,
            "available_models": models,
        });
        self.runtime.emit(AgentStreamEvent::AcpModelInfo(info));
    }

    /// Read messages from the WebSocket and process them.
    async fn run_ws_reader(
        self: Arc<Self>,
        mut stream: futures_util::stream::SplitStream<
            tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
        >,
    ) {
        while let Some(msg) = stream.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    self.runtime.bump_activity();
                    match serde_json::from_str::<Value>(&text) {
                        Ok(raw_json) => self.handle_raw_event(raw_json).await,
                        Err(e) => {
                            debug!(
                                conversation_id = %self.runtime.conversation_id(),
                                error = %ErrorChain(&e),
                                "Non-JSON WebSocket message, skipping"
                            );
                        }
                    }
                }
                Ok(Message::Close(_)) => {
                    debug!(
                        conversation_id = %self.runtime.conversation_id(),
                        "Remote WebSocket closed"
                    );
                    break;
                }
                Err(e) => {
                    warn!(
                        conversation_id = %self.runtime.conversation_id(),
                        error = %ErrorChain(&e),
                        "WebSocket read error"
                    );
                    break;
                }
                _ => {} // Ignore ping/pong/binary
            }
        }

        {
            let mut state = self.state.write().await;
            state.connection_status = RemoteAgentStatus::Error;
        }
        if self.runtime.status() == Some(ConversationStatus::Running) {
            self.runtime.transition_to(ConversationStatus::Finished);
        }
    }

    async fn handle_raw_event(&self, raw: Value) {
        let stream_event = match serde_json::from_value::<AgentStreamEvent>(raw.clone()) {
            Ok(event) => event,
            Err(_) => {
                debug!(
                    conversation_id = %self.runtime.conversation_id(),
                    "Unrecognized remote event, skipping"
                );
                return;
            }
        };

        self.update_state_from_event(&stream_event).await;
        self.runtime.emit(stream_event);
    }

    async fn update_state_from_event(&self, event: &AgentStreamEvent) {
        match event {
            AgentStreamEvent::Start(data) => {
                self.runtime.transition_to(ConversationStatus::Running);
                if let Some(ref sid) = data.session_id {
                    let mut state = self.state.write().await;
                    state.session_key = Some(sid.clone());
                }
            }
            AgentStreamEvent::Finish(data) => {
                self.runtime.transition_to(ConversationStatus::Finished);
                if let Some(ref sid) = data.session_id {
                    let mut state = self.state.write().await;
                    state.session_key = Some(sid.clone());
                }
            }
            AgentStreamEvent::Error(_) => {
                self.runtime.transition_to(ConversationStatus::Finished);
            }
            AgentStreamEvent::AcpPermission(data) => {
                if let Some(conf) = data.as_confirmation() {
                    let mut guard = self.state.write().await;
                    if let Some(existing) = guard.confirmations.iter_mut().find(|c| c.call_id == conf.call_id) {
                        *existing = conf;
                    } else {
                        guard.confirmations.push(conf);
                    }
                }
            }
            _ => {}
        }
    }

    /// Send a JSON message over the WebSocket.
    async fn ws_send(&self, payload: &Value) -> Result<(), AppError> {
        let text = serde_json::to_string(payload)
            .map_err(|e| AppError::Internal(format!("Failed to serialize WebSocket message: {e}")))?;

        let mut guard = self.ws_sink.lock().await;
        let sink = guard
            .as_mut()
            .ok_or_else(|| AppError::Internal("WebSocket not connected".into()))?;

        sink.send(Message::Text(text.into())).await.map_err(|e| {
            error!(
                conversation_id = %self.runtime.conversation_id(),
                error = %ErrorChain(&e),
                "Failed to send WebSocket message"
            );
            AppError::Internal(format!("WebSocket send failed: {e}"))
        })
    }

    /// Ensure a `LocalFsMcpServer` is running and registered with the
    /// remote OpenCode. Idempotent: returns immediately if already
    /// registered. Failures here are logged but never returned — the
    /// agent must still function (degraded) if MCP registration fails.
    async fn ensure_local_fs_mcp(&self, base_url: &str, auth_header: Option<&str>) {
        {
            let guard = self.local_fs_mcp.lock().await;
            if guard.is_some() {
                return;
            }
        }
        let workspace = self.runtime.workspace().to_string();
        let conversation_id = self.runtime.conversation_id().to_string();
        match opencode_mcp::start_and_register(&self.http_client, base_url, auth_header, &conversation_id, &workspace)
            .await
        {
            Ok(server) => {
                *self.local_fs_mcp.lock().await = Some(server);
            }
            Err(e) => {
                warn!(
                    conversation_id = %conversation_id,
                    error = %e,
                    "failed to start/register local fs MCP — agent will run without client-side fs"
                );
            }
        }
    }

    async fn opencode_create_session(&self, base_url: &str) -> Result<String, AppError> {
        let url = format!("{base_url}/session");
        let auth_header = build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());

        // Register the client-side fs MCP with the remote OpenCode before
        // creating the session, so any tool the agent emits on its first
        // turn already sees our tools advertised. Best-effort: failure is
        // logged but does not block session create — the agent will still
        // function, just without client-side fs (matching prior behavior).
        self.ensure_local_fs_mcp(base_url, auth_header.as_deref()).await;

        let session_body = json!({
            "permission": [
                { "permission": "bash",  "pattern": "*", "action": "deny" },
                { "permission": "read",  "pattern": "*", "action": "deny" },
                { "permission": "edit",  "pattern": "*", "action": "deny" },
                { "permission": "glob",  "pattern": "*", "action": "deny" },
                { "permission": "grep",  "pattern": "*", "action": "deny" }
            ]
        });

        let mut req = self
            .http_client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(serde_json::to_string(&session_body).unwrap())
            .timeout(Duration::from_secs(10));

        if let Some(ref h) = auth_header {
            req = req.header(AUTHORIZATION, h.as_str());
        }

        let resp = req
            .send()
            .await
            .map_err(|e| AppError::Internal(format!("OpenCode create session failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body_text = resp.text().await.unwrap_or_default();
            return Err(AppError::Internal(format!(
                "OpenCode create session returned {status}: {body_text}"
            )));
        }

        let body: Value = resp
            .json()
            .await
            .map_err(|e| AppError::Internal(format!("OpenCode create session response was not JSON: {e}")))?;

        body.get("id")
            .and_then(|v| v.as_str())
            .map(String::from)
            .ok_or_else(|| AppError::Internal(format!("OpenCode create session response missing id: {body}")))
    }

    /// Send a message via OpenCode HTTP prompt_async.
    async fn opencode_send(&self, content: &str) -> Result<(), AppError> {
        let base_url = normalize_base_url(&self.remote_config.url);

        let session_id = {
            let mut state = self.state.write().await;
            if state.opencode_session_id.is_none() {
                let id = self.opencode_create_session(&base_url).await?;
                state.opencode_session_id = Some(id);
            }
            state.opencode_session_id.clone().unwrap()
        };

        let url = format!("{base_url}/session/{session_id}/prompt_async");
        let auth_header = build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());
        let model = self.state.read().await.desired_model.clone();

        let workspace = self.runtime.workspace().to_string();
        let system_hint = format!(
            "The user's project is located at {workspace} on their local machine. \
             Use ONLY the mcp__aionui-local-fs-* tools for all file operations. \
             These tools operate on the user's actual project files. \
             All file paths should be relative to {workspace}."
        );

        let mut body = json!({
            "parts": [{"type": "text", "text": content}],
            "system": system_hint
        });
        if let Some(ref m) = model {
            if let Some(id) = m.get("id") {
                body["model"] = json!({
                    "providerID": m.get("providerID").and_then(|v| v.as_str()).unwrap_or("opencode-go"),
                    "modelID": id,
                    "variant": m.get("variant").and_then(|v| v.as_str()).unwrap_or("default"),
                });
            } else {
                body["model"] = m.clone();
            }
        }

        let mut req = self
            .http_client
            .post(&url)
            .json(&body)
            .timeout(Duration::from_secs(120));

        if let Some(ref h) = auth_header {
            req = req.header(AUTHORIZATION, h.as_str());
        }

        let resp = req
            .send()
            .await
            .map_err(|e| AppError::Internal(format!("OpenCode prompt_async failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body_text = resp.text().await.unwrap_or_default();
            return Err(AppError::Internal(format!(
                "OpenCode prompt_async returned {status}: {body_text}"
            )));
        }

        Ok(())
    }

    /// Get the connection status.
    pub async fn connection_status(&self) -> RemoteAgentStatus {
        self.state.read().await.connection_status
    }

    /// Set the desired model for OpenCode protocol.
    pub async fn set_model(&self, model_id: &str) -> Result<(), AppError> {
        if !is_opencode_protocol(&self.remote_config.protocol) {
            return Ok(());
        }
        // model_id may be "providerID::modelID" (from fetch_opencode_models)
        // or just "modelID" (from other sources).
        let (provider_id, actual_model_id) = if let Some((p, m)) = model_id.split_once("::") {
            (p.to_string(), m.to_string())
        } else {
            let existing_provider = self
                .state
                .read()
                .await
                .desired_model
                .as_ref()
                .and_then(|m| m.get("providerID"))
                .and_then(|v| v.as_str())
                .unwrap_or("opencode-go")
                .to_string();
            (existing_provider, model_id.to_string())
        };
        let mut state = self.state.write().await;
        state.desired_model = Some(json!({
            "modelID": actual_model_id,
            "providerID": provider_id,
            "variant": "default"
        }));
        Ok(())
    }

    /// Get the current model info for display.
    pub async fn get_model(&self) -> Result<aionui_api_types::GetModelInfoResponse, AppError> {
        let guard = self.state.read().await;
        let current = guard.desired_model.as_ref();
        let available = self.fetch_opencode_models().await.unwrap_or_default();
        Ok(aionui_api_types::GetModelInfoResponse {
            model_info: Some(aionui_api_types::ModelInfoPayload {
                current_model_id: current
                    .and_then(|m| m.get("modelID"))
                    .and_then(|v| v.as_str())
                    .map(String::from),
                current_model_label: current
                    .and_then(|m| m.get("modelID"))
                    .and_then(|v| v.as_str())
                    .map(String::from),
                available_models: available,
            }),
        })
    }

    async fn fetch_opencode_models(&self) -> Result<Vec<aionui_api_types::ModelInfoEntry>, AppError> {
        let base_url = normalize_base_url(&self.remote_config.url);
        let auth_header = build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());

        let mut req = self
            .http_client
            .get(format!("{base_url}/provider"))
            .timeout(Duration::from_secs(10));
        if let Some(ref h) = auth_header {
            req = req.header(AUTHORIZATION, h.as_str());
        }

        let resp = match req.send().await {
            Ok(r) => r,
            Err(_) => return Ok(Vec::new()),
        };
        if !resp.status().is_success() {
            return Ok(Vec::new());
        }

        let body: Value = match resp.json().await {
            Ok(v) => v,
            Err(_) => return Ok(Vec::new()),
        };

        let mut entries = Vec::new();
        if let Some(all) = body.get("all").and_then(|v| v.as_array()) {
            // Only include models from connected (authenticated) providers.
            let connected: std::collections::HashSet<&str> = body
                .get("connected")
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
                .unwrap_or_default();

            for provider in all {
                let provider_id = match provider.get("id").and_then(|v| v.as_str()) {
                    Some(id) if connected.contains(id) => id,
                    _ => continue,
                };
                if let Some(models) = provider.get("models").and_then(|v| v.as_object()) {
                    for (model_id, model) in models {
                        let label = model.get("name").and_then(|v| v.as_str()).unwrap_or(model_id);
                        // Encode as "providerID::modelID" so set_model can split it correctly.
                        entries.push(aionui_api_types::ModelInfoEntry {
                            id: format!("{provider_id}::{model_id}"),
                            label: format!("[{provider_id}] {label}"),
                        });
                    }
                }
            }
        }
        Ok(entries)
    }
}

use crate::shared_kernel::approval_key;

#[async_trait::async_trait]
impl crate::agent_task::IAgentTask for RemoteAgentManager {
    fn agent_type(&self) -> AgentType {
        AgentType::Remote
    }

    fn conversation_id(&self) -> &str {
        self.runtime.conversation_id()
    }

    fn workspace(&self) -> &str {
        self.runtime.workspace()
    }

    fn status(&self) -> Option<ConversationStatus> {
        self.runtime.status()
    }

    fn last_activity_at(&self) -> TimestampMs {
        self.runtime.last_activity_at()
    }

    fn subscribe(&self) -> broadcast::Receiver<AgentStreamEvent> {
        self.runtime.subscribe()
    }

    async fn send_message(&self, data: SendMessageData) -> Result<(), AppError> {
        self.runtime.bump_activity();

        let is_first = {
            let mut state = self.state.write().await;
            let first = !state.has_messages;
            state.has_messages = true;
            first
        };
        self.runtime.transition_to(ConversationStatus::Running);

        if is_opencode_protocol(&self.remote_config.protocol) {
            if is_first {
                self.emit_model_info().await;
            }
            self.opencode_send(&data.content).await
        } else if is_first {
            let payload = json!({
                "type": "sessionsReset",
                "data": {
                    "conversationId": self.runtime.conversation_id(),
                    "message": data.content,
                    "msgId": data.msg_id,
                }
            });
            self.ws_send(&payload).await
        } else {
            let session_key = self.state.read().await.session_key.clone();
            let mut payload = json!({
                "type": "sendMessage",
                "data": {
                    "message": data.content,
                    "msgId": data.msg_id,
                }
            });
            if let Some(ref key) = session_key {
                payload["data"]["sessionKey"] = json!(key);
            }
            if !data.files.is_empty() {
                payload["data"]["files"] = json!(data.files);
            }
            self.ws_send(&payload).await
        }
    }

    async fn cancel(&self) -> Result<(), AppError> {
        if is_opencode_protocol(&self.remote_config.protocol) {
            return Ok(());
        }
        if self.ws_sink.lock().await.is_none() {
            return Err(AppError::Conflict("WebSocket not connected; nothing to cancel".into()));
        }
        let payload = json!({ "type": "session/cancel", "data": {} });
        self.ws_send(&payload).await?;

        let mut state = self.state.write().await;
        state.confirmations.clear();
        Ok(())
    }

    fn kill(&self, reason: Option<AgentKillReason>) -> Result<(), AppError> {
        info!(
            conversation_id = %self.runtime.conversation_id(),
            ?reason,
            "Killing Remote agent"
        );

        if let Ok(mut guard) = self.ws_sink.try_lock() {
            *guard = None;
        }

        // Take the MCP server out synchronously so the OS port frees
        // immediately on Drop; the OpenCode disconnect runs on a
        // detached task because kill() is sync.
        if let Ok(mut guard) = self.local_fs_mcp.try_lock()
            && let Some(server) = guard.take()
        {
            let http_client = self.http_client.clone();
            let base_url = normalize_base_url(&self.remote_config.url);
            let auth_header =
                build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());
            let conversation_id = self.runtime.conversation_id().to_string();
            tokio::spawn(async move {
                opencode_mcp::disconnect_from_opencode(
                    &http_client,
                    &base_url,
                    auth_header.as_deref(),
                    &conversation_id,
                )
                .await;
                server.shutdown().await;
            });
        }

        Ok(())
    }
}

impl RemoteAgentManager {
    pub fn kill_and_wait(
        &self,
        reason: Option<AgentKillReason>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        let _ = crate::agent_task::IAgentTask::kill(self, reason);
        Box::pin(std::future::ready(()))
    }
}

/// Remote-specific operations reached through `AgentInstance::Remote(..)`.
impl RemoteAgentManager {
    pub fn confirm(&self, _msg_id: &str, call_id: &str, data: Value, always_allow: bool) -> Result<(), AppError> {
        if let Ok(mut state) = self.state.try_write() {
            if always_allow && let Some(conf) = state.confirmations.iter().find(|c| c.call_id == call_id) {
                let key = approval_key(conf.action.as_deref(), conf.command_type.as_deref());
                state.approval_memory.insert(key, true);
            }
            state.confirmations.retain(|c| c.call_id != call_id);
        }

        if is_opencode_protocol(&self.remote_config.protocol) {
            // Translate the UI's choice into the OpenCode reply string.
            // Prefer the option value the frontend sent (we attached
            // "once"/"always"/"reject" in the permission.asked handler).
            // Fall back to the always_allow flag, then "once".
            let reply = data
                .as_str()
                .map(str::to_owned)
                .or_else(|| data.get("value").and_then(|v| v.as_str()).map(str::to_owned))
                .unwrap_or_else(|| {
                    if always_allow {
                        "always".to_string()
                    } else {
                        "once".to_string()
                    }
                });
            let reply = match reply.as_str() {
                "once" | "always" | "reject" => reply,
                _ => {
                    if always_allow {
                        "always".to_string()
                    } else {
                        "once".to_string()
                    }
                }
            };

            let base_url = normalize_base_url(&self.remote_config.url);
            let auth_header =
                build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());
            let http_client = self.http_client.clone();
            let conversation_id = self.runtime.conversation_id().to_string();
            let call_id = call_id.to_string();
            tokio::spawn(async move {
                let url = format!("{base_url}/permission/{call_id}/reply");
                let mut req = http_client
                    .post(&url)
                    .json(&json!({ "reply": reply }))
                    .timeout(Duration::from_secs(10));
                if let Some(h) = auth_header {
                    req = req.header(AUTHORIZATION, h);
                }
                match req.send().await {
                    Ok(resp) if resp.status().is_success() => {
                        info!(
                            conversation_id = %conversation_id,
                            request_id = %call_id,
                            reply = %reply,
                            "OpenCode permission reply sent"
                        );
                    }
                    Ok(resp) => {
                        let status = resp.status();
                        let body = resp.text().await.unwrap_or_default();
                        warn!(
                            conversation_id = %conversation_id,
                            request_id = %call_id,
                            status = %status,
                            body = %body,
                            "OpenCode permission reply returned non-success"
                        );
                    }
                    Err(e) => {
                        warn!(
                            conversation_id = %conversation_id,
                            request_id = %call_id,
                            error = %e,
                            "OpenCode permission reply request failed"
                        );
                    }
                }
            });
            return Ok(());
        }

        warn!(
            conversation_id = %self.runtime.conversation_id(),
            call_id = call_id,
            "Remote agent confirm: WebSocket send deferred to integration phase"
        );

        Ok(())
    }

    pub fn get_confirmations(&self) -> Vec<Confirmation> {
        self.state
            .try_read()
            .map(|g| g.confirmations.clone())
            .unwrap_or_default()
    }

    pub fn check_approval(&self, action: &str, command_type: Option<&str>) -> bool {
        self.state
            .try_read()
            .map(|g| {
                let key = approval_key(Some(action), command_type);
                g.approval_memory.get(&key).copied().unwrap_or(false)
            })
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_task::IAgentTask;

    #[test]
    fn normalize_url_strips_trailing_slash() {
        assert_eq!(normalize_base_url("http://127.0.0.1:4096/"), "http://127.0.0.1:4096");
        assert_eq!(normalize_base_url("http://127.0.0.1:4096"), "http://127.0.0.1:4096");
    }

    #[test]
    fn is_opencode_detects_protocol() {
        assert!(is_opencode_protocol("opencode"));
        assert!(!is_opencode_protocol("openclaw"));
        assert!(!is_opencode_protocol("acp"));
    }

    #[test]
    fn auth_header_bearer() {
        let h = build_auth_header("bearer", Some("secret"));
        assert_eq!(h, Some("Bearer secret".to_string()));
    }

    #[test]
    fn auth_header_password() {
        let h = build_auth_header("password", Some("secret"));
        let expected = format!("Basic {}", BASE64.encode("opencode:secret"));
        assert_eq!(h, Some(expected));
    }

    #[test]
    fn auth_header_none_returns_none() {
        let h = build_auth_header("none", Some("secret"));
        assert_eq!(h, None);
    }

    #[test]
    fn auth_header_empty_token_returns_none() {
        let h = build_auth_header("bearer", Some(""));
        assert_eq!(h, None);
        let h = build_auth_header("bearer", None);
        assert_eq!(h, None);
    }

    #[tokio::test]
    async fn config_includes_protocol() {
        let config = RemoteAgentConfig {
            remote_agent_id: "ra_test".to_string(),
            protocol: "opencode".to_string(),
            url: "http://127.0.0.1:4096".to_string(),
            auth_type: "none".to_string(),
            auth_token: None,
            allow_insecure: false,
        };
        assert_eq!(config.protocol, "opencode");
    }

    #[tokio::test]
    async fn new_creates_agent_without_connect() {
        let config = RemoteAgentConfig {
            remote_agent_id: "ra_test".to_string(),
            protocol: "opencode".to_string(),
            url: "http://127.0.0.1:4096".to_string(),
            auth_type: "none".to_string(),
            auth_token: None,
            allow_insecure: false,
        };
        let agent = RemoteAgentManager::new("conv1".to_string(), "/ws".to_string(), config)
            .await
            .unwrap();
        assert_eq!(agent.agent_type(), AgentType::Remote);
        assert_eq!(agent.conversation_id(), "conv1");
        assert_eq!(agent.status(), None);
    }
}
