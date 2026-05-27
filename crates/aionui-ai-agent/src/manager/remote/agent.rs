use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use aionui_api_types::SlashCommandItem;
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
use crate::manager::remote::local_fs_mcp::project_tree::render_project_tree_default;
use crate::manager::remote::opencode_commands::{self, OpenCodeCommand};
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
    /// Assistant message IDs we've already emitted `AssistantModelInfo` for in this
    /// session. OpenCode's `message.updated` fires multiple times per message
    /// (creation, every part update, finish); we only need the first to capture
    /// `info.modelID` / `info.providerID`.
    /// Lifecycle: written in the `message.updated` handler (`agent.rs` event
    /// dispatch); read alongside. Set lives for the lifetime of this
    /// `RemoteAgentManager` instance (same as `reasoning_parts`).
    model_info_emitted: HashSet<String>,
    /// The desired model for the next prompt (opencode format: `{"providerID":"...","id":"...","variant":"..."}`).
    desired_model: Option<Value>,
    /// The desired OpenCode agent (`"build"` / `"plan"`) for the next prompt.
    /// Mirrors the `agent` field of OpenCode's `PromptInput`. Updated by
    /// `set_mode` (client-initiated switch) and the
    /// `session.next.agent.switched` SSE event (server-initiated). `None`
    /// before the first selection — `opencode_send` omits the field so the
    /// server picks its default ("build").
    desired_agent: Option<String>,
    /// Cached OpenCode slash-command catalog (`GET /command`). `None`
    /// before the first fetch; `Some(vec)` afterwards (empty vec on
    /// fetch failure is allowed so we don't retry every keystroke).
    /// Read by the menu (`get_slash_commands_impl`) and by
    /// `opencode_send` for template expansion. Lifetime: tied to this
    /// `RemoteAgentManager` instance — re-fetched only on reconnect.
    opencode_commands: Option<Vec<OpenCodeCommand>>,
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
                model_info_emitted: HashSet::new(),
                desired_model: None,
                desired_agent: None,
                opencode_commands: None,
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

        // Prime the slash-command cache eagerly so the menu is populated
        // before the user types `/`. Best-effort: on failure we cache
        // an empty list rather than retry — see `ensure_opencode_commands`.
        let _ = self.ensure_opencode_commands().await;

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
                {
                    let mut state = self.state.write().await;
                    state.desired_agent = Some(agent.to_owned());
                }
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
                    let is_assistant = info.get("role").and_then(|v| v.as_str()) == Some("assistant");
                    if is_assistant {
                        // Emit AssistantModelInfo once per assistant message,
                        // on the first `message.updated` that carries
                        // `info.modelID` / `info.providerID`. This fires at
                        // message creation, before any `message.part.delta`,
                        // so the renderer can stamp the model onto the
                        // in-flight bubble before text streams in.
                        if let (Some(message_id), Some(model_id), Some(provider_id)) = (
                            info.get("id").and_then(|v| v.as_str()),
                            info.get("modelID").and_then(|v| v.as_str()),
                            info.get("providerID").and_then(|v| v.as_str()),
                        ) {
                            let mut state = self.state.write().await;
                            if state.model_info_emitted.insert(message_id.to_string()) {
                                drop(state);
                                self.runtime.emit(AgentStreamEvent::AssistantModelInfo(
                                    crate::protocol::events::AssistantModelInfoEventData {
                                        message_id: message_id.to_string(),
                                        provider_id: provider_id.to_string(),
                                        model_id: model_id.to_string(),
                                    },
                                ));
                            }
                        }

                        if info.get("finish").and_then(|v| v.as_str()) == Some("stop") {
                            self.runtime.emit(AgentStreamEvent::Finish(FinishEventData {
                                session_id: session_id.clone(),
                            }));
                        }
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

    /// Populate the cached slash-command catalog. Idempotent: returns
    /// the cached list immediately if already fetched. Best-effort — a
    /// network failure stores an empty vec rather than leaving `None`,
    /// so we don't hammer the server on every menu open.
    async fn ensure_opencode_commands(&self) -> Vec<OpenCodeCommand> {
        {
            let guard = self.state.read().await;
            if let Some(ref cached) = guard.opencode_commands {
                return cached.clone();
            }
        }
        let base_url = normalize_base_url(&self.remote_config.url);
        let auth_header = build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());
        let fetched = opencode_commands::fetch(&self.http_client, &base_url, auth_header.as_deref()).await;
        debug!(
            conversation_id = %self.runtime.conversation_id(),
            command_count = fetched.len(),
            "Populated OpenCode slash-command cache"
        );
        let mut guard = self.state.write().await;
        guard.opencode_commands = Some(fetched.clone());
        fetched
    }

    /// Slash-command list exposed via `IAgentTask::get_slash_commands`
    /// for the Remote variant. Empty for non-opencode protocols.
    pub async fn get_slash_commands_impl(&self) -> Result<Vec<SlashCommandItem>, AppError> {
        if !is_opencode_protocol(&self.remote_config.protocol) {
            return Ok(Vec::new());
        }
        let cmds = self.ensure_opencode_commands().await;
        Ok(cmds.iter().map(OpenCodeCommand::to_slash_item).collect())
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
    ///
    /// If `content` starts with `/`, looks it up in the cached command
    /// catalog and expands the template before sending. OpenCode's
    /// server does not intercept `/`-prefixed prompts, so without this
    /// step the raw `/cmd` string would be forwarded to the LLM as-is.
    /// Unknown `/cmd` strings fall through unchanged — the user may
    /// have typed something the server doesn't advertise.
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

        // Resolve slash-command expansion. The per-command `agent`/`model`
        // override only applies to *this* prompt and must not clobber the
        // session-level `desired_agent`/`desired_model` the user picked
        // from the mode/model selectors.
        let (expanded_content, override_agent, override_model) = {
            if let Some((name, args)) = opencode_commands::parse_invocation(content) {
                let cmds = self.ensure_opencode_commands().await;
                if let Some(cmd) = cmds.iter().find(|c| c.name == name) {
                    let body = match cmd.template.as_deref() {
                        Some(t) => opencode_commands::expand_template(t, args),
                        // No template — pass the args through as the prompt.
                        // Empty args fall back to the bare command name so
                        // the LLM at least sees what was requested.
                        None => {
                            if args.is_empty() {
                                cmd.name.clone()
                            } else {
                                args.to_string()
                            }
                        }
                    };
                    (body, cmd.agent.clone(), cmd.model.clone())
                } else {
                    (content.to_string(), None, None)
                }
            } else {
                (content.to_string(), None, None)
            }
        };

        let (model, agent) = {
            let state = self.state.read().await;
            (
                override_model
                    .map(|m| {
                        // Per-command model override: encode as the same
                        // shape `set_model` produces so the body builder
                        // below handles it uniformly.
                        let (provider_id, model_id) = m
                            .split_once("::")
                            .map(|(p, m)| (p.to_string(), m.to_string()))
                            .unwrap_or_else(|| ("opencode-go".to_string(), m));
                        json!({
                            "providerID": provider_id,
                            "id": model_id,
                            "variant": "default",
                        })
                    })
                    .or_else(|| state.desired_model.clone()),
                override_agent.or_else(|| state.desired_agent.clone()),
            )
        };
        let content = expanded_content.as_str();

        let workspace = self.runtime.workspace().to_string();
        let tree = {
            let root = std::path::PathBuf::from(&workspace);
            tokio::task::spawn_blocking(move || render_project_tree_default(&root))
                .await
                .unwrap_or_else(|_| String::from("(failed to enumerate project)"))
        };
        let system_hint = format!(
            "The user's project is located at {workspace} on their local machine. \
             Use ONLY the mcp__aionui-local-fs-* tools for all file operations. \
             These tools operate on the user's actual project files. \
             All file paths should be relative to the project root (e.g. \"src/main.rs\"), \
             not absolute. Before claiming a file or directory does not exist, ALWAYS call \
             list_dir or read_file on it — do not rely on memory of prior turns. The current \
             project layout (gitignore-respecting; may be truncated) is:\n\n{tree}"
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
        if let Some(ref a) = agent {
            body["agent"] = json!(a);
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

    /// Set the desired OpenCode agent (`build` / `plan`) for the next prompt.
    ///
    /// OpenCode has no dedicated mode-switch endpoint — the agent is selected
    /// per-prompt via the `agent` field of `PromptInput`. Stashing it on
    /// `RemoteState` lets the next `opencode_send` pick it up; the
    /// `session.next.agent.switched` SSE event will then reflect the change
    /// back to the UI via `AcpModeInfo`.
    ///
    /// Non-opencode protocols return `BadRequest` rather than silently
    /// no-op'ing, so callers learn the operation is unsupported.
    pub async fn set_mode(&self, mode: &str) -> Result<(), AppError> {
        if !is_opencode_protocol(&self.remote_config.protocol) {
            return Err(AppError::BadRequest(format!(
                "Mode switching is not supported for remote protocol '{}'",
                self.remote_config.protocol
            )));
        }
        let normalized = mode.trim();
        if !matches!(normalized, "build" | "plan") {
            return Err(AppError::BadRequest(format!(
                "Unsupported OpenCode mode '{normalized}'; expected 'build' or 'plan'"
            )));
        }
        {
            let mut state = self.state.write().await;
            state.desired_agent = Some(normalized.to_owned());
        }
        // Mirror the same UI sync path the SSE handler uses so the selector
        // updates immediately instead of waiting for the next prompt round-trip.
        self.runtime
            .emit(AgentStreamEvent::AcpModeInfo(json!({"mode": normalized})));
        Ok(())
    }

    /// Return the current mode for the conversation mode API.
    ///
    /// `initialized = false` before any selection or server-emitted switch —
    /// matches the contract `AgentModeSelector` expects so it doesn't clobber
    /// `initialMode` while the agent is warming up.
    pub async fn mode(&self) -> Result<aionui_api_types::AgentModeResponse, AppError> {
        let guard = self.state.read().await;
        match guard.desired_agent.as_deref() {
            Some(m) => Ok(aionui_api_types::AgentModeResponse {
                mode: m.to_owned(),
                initialized: true,
            }),
            None => Ok(aionui_api_types::AgentModeResponse {
                mode: "build".into(),
                initialized: false,
            }),
        }
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

    async fn opencode_test_agent() -> RemoteAgentManager {
        let config = RemoteAgentConfig {
            remote_agent_id: "ra_test".to_string(),
            protocol: "opencode".to_string(),
            url: "http://127.0.0.1:4096".to_string(),
            auth_type: "none".to_string(),
            auth_token: None,
            allow_insecure: false,
        };
        RemoteAgentManager::new("conv_model_info".to_string(), "/ws".to_string(), config)
            .await
            .unwrap()
    }

    /// Drains all events currently buffered in `rx` (non-blocking).
    fn drain_events(rx: &mut broadcast::Receiver<AgentStreamEvent>) -> Vec<AgentStreamEvent> {
        let mut events = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            events.push(ev);
        }
        events
    }

    #[tokio::test]
    async fn message_updated_emits_assistant_model_info_once() {
        let agent = opencode_test_agent().await;
        let mut rx = agent.runtime.subscribe();

        // Two `message.updated` payloads for the same assistant message.
        // OpenCode fires this event multiple times per message (creation,
        // every part update, finish); we should only emit `AssistantModelInfo`
        // on the first one.
        let creation_event = json!({
            "type": "message.updated",
            "properties": {
                "sessionID": "sess_1",
                "info": {
                    "id": "msg_01",
                    "role": "assistant",
                    "modelID": "claude-sonnet-4-5",
                    "providerID": "anthropic",
                }
            }
        })
        .to_string();
        let finish_event = json!({
            "type": "message.updated",
            "properties": {
                "sessionID": "sess_1",
                "info": {
                    "id": "msg_01",
                    "role": "assistant",
                    "modelID": "claude-sonnet-4-5",
                    "providerID": "anthropic",
                    "finish": "stop",
                }
            }
        })
        .to_string();

        agent.handle_opencode_sse_event(&creation_event).await;
        agent.handle_opencode_sse_event(&finish_event).await;

        let events = drain_events(&mut rx);
        let model_info_count = events
            .iter()
            .filter(|e| matches!(e, AgentStreamEvent::AssistantModelInfo(_)))
            .count();
        assert_eq!(
            model_info_count, 1,
            "expected exactly one AssistantModelInfo emission, got {model_info_count}"
        );
        let model_info = events
            .iter()
            .find_map(|e| match e {
                AgentStreamEvent::AssistantModelInfo(d) => Some(d),
                _ => None,
            })
            .expect("AssistantModelInfo not emitted");
        assert_eq!(model_info.message_id, "msg_01");
        assert_eq!(model_info.provider_id, "anthropic");
        assert_eq!(model_info.model_id, "claude-sonnet-4-5");

        // Finish should still be emitted on the second event.
        assert!(
            events.iter().any(|e| matches!(e, AgentStreamEvent::Finish(_))),
            "Finish event not emitted on stop"
        );
    }

    #[tokio::test]
    async fn message_updated_user_role_does_not_emit_model_info() {
        let agent = opencode_test_agent().await;
        let mut rx = agent.runtime.subscribe();

        let user_event = json!({
            "type": "message.updated",
            "properties": {
                "sessionID": "sess_1",
                "info": {
                    "id": "msg_user_01",
                    "role": "user",
                }
            }
        })
        .to_string();
        agent.handle_opencode_sse_event(&user_event).await;

        let events = drain_events(&mut rx);
        assert!(
            !events.iter().any(|e| matches!(e, AgentStreamEvent::AssistantModelInfo(_))),
            "AssistantModelInfo must not fire for user messages"
        );
    }

    #[tokio::test]
    async fn message_updated_different_assistant_messages_each_emit_model_info() {
        let agent = opencode_test_agent().await;
        let mut rx = agent.runtime.subscribe();

        for (msg_id, model) in [("msg_01", "claude-sonnet-4-5"), ("msg_02", "claude-opus-4-7")] {
            let ev = json!({
                "type": "message.updated",
                "properties": {
                    "sessionID": "sess_1",
                    "info": {
                        "id": msg_id,
                        "role": "assistant",
                        "modelID": model,
                        "providerID": "anthropic",
                    }
                }
            })
            .to_string();
            agent.handle_opencode_sse_event(&ev).await;
        }

        let events = drain_events(&mut rx);
        let model_infos: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                AgentStreamEvent::AssistantModelInfo(d) => Some(d),
                _ => None,
            })
            .collect();
        assert_eq!(model_infos.len(), 2, "expected one emission per distinct message id");
        assert_eq!(model_infos[0].model_id, "claude-sonnet-4-5");
        assert_eq!(model_infos[1].model_id, "claude-opus-4-7");
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
