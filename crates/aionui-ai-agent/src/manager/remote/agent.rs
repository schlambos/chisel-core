use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use aionui_api_types::SlashCommandItem;
use aionui_common::{
    AgentKillReason, AgentType, AppError, Confirmation, ConversationStatus, ErrorChain, RemoteAgentStatus, TimestampMs,
    now_ms,
};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures_util::{SinkExt, StreamExt};
use reqwest::header::AUTHORIZATION;
use serde_json::{Value, json};
use tokio::sync::{Mutex, RwLock, broadcast, oneshot};
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::agent_runtime::AgentRuntime;
use crate::manager::remote::local_fs_mcp::project_tree::render_project_tree_default;
use crate::manager::remote::local_fs_mcp::{
    ElicitationHandler, ElicitationOutcome, ElicitationRequest, LocalFsMcpServer, McpRequestContext, ShellApproval,
    ShellApprover,
};
use crate::manager::remote::opencode_commands::{self, OpenCodeCommand};
use crate::manager::remote::opencode_mcp;
use crate::manager::remote::opencode_models;
use crate::manager::remote::opencode_stream;
use crate::manager::remote::opencode_tool_call;
use crate::manager::remote::subagent::{self, ChildSessionRegistry};
use crate::protocol::events::{
    AcpPermissionEventData, AcpToolCallSessionUpdateKind, AgentStreamEvent, FinishEventData, OpencodeSubtaskStatus,
    PlanEventData, StartEventData, TextEventData, ThinkingEventData,
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
    /// Cached `model_id -> context_window` map (`GET /config/providers`).
    /// `None` before the first fetch; `Some(map)` afterwards (empty map on
    /// fetch failure is allowed so we don't retry every turn). Used to fill
    /// the `size` field of the synthesized `acp_context_usage` event.
    /// Lifetime: tied to this `RemoteAgentManager` instance.
    model_context_limits: Option<HashMap<String, u64>>,
    /// In-flight `run_shell` approvals raised by the local fs MCP server,
    /// keyed by the synthetic confirmation `call_id` (`shell-…`). The MCP
    /// dispatch parks a `oneshot::Sender` here and awaits the receiver;
    /// `confirm()` (driven by the UI's reply) removes the entry and sends
    /// the decision, waking the parked tool call. Dropped on cancel/kill so
    /// any waiting command fails closed.
    pending_shell_approvals: HashMap<String, oneshot::Sender<ShellApproval>>,
    /// In-flight MCP elicitation requests raised by the local fs MCP server,
    /// keyed by the synthetic confirmation `call_id` (`elicit-…`). The MCP
    /// tool parks a `oneshot::Sender<Option<Value>>` here and awaits the
    /// receiver; `confirm()` decodes the user's payload (or `None` on
    /// cancel/decline) and forwards it. Dropped on cancel/kill so any
    /// waiting tool fails closed via [`ElicitationOutcome::Declined`].
    pending_elicitations: HashMap<String, oneshot::Sender<Option<Value>>>,
    /// Recently-replied OpenCode permission ids → reply timestamp (ms). Used
    /// to suppress duplicate `POST /permission/.../reply` calls when the UI
    /// double-fires (re-render race, double-click, batch "approve all"
    /// hitting the same id twice). Without this, OpenCode returns
    /// `PermissionNotFoundError` on the second POST, which surfaces in logs
    /// as a noisy 404 and confuses error reporting. Entries are pruned by
    /// age (60 s TTL) and total count (capped at 1000) inside `confirm()`.
    /// Mirrors the `responded` Map in OpenCode's own `permission.tsx` SDK.
    recently_replied_permissions: HashMap<String, TimestampMs>,
    /// Path prefixes the user has blessed for the rest of this conversation.
    /// When a `permission.asked` arrives whose target path (extracted from
    /// `metadata.filepath` / `metadata.path` / `metadata.parentDir`) is
    /// covered by any prefix in this set, the prompt is auto-resolved with
    /// `response: once` to OpenCode and never surfaces to the UI.
    ///
    /// Mirrors the `autoAccept` map in OpenCode's own
    /// `context/permission.tsx` — except we key by path prefix rather than
    /// `directoryAcceptKey(directory)`, because Chisl conversations cross
    /// arbitrary user paths (the workspace is a synthetic temp dir, not the
    /// user's project root) and OpenCode's `external_directory` permission
    /// fires per-path. In-memory only; cleared on conversation teardown.
    auto_accept_paths: HashSet<String>,
    /// Sub-agent session ids whose permissions the user has blessed for the
    /// rest of this conversation. When a permission's `sessionID` (or any of
    /// its ancestors in the child-session graph) is in this set, the prompt
    /// is auto-resolved without surfacing.
    auto_accept_sessions: HashSet<String>,
    /// OpenCode tool `callID`s we've already announced to the relay as
    /// `AcpToolCallSessionUpdateKind::ToolCall` (insert). The first time we
    /// see a `message.part.updated` for a `type=tool` part we flip the
    /// event to the insert variant so the persistence layer creates the
    /// row; every subsequent tick of the same `callID` stays as
    /// `ToolCallUpdate` (merge). Mirrors how the ACP WS path semantically
    /// separates "new tool call" from "tool call updated" without
    /// requiring OpenCode itself to emit two distinct event kinds.
    /// Lifetime: tied to this `RemoteAgentManager` — cleared on
    /// reconnect/teardown alongside `reasoning_parts`.
    opencode_tool_call_ids: HashSet<String>,
    /// Registered OpenCode child sessions (sub-agent invocations) whose
    /// `parentID` matches `opencode_session_id`. Events on the global
    /// `/global/event` stream whose `sessionID` matches a registered child are
    /// routed through this manager's runtime so the renderer can render
    /// the sub-agent's transcript inline. See [`super::subagent`].
    ///
    /// Lifecycle: written when `session.created` arrives with a matching
    /// parent (registration), updated on each child event (rolling
    /// summary), and frozen with a status on `session.idle`. Cleared on
    /// reconnect/teardown alongside `opencode_tool_call_ids`.
    child_sessions: ChildSessionRegistry,
    /// Last wall-clock millisecond at which we emitted an
    /// `OpencodeSubtask::Progress` event per child. Used to throttle
    /// progress emission so a busy sub-agent doesn't spam the renderer
    /// at OpenCode's full tick rate. 500 ms cadence is the floor.
    last_subtask_progress_ms: HashMap<String, i64>,
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

/// Unwrap an OpenCode SSE event payload.
///
/// The canonical `/global/event` stream wraps each event under a `payload`
/// key: `{"payload": {"id", "type", "properties"}}`. The legacy `/event`
/// stream emits the event object raw: `{"id", "type", "properties"}`.
///
/// This helper normalizes both shapes to the inner event object so the rest
/// of the dispatcher can read `type` / `properties` directly, regardless of
/// which endpoint the server build serves. Applied once at the parser
/// boundary (see `handle_opencode_sse_event`).
///
/// A no-op for the legacy raw shape (no `payload` key) — safe during the
/// rollout and against older self-hosted servers.
fn unwrap_event(raw: Value) -> Value {
    match raw {
        Value::Object(mut map) => match map.remove("payload") {
            Some(payload) => payload,
            None => Value::Object(map),
        },
        other => other,
    }
}

/// Decide which SSE path to subscribe to (`/global/event` vs legacy `/event`).
///
/// Probes the server's OpenAPI document (`GET /doc`) once at connect time and
/// returns `"/global/event"` if that route is listed, otherwise `"/event"`.
/// This protects users on older self-hosted OpenCode builds that predate
/// `/global/event` while defaulting everyone else to the canonical stream.
///
/// Best-effort: any failure (network, non-2xx, unparsable JSON, or simply not
/// finding the key) falls back to `/global/event` for modern servers — the
/// probe only *downgrades* to `/event` when it can positively confirm the
/// canonical route is absent. Returns a `&'static str` so the caller can build
/// the full URL without extra allocation.
async fn resolve_event_path(client: &reqwest::Client, base_url: &str, auth_header: Option<&str>) -> &'static str {
    let mut req = client.get(format!("{base_url}/doc")).timeout(Duration::from_secs(5));
    if let Some(h) = auth_header {
        req = req.header(AUTHORIZATION, h);
    }
    match req.send().await {
        Ok(resp) if resp.status().is_success() => match resp.json::<Value>().await {
            Ok(doc) => {
                let has_global = doc
                    .get("paths")
                    .and_then(|p| p.as_object())
                    .map(|paths| paths.contains_key("/global/event"))
                    .unwrap_or(false);
                if has_global {
                    "/global/event"
                } else if doc
                    .get("paths")
                    .and_then(|p| p.as_object())
                    .map(|paths| paths.contains_key("/event"))
                    .unwrap_or(false)
                {
                    // Positively confirmed: canonical absent, legacy present.
                    "/event"
                } else {
                    // Doc shape unexpected — default to canonical.
                    "/global/event"
                }
            }
            Err(_) => "/global/event",
        },
        _ => "/global/event",
    }
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

/// Build the canonical OpenCode permission-reply request `(url, body)` for a
/// given permission id and decision.
///
/// ## Endpoint discovery (verified against `http://192.168.0.5:4096/doc`,
/// opencode 1.15.11, OpenAPI 3.1.0 — re-verify with
/// `curl -s <server>/doc | jq '.paths["/permission/{requestID}/reply"]'`):
///
/// - `POST /permission/{requestID}/reply` — `operationId: permission.reply`,
///   **NOT deprecated** (canonical). Body:
///   `{ "reply": "once"|"always"|"reject", "message"?: string }` (`reply` required).
///   Path param is the permission id only; no sessionID required.
/// - `POST /session/{sessionID}/permissions/{permissionID}` —
///   `operationId: permission.respond`, **`deprecated: true`**. Body:
///   `{ "response": "once"|"always"|"reject" }` (`response` required).
///
/// Both return `200` with a JSON boolean on success and `404`
/// (`PermissionNotFoundError`) when the id is unknown / already resolved.
///
/// Because the session-scoped variant is deprecated and the
/// permission-id-only variant is canonical and needs no sessionID, we always
/// use `/permission/{id}/reply` with the `{ "reply" }` field. The
/// `session_id` parameter is retained for call-site compatibility and future
/// diagnostics but is intentionally not required to construct the request.
///
/// `decision` must already be a wire-canonical value: `once` | `always` |
/// `reject`. (Chisl-internal `allow_dir` / `allow_session` are mapped to
/// `once` by the caller before reaching here.)
fn build_permission_reply_request(base_url: &str, request_id: &str, decision: &str) -> (String, Value) {
    (
        format!("{base_url}/permission/{request_id}/reply"),
        json!({ "reply": decision }),
    )
}

/// Approval-memory key for an "allow always" decision on the shell tool.
/// Mirrors the `(action, command_type)` pair the confirmation carries.
fn shell_approval_key() -> String {
    approval_key(Some("run_shell"), Some("run_shell"))
}

/// Pull the filesystem target from an OpenCode permission's `metadata` blob.
/// `external_directory` requests pack the touched path under `filepath` (with
/// the ancestor as `parentDir`); shell-style metadata sometimes uses `path`.
/// Returns the most-specific path available so the renderer can offer a
/// well-scoped "Allow this directory tree" affordance.
fn extract_permission_target_path(metadata: &Value) -> Option<String> {
    metadata
        .get("filepath")
        .or_else(|| metadata.get("path"))
        .or_else(|| metadata.get("parentDir"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

/// True when `target` is a descendant of (or equal to) any blessed prefix in
/// `prefixes`. Comparison is a normalized path-prefix check: we treat
/// `/foo/bar` and `/foo/bar/baz` as a hit, but NOT `/foo/barber` (which would
/// match a naive `starts_with`). Both sides have trailing slashes trimmed
/// before comparison so `/foo` covers `/foo/x`.
fn path_is_under_blessed(target: &str, prefixes: &HashSet<String>) -> bool {
    let normalized_target = target.trim_end_matches('/');
    prefixes.iter().any(|prefix| {
        let p = prefix.trim_end_matches('/');
        if normalized_target == p {
            return true;
        }
        if let Some(rest) = normalized_target.strip_prefix(p) {
            rest.starts_with('/')
        } else {
            false
        }
    })
}

/// Walk a child session's ancestry through the registry and return true if
/// any ancestor's session id is in the blessed set. Used by the auto-respond
/// fast path so blessing a sub-agent's parent automatically covers all of
/// its descendants — the same lineage walk OpenCode's
/// `autoRespondsPermission` does in `permission-auto-respond.ts`.
fn session_or_ancestor_blessed(
    session_id: &str,
    blessed: &HashSet<String>,
    registry: &subagent::ChildSessionRegistry,
) -> bool {
    if blessed.contains(session_id) {
        return true;
    }
    let mut current = registry.get(session_id);
    let mut seen = HashSet::new();
    while let Some(child) = current {
        if !seen.insert(child.child_session_id.clone()) {
            break;
        }
        if blessed.contains(&child.child_session_id) {
            return true;
        }
        current = None;
        // Registry doesn't currently track parentID per child (we store only
        // the parent-of-this-conversation as a single field). If/when we add
        // sub-sub-agents, this walk would consult that field. For V1, a
        // single hop covers the common case (parent blesses, child auto-runs).
        let _ = child;
    }
    false
}

/// Bridges `run_shell` tool calls from the local fs MCP server back to the
/// user's confirmation UI.
///
/// Holds only the shared pieces it needs — the state behind its `Arc` and a
/// runtime handle to emit events — so it can be handed to the MCP server as
/// a `'static` trait object without coupling to the manager's lifetime or
/// requiring an `Arc<RemoteAgentManager>` at the (borrowed-`self`) call site
/// that starts the server.
struct RemoteShellApprover {
    runtime: AgentRuntime,
    state: Arc<RwLock<RemoteState>>,
}

#[async_trait::async_trait]
impl ShellApprover for RemoteShellApprover {
    async fn approve_shell(&self, command: &str, cwd: &str) -> ShellApproval {
        // Default path with no session context (e.g. legacy approver callers).
        self.approve_shell_with_context(command, cwd, &McpRequestContext::default())
            .await
    }

    async fn approve_shell_with_context(&self, command: &str, cwd: &str, context: &McpRequestContext) -> ShellApproval {
        // A prior "allow always" for this session short-circuits the prompt.
        {
            let state = self.state.read().await;
            if state
                .approval_memory
                .get(&shell_approval_key())
                .copied()
                .unwrap_or(false)
            {
                return ShellApproval::Allow;
            }
        }

        // Synthetic id namespaced so `confirm()` can tell our in-process
        // approvals apart from OpenCode's `per_…` permission ids.
        let call_id = format!("shell-{}", Uuid::new_v4());
        let (tx, rx) = oneshot::channel();

        let confirmation = Confirmation {
            id: call_id.clone(),
            call_id: call_id.clone(),
            title: Some("Run a command on your machine?".to_string()),
            action: Some("run_shell".to_string()),
            description: format!("{} — {cwd}\n\n$ {command}", super::local_fs_mcp::shell::shell_hint()),
            command_type: Some("run_shell".to_string()),
            // Note the spelled-out scope on the "always" option. The bare
            // "Allow always" label was easy to misread as "always for this
            // command" when it actually silences *every* shell prompt for
            // the remainder of the session via `approval_memory`. Making
            // the scope explicit on the wire avoids a user picking it by
            // accident and then wondering why later parallel shell tools
            // run without ever asking again.
            options: vec![
                ConfirmationOption {
                    label: "Allow once".to_string(),
                    value: Value::String("once".to_string()),
                    params: None,
                },
                ConfirmationOption {
                    label: "Skip shell prompts (this session)".to_string(),
                    value: Value::String("always".to_string()),
                    params: None,
                },
                ConfirmationOption {
                    label: "Reject".to_string(),
                    value: Value::String("reject".to_string()),
                    params: None,
                },
            ],
            // Stamp the originating OpenCode session id (and its parent, for
            // sub-agent calls) so the renderer attaches the prompt to the
            // right nested transcript instead of bubbling it up at the
            // conversation level. Both `None` for older OpenCode without the
            // header extension — the prompt then surfaces at conversation
            // level as before.
            session_id: context.session_id.clone(),
            parent_session_id: context.parent_session_id.clone(),
        };

        {
            let mut state = self.state.write().await;
            state.confirmations.retain(|c| c.call_id != call_id);
            state.confirmations.push(confirmation.clone());
            state.pending_shell_approvals.insert(call_id.clone(), tx);
        }

        info!(
            conversation_id = %self.runtime.conversation_id(),
            %call_id,
            session_id = ?context.session_id,
            parent_session_id = ?context.parent_session_id,
            "awaiting user approval for a local shell command"
        );
        self.runtime
            .emit(AgentStreamEvent::AcpPermission(AcpPermissionEventData::Confirmation(
                confirmation,
            )));

        // Park until the UI replies via `confirm()`. A dropped sender
        // (cancel/kill clears the map) closes the channel → fail closed.
        match rx.await {
            Ok(decision) => decision,
            Err(_) => {
                let mut state = self.state.write().await;
                state.pending_shell_approvals.remove(&call_id);
                state.confirmations.retain(|c| c.call_id != call_id);
                ShellApproval::Reject
            }
        }
    }
}

/// Elicitation passthrough for the local-fs MCP server.
///
/// MCP's `elicitation/create` is a server→client reverse-call protocol; our
/// HTTP-only MCP server can't natively do that, so we fold the flow into the
/// same Confirmation queue that the shell approver uses. The tool calls
/// [`Self::request_elicitation`] and parks; the UI surfaces a
/// schema-driven prompt; the user's response is delivered back through the
/// existing `confirmMessage` IPC path.
///
/// The `Confirmation` carries the elicitation schema in `command_type =
/// "mcp_elicitation"` plus the schema serialized into the first option's
/// `params`, so the renderer can pick a schema-aware form. When the
/// renderer can't honour the schema it falls back to a free-text input and
/// returns `{ raw: <text> }`.
#[async_trait::async_trait]
impl ElicitationHandler for RemoteShellApprover {
    async fn request_elicitation(
        &self,
        request: ElicitationRequest<'_>,
        context: &McpRequestContext,
    ) -> ElicitationOutcome {
        let call_id = format!("elicit-{}", Uuid::new_v4());
        let (tx, rx) = oneshot::channel::<Option<Value>>();

        // Serialize the schema into the option's `params` so the renderer
        // can build a form without inventing a new ipcBridge surface. An
        // absent schema → free-text fallback.
        let mut schema_params = std::collections::HashMap::new();
        if let Some(ref schema) = request.requested_schema {
            schema_params.insert("schema".to_string(), schema.to_string());
        }

        let confirmation = Confirmation {
            id: call_id.clone(),
            call_id: call_id.clone(),
            title: Some(format!("{}: input required", request.tool_name)),
            action: Some("mcp_elicitation".to_string()),
            description: request.message.to_string(),
            command_type: Some("mcp_elicitation".to_string()),
            options: vec![
                ConfirmationOption {
                    label: "Submit".to_string(),
                    value: Value::String("submit".to_string()),
                    params: if schema_params.is_empty() {
                        None
                    } else {
                        Some(schema_params)
                    },
                },
                ConfirmationOption {
                    label: "Cancel".to_string(),
                    value: Value::String("cancel".to_string()),
                    params: None,
                },
            ],
            session_id: context.session_id.clone(),
            parent_session_id: context.parent_session_id.clone(),
        };

        {
            let mut state = self.state.write().await;
            state.confirmations.retain(|c| c.call_id != call_id);
            state.confirmations.push(confirmation.clone());
            state.pending_elicitations.insert(call_id.clone(), tx);
        }

        info!(
            conversation_id = %self.runtime.conversation_id(),
            %call_id,
            tool = request.tool_name,
            session_id = ?context.session_id,
            parent_session_id = ?context.parent_session_id,
            "awaiting user elicitation response"
        );
        self.runtime
            .emit(AgentStreamEvent::AcpPermission(AcpPermissionEventData::Confirmation(
                confirmation,
            )));

        match rx.await {
            Ok(Some(payload)) => ElicitationOutcome::Accepted(payload),
            Ok(None) | Err(_) => {
                let mut state = self.state.write().await;
                state.pending_elicitations.remove(&call_id);
                state.confirmations.retain(|c| c.call_id != call_id);
                ElicitationOutcome::Declined
            }
        }
    }
}

/// Manages a Remote Agent via WebSocket or HTTP/SSE transport.
///
/// OpenClaw / ACP protocols use WebSocket. OpenCode uses HTTP POST + SSE.
pub struct RemoteAgentManager {
    runtime: AgentRuntime,
    remote_config: RemoteAgentConfig,
    /// Shared so the local fs MCP server's shell approver can reach the
    /// confirmation queue and approval memory without holding the whole
    /// manager. `Arc<RwLock<_>>` derefs to `RwLock<_>`, so all existing
    /// `self.state.read()/write()` call sites are unaffected.
    state: Arc<RwLock<RemoteState>>,
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
    /// Background task that re-registers the local fs MCP's advertised
    /// address when the network route to OpenCode changes. Paired with
    /// `local_fs_mcp`; aborted on teardown.
    reachability_guardian: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Optional handle to Chisl's conversation/messages store. When
    /// present, Phase 4c's stale-session rebroker reads the local
    /// transcript here so it can prepend it to the next prompt and
    /// reconstruct context on a freshly created OpenCode session.
    /// `None` for unit-test constructors that don't exercise the
    /// stale-session path.
    conversation_repo: Option<Arc<dyn aionui_db::IConversationRepository>>,
}

impl RemoteAgentManager {
    /// Create a new Remote agent.
    pub async fn new(
        conversation_id: String,
        workspace: String,
        remote_config: RemoteAgentConfig,
        resume_session_id: Option<String>,
    ) -> Result<Self, AppError> {
        Self::new_with_history(conversation_id, workspace, remote_config, resume_session_id, None).await
    }

    /// Like [`Self::new`] but also accepts a conversation repository
    /// handle for Phase 4c transcript-prepended rebrokering. Existing
    /// call sites use [`Self::new`] (no repo) until they are migrated
    /// to provide one — the absence of a repo just disables the
    /// stale-session context-dump fallback.
    pub async fn new_with_history(
        conversation_id: String,
        workspace: String,
        remote_config: RemoteAgentConfig,
        resume_session_id: Option<String>,
        conversation_repo: Option<Arc<dyn aionui_db::IConversationRepository>>,
    ) -> Result<Self, AppError> {
        let runtime = AgentRuntime::new(conversation_id, workspace.clone(), 256);

        let http_client = reqwest::Client::builder()
            .danger_accept_invalid_certs(remote_config.allow_insecure)
            .build()
            .map_err(|e| AppError::Internal(format!("Failed to build HTTP client: {e}")))?;

        // Pre-bless the conversation's workspace as an auto-accept path so
        // every file read OpenCode would otherwise flag as
        // `external_directory` auto-passes silently. Without this, the
        // user's prompts that reference files anywhere under their actual
        // project tree triggered a permission cascade on every single read
        // (the MCP project_root is a synthetic temp dir, not the user's
        // project, so OpenCode treats every real-path read as "external").
        // Mirrors OpenCode's `permission: "allow"` config default applied
        // to the session's working directory — see
        // `permission.tsx::permissionsEnabled` in anomalyco/opencode.
        let mut initial_auto_accept_paths = HashSet::new();
        let normalized_workspace = workspace.trim_end_matches('/').to_string();
        if !normalized_workspace.is_empty() && normalized_workspace.starts_with('/') {
            initial_auto_accept_paths.insert(normalized_workspace);
        }

        Ok(Self {
            runtime,
            remote_config,
            state: Arc::new(RwLock::new(RemoteState {
                session_key: None,
                confirmations: Vec::new(),
                has_messages: false,
                approval_memory: HashMap::new(),
                connection_status: RemoteAgentStatus::Unknown,
                // Seed from the persisted `conversation.extra.sessionKey` so
                // `connect_opencode` can validate it and `opencode_send` reuses
                // it instead of creating a fresh server-side session. `None` for
                // a brand-new conversation. Only consumed on the OpenCode HTTP
                // path; harmless for WS protocols, which never read this field.
                opencode_session_id: resume_session_id,
                reasoning_parts: HashSet::new(),
                model_info_emitted: HashSet::new(),
                desired_model: None,
                desired_agent: None,
                opencode_commands: None,
                model_context_limits: None,
                pending_shell_approvals: HashMap::new(),
                pending_elicitations: HashMap::new(),
                recently_replied_permissions: HashMap::new(),
                auto_accept_paths: initial_auto_accept_paths,
                auto_accept_sessions: HashSet::new(),
                opencode_tool_call_ids: HashSet::new(),
                child_sessions: ChildSessionRegistry::default(),
                last_subtask_progress_ms: HashMap::new(),
            })),
            ws_sink: Mutex::new(None),
            _reader_handle: Mutex::new(None),
            http_client,
            local_fs_mcp: Mutex::new(None),
            reachability_guardian: Mutex::new(None),
            conversation_repo,
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

        // Validate a resumed session id (seeded from persisted
        // `conversation.extra.sessionKey`) before reuse. OpenCode persists
        // sessions on disk, so the id usually survives our restart — but it
        // may have been deleted/expired server-side. Probing `GET /session/{id}`
        // here means a stale id is cleared up front; `opencode_send` then
        // transparently creates a fresh session rather than failing the first
        // `prompt_async`. Runs only on the OpenCode path (this fn is opencode-only).
        let resume_id = { self.state.read().await.opencode_session_id.clone() };
        if let Some(session_id) = resume_id {
            let mut req = self
                .http_client
                .get(format!("{base_url}/session/{session_id}"))
                .timeout(Duration::from_secs(10));
            if let Some(ref h) = auth_header {
                req = req.header(AUTHORIZATION, h.as_str());
            }
            let valid = matches!(req.send().await, Ok(resp) if resp.status().is_success());
            if valid {
                info!(
                    conversation_id = %self.runtime.conversation_id(),
                    session_id = %session_id,
                    "Resuming persisted OpenCode session"
                );
                // Re-register the client-side fs MCP on resume. The previous
                // process's `LocalFsMcpServer` is gone (its loopback/LAN port
                // was process-scoped), but the resumed OpenCode session may
                // still hold the stale registration on the server side. Re-
                // registering replaces the dead URL with the new one so
                // `aionui-local-fs_*` tool calls dial a live local server.
                // Without this, the very first prompt on a resumed session
                // fails with "Unable to connect" from the model because
                // `opencode_create_session` (the other call site) is skipped
                // when a session id already exists.
                self.ensure_local_fs_mcp(&base_url, auth_header.as_deref()).await;
            } else {
                warn!(
                    conversation_id = %self.runtime.conversation_id(),
                    session_id = %session_id,
                    "Persisted OpenCode session is no longer valid; starting a fresh session"
                );
                self.state.write().await.opencode_session_id = None;
            }
        }

        // Prime the slash-command cache eagerly so the menu is populated
        // before the user types `/`. Best-effort: on failure we cache
        // an empty list rather than retry — see `ensure_opencode_commands`.
        let _ = self.ensure_opencode_commands().await;

        let this = Arc::clone(self);
        // Prefer the canonical `/global/event` stream (events wrapped under
        // `payload`, cross-directory lifecycle events emitted). Fall back to
        // the legacy `/event` stream for older self-hosted servers that don't
        // list `/global/event` in their OpenAPI document. The parser
        // (`handle_opencode_sse_event` → `unwrap_event`) tolerates both shapes.
        let event_path = resolve_event_path(&self.http_client, &base_url, auth_header.as_deref()).await;
        let event_url = format!("{base_url}{event_path}");
        info!(
            conversation_id = %self.runtime.conversation_id(),
            event_url = %event_url,
            "Subscribing to OpenCode SSE stream"
        );
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
        let parsed: Value = match serde_json::from_str(data) {
            Ok(v) => v,
            Err(_) => return,
        };

        // `/global/event` wraps the event under `payload`; `/event` (legacy)
        // emits it raw. Normalize both to the inner event object here so every
        // `raw.get("type")` / `raw.get("properties")` below works unchanged.
        let raw = unwrap_event(parsed);

        let event_type = match raw.get("type").and_then(|v| v.as_str()) {
            Some(t) => t,
            None => return,
        };

        let props = match raw.get("properties") {
            Some(p) => p,
            None => return,
        };

        let session_id = props.get("sessionID").and_then(|v| v.as_str()).map(String::from);

        // OpenCode's `/global/event` SSE stream is global: a single connection
        // receives events for EVERY session on the server, including sessions
        // owned by other AionUi conversations pointed at the same server. Each
        // `RemoteAgentManager` runs its own reader, so without this guard every
        // manager would process every other conversation's events — bleeding
        // one thread's Start/text/Finish into another's stream.
        //
        // Three classes of `sessionID`s pass through the gate:
        //   1. Our own parent session (`opencode_session_id`).
        //   2. A registered child (sub-agent) of our parent — see
        //      [`subagent`]. Without this, every sub-agent invocation goes
        //      invisible (the reason the UI used to sit on `server.heartbeat`
        //      for minutes after an `Explore Task` chip landed).
        //   3. A `session.created` event whose `parentID` matches our parent;
        //      we register the child here and then continue processing.
        //
        // Events with no `sessionID` (`server.connected`, `server.heartbeat`,
        // etc.) are not session-scoped and always pass through.
        //
        // `is_child` is captured for the downstream handlers so they can stamp
        // `parent_session_id` onto the outgoing canonical events.
        let (is_child, parent_session_id) = if let Some(ref event_session) = session_id {
            let (own, is_registered_child) = {
                let state = self.state.read().await;
                (
                    state.opencode_session_id.clone(),
                    state.child_sessions.contains(event_session.as_str()),
                )
            };
            let own_ref = own.as_deref();
            let matches_own = own_ref == Some(event_session.as_str());

            // `session.created` is the registration trigger: examine the
            // payload's `parentID` *before* the gate, so a newly spawned child
            // session is recognized on its first event rather than being
            // dropped silently.
            let just_registered = if event_type == "session.created" && !matches_own {
                if let Some(own_id) = own_ref {
                    let now = now_ms();
                    let mut state = self.state.write().await;
                    if let Some(child) =
                        subagent::try_register_from_session_created(props, own_id, &mut state.child_sessions, now)
                    {
                        drop(state);
                        subagent::emit_started(&self.runtime, own_id, &child);
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            };

            if !matches_own && !is_registered_child && !just_registered {
                return;
            }
            // If we just registered, the rest of this `session.created` event
            // is consumed — there's no other useful work to do for it.
            if just_registered {
                return;
            }
            (
                !matches_own && is_registered_child,
                if matches_own { None } else { own.clone() },
            )
        } else {
            (false, None)
        };

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
                        // CRITICAL: only the ROOT session going idle ends the
                        // conversation turn. Each child sub-agent emits its
                        // own `session.status idle` when its sub-task wraps
                        // up; firing `Finish` for a child would kill the
                        // parent's stream relay while siblings are still
                        // working — see OpenCode's own event-reducer.ts which
                        // never treats child session.status as terminal.
                        if is_child {
                            if let (Some(child_id), Some(parent_id)) = (session_id.as_ref(), parent_session_id.as_ref())
                            {
                                let now = now_ms();
                                let snapshot = {
                                    let mut state = self.state.write().await;
                                    subagent::mark_completed(
                                        &mut state.child_sessions,
                                        child_id.as_str(),
                                        OpencodeSubtaskStatus::Completed,
                                        None,
                                        now,
                                    )
                                };
                                if let Some(child) = snapshot {
                                    subagent::emit_completed(&self.runtime, parent_id, &child, None);
                                }
                            }
                        } else {
                            self.runtime.emit(AgentStreamEvent::Finish(FinishEventData {
                                session_id: session_id.clone(),
                            }));
                            self.runtime.transition_to(ConversationStatus::Finished);
                            self.release_turn_slot().await;
                        }
                    }
                    _ => {}
                }
            }
            "session.idle" => {
                // A child (sub-agent) going idle does NOT end the parent turn —
                // it only marks the sub-agent complete. Emitting `Finish` for a
                // child would terminate the parent's stream relay, dropping any
                // subsequent text/tool events the parent still has to produce.
                if is_child {
                    if let (Some(child_id), Some(parent_id)) = (session_id.as_ref(), parent_session_id.as_ref()) {
                        let now = now_ms();
                        let snapshot = {
                            let mut state = self.state.write().await;
                            subagent::mark_completed(
                                &mut state.child_sessions,
                                child_id.as_str(),
                                OpencodeSubtaskStatus::Completed,
                                None,
                                now,
                            )
                        };
                        if let Some(child) = snapshot {
                            subagent::emit_completed(&self.runtime, parent_id, &child, None);
                        }
                    }
                } else {
                    self.runtime.emit(AgentStreamEvent::Finish(FinishEventData {
                        session_id: session_id.clone(),
                    }));
                    self.runtime.transition_to(ConversationStatus::Finished);
                    self.release_turn_slot().await;
                }
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
                if is_child {
                    if let (Some(child_id), Some(parent_id)) = (session_id.as_ref(), parent_session_id.as_ref()) {
                        warn!(
                            conversation_id = %self.runtime.conversation_id(),
                            child_session = %child_id,
                            error = message,
                            "OpenCode sub-agent session error"
                        );
                        let now = now_ms();
                        let snapshot = {
                            let mut state = self.state.write().await;
                            subagent::mark_completed(
                                &mut state.child_sessions,
                                child_id.as_str(),
                                OpencodeSubtaskStatus::Failed,
                                Some(message.to_string()),
                                now,
                            )
                        };
                        if let Some(child) = snapshot {
                            subagent::emit_completed(&self.runtime, parent_id, &child, Some(message.to_string()));
                        }
                    }
                } else {
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
                    match part.get("type").and_then(|v| v.as_str()) {
                        Some("reasoning") => {
                            // Track reasoning part IDs so `message.part.delta`
                            // can route deltas into the Thinking stream
                            // instead of the user-visible Text stream.
                            if let Some(part_id) = part.get("id").and_then(|v| v.as_str()) {
                                self.state.write().await.reasoning_parts.insert(part_id.to_string());
                            }
                        }
                        Some("tool") => {
                            // OpenCode streams shell/grep/edit/etc tool execution
                            // through repeated `message.part.updated` events whose
                            // `state.metadata.output` grows cumulatively. Translate
                            // each tick into an `AcpToolCall` event so the existing
                            // inline tool-card UI renders the live output. See
                            // `opencode_tool_call` for the mapping rules.
                            //
                            // When the event originates from a sub-agent (child)
                            // session, `parent_session_id` is threaded through so
                            // the renderer attaches the bubble to the nested
                            // transcript rather than the parent's top-level one.
                            if let Some(mut event) = opencode_tool_call::translate_message_part_updated(
                                props,
                                session_id.clone(),
                                parent_session_id.clone(),
                            ) {
                                // The translator defaults every event to `ToolCallUpdate`
                                // (merge) because OpenCode does not distinguish "new" from
                                // "updated" parts on the wire. The persistence layer
                                // (`stream_relay::persist_acp_tool_call`) requires the FIRST
                                // event for a given `tool_call_id` to be `ToolCall` (insert)
                                // — otherwise the update fails with "Record not found" and
                                // the tool card never lands in the DB (UI works in-flight
                                // but disappears on reload). Promote the first occurrence
                                // here while holding the write lock so concurrent SSE
                                // events for the same id can't both race to "first".
                                let is_first = {
                                    let mut state = self.state.write().await;
                                    state.opencode_tool_call_ids.insert(event.update.tool_call_id.clone())
                                };
                                if is_first {
                                    event.update.session_update = AcpToolCallSessionUpdateKind::ToolCall;
                                }
                                debug!(
                                    conversation_id = %self.runtime.conversation_id(),
                                    tool_call_id = %event.update.tool_call_id,
                                    first = is_first,
                                    is_child = is_child,
                                    "Forwarding OpenCode tool part update"
                                );
                                self.runtime.emit(AgentStreamEvent::AcpToolCall(event));

                                // For child sessions, also tick the rolling
                                // sub-agent summary so the collapsed Task chip
                                // can display `3 toolcalls · reading src/...`
                                // without forcing the user to expand.
                                if is_child
                                    && let (Some(child_id), Some(parent_id)) =
                                        (session_id.as_ref(), parent_session_id.as_ref())
                                {
                                    self.tick_subagent_progress(
                                        parent_id,
                                        child_id,
                                        part.get("callID").and_then(|v| v.as_str()).unwrap_or(""),
                                        part.get("tool").and_then(|v| v.as_str()),
                                    )
                                    .await;
                                }
                            }
                        }
                        _ => {}
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

                        if info.get("finish").and_then(|v| v.as_str()) == Some("stop") && !is_child {
                            // OpenCode emits no native usage event, but the
                            // finished assistant message carries `info.tokens`.
                            // Pair it with the model's context window (from the
                            // provider catalog) to synthesize the
                            // `acp_context_usage` event the renderer's meter
                            // already consumes (`{ used, size }`).
                            //
                            // This MUST be emitted before `Finish`: the stream
                            // relay treats `Finish` as terminal and breaks its
                            // loop, dropping any event emitted afterwards.
                            //
                            // CRITICAL gate on `!is_child`: each sub-agent's
                            // own assistant message also hits `finish=stop`
                            // when its sub-task ends. Without this guard, the
                            // first child to finish would terminate the
                            // parent's stream relay, abandoning siblings and
                            // any in-flight permission prompts (the symptom
                            // was "OpenCode quickly returns a failure" when
                            // the user couldn't approve in time — it wasn't
                            // OpenCode failing, it was us prematurely
                            // closing the relay on a sibling sub-agent's
                            // completion).
                            if let Some(tokens) = info.get("tokens") {
                                let used = opencode_models::context_tokens_used(tokens);
                                if used > 0 {
                                    let size = match info.get("modelID").and_then(|v| v.as_str()) {
                                        Some(model_id) => self.context_limit_for(model_id).await,
                                        None => 0,
                                    };
                                    self.runtime.emit(AgentStreamEvent::AcpContextUsage(json!({
                                        "used": used,
                                        "size": size,
                                    })));
                                }
                            }

                            self.runtime.emit(AgentStreamEvent::Finish(FinishEventData {
                                session_id: session_id.clone(),
                            }));
                            self.release_turn_slot().await;
                        }
                    }
                }
            }
            "todo.updated" => {
                // OpenCode emits `todo.updated` as a dedicated SSE event whenever
                // an agent calls the `todowrite` tool. Payload shape:
                //   { type: "todo.updated",
                //     properties: { sessionID: "ses_...", todos: [{content, status, priority}] } }
                // Map directly to the existing `Plan` event the frontend already
                // renders for ACP `SessionUpdate::Plan` notifications.
                if let Some(entries) = extract_opencode_todo_entries(props) {
                    self.runtime.emit(AgentStreamEvent::Plan(PlanEventData {
                        session_id: session_id.clone(),
                        entries,
                    }));
                }
            }
            "permission.asked" => {
                // Map OpenCode's permission request to AionUi's Confirmation
                // queue and emit the event the UI listens for. The user's
                // reply flows back through `confirm()` → POST
                // `/permission/{permID}/reply` (see the IAgentTask impl
                // below and `build_permission_reply_request`).
                //
                // Before queueing, consult the per-conversation auto-accept
                // sets so a user who has already blessed this directory tree
                // or this sub-agent gets the prompt resolved silently. This
                // is the path OpenCode's own `permission.tsx` walks via
                // `autoRespondsPermission` + `respondOnce`.
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
                let target_path = extract_permission_target_path(&metadata);

                // Auto-accept fast path. Walk both the path-prefix set and
                // the session-blessing set; either match short-circuits the
                // UI prompt.
                let auto_accept_hit = {
                    let state = self.state.read().await;
                    let path_hit = target_path
                        .as_deref()
                        .map(|t| path_is_under_blessed(t, &state.auto_accept_paths))
                        .unwrap_or(false);
                    let session_hit = session_id
                        .as_deref()
                        .map(|sid| session_or_ancestor_blessed(sid, &state.auto_accept_sessions, &state.child_sessions))
                        .unwrap_or(false);
                    path_hit || session_hit
                };
                if auto_accept_hit {
                    info!(
                        conversation_id = %self.runtime.conversation_id(),
                        request_id = %request_id,
                        permission = %permission,
                        session_id = ?session_id,
                        path = ?target_path,
                        "auto-accepting OpenCode permission via blessed prefix/session"
                    );
                    self.spawn_permission_response(request_id.clone(), session_id.clone(), "once".to_string());
                    // Stamp the dedupe map too so a stray `permission.asked`
                    // re-emit (OpenCode re-fires on reconnect) doesn't double-POST.
                    {
                        let mut state = self.state.write().await;
                        state.recently_replied_permissions.insert(request_id.clone(), now_ms());
                    }
                    return;
                }

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
                    .or_else(|| target_path.clone())
                    .unwrap_or_else(|| {
                        if metadata.as_object().map(|m| !m.is_empty()).unwrap_or(false) {
                            metadata.to_string()
                        } else if patterns.is_empty() {
                            String::new()
                        } else {
                            patterns.join(", ")
                        }
                    });

                // Build the option list. When the request carries a target
                // path we add "Allow this directory tree" so one click can
                // bless the whole tree for the rest of the conversation —
                // the fix for the cascade the user hit on the explore prompt.
                let mut options = vec![ConfirmationOption {
                    label: "Allow once".to_string(),
                    value: Value::String("once".to_string()),
                    params: None,
                }];
                if let Some(ref path) = target_path {
                    let mut params = std::collections::HashMap::new();
                    params.insert("path".to_string(), path.clone());
                    options.push(ConfirmationOption {
                        label: "Allow this directory tree (session)".to_string(),
                        value: Value::String("allow_dir".to_string()),
                        params: Some(params),
                    });
                }
                if session_id.is_some() && parent_session_id.is_some() {
                    // Sub-agent-attributed: offer "allow rest of this sub-agent"
                    let mut params = std::collections::HashMap::new();
                    if let Some(ref sid) = session_id {
                        params.insert("sessionID".to_string(), sid.clone());
                    }
                    options.push(ConfirmationOption {
                        label: "Allow rest of this sub-agent".to_string(),
                        value: Value::String("allow_session".to_string()),
                        params: Some(params),
                    });
                }
                options.push(ConfirmationOption {
                    label: "Allow always".to_string(),
                    value: Value::String("always".to_string()),
                    params: None,
                });
                options.push(ConfirmationOption {
                    label: "Reject".to_string(),
                    value: Value::String("reject".to_string()),
                    params: None,
                });

                let confirmation = Confirmation {
                    id: request_id.clone(),
                    call_id: request_id.clone(),
                    title: Some(title),
                    action: Some(permission.clone()),
                    description,
                    command_type: Some(permission.clone()),
                    options,
                    // Stamp the originating sessionId on the confirmation so
                    // the renderer can route the prompt to the right nested
                    // sub-agent transcript. `parent_session_id` is `None` for
                    // parent-session prompts and the parent's id for
                    // sub-agent-originated prompts.
                    session_id: session_id.clone(),
                    parent_session_id: parent_session_id.clone(),
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
            "session.next.tool.input.started"
            | "session.next.tool.input.delta"
            | "session.next.tool.input.ended"
            | "session.next.tool_input.started"
            | "session.next.tool_input.delta"
            | "session.next.tool_input.ended"
            | "message.next.tool.input.started"
            | "message.next.tool.input.delta"
            | "message.next.tool.input.ended" => {
                // Streamed tool-input arguments — the model constructing JSON
                // before the tool is invoked. Surfaces a "Constructing
                // arguments…" affordance per tool call so the user can pre-empt
                // a wrong call before execution. See [`opencode_stream`].
                let sid = match session_id.as_deref() {
                    Some(s) => s,
                    None => return,
                };
                if let Some(event) =
                    opencode_stream::translate_tool_input(event_type, props, sid, parent_session_id.clone())
                {
                    self.runtime.emit(event);
                }
            }
            "session.next.tool.progress" | "session.next.tool_progress" | "message.next.tool.progress" => {
                // Long-running tool progress — `bash` stdout chunks, `grep`
                // file counters, MCP `{step, percent}` shapes, etc. See
                // [`opencode_stream::translate_tool_progress`].
                let sid = match session_id.as_deref() {
                    Some(s) => s,
                    None => return,
                };
                if let Some(event) =
                    opencode_stream::translate_tool_progress(props, sid, parent_session_id.clone(), now_ms())
                {
                    self.runtime.emit(event);
                }
            }
            _ => {
                debug!(
                    conversation_id = %self.runtime.conversation_id(),
                    event_type = event_type,
                    is_child = is_child,
                    "Unhandled OpenCode event"
                );
            }
        }
    }

    /// Update a child session's rolling summary on each tool-part tick and
    /// emit a debounced [`crate::protocol::events::OpencodeSubtaskEventData`]
    /// progress event when something user-visible changed. Throttled to 500 ms
    /// cadence per child so a busy sub-agent does not spam the renderer.
    async fn tick_subagent_progress(&self, parent_id: &str, child_id: &str, part_id: &str, tool_name: Option<&str>) {
        let now = now_ms();
        let (changed, last_ms, snapshot) = {
            let mut state = self.state.write().await;
            let changed = subagent::note_tool_part(&mut state.child_sessions, child_id, part_id, tool_name, now);
            let last = state.last_subtask_progress_ms.get(child_id).copied().unwrap_or(0);
            let snap = state.child_sessions.get(child_id).cloned();
            (changed, last, snap)
        };
        let due = now.saturating_sub(last_ms) >= 500;
        if changed
            && due
            && let Some(child) = snapshot
        {
            {
                let mut state = self.state.write().await;
                state.last_subtask_progress_ms.insert(child_id.to_string(), now);
            }
            subagent::emit_progress(&self.runtime, parent_id, &child);
        }
    }

    /// Fire-and-forget POST of an OpenCode permission response via the
    /// canonical `POST /permission/{permID}/reply` endpoint (body `{reply}`).
    /// See [`build_permission_reply_request`] for the endpoint-discovery notes.
    ///
    /// `session_id` is retained for call-site compatibility (and possible
    /// future diagnostics) but is not required to address the permission —
    /// the canonical endpoint is keyed by permission id alone.
    ///
    /// Shared by the auto-accept short-circuit in the `permission.asked`
    /// handler and by `confirm()` itself. Returns immediately; the task
    /// completes in the background. Errors are logged but don't propagate.
    fn spawn_permission_response(&self, request_id: String, session_id: Option<String>, reply: String) {
        if !is_opencode_protocol(&self.remote_config.protocol) {
            return;
        }
        let _ = &session_id;
        let base_url = normalize_base_url(&self.remote_config.url);
        let auth_header = build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());
        let http_client = self.http_client.clone();
        let conversation_id = self.runtime.conversation_id().to_string();
        tokio::spawn(async move {
            let (url, body) = build_permission_reply_request(&base_url, &request_id, &reply);
            let mut req = http_client.post(&url).json(&body).timeout(Duration::from_secs(10));
            if let Some(h) = auth_header {
                req = req.header(AUTHORIZATION, h);
            }
            match req.send().await {
                Ok(resp) if resp.status().is_success() => {
                    debug!(
                        conversation_id = %conversation_id,
                        request_id = %request_id,
                        reply = %reply,
                        endpoint = %url,
                        "OpenCode permission response sent (auto)"
                    );
                }
                Ok(resp) => {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    warn!(
                        conversation_id = %conversation_id,
                        request_id = %request_id,
                        status = %status,
                        body = %body,
                        endpoint = %url,
                        "OpenCode permission response returned non-success (auto)"
                    );
                }
                Err(e) => {
                    warn!(
                        conversation_id = %conversation_id,
                        request_id = %request_id,
                        error = %e,
                        endpoint = %url,
                        "OpenCode permission response request failed (auto)"
                    );
                }
            }
        });
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

    /// Resolve a model's context window (in tokens) from OpenCode's provider
    /// catalog, fetching and caching it on first use. Returns `0` when the
    /// model is unknown or the catalog can't be reached — the renderer then
    /// falls back to its default context limit.
    async fn context_limit_for(&self, model_id: &str) -> u64 {
        {
            let guard = self.state.read().await;
            if let Some(ref cached) = guard.model_context_limits {
                return cached.get(model_id).copied().unwrap_or(0);
            }
        }
        let base_url = normalize_base_url(&self.remote_config.url);
        let auth_header = build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());
        let fetched = opencode_models::fetch_context_limits(&self.http_client, &base_url, auth_header.as_deref()).await;
        debug!(
            conversation_id = %self.runtime.conversation_id(),
            model_count = fetched.len(),
            "Populated OpenCode model context-limit cache"
        );
        let limit = fetched.get(model_id).copied().unwrap_or(0);
        let mut guard = self.state.write().await;
        guard.model_context_limits = Some(fetched);
        limit
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

    /// Release this conversation's hold on the per-OpenCode turn slot
    /// acquired in [`Self::opencode_send`]. Safe to call whether or not
    /// we currently own it — no-op when another conversation has taken
    /// over (after a wait-timeout). Idempotent across the multiple
    /// `Finish` emission paths (`session.updated → idle`, `session.idle`,
    /// `message.part.updated → finish=stop`), only the first call does
    /// the work.
    async fn release_turn_slot(&self) {
        let base_url = normalize_base_url(&self.remote_config.url);
        opencode_mcp::release_turn(&base_url, self.runtime.conversation_id()).await;
    }

    /// Ensure this conversation owns the OpenCode `aionui-local-fs` slot
    /// and has a live `LocalFsMcpServer` backing it.
    ///
    /// The OpenCode MCP registry is instance-global: a single slot named
    /// [`opencode_mcp::MCP_NAME`] is shared across every AionUI
    /// conversation talking to the same OpenCode instance. This method has
    /// three modes:
    ///
    /// 1. **No local server yet** — start one for this conversation's
    ///    workspace, register it with OpenCode, claim the slot.
    /// 2. **Local server exists and we still own the slot** — fast no-op.
    /// 3. **Local server exists but another conversation took the slot** —
    ///    re-register the existing server (no restart, port stays stable),
    ///    re-claim. This is the typical case when the user switches tabs:
    ///    the other tab's prompt re-pointed the slot, and now we need it
    ///    back before our own prompt is sent.
    ///
    /// Failures are logged, never returned — the agent must still
    /// function (degraded) if MCP registration fails.
    async fn ensure_local_fs_mcp(&self, base_url: &str, auth_header: Option<&str>) {
        let conversation_id = self.runtime.conversation_id().to_string();

        // Cheap fast-path: if we have a server AND we own the slot, nothing to do.
        // Snapshot the server's identity outside the lock so we don't hold the
        // mutex across the (potentially network-bound) re-registration call.
        let existing = {
            let guard = self.local_fs_mcp.lock().await;
            guard
                .as_ref()
                .map(|s| (s.bind_addr().port(), s.auth_token().to_string(), s.contact_probe()))
        };

        if let Some((port, token, probe)) = existing {
            if opencode_mcp::owns_slot(base_url, &conversation_id, port) {
                return;
            }
            // Server is alive, but the slot belongs to another conversation
            // (or no one). Re-register the existing server URL to take it
            // back. Port/token stay stable, so the OpenCode mcp.add just
            // replaces the URL/headers on its side.
            if let Err(e) = opencode_mcp::ensure_slot_owned(
                &self.http_client,
                base_url,
                auth_header,
                &conversation_id,
                port,
                &token,
                &probe,
            )
            .await
            {
                warn!(
                    conversation_id = %conversation_id,
                    error = %e,
                    "failed to reclaim local fs MCP slot — client-side fs may misroute this turn"
                );
            }
            return;
        }

        // No local server yet — cold start.
        let workspace = self.runtime.workspace().to_string();

        // Approver lets the MCP server's `run_shell` tool gate each command
        // on the user's confirmation UI. Built from shared handles (cloned
        // `Arc` + `AgentRuntime`) so it outlives this borrowed-`self` call.
        // The same struct also implements `ElicitationHandler`, so tools that
        // need to raise a free-form schema-driven prompt can park on it the
        // same way. See [`RemoteShellApprover`].
        let approver = Arc::new(RemoteShellApprover {
            runtime: self.runtime.clone(),
            state: Arc::clone(&self.state),
        });
        let shell_approver: Arc<dyn ShellApprover> = approver.clone();
        let elicitation_handler: Arc<dyn crate::manager::remote::local_fs_mcp::ElicitationHandler> = approver;

        match opencode_mcp::start_and_register(
            &self.http_client,
            base_url,
            auth_header,
            &conversation_id,
            &workspace,
            Some(shell_approver),
            Some(elicitation_handler),
        )
        .await
        {
            Ok(server) => {
                // Capture what the guardian needs before the server moves
                // into the Mutex; the server keeps running on the same port
                // across network changes, so re-registration only needs the
                // port, token, and contact probe.
                let port = server.bind_addr().port();
                let token = server.auth_token().to_string();
                let probe = server.contact_probe();
                *self.local_fs_mcp.lock().await = Some(server);

                let guardian = opencode_mcp::spawn_reachability_guardian(
                    self.http_client.clone(),
                    base_url.to_string(),
                    auth_header.map(str::to_string),
                    conversation_id.clone(),
                    port,
                    token,
                    probe,
                );
                if let Some(old) = self.reachability_guardian.lock().await.replace(guardian) {
                    old.abort();
                }
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
        let conversation_id = self.runtime.conversation_id().to_string();

        // Serialize prompts per OpenCode instance: block until any other
        // conversation's in-flight turn finishes on this `base_url`. The
        // `aionui-local-fs` MCP slot is a single named registration per
        // OpenCode instance, so two conversations cannot have overlapping
        // tool calls without one of them landing on the wrong workspace
        // (and surfacing approval prompts in the wrong UI tab — see
        // `opencode_mcp::TURN_SIGNALS` for the failure mode). Released on
        // every `Finish` emission below, in `kill()` teardown, OR in the
        // error-path arm at the bottom of this function if we never even
        // got to `POST /prompt_async`.
        opencode_mcp::acquire_turn(&base_url, &conversation_id).await;

        let result = self.opencode_send_after_acquire(content, &base_url).await;
        if result.is_err() {
            // No prompt was dispatched (or it was rejected outright) — no
            // SSE Finish event will ever fire, so we must drop the slot
            // ourselves or the next conversation deadlocks for
            // `TURN_WAIT_TIMEOUT` before force-acquiring.
            opencode_mcp::release_turn(&base_url, &conversation_id).await;
        }
        result
    }

    /// Body of [`Self::opencode_send`] after the per-base-url turn slot has
    /// been acquired. Split out so the caller can centralize the
    /// release-on-error logic without sprinkling `release_turn` calls at
    /// every `?` site.
    async fn opencode_send_after_acquire(&self, content: &str, base_url: &str) -> Result<(), AppError> {
        let base_url = base_url.to_string();
        // Re-confirm ownership of the OpenCode `aionui-local-fs` slot
        // before every prompt. If another conversation prompted last on
        // the same OpenCode instance, the slot now points at *their* MCP
        // server (rooted at *their* workspace). Re-registering here puts
        // it back on our server so the tool calls the model emits this
        // turn land on the right project. No-op when we already own the
        // slot. Best-effort — failures are logged inside, never thrown.
        let auth_header_for_mcp =
            build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());
        self.ensure_local_fs_mcp(&base_url, auth_header_for_mcp.as_deref())
            .await;

        // Track whether this call created a fresh server-side session.
        // Phase 4c: when we just spun up a new session AND the
        // conversation has prior turns in Chisl's local DB, prepend
        // a framed transcript so the agent picks up where it left off
        // even though OpenCode's own context is empty.
        let mut session_just_created = false;
        let session_id = {
            let mut state = self.state.write().await;
            if state.opencode_session_id.is_none() {
                let id = self.opencode_create_session(&base_url).await?;
                state.opencode_session_id = Some(id);
                session_just_created = true;
            }
            state.opencode_session_id.clone().unwrap()
        };
        let context_prefix = if session_just_created {
            self.build_context_transcript_prefix().await
        } else {
            None
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
        let shell_hint = super::local_fs_mcp::shell::shell_hint();
        let system_hint = format!(
            "The user's project is located at {workspace} on their local machine. \
             Use ONLY the aionui-local-fs_* tools for all file operations \
             (aionui-local-fs_read_file, aionui-local-fs_list_dir, aionui-local-fs_write_file, \
             aionui-local-fs_grep_dir, aionui-local-fs_run_shell, etc.). \
             These tools operate on the user's actual project files. \
             All file paths should be relative to the project root (e.g. \"src/main.rs\"), \
             not absolute. Before claiming a file or directory does not exist, ALWAYS call \
             aionui-local-fs_list_dir or aionui-local-fs_read_file on it — do not rely on \
             memory of prior turns. To run terminal commands — build, test, lint, git, anything \
             you need to verify your work — you MUST use the aionui-local-fs_run_shell tool; \
             your own built-in shell runs on a different machine and cannot see this project. \
             Commands execute on the user's machine ({shell_hint}), in the project root, and \
             each one requires the user to approve it first, so write commands in that shell's \
             syntax and prefer one combined command over many. The current project layout \
             (gitignore-respecting; may be truncated) is:\n\n{tree}"
        );

        // Phase 4c: if we just brokered a fresh session and Chisl has
        // a local transcript for this conversation, prepend it. We use
        // a single combined text part rather than two parts so the
        // model sees the framing as one user turn — splitting it would
        // make OpenCode treat the framing block as a standalone prompt
        // and emit an extra assistant response before the real one.
        let prompt_text = match context_prefix.as_deref() {
            Some(prefix) if !prefix.is_empty() => format!("{prefix}\n\n{content}"),
            _ => content.to_string(),
        };
        let mut body = json!({
            "parts": [{"type": "text", "text": prompt_text}],
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

        // Surface the silent failure mode where the system hint instructs the
        // model to use `aionui-local-fs_*` tools but no local fs MCP is
        // registered. The user-visible symptom is "Unable to connect" from the
        // model; without this log there is nothing in production logs to
        // explain why. Best-effort observability — never blocks the prompt.
        if self.local_fs_mcp.lock().await.is_none() {
            warn!(
                conversation_id = %self.runtime.conversation_id(),
                "dispatching OpenCode prompt without a local fs MCP registration — \
                 client-side filesystem tools will not work this turn"
            );
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

    /// Build a framed transcript of the conversation's prior local
    /// turns so the agent has context after a fresh server-side
    /// session is created. Phase 4c: this only runs when the old
    /// OpenCode session id was found dead in `connect()` AND
    /// `opencode_send` is creating a new one. Returns `None` when:
    ///
    /// - no `conversation_repo` was wired in (test constructors),
    /// - the conversation has no usable history rows, or
    /// - the read fails (best-effort — never block the user's prompt).
    ///
    /// Format: a single `<chisl-context>` block with `[USER]:` /
    /// `[ASSISTANT]:` markers, followed by a "Continue from this
    /// context." instruction. The user's actual prompt is appended
    /// after the block by the caller, so the model sees one merged
    /// user turn — splitting would make OpenCode emit an assistant
    /// reply to the framing before reaching the real question.
    async fn build_context_transcript_prefix(&self) -> Option<String> {
        let repo = self.conversation_repo.as_ref()?;
        let conv_id = self.runtime.conversation_id().to_string();
        // 10k page size matches the renderer's load size — handles any
        // realistic transcript in a single round-trip without paging.
        let result = match repo.get_messages(&conv_id, 0, 10_000, aionui_db::SortOrder::Asc).await {
            Ok(r) => r,
            Err(e) => {
                warn!(
                    conversation_id = %conv_id,
                    error = %e,
                    "could not read local transcript for context-dump; new session starts without prior context"
                );
                return None;
            }
        };
        let mut lines = Vec::<String>::new();
        for row in &result.items {
            let position = row.position.as_deref().unwrap_or("");
            // Skip the just-inserted user message at the end of the
            // transcript — the caller appends it again via `content`.
            // We detect it as the final row whose position == "right".
            let speaker = match (position, row.r#type.as_str()) {
                ("right", "text") => "[USER]",
                ("left", "text") => "[ASSISTANT]",
                // Tool calls and thinking blocks are noisy and rarely
                // useful as raw context — surface them as compact
                // descriptors so the agent sees that work happened
                // without being confused by stale tool payloads.
                ("left", "thinking") => {
                    lines.push("[ASSISTANT][thinking]".to_string());
                    continue;
                }
                ("left", "tool_call") => {
                    let tool = serde_json::from_str::<serde_json::Value>(&row.content)
                        .ok()
                        .and_then(|v| v.get("name").and_then(|n| n.as_str()).map(String::from))
                        .unwrap_or_else(|| "tool".to_string());
                    lines.push(format!("[ASSISTANT][used tool: {tool}]"));
                    continue;
                }
                _ => continue,
            };
            let parsed: serde_json::Value = match serde_json::from_str(&row.content) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let text = parsed.get("content").and_then(|v| v.as_str()).unwrap_or("").trim();
            if text.is_empty() {
                continue;
            }
            lines.push(format!("{speaker}: {text}"));
        }
        // Drop the trailing user line: that's the prompt we're about
        // to send. Without this, the agent would see the new prompt
        // twice — once inside the transcript and once as the real
        // user turn appended after.
        if matches!(lines.last().map(String::as_str), Some(s) if s.starts_with("[USER]:")) {
            lines.pop();
        }
        if lines.is_empty() {
            return None;
        }
        let transcript = lines.join("\n");
        info!(
            conversation_id = %conv_id,
            turns = result.items.len(),
            "injecting Chisl-local transcript into freshly brokered OpenCode session"
        );
        Some(format!(
            "<chisl-context>\nThe OpenCode session backing this conversation was \
             reset, so the server has no memory of the prior turns. The conversation \
             so far on the user's machine was:\n\n{transcript}\n</chisl-context>\n\n\
             Continue from this context. The next message is the user's new prompt."
        ))
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

        // Auto-reject any permissions left over from a prior turn. If the
        // model parked itself on an un-answered tool approval and the user
        // then typed a new prompt rather than approving, the prior turn must
        // be released before this turn can stream — otherwise the parked
        // `run_shell` (or OpenCode permission) keeps the previous assistant
        // message in `busy` state, and the new prompt's events interleave
        // with stale tool output.
        self.reject_pending_confirmations("new_prompt").await;

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
            // Nothing has been started on the server until a session exists.
            let session_id = { self.state.read().await.opencode_session_id.clone() };
            let Some(session_id) = session_id else {
                return Ok(());
            };

            // Fire-and-forget the interrupt so the UI's stop button returns
            // instantly instead of blocking on a network round-trip. OpenCode's
            // `POST /session/{id}/abort` halts in-flight generation server-side,
            // which is what actually stops token spend.
            let http_client = self.http_client.clone();
            let base_url = normalize_base_url(&self.remote_config.url);
            let auth_header =
                build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());
            let conversation_id = self.runtime.conversation_id().to_string();
            tokio::spawn(async move {
                let url = format!("{base_url}/session/{session_id}/abort");
                let mut req = http_client.post(&url).timeout(Duration::from_secs(10));
                if let Some(ref h) = auth_header {
                    req = req.header(AUTHORIZATION, h.as_str());
                }
                match req.send().await {
                    Ok(resp) if resp.status().is_success() => {
                        info!(%conversation_id, %session_id, "OpenCode session aborted");
                    }
                    Ok(resp) => {
                        let status = resp.status();
                        let body = resp.text().await.unwrap_or_default();
                        warn!(%conversation_id, %session_id, %status, %body, "OpenCode abort returned non-success");
                    }
                    Err(e) => {
                        warn!(%conversation_id, %session_id, error = %e, "OpenCode abort request failed");
                    }
                }
            });

            // Explicit auto-reject so OpenCode-side permissions get an HTTP
            // reply and the parked `run_shell` MCP calls all wake up with
            // Reject — the previous in-line `clear()` only dropped the senders
            // (which still produces Reject via the channel-closed handler)
            // but never told the server-side permissions they were cancelled.
            self.reject_pending_confirmations("cancel").await;
            return Ok(());
        }
        if self.ws_sink.lock().await.is_none() {
            return Err(AppError::Conflict("WebSocket not connected; nothing to cancel".into()));
        }
        let payload = json!({ "type": "session/cancel", "data": {} });
        self.ws_send(&payload).await?;
        self.reject_pending_confirmations("cancel").await;
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

        // Drop any parked shell approvals first. Each one is holding a
        // `run_shell` MCP request open while it awaits the user; dropping the
        // senders makes them resolve to Reject so those requests complete —
        // otherwise the server's graceful shutdown below would block on them.
        if let Ok(mut state) = self.state.try_write() {
            state.confirmations.clear();
            state.pending_shell_approvals.clear();
            // Pending elicitations also park on a oneshot — dropping the
            // senders resolves them to `Declined` so the tool calls unblock
            // and the MCP server can shut down without hanging.
            state.pending_elicitations.clear();
        }

        // Stop the reachability guardian before teardown so it can't
        // re-register against a server we're about to drop.
        if let Ok(mut guard) = self.reachability_guardian.try_lock()
            && let Some(handle) = guard.take()
        {
            handle.abort();
        }

        // Take the MCP server out synchronously so the OS port frees
        // immediately on Drop; the OpenCode disconnect runs on a
        // detached task because kill() is sync. The detached task also
        // releases the per-base-url turn slot so a waiting conversation
        // can proceed — without this, killing mid-turn would leave the
        // slot held until the timeout fires (`TURN_WAIT_TIMEOUT`).
        if let Ok(mut guard) = self.local_fs_mcp.try_lock()
            && let Some(server) = guard.take()
        {
            let http_client = self.http_client.clone();
            let base_url = normalize_base_url(&self.remote_config.url);
            let auth_header =
                build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());
            let conversation_id = self.runtime.conversation_id().to_string();
            tokio::spawn(async move {
                opencode_mcp::release_turn(&base_url, &conversation_id).await;
                opencode_mcp::disconnect_from_opencode(
                    &http_client,
                    &base_url,
                    auth_header.as_deref(),
                    &conversation_id,
                )
                .await;
                server.shutdown().await;
            });
        } else {
            // No local fs MCP to dispose, but still need to free a turn
            // slot we may be holding (e.g. kill before a Finish fired).
            let base_url = normalize_base_url(&self.remote_config.url);
            let conversation_id = self.runtime.conversation_id().to_string();
            tokio::spawn(async move {
                opencode_mcp::release_turn(&base_url, &conversation_id).await;
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
        // Normalize the UI's choice up front — both the local shell-approval
        // path and the OpenCode path need it. Prefer the explicit option
        // value the frontend sent. Five values are accepted:
        //   - "once" / "always" / "reject": canonical OpenCode replies that
        //     are POSTed as-is to the permission endpoint.
        //   - "allow_dir": "Allow this directory tree (session)" — adds the
        //     `data.params.path` (or `data.path`) to the per-conversation
        //     auto-accept set, replies "once" for this request, and drains
        //     other pending confirmations whose target paths are descendants.
        //   - "allow_session": "Allow rest of this sub-agent" — adds the
        //     `data.params.sessionID` to the per-conversation auto-accept
        //     sessions set, replies "once", and drains matching pending
        //     confirmations.
        // Fall back to the always_allow flag, then "once".
        let raw_reply = data
            .as_str()
            .map(str::to_owned)
            .or_else(|| data.get("value").and_then(|v| v.as_str()).map(str::to_owned));
        let reply = raw_reply
            .clone()
            .filter(|r| matches!(r.as_str(), "once" | "always" | "reject" | "allow_dir" | "allow_session"))
            .unwrap_or_else(|| {
                if always_allow {
                    "always".to_string()
                } else {
                    "once".to_string()
                }
            });

        // Extract the path/sessionID parameters that the "allow_dir" /
        // "allow_session" options carry. The UI sends them as
        // `data.params.{path,sessionID}` (mirrors how `ConfirmationOption.params`
        // round-trips through `conversation.confirmMessage`).
        let extra_path = data
            .get("params")
            .and_then(|p| p.get("path"))
            .or_else(|| data.get("path"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let extra_session = data
            .get("params")
            .and_then(|p| p.get("sessionID"))
            .or_else(|| data.get("sessionID"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        // Snapshotted across the state lock so the spawned HTTP task can
        // address the canonical per-session endpoint.
        let mut originating_session_id: Option<String> = None;

        if let Ok(mut state) = self.state.try_write() {
            // In-process shell approval? These originate from our own local
            // fs MCP server (call_id "shell-…"), so resolve them by waking
            // the parked `run_shell` tool call — there is no OpenCode-side
            // permission to reply to (that POST would 404).
            if let Some(tx) = state.pending_shell_approvals.remove(call_id) {
                if reply == "always" {
                    state.approval_memory.insert(shell_approval_key(), true);
                }
                state.confirmations.retain(|c| c.call_id != call_id);
                drop(state);
                let decision = if reply == "reject" {
                    ShellApproval::Reject
                } else {
                    ShellApproval::Allow
                };
                let _ = tx.send(decision);
                return Ok(());
            }

            // In-process MCP elicitation? `call_id` "elicit-…" identifies a
            // parked tool call waiting for the user's schema-driven response.
            // The renderer's reply lands in `data` either as the raw response
            // payload or as `{ value: "submit"|"cancel", payload: <user JSON> }`.
            // We accept both shapes here so a minimal renderer that only sends
            // `value` cleanly cancels.
            if let Some(tx) = state.pending_elicitations.remove(call_id) {
                state.confirmations.retain(|c| c.call_id != call_id);
                drop(state);
                let payload = if reply == "cancel" || reply == "reject" {
                    None
                } else {
                    // Prefer an explicit `payload` field when present; fall
                    // back to the entire `data` value (allows a renderer to
                    // POST `{value: "submit", payload: ...}` or just the bare
                    // submitted object).
                    Some(data.get("payload").cloned().unwrap_or_else(|| data.clone()))
                };
                let _ = tx.send(payload);
                return Ok(());
            }

            if reply == "always"
                && let Some(conf) = state.confirmations.iter().find(|c| c.call_id == call_id)
            {
                let key = approval_key(conf.action.as_deref(), conf.command_type.as_deref());
                state.approval_memory.insert(key, true);
            }
            // Snapshot the originating session id (for sub-agent-attributed
            // permissions) BEFORE we strip the confirmation. The canonical
            // OpenCode permission-reply endpoint is `/permission/{permID}/reply`
            // (body `{reply}`) — the session-scoped variant
            // `/session/{sessionID}/permissions/{permID}` is deprecated as of
            // opencode 1.15.11 (see `build_permission_reply_request`). The
            // sessionID is no longer required to address the permission, but we
            // still snapshot it for diagnostics / future use.
            originating_session_id = state
                .confirmations
                .iter()
                .find(|c| c.call_id == call_id)
                .and_then(|c| c.session_id.clone());
            state.confirmations.retain(|c| c.call_id != call_id);

            // For "allow_dir" / "allow_session", record the blessing and
            // build the list of OTHER pending confirmations whose target
            // path / session matches — those will be auto-resolved with
            // `once` after we drop the lock. Stash the list locally; the
            // POSTs happen in the spawned task block further down.
            //
            // We collect into `drain_now: Vec<(call_id, session_id)>` so
            // that 14 cascading external_directory prompts collapse into
            // one user click. The HTTP POSTs use the same
            // `spawn_permission_response` helper as the auto-accept fast
            // path, so they hit the canonical endpoint.
            //
            // `drain_now` is held outside the lock and processed below.
        }

        // Apply "allow_dir" / "allow_session" effects: update the auto-accept
        // set and collect matching pending confirmations to drain. Must
        // re-acquire the lock since `try_write` released above.
        let mut drain_now: Vec<(String, Option<String>)> = Vec::new();
        if (reply == "allow_dir" || reply == "allow_session")
            && let Ok(mut state) = self.state.try_write()
        {
            if reply == "allow_dir" {
                if let Some(ref p) = extra_path {
                    let normalized = p.trim_end_matches('/').to_string();
                    let was_new = state.auto_accept_paths.insert(normalized.clone());
                    if was_new {
                        info!(
                            conversation_id = %self.runtime.conversation_id(),
                            path = %normalized,
                            "user blessed directory tree for the rest of this conversation"
                        );
                    }
                }
            } else if reply == "allow_session"
                && let Some(ref sid) = extra_session
            {
                let was_new = state.auto_accept_sessions.insert(sid.clone());
                if was_new {
                    info!(
                        conversation_id = %self.runtime.conversation_id(),
                        session_id = %sid,
                        "user blessed sub-agent for the rest of this conversation"
                    );
                }
            }

            // Walk currently-queued confirmations and drain ones that
            // now match (besides the one we just answered). Snapshot the
            // child-session registry first so the retain closure doesn't
            // borrow `state` twice.
            let blessed_paths = state.auto_accept_paths.clone();
            let blessed_sessions = state.auto_accept_sessions.clone();
            let registry_snapshot = state.child_sessions.clone();
            let mut to_drain: Vec<(String, Option<String>)> = Vec::new();
            state.confirmations.retain(|c| {
                if c.call_id == call_id {
                    return false; // already removed above; defensive
                }
                // The Confirmation doesn't carry the original metadata
                // path, so we approximate by checking whether the
                // description (which we populated from
                // `metadata.filepath`/`parentDir` in the `permission.asked`
                // handler) is covered by any blessed prefix. Best-effort —
                // session-hit below is the exact match.
                let path_hit = !blessed_paths.is_empty() && path_is_under_blessed(&c.description, &blessed_paths);
                let sess_hit = c
                    .session_id
                    .as_deref()
                    .map(|sid| session_or_ancestor_blessed(sid, &blessed_sessions, &registry_snapshot))
                    .unwrap_or(false);
                if path_hit || sess_hit {
                    to_drain.push((c.call_id.clone(), c.session_id.clone()));
                    false
                } else {
                    true
                }
            });
            // Mark them in the dedupe map so a stray duplicate POST or
            // re-emit won't double-fire after our auto-respond.
            let now = now_ms();
            for (id, _) in &to_drain {
                state.recently_replied_permissions.insert(id.clone(), now);
            }
            drain_now = to_drain;
        }

        if is_opencode_protocol(&self.remote_config.protocol) {
            // Dedupe rapid re-fires of the same call_id (re-render races, the
            // user double-clicking, batched "approve all" hitting the same id
            // twice). Without this, OpenCode returns 404
            // PermissionNotFoundError on the second POST. 60 s TTL, capped at
            // 1024 entries.
            const REPLY_DEDUP_TTL_MS: i64 = 60_000;
            const REPLY_DEDUP_CAP: usize = 1024;
            let now = now_ms();
            let already_replied = if let Ok(mut state) = self.state.try_write() {
                state
                    .recently_replied_permissions
                    .retain(|_, ts| now.saturating_sub(*ts) < REPLY_DEDUP_TTL_MS);
                if state.recently_replied_permissions.len() > REPLY_DEDUP_CAP {
                    let oldest: Vec<String> = {
                        let mut items: Vec<(&String, &TimestampMs)> =
                            state.recently_replied_permissions.iter().collect();
                        items.sort_by_key(|(_, ts)| **ts);
                        items
                            .iter()
                            .take(state.recently_replied_permissions.len() - REPLY_DEDUP_CAP)
                            .map(|(k, _)| (*k).clone())
                            .collect()
                    };
                    for k in oldest {
                        state.recently_replied_permissions.remove(&k);
                    }
                }
                if state.recently_replied_permissions.contains_key(call_id) {
                    true
                } else {
                    state.recently_replied_permissions.insert(call_id.to_string(), now);
                    false
                }
            } else {
                false
            };
            if already_replied {
                debug!(
                    conversation_id = %self.runtime.conversation_id(),
                    request_id = %call_id,
                    "suppressed duplicate OpenCode permission reply"
                );
                return Ok(());
            }

            // Translate Chisl-internal "allow_dir" / "allow_session" reply
            // values into OpenCode's canonical "once" for the wire — the
            // blessing has already been recorded in
            // `auto_accept_paths`/`auto_accept_sessions` above so subsequent
            // requests auto-resolve.
            let wire_reply = match reply.as_str() {
                "allow_dir" | "allow_session" => "once".to_string(),
                _ => reply.clone(),
            };

            // Drain any pending confirmations that the blessing covers — for
            // each, fire the canonical POST in the background. We've already
            // removed them from `state.confirmations` above, so the UI will
            // stop showing them on its next list refresh. Stamping them in
            // `recently_replied_permissions` above protects against a
            // double-POST if OpenCode also re-emits `permission.asked`.
            let drain_count = drain_now.len();
            for (drain_id, drain_session) in drain_now {
                self.spawn_permission_response(drain_id, drain_session, "once".to_string());
            }
            if drain_count > 0 {
                info!(
                    conversation_id = %self.runtime.conversation_id(),
                    drain_count,
                    "auto-resolved {drain_count} pending permissions via blessing"
                );
            }

            let base_url = normalize_base_url(&self.remote_config.url);
            let auth_header =
                build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());
            let http_client = self.http_client.clone();
            let conversation_id = self.runtime.conversation_id().to_string();
            let call_id = call_id.to_string();
            let _originating_session_id = originating_session_id;
            let wire_for_log = wire_reply.clone();
            // Clone the state handle so the background task can clear the
            // dedup entry on a confirmed 2xx (plan C05 §3.3) — letting a quick
            // re-prompt with the same id re-reply instead of being suppressed.
            let dedup_state = Arc::clone(&self.state);
            tokio::spawn(async move {
                // Canonical permission-reply endpoint (`/permission/{id}/reply`,
                // body `{reply}`) — see `build_permission_reply_request`.
                let (url, body) = build_permission_reply_request(&base_url, &call_id, &wire_reply);
                let mut req = http_client.post(&url).json(&body).timeout(Duration::from_secs(10));
                if let Some(h) = auth_header {
                    req = req.header(AUTHORIZATION, h);
                }
                match req.send().await {
                    Ok(resp) if resp.status().is_success() => {
                        // Confirmed success — drop the dedup stamp so a fresh
                        // re-prompt with the same id can be replied to again.
                        dedup_state.write().await.recently_replied_permissions.remove(&call_id);
                        info!(
                            conversation_id = %conversation_id,
                            request_id = %call_id,
                            reply = %wire_for_log,
                            endpoint = %url,
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
                            endpoint = %url,
                            "OpenCode permission reply returned non-success"
                        );
                    }
                    Err(e) => {
                        warn!(
                            conversation_id = %conversation_id,
                            request_id = %call_id,
                            error = %e,
                            endpoint = %url,
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

    /// The OpenCode session id (`ses_...`) to persist for resume, if one has
    /// been established. Read by the conversation service after each turn and
    /// written to `conversation.extra.sessionKey`. Mirrors
    /// `OpenClawAgentManager::get_session_key`. Returns `None` for non-OpenCode
    /// protocols (the field is only ever set on the OpenCode HTTP path) and
    /// before the first session is created.
    pub fn get_session_key(&self) -> Option<String> {
        self.state.try_read().ok().and_then(|g| g.opencode_session_id.clone())
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

    /// Auto-reject every pending confirmation on this conversation, releasing
    /// the model from any parked tool calls.
    ///
    /// Called when:
    /// - The conversation is cancelled (`cancel()`) — the existing teardown
    ///   already drops shell senders, but this helper also fires the explicit
    ///   `POST /permission/{id}/reply` so the OpenCode server doesn't keep
    ///   server-side permissions in limbo across abort + resume cycles.
    /// - A fresh user prompt arrives while the prior turn is still parked on
    ///   un-answered permissions (`send_message()` entry). Without this the
    ///   model from the previous turn stays blocked forever, and the new
    ///   prompt's response can't start until those orphans are resolved.
    ///
    /// Mirrors the behaviour of `confirm(call_id, "reject")` for each pending
    /// call_id but in a single state-lock acquisition for atomicity.
    ///
    /// `reason` is logged to make it obvious in production traces why a wave
    /// of rejects fired (`"cancel"` vs `"new_prompt"`).
    pub async fn reject_pending_confirmations(&self, reason: &'static str) {
        // Snapshot the work we need to do, then drop the write guard so the
        // OpenCode-permission HTTP replies below don't run while holding it.
        let (shell_senders, elicitation_senders, opencode_call_ids) = {
            let mut state = self.state.write().await;
            if state.confirmations.is_empty()
                && state.pending_shell_approvals.is_empty()
                && state.pending_elicitations.is_empty()
            {
                return;
            }

            // Collect every parked shell sender so dropping/Reject-signalling
            // them happens outside the lock.
            let shell_senders: Vec<(String, oneshot::Sender<ShellApproval>)> =
                state.pending_shell_approvals.drain().collect();
            // Same for parked MCP elicitations — we resolve each with `None`
            // (`Declined`) so the calling tool can fail closed.
            let elicitation_senders: Vec<(String, oneshot::Sender<Option<Value>>)> =
                state.pending_elicitations.drain().collect();

            // Anything left in state.confirmations after stripping shell- and
            // elicitation-rooted entries is an OpenCode-side permission that
            // needs an HTTP reject.
            let local_call_ids: HashSet<String> = shell_senders
                .iter()
                .map(|(id, _)| id.clone())
                .chain(elicitation_senders.iter().map(|(id, _)| id.clone()))
                .collect();
            // Pair each call_id with its originating session_id. The reject
            // HTTP hits the canonical `/permission/{permID}/reply` endpoint
            // (see `build_permission_reply_request`); the session_id is kept
            // for diagnostics only.
            let opencode_call_ids: Vec<(String, Option<String>)> = state
                .confirmations
                .iter()
                .filter(|c| !local_call_ids.contains(&c.call_id))
                .map(|c| (c.call_id.clone(), c.session_id.clone()))
                .collect();

            state.confirmations.clear();
            (shell_senders, elicitation_senders, opencode_call_ids)
        };

        let total = shell_senders.len() + elicitation_senders.len() + opencode_call_ids.len();
        if total == 0 {
            return;
        }
        info!(
            conversation_id = %self.runtime.conversation_id(),
            reason,
            shell_count = shell_senders.len(),
            elicitation_count = elicitation_senders.len(),
            opencode_count = opencode_call_ids.len(),
            "auto-rejecting pending confirmations"
        );

        // Wake every parked `run_shell` MCP request with an explicit Reject so
        // the local fs MCP returns an error to OpenCode; the model then sees
        // the rejection and moves on instead of hanging on the MCP response.
        for (_call_id, tx) in shell_senders {
            let _ = tx.send(ShellApproval::Reject);
        }
        // Same for elicitations — `None` signals Declined.
        for (_call_id, tx) in elicitation_senders {
            let _ = tx.send(None);
        }

        // Best-effort reject the OpenCode-side permissions. We don't await any
        // of these — same pattern as `confirm()` — so cancel/new-prompt stays
        // responsive even if the server is slow.
        if !opencode_call_ids.is_empty() && is_opencode_protocol(&self.remote_config.protocol) {
            let base_url = normalize_base_url(&self.remote_config.url);
            let auth_header =
                build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());
            let http_client = self.http_client.clone();
            let conversation_id = self.runtime.conversation_id().to_string();
            tokio::spawn(async move {
                for (call_id, _session_for_url) in opencode_call_ids {
                    // Canonical permission-reply endpoint — see
                    // `build_permission_reply_request`.
                    let (url, body) = build_permission_reply_request(&base_url, &call_id, "reject");
                    let mut req = http_client.post(&url).json(&body).timeout(Duration::from_secs(5));
                    if let Some(ref h) = auth_header {
                        req = req.header(AUTHORIZATION, h.as_str());
                    }
                    match req.send().await {
                        Ok(resp) if resp.status().is_success() => {
                            debug!(
                                %conversation_id, %call_id, %reason,
                                "auto-rejected OpenCode permission"
                            );
                        }
                        Ok(resp) => {
                            // 404 here is normal — the permission may already
                            // have been resolved server-side by the abort.
                            debug!(
                                %conversation_id, %call_id, %reason,
                                status = %resp.status(),
                                "OpenCode permission auto-reject returned non-success"
                            );
                        }
                        Err(e) => {
                            warn!(
                                %conversation_id, %call_id, %reason,
                                error = %e,
                                "OpenCode permission auto-reject request failed"
                            );
                        }
                    }
                }
            });
        }
    }
}

/// Extract todo entries from OpenCode's `todo.updated` SSE event.
///
/// Payload shape:
///   `{ "properties": { "sessionID": "ses_...", "todos": [{content, status, priority}] } }`
///
/// Returns the `todos` array verbatim (including the empty array, which represents
/// "todos cleared"). Returns `None` only when the `todos` field is missing or
/// malformed, so callers can distinguish "no event" from "explicit clear".
fn extract_opencode_todo_entries(props: &Value) -> Option<Vec<Value>> {
    let todos = props.get("todos")?.as_array()?;
    Some(todos.clone())
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

    #[test]
    fn unwrap_event_unwraps_global_payload() {
        // `/global/event` shape: event nested under `payload`.
        let wrapped = json!({
            "payload": { "id": "evt_1", "type": "server.connected", "properties": {} }
        });
        let inner = unwrap_event(wrapped);
        assert_eq!(inner.get("type").and_then(|v| v.as_str()), Some("server.connected"));
        assert!(inner.get("payload").is_none());
    }

    #[test]
    fn unwrap_event_passthrough_for_legacy_shape() {
        // Legacy `/event` shape: raw event object with no `payload` key.
        let raw = json!({ "id": "evt_2", "type": "server.heartbeat", "properties": {} });
        let inner = unwrap_event(raw.clone());
        assert_eq!(inner, raw);
        assert_eq!(inner.get("type").and_then(|v| v.as_str()), Some("server.heartbeat"));
    }

    #[test]
    fn unwrap_event_non_object_is_identity() {
        let v = json!("not-an-object");
        assert_eq!(unwrap_event(v.clone()), v);
    }

    #[test]
    fn permission_reply_uses_canonical_endpoint() {
        // Canonical (non-deprecated) endpoint verified against opencode
        // 1.15.11: POST /permission/{id}/reply with body { "reply": <decision> }.
        let (url, body) = build_permission_reply_request("http://127.0.0.1:4096", "per_abc", "once");
        assert_eq!(url, "http://127.0.0.1:4096/permission/per_abc/reply");
        assert_eq!(body, json!({ "reply": "once" }));

        let (_, body) = build_permission_reply_request("http://h", "per_x", "reject");
        assert_eq!(body, json!({ "reply": "reject" }));
        // The body must NOT use the deprecated session-scoped `response` field.
        assert!(body.get("response").is_none());
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
        let agent = RemoteAgentManager::new("conv_model_info".to_string(), "/ws".to_string(), config, None)
            .await
            .unwrap();
        // Simulate an established session so SSE events for `sess_1` (used by
        // the handler tests below) pass the per-session ownership filter in
        // `handle_opencode_sse_event`.
        agent.state.write().await.opencode_session_id = Some("sess_1".to_string());
        agent
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
            !events
                .iter()
                .any(|e| matches!(e, AgentStreamEvent::AssistantModelInfo(_))),
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
    async fn sse_event_for_foreign_session_is_ignored() {
        // The OpenCode `/global/event` stream is global; this manager owns `sess_1`
        // (seeded by `opencode_test_agent`). An event tagged with a different
        // session must not bleed into this conversation's stream.
        let agent = opencode_test_agent().await;
        let mut rx = agent.runtime.subscribe();

        let foreign = json!({
            "type": "message.part.delta",
            "properties": {
                "sessionID": "sess_OTHER",
                "field": "text",
                "delta": "text from another conversation",
                "partID": "prt_x",
            }
        })
        .to_string();
        agent.handle_opencode_sse_event(&foreign).await;
        assert!(
            drain_events(&mut rx).is_empty(),
            "events for a foreign session must be dropped"
        );

        // Sanity: the same event for THIS session is delivered.
        let own = json!({
            "type": "message.part.delta",
            "properties": {
                "sessionID": "sess_1",
                "field": "text",
                "delta": "hello",
                "partID": "prt_y",
            }
        })
        .to_string();
        agent.handle_opencode_sse_event(&own).await;
        let events = drain_events(&mut rx);
        assert!(
            events
                .iter()
                .any(|e| matches!(e, AgentStreamEvent::Text(d) if d.content == "hello")),
            "events for this conversation's own session must be delivered"
        );
    }

    #[tokio::test]
    async fn sse_event_without_session_id_passes_through() {
        // Non-session-scoped events (no `sessionID`) are not filtered. Use a
        // `session.error` payload, which emits an Error regardless of session.
        let agent = opencode_test_agent().await;
        let mut rx = agent.runtime.subscribe();

        let ev = json!({
            "type": "session.error",
            "properties": { "error": { "data": { "message": "boom" } } }
        })
        .to_string();
        agent.handle_opencode_sse_event(&ev).await;
        assert!(
            drain_events(&mut rx)
                .iter()
                .any(|e| matches!(e, AgentStreamEvent::Error(_))),
            "events with no sessionID must pass through"
        );
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
        let agent = RemoteAgentManager::new("conv1".to_string(), "/ws".to_string(), config, None)
            .await
            .unwrap();
        assert_eq!(agent.agent_type(), AgentType::Remote);
        assert_eq!(agent.conversation_id(), "conv1");
        assert_eq!(agent.status(), None);
    }

    #[tokio::test]
    async fn new_seeds_resume_session_id_into_get_session_key() {
        let config = RemoteAgentConfig {
            remote_agent_id: "ra_test".to_string(),
            protocol: "opencode".to_string(),
            url: "http://127.0.0.1:4096".to_string(),
            auth_type: "none".to_string(),
            auth_token: None,
            allow_insecure: false,
        };
        // Seeded id is exposed via get_session_key for persistence/reuse.
        let resumed = RemoteAgentManager::new(
            "conv_resume".to_string(),
            "/ws".to_string(),
            config.clone(),
            Some("ses_resume_123".to_string()),
        )
        .await
        .unwrap();
        assert_eq!(resumed.get_session_key().as_deref(), Some("ses_resume_123"));

        // A brand-new conversation (no seed) exposes no session key until the
        // first send creates one.
        let fresh = RemoteAgentManager::new("conv_fresh".to_string(), "/ws".to_string(), config, None)
            .await
            .unwrap();
        assert_eq!(fresh.get_session_key(), None);
    }

    #[test]
    fn extract_todo_entries_returns_todos_array() {
        let props = json!({
            "sessionID": "sess_1",
            "todos": [
                { "content": "Create Makefile", "status": "completed", "priority": "high" },
                { "content": "Port PPPP protocol", "status": "in_progress", "priority": "medium" }
            ]
        });
        let entries = extract_opencode_todo_entries(&props).expect("todos array present");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0]["content"], "Create Makefile");
        assert_eq!(entries[1]["status"], "in_progress");
    }

    #[test]
    fn extract_todo_entries_returns_empty_array_when_cleared() {
        // OpenCode publishes `todos: []` when an agent clears its plan; the
        // frontend needs that explicit signal to hide the Todos tab.
        let props = json!({ "sessionID": "sess_1", "todos": [] });
        let entries = extract_opencode_todo_entries(&props).expect("empty array is a valid clear signal");
        assert!(entries.is_empty());
    }

    #[test]
    fn extract_todo_entries_returns_none_when_field_missing() {
        let props = json!({ "sessionID": "sess_1" });
        assert!(extract_opencode_todo_entries(&props).is_none());
    }

    #[test]
    fn extract_todo_entries_returns_none_when_field_wrong_type() {
        let props = json!({ "sessionID": "sess_1", "todos": "not-an-array" });
        assert!(extract_opencode_todo_entries(&props).is_none());
    }

    #[tokio::test]
    async fn todo_updated_event_emits_plan_event() {
        let agent = opencode_test_agent().await;
        let mut rx = agent.runtime.subscribe();

        let event = json!({
            "type": "todo.updated",
            "properties": {
                "sessionID": "sess_1",
                "todos": [
                    { "content": "Step one", "status": "completed", "priority": "high" },
                    { "content": "Step two", "status": "pending", "priority": "medium" }
                ]
            }
        })
        .to_string();

        agent.handle_opencode_sse_event(&event).await;

        let events = drain_events(&mut rx);
        let plan = events
            .iter()
            .find_map(|e| match e {
                AgentStreamEvent::Plan(d) => Some(d),
                _ => None,
            })
            .expect("Plan event should be emitted from todo.updated");
        assert_eq!(plan.entries.len(), 2);
        assert_eq!(plan.entries[0]["content"], "Step one");
        assert_eq!(plan.session_id.as_deref(), Some("sess_1"));
    }

    #[tokio::test]
    async fn todo_updated_event_for_foreign_session_is_ignored() {
        // todo.updated carries sessionID; the per-session ownership filter at the
        // top of handle_opencode_sse_event must drop events for other sessions.
        let agent = opencode_test_agent().await;
        let mut rx = agent.runtime.subscribe();

        let event = json!({
            "type": "todo.updated",
            "properties": {
                "sessionID": "ses_other",
                "todos": [{ "content": "Other session", "status": "pending", "priority": "low" }]
            }
        })
        .to_string();

        agent.handle_opencode_sse_event(&event).await;

        assert!(
            !drain_events(&mut rx)
                .iter()
                .any(|e| matches!(e, AgentStreamEvent::Plan(_))),
            "todo.updated for a foreign session must not emit Plan"
        );
    }

    fn fake_confirmation(call_id: &str) -> Confirmation {
        Confirmation {
            id: call_id.to_string(),
            call_id: call_id.to_string(),
            title: Some("Run a command on your machine?".to_string()),
            action: Some("run_shell".to_string()),
            description: format!("dummy command for {call_id}"),
            command_type: Some("run_shell".to_string()),
            options: vec![],
            session_id: None,
            parent_session_id: None,
        }
    }

    #[tokio::test]
    async fn reject_pending_is_noop_when_state_is_empty() {
        // Calling the helper on an idle agent must not log, panic, or block —
        // both cancel() and send_message() reach this path unconditionally.
        let agent = opencode_test_agent().await;
        agent.reject_pending_confirmations("noop_test").await;
        let state = agent.state.read().await;
        assert!(state.confirmations.is_empty());
        assert!(state.pending_shell_approvals.is_empty());
    }

    #[tokio::test]
    async fn reject_pending_clears_state_and_resolves_shell_senders() {
        // The auto-reject helper is the unblock mechanism for parked
        // `run_shell` MCP calls: the parked task is awaiting its
        // oneshot::Receiver and must wake with `ShellApproval::Reject` so
        // the MCP request finishes (otherwise the previous turn never
        // completes and the new prompt can't be processed).
        let agent = opencode_test_agent().await;
        let (tx_a, rx_a) = oneshot::channel::<ShellApproval>();
        let (tx_b, rx_b) = oneshot::channel::<ShellApproval>();

        {
            let mut state = agent.state.write().await;
            state.confirmations.push(fake_confirmation("shell-a"));
            state.confirmations.push(fake_confirmation("shell-b"));
            // Also drop in an opencode-side permission to confirm it's
            // counted separately but still removed from local state.
            state.confirmations.push(fake_confirmation("perm-c"));
            state.pending_shell_approvals.insert("shell-a".to_string(), tx_a);
            state.pending_shell_approvals.insert("shell-b".to_string(), tx_b);
        }

        agent.reject_pending_confirmations("new_prompt").await;

        // Every parked shell sender wakes with Reject — this is what
        // releases the MCP tool calls so the model can continue.
        assert_eq!(rx_a.await.unwrap(), ShellApproval::Reject);
        assert_eq!(rx_b.await.unwrap(), ShellApproval::Reject);

        // Local state is wiped so a future click on a stale card has no
        // ghost entry to act on.
        let state = agent.state.read().await;
        assert!(state.confirmations.is_empty());
        assert!(state.pending_shell_approvals.is_empty());
    }
}
