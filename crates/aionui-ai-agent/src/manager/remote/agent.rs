use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use aionui_api_types::{AgentModeOption, RemoteSkillInfo, SlashCommandItem};
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
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

use crate::agent_runtime::AgentRuntime;
use crate::manager::remote::local_fs_mcp::project_tree::render_project_tree_default;
use crate::manager::remote::local_fs_mcp::{
    ElicitationHandler, ElicitationOutcome, ElicitationRequest, LocalFsMcpServer, McpRequestContext, ShellApproval,
    ShellApprover,
};
use crate::manager::remote::opencode_commands::{self, OpenCodeCommand};
use crate::manager::remote::opencode_delta_batcher::DeltaBatcherHandle;
use crate::manager::remote::opencode_log_forwarder;
use crate::manager::remote::opencode_mcp;
use crate::manager::remote::opencode_models;
use crate::manager::remote::opencode_question;
use crate::manager::remote::opencode_stream;
use crate::manager::remote::opencode_tool_call;
use crate::manager::remote::subagent::{self, ChildSessionRegistry};
use crate::protocol::events::{
    AcpPermissionEventData, AcpToolCallSessionUpdateKind, AgentStreamEvent, FinishEventData, OpencodeSubtaskStatus,
    PlanEventData, RetryEventData, SessionErrorRecoveredEventData, SessionIdleEventData, SessionStatusEventData,
    StartEventData, TypedErrorData,
};
use crate::types::SendMessageData;
use aionui_common::ConfirmationOption;

/// Internal mutable state for the Remote agent.
struct RemoteState {
    session_key: Option<String>,
    confirmations: Vec<Confirmation>,
    has_messages: bool,
    /// Whether a root-session turn is currently in flight — armed when the
    /// root session goes `busy` (turn start) and disarmed when we emit the
    /// turn's terminal `Finish`. OpenCode emits a terminal trio per turn
    /// (`message.updated finish=stop`, `session.status idle`, `session.idle`),
    /// and a trailing `idle` from the previous turn can be delivered just as
    /// the next turn's relay subscribes. Gating `Finish` on this flag (a) makes
    /// the redundant idle/finish events no-ops within one turn, and (b) ignores
    /// a stray pre-`busy` idle so it can't instantly terminate the new turn's
    /// stream relay (the "2nd message returns nothing" bug). OpenCode always
    /// sends `busy` before any real `idle`, so this never drops a real Finish.
    root_turn_active: bool,
    /// Locks out root `busy` events from re-arming `root_turn_active` after
    /// `emit_root_turn_finish` has already fired for this user turn. OpenCode
    /// emits a `busy → idle` finalization burst after `message.updated finish=stop`
    /// — without this lockout that burst re-arms the gate and the trailing
    /// `idle` emits a phantom Finish that lands on the NEXT user turn's relay
    /// (the "2nd message returns nothing" bug). Cleared in `send_message` when
    /// the user submits a new prompt.
    finished_current_user_turn: bool,
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
    /// Cached OpenCode primary agents from `GET /agent`, exposed as selectable modes.
    /// `None` before first fetch; `Some(vec)` afterwards. Defaults are merged in
    /// so older/failing servers still expose build/plan.
    opencode_agents: Option<Vec<AgentModeOption>>,
    /// Cached OpenCode skill catalog from `GET /skill` (M10). `None` before
    /// first fetch; `Some(vec)` afterwards (empty vec on fetch failure so we
    /// don't retry every keystroke). Invalidated by `skill.updated` SSE events
    /// so server-side edits surface without a full reconnect.
    opencode_skills: Option<Vec<RemoteSkillInfo>>,
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
    /// In-flight OpenCode `/question` requests (M09), keyed by `requestID`.
    /// A `question.asked` event stores one entry here and emits one Approvals
    /// card per question; `confirm()` accumulates the per-question answer into
    /// the buffer and, once every question is answered, POSTs the full reply.
    /// Cleared on reply/reject, on a `question.replied`/`question.rejected`
    /// reconciliation, and on teardown.
    pending_questions: HashMap<String, opencode_question::PendingQuestion>,
    /// Recently-answered question `requestID`s → reply timestamp (ms). Mirrors
    /// `recently_replied_permissions`: suppresses a double reply when the SSE
    /// `question.replied`/`question.rejected` echo races our own POST, and
    /// drops a re-emitted `question.asked` on reconnect. Pruned by the same
    /// TTL/cap logic in `confirm()`.
    recently_replied_questions: HashMap<String, TimestampMs>,
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
    /// Tool-host mode for OpenCode agents (C04): "local" (default) injects the
    /// client-side fs MCP and denies the server's built-in tools; "server"
    /// skips the MCP and uses the server's own tools (permission prompts flow
    /// through the normal `permission.asked` handler). Empty/unknown → "local".
    pub tool_host: String,
}

/// Whether this config requests the OpenCode server's own tools instead of the
/// client-side local-fs MCP. Only meaningful for the opencode protocol; any
/// value other than exactly "server" is treated as the default "local".
fn is_server_tool_host(cfg: &RemoteAgentConfig) -> bool {
    is_opencode_protocol(&cfg.protocol) && cfg.tool_host == "server"
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

/// OpenCode SSE event types that legitimately arrive on `/global/event` but
/// require no client-side action in the remote manager (E02 — full event
/// coverage). Three families live here:
///
///   1. **Server/global-scoped** events that are not tied to a single
///      conversation (`server.*`, `global.*`, `account.*`, `installation.*`
///      *handled explicitly elsewhere*, `lsp.*`, `mcp.*`, `project.*`,
///      `vcs.*`, `file.*`, `command.executed`, `workspace.*`, `worktree.*`,
///      `pty.*`, `tui.*`). A per-conversation manager would multiply any
///      user-facing reaction N-fold across open conversations, so these are
///      acknowledged quietly. Feature plans M10–M19 may promote individual
///      entries out of this set later.
///   2. **V2 streaming mirrors** (`session.next.text.*`, `…reasoning.*`,
///      `…step.*`, `…tool.called|success|failed`, `…shell.*`,
///      `…compaction.*`, `…prompted`, `…synthetic`, `…retried`). These
///      duplicate information the dispatcher already consumes through the
///      `message.part.updated` / `message.part.delta` / `message.updated`
///      path — the product renders text, reasoning and tool lifecycle from
///      those parts today, which proves the `session.next.*` mirrors are
///      redundant. Handling them too would double-process every turn.
///   3. **Session-scoped feature stubs** delegated to later plans
///      (`session.updated`→M06 title sync, `session.deleted`→M06,
///      `session.diff`→M05, `session.compacted`→M04, `question.*`→M09,
///      `message.removed` / `message.part.removed`→M07).
///
/// Membership here only changes the log level (trace vs debug); it never
/// alters stream behaviour. Anything NOT in this set and NOT explicitly
/// matched falls through to the fingerprinted `debug` fallback so genuinely
/// new server event types stay visible in diagnostics.
const KNOWN_IGNORED_EVENTS: &[&str] = &[
    // 1. server / global scoped
    "server.connected",
    "server.heartbeat",
    "server.instance.disposed",
    "global.disposed",
    "account.added",
    "account.removed",
    "account.switched",
    "file.edited",
    "file.watcher.updated",
    "command.executed",
    "lsp.updated",
    "lsp.client.diagnostics",
    "mcp.tools.changed",
    "mcp.browser.open.failed",
    "project.updated",
    "vcs.branch.updated",
    "workspace.failed",
    "workspace.ready",
    "workspace.status",
    "worktree.failed",
    "worktree.ready",
    "pty.created",
    "pty.deleted",
    "pty.exited",
    "pty.updated",
    "tui.command.execute",
    "tui.prompt.append",
    "tui.session.select",
    "tui.toast.show",
    // 2. V2 streaming mirrors of the message.part.* path we already consume
    "session.next.prompted",
    "session.next.synthetic",
    "session.next.step.started",
    "session.next.step.ended",
    "session.next.step.failed",
    "session.next.text.started",
    "session.next.text.delta",
    "session.next.text.ended",
    "session.next.reasoning.started",
    "session.next.reasoning.delta",
    "session.next.reasoning.ended",
    "session.next.shell.started",
    "session.next.shell.ended",
    "session.next.tool.called",
    "session.next.tool.success",
    "session.next.tool.failed",
    "session.next.compaction.started",
    "session.next.compaction.delta",
    "session.next.compaction.ended",
    // 3. session-scoped feature stubs delegated to later plans
    // `session.created` for our own root session reaches the dispatcher after
    // child-registration has already run in the gate; there is no further work
    // for the root case, so acknowledge it quietly rather than as "unhandled".
    "session.created",
    "session.updated",
    "session.deleted",
    "session.diff",
    "message.removed",
    "message.part.removed",
];

/// Whether an event type is a known, intentionally-unhandled OpenCode event
/// (see [`KNOWN_IGNORED_EVENTS`]). Used by the dispatcher fallback to pick the
/// log level so the noisy-but-benign global stream does not masquerade as an
/// unknown event in diagnostics.
fn is_known_ignored_event(event_type: &str) -> bool {
    KNOWN_IGNORED_EVENTS.contains(&event_type)
}

fn opencode_status(raw: Option<&str>) -> (&'static str, Option<String>) {
    match raw {
        Some("busy") | Some("running") => ("running", None),
        Some("idle") => ("idle", None),
        Some("aborting") => ("aborting", None),
        Some("aborted") => ("aborted", Some("aborted".to_string())),
        Some("error") | Some("errored") => ("errored", Some("errored".to_string())),
        Some(other) => ("idle", Some(other.to_string())),
        None => ("idle", None),
    }
}

fn opencode_idle_reason(props: &Value) -> String {
    match props.get("reason").and_then(|v| v.as_str()) {
        Some("completed") | Some("aborted") | Some("errored") | Some("compacted") => {
            props.get("reason").and_then(|v| v.as_str()).unwrap().to_string()
        }
        Some(other) if other.contains("abort") => "aborted".to_string(),
        Some(other) if other.contains("error") => "errored".to_string(),
        Some(other) if other.contains("compact") => "compacted".to_string(),
        _ => "completed".to_string(),
    }
}

fn retry_reason(raw: Option<&str>) -> String {
    match raw {
        Some("rate_limit")
        | Some("transient")
        | Some("tool_error")
        | Some("provider_error")
        | Some("context_overflow") => raw.unwrap().to_string(),
        Some(other) if other.contains("rate") || other.contains("limit") => "rate_limit".to_string(),
        Some(other) if other.contains("context") => "context_overflow".to_string(),
        Some(other) if other.contains("tool") => "tool_error".to_string(),
        Some(other) if other.contains("provider") => "provider_error".to_string(),
        Some(other) if other.contains("timeout") || other.contains("temporary") || other.contains("transient") => {
            "transient".to_string()
        }
        _ => "unknown".to_string(),
    }
}

fn redact_sensitive_text(input: &str) -> String {
    let mut redact_next = false;
    let words = input
        .split_whitespace()
        .map(|word| {
            if redact_next {
                redact_next = false;
                return "[REDACTED]".to_string();
            }
            if word.eq_ignore_ascii_case("Bearer") {
                redact_next = true;
                return "[REDACTED]".to_string();
            }
            if word.starts_with("sk-") {
                "[REDACTED]".to_string()
            } else {
                word.to_string()
            }
        })
        .collect::<Vec<_>>();
    words.join(" ")
}

fn truncate_500(input: &str) -> String {
    input.chars().take(500).collect()
}

fn typed_error_from_props(props: &Value) -> TypedErrorData {
    let error = props.get("error").unwrap_or(&Value::Null);
    let name = error
        .get("name")
        .or_else(|| error.get("type"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let data = error.get("data").unwrap_or(error);
    let raw_message = data
        .get("message")
        .or_else(|| error.get("message"))
        .and_then(|v| v.as_str())
        .or_else(|| props.get("error").and_then(|v| v.as_str()))
        .unwrap_or("OpenCode session error");
    let message = redact_sensitive_text(&truncate_500(raw_message));
    let body_text = data.get("body").and_then(|v| v.as_str()).unwrap_or("");
    let combined_lower = format!("{raw_message} {body_text}").to_lowercase();
    let kind = match name {
        _ if combined_lower.contains("context") || combined_lower.contains("window") => "context_overflow",
        _ if combined_lower.contains("maximum context")
            || combined_lower.contains("token") && combined_lower.contains("limit") =>
        {
            "context_overflow"
        }
        "ProviderAuthError" => "provider_auth",
        "ContextOverflowError" => "context_overflow",
        "MessageOutputLengthError" => "output_length",
        "MessageAbortedError" => "aborted",
        "StructuredOutputError" => "structured_output",
        "APIError" => "api",
        _ if raw_message.to_lowercase().contains("auth") => "provider_auth",
        _ if raw_message.to_lowercase().contains("abort") => "aborted",
        _ => "unknown",
    }
    .to_string();

    let mut metadata = serde_json::Map::new();
    if let Some(provider_id) = data
        .get("providerID")
        .or_else(|| data.get("providerId"))
        .and_then(|v| v.as_str())
    {
        metadata.insert("provider_id".to_string(), json!(provider_id));
    }
    if let Some(used) = data.get("used").and_then(|v| v.as_u64()) {
        metadata.insert("used".to_string(), json!(used));
    }
    if let Some(limit) = data.get("limit").and_then(|v| v.as_u64()) {
        metadata.insert("limit".to_string(), json!(limit));
    }
    if let Some(status_code) = data
        .get("statusCode")
        .or_else(|| data.get("status_code"))
        .and_then(|v| v.as_u64())
    {
        metadata.insert("status_code".to_string(), json!(status_code));
    }
    if let Some(body) = data.get("body").and_then(|v| v.as_str()) {
        metadata.insert("body".to_string(), json!(redact_sensitive_text(&truncate_500(body))));
    }
    if let Some(schema) = data
        .get("schema")
        .or_else(|| data.get("schemaName"))
        .and_then(|v| v.as_str())
    {
        metadata.insert("schema".to_string(), json!(schema));
    }
    if let Some(partial) = data.get("partial").or_else(|| data.get("partialJson")) {
        metadata.insert("partial".to_string(), partial.clone());
    }
    let recoverable = props
        .get("recoverable")
        .and_then(|v| v.as_bool())
        .or_else(|| data.get("recoverable").and_then(|v| v.as_bool()))
        .unwrap_or(false);
    TypedErrorData {
        message,
        kind,
        metadata: (!metadata.is_empty()).then_some(Value::Object(metadata)),
        recoverable,
    }
}

/// Stable, **non-sensitive** fingerprint of an event's `properties` object,
/// derived from the sorted set of top-level property *keys* only (never their
/// values). Lets `log_unhandled` distinguish genuinely-new event shapes in
/// diagnostics without ever recording payload contents — satisfying the
/// AGENTS.md rule that production logs must not contain prompts, tool I/O,
/// file contents, or secrets. Returns a 16-hex-char digest.
fn event_property_fingerprint(props: &Value) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut keys: Vec<&str> = match props.as_object() {
        Some(map) => map.keys().map(String::as_str).collect(),
        None => Vec::new(),
    };
    keys.sort_unstable();
    let mut hasher = DefaultHasher::new();
    keys.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Upper bound on the `recently_replied_questions` dedup map (M09).
const QUESTION_DEDUP_CAP: usize = 1024;

/// Cap a recently-replied dedup map at `cap` entries, evicting the oldest by
/// timestamp first. Keeps the question dedup map bounded over a long session.
fn prune_replied_map(map: &mut HashMap<String, TimestampMs>, cap: usize) {
    if map.len() <= cap {
        return;
    }
    let mut entries: Vec<(String, TimestampMs)> = map.iter().map(|(k, v)| (k.clone(), *v)).collect();
    entries.sort_by_key(|(_, ts)| *ts);
    let remove = map.len() - cap;
    for (k, _) in entries.into_iter().take(remove) {
        map.remove(&k);
    }
}

/// Initial reconnect delay after the SSE reader drops (C02). Doubles each
/// failed pass up to [`RECONNECT_DELAY_MAX`]; resets to this on a confirmed
/// `server.connected`.
const RECONNECT_DELAY_MIN: Duration = Duration::from_millis(250);
/// Upper bound on the exponential reconnect backoff.
const RECONNECT_DELAY_MAX: Duration = Duration::from_secs(5);
/// If no SSE event (any type, including `server.heartbeat`) arrives within this
/// window, the reader assumes a silent half-open connection and exits with
/// [`ReaderExit::HeartbeatTimeout`] so the supervisor reconnects.
///
/// Sized comfortably above the server's observed heartbeat cadence (~10 s) and
/// above the worst-case delay before the *first* heartbeat after
/// `server.connected` (which can lag when a turn is queued behind the shared
/// MCP slot on a multi-session server). Too small a value tears down a slow but
/// healthy stream mid-turn — see the C02 regression where a 15 s window killed
/// a session that was simply waiting for its first heartbeat.
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(30);

/// Why a single `run_event_reader` pass returned. Drives the supervisor's
/// decision to reconnect or stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReaderExit {
    /// The initial HTTP request for the SSE stream failed.
    ConnectFailed,
    /// The stream ended cleanly (server closed the connection).
    Eof,
    /// A transport error occurred mid-stream.
    StreamError,
    /// No event arrived within [`HEARTBEAT_TIMEOUT`].
    HeartbeatTimeout,
    /// `server.instance.disposed` was observed — terminal, do not reconnect.
    ServerDisposed,
}

/// Fetch the direct child sessions of `parent_session_id` via
/// `GET /session/{id}/children` (M08). Returns the raw `Session` JSON objects
/// (an array per `/doc`); best-effort — any transport/HTTP/JSON failure yields
/// an empty vec so backfill silently no-ops rather than disrupting connect.
async fn fetch_child_sessions(
    client: &reqwest::Client,
    base_url: &str,
    auth_header: Option<&str>,
    parent_session_id: &str,
) -> Vec<Value> {
    let url = format!("{base_url}/session/{parent_session_id}/children");
    let mut req = client.get(&url).timeout(Duration::from_secs(10));
    if let Some(h) = auth_header {
        req = req.header(AUTHORIZATION, h);
    }
    match req.send().await {
        Ok(resp) if resp.status().is_success() => match resp.json::<Value>().await {
            Ok(Value::Array(arr)) => arr,
            _ => Vec::new(),
        },
        Ok(resp) => {
            debug!(status = %resp.status(), endpoint = %url, "OpenCode children fetch returned non-success");
            Vec::new()
        }
        Err(e) => {
            debug!(error = %e, endpoint = %url, "OpenCode children fetch failed");
            Vec::new()
        }
    }
}

/// Run one SSE reader pass against `event_url`. Returns a [`ReaderExit`]
/// describing why it stopped so the supervised loop in `connect_opencode` can
/// decide whether to reconnect.
///
/// Heartbeat tracking: every parsed event (including `server.heartbeat`) resets
/// an idle timer; if [`HEARTBEAT_TIMEOUT`] elapses with no event, the pass
/// returns `HeartbeatTimeout`. On the first `server.connected` of a pass the
/// shared `connection_status` flips to `Connected` (so the supervisor can reset
/// its backoff). `server.instance.disposed` short-circuits to `ServerDisposed`.
async fn run_event_reader(
    this: &Arc<RemoteAgentManager>,
    client: &reqwest::Client,
    event_url: &str,
    auth: Option<&str>,
    conversation_id: &str,
) -> ReaderExit {
    let mut req_builder = client.get(event_url).header("Accept", "text/event-stream");
    if let Some(h) = auth {
        req_builder = req_builder.header(AUTHORIZATION, h);
    }

    let resp = match req_builder.send().await {
        Ok(r) => r,
        Err(e) => {
            warn!(
                conversation_id = %conversation_id,
                error = %ErrorChain(&e),
                "OpenCode SSE connection failed"
            );
            return ReaderExit::ConnectFailed;
        }
    };

    let mut stream = resp.bytes_stream();
    let mut buffer = String::new();
    let mut saw_connected = false;

    loop {
        let next = tokio::time::timeout(HEARTBEAT_TIMEOUT, stream.next()).await;
        let chunk_result = match next {
            // No bytes within the heartbeat window — assume a silent half-open
            // connection and let the supervisor reconnect. (We never treat a
            // missing heartbeat as fatal: the first heartbeat after connect can
            // legitimately lag, and any real activity resets this timer.)
            Err(_elapsed) => return ReaderExit::HeartbeatTimeout,
            Ok(None) => return ReaderExit::Eof,
            Ok(Some(r)) => r,
        };

        let chunk = match chunk_result {
            Ok(c) => c,
            Err(e) => {
                warn!(
                    conversation_id = %conversation_id,
                    error = %ErrorChain(&e),
                    "OpenCode SSE stream error"
                );
                return ReaderExit::StreamError;
            }
        };

        let text = String::from_utf8_lossy(&chunk);
        buffer.push_str(&text);

        while let Some(pos) = buffer.find("\n\n") {
            let event_text = buffer[..pos].to_string();
            buffer = buffer[pos + 2..].to_string();

            for line in event_text.lines() {
                if let Some(data) = line.strip_prefix("data: ") {
                    // Peek the event type for lifecycle handling before the full
                    // dispatch. Cheap parse; the dispatcher re-parses but this
                    // path only runs per-event and avoids threading state out of
                    // the handler.
                    if let Ok(parsed) = serde_json::from_str::<Value>(data) {
                        let inner = unwrap_event(parsed);
                        match inner.get("type").and_then(|v| v.as_str()) {
                            Some("server.connected") if !saw_connected => {
                                saw_connected = true;
                                let mut state = this.state.write().await;
                                state.connection_status = RemoteAgentStatus::Connected;
                            }
                            Some("server.instance.disposed") => {
                                return ReaderExit::ServerDisposed;
                            }
                            _ => {}
                        }
                    }
                    this.handle_opencode_sse_event(data).await;
                }
            }
        }
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

/// Given an OpenCode `GET /session/{id}/message` response (an array of
/// `{info:{id,...}, parts:[...]}`), return the id of the message that
/// immediately follows `message_id`. Returns `None` when `message_id` is the
/// last message or cannot be found — callers treat `None` as "fork from the
/// tip" so no history is lost. Used to make "fork from here" inclusive of the
/// selected message despite OpenCode's fork being exclusive (see
/// `opencode_fork`).
fn next_opencode_message_id(messages: &Value, message_id: &str) -> Option<String> {
    let array = messages.as_array()?;
    let ids: Vec<&str> = array
        .iter()
        .filter_map(|m| m.get("info").and_then(|i| i.get("id")).and_then(|v| v.as_str()))
        .collect();
    ids.iter()
        .position(|id| *id == message_id)
        .and_then(|idx| ids.get(idx + 1))
        .map(|s| s.to_string())
}

fn build_auth_header(auth_type: &str, auth_token: Option<&str>) -> Option<String> {
    let token = auth_token.filter(|t| !t.is_empty())?;
    let value = match auth_type {
        "bearer" | "Bearer" => format!("Bearer {token}"),
        "basic" | "Basic" => format!("Basic {}", BASE64.encode(token)),
        "password" | "Password" => format!("Basic {}", BASE64.encode(format!("opencode:{token}"))),
        _ => return None,
    };
    Some(value)
}

pub(crate) fn default_opencode_agent_modes() -> Vec<AgentModeOption> {
    vec![
        AgentModeOption {
            id: "build".to_string(),
            name: Some("Build".to_string()),
            description: None,
        },
        AgentModeOption {
            id: "plan".to_string(),
            name: Some("Plan".to_string()),
            description: None,
        },
    ]
}

pub(crate) fn parse_opencode_agent_modes(body: &Value) -> Vec<AgentModeOption> {
    let mut modes = default_opencode_agent_modes();
    let mut seen: HashSet<String> = modes.iter().map(|m| m.id.clone()).collect();

    let Some(items) = body.as_array() else {
        return modes;
    };

    for item in items {
        if item.get("hidden").and_then(|v| v.as_bool()).unwrap_or(false) {
            continue;
        }
        // Selectable session modes are agents usable as the primary agent:
        // `mode == "primary"` (native build/plan) or `mode == "all"` (custom
        // agents usable as both primary and subagent). `subagent`-only agents
        // (explore, general) are invoked via the task tool, not selectable here.
        if !matches!(item.get("mode").and_then(|v| v.as_str()), Some("primary") | Some("all")) {
            continue;
        }
        let Some(id) = item.get("name").and_then(|v| v.as_str()).filter(|s| !s.is_empty()) else {
            continue;
        };
        if !seen.insert(id.to_string()) {
            continue;
        }
        modes.push(AgentModeOption {
            id: id.to_string(),
            name: Some(id.to_string()),
            description: item.get("description").and_then(|v| v.as_str()).map(String::from),
        });
    }

    modes
}

/// Parse the `GET /skill` response into a compact client-facing list.
/// OpenCode returns `[{ name, description?, location, content }]`; we keep
/// only `name` and `description` since the server-local paths and markdown
/// content are not meaningful on the client machine.
pub(crate) fn parse_opencode_skills(body: &Value) -> Vec<RemoteSkillInfo> {
    let Some(items) = body.as_array() else {
        return Vec::new();
    };
    let mut skills = Vec::with_capacity(items.len());
    let mut seen: HashSet<String> = HashSet::new();
    for item in items {
        let Some(name) = item.get("name").and_then(|v| v.as_str()).filter(|s| !s.is_empty()) else {
            continue;
        };
        if !seen.insert(name.to_string()) {
            continue;
        }
        skills.push(RemoteSkillInfo {
            name: name.to_string(),
            description: item.get("description").and_then(|v| v.as_str()).map(String::from),
        });
    }
    skills
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
    /// Per-part SSE delta accumulator (E03). Token deltas for the same
    /// `(messageID, partID, field)` are coalesced on a ~16 ms frame and
    /// emitted as a single `Text`/`Thinking` event, rather than firing one
    /// IPC event per token. See
    /// [`opencode_delta_batcher`](super::opencode_delta_batcher) for the
    /// flush rules. The handle is cheap to clone, and is also flushed
    /// forcibly on `message.part.updated` (per-part) and `emit_root_turn_finish`
    /// (per-turn) so no streamed text lingers past terminal events.
    delta_batcher: DeltaBatcherHandle,
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

        let delta_batcher = DeltaBatcherHandle::new(runtime.clone());

        Ok(Self {
            runtime,
            remote_config,
            state: Arc::new(RwLock::new(RemoteState {
                session_key: None,
                confirmations: Vec::new(),
                has_messages: false,
                root_turn_active: false,
                finished_current_user_turn: false,
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
                opencode_agents: None,
                opencode_skills: None,
                model_context_limits: None,
                pending_shell_approvals: HashMap::new(),
                pending_elicitations: HashMap::new(),
                recently_replied_permissions: HashMap::new(),
                auto_accept_paths: initial_auto_accept_paths,
                auto_accept_sessions: HashSet::new(),
                opencode_tool_call_ids: HashSet::new(),
                child_sessions: ChildSessionRegistry::default(),
                last_subtask_progress_ms: HashMap::new(),
                pending_questions: HashMap::new(),
                recently_replied_questions: HashMap::new(),
            })),
            ws_sink: Mutex::new(None),
            _reader_handle: Mutex::new(None),
            http_client,
            local_fs_mcp: Mutex::new(None),
            reachability_guardian: Mutex::new(None),
            conversation_repo,
            delta_batcher,
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
                //
                // C04: skip entirely in server-tools mode — that mode never
                // registers a local-fs MCP, so there is nothing to re-register.
                if !is_server_tool_host(&self.remote_config) {
                    self.ensure_local_fs_mcp(&base_url, auth_header.as_deref()).await;
                }
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
        let _ = self.ensure_opencode_agents().await;
        // M10: prime the skill catalog so the picker is populated before
        // the user types. Same failure mode as commands: empty list cached.
        let _ = self.ensure_opencode_skills().await;

        // M14: register the per-conversation log forwarder so any tracing
        // event downstream that carries this `conversation_id` is shipped
        // to the OpenCode server's `POST /log`. Idempotent — re-registering
        // on reconnect replaces the previous channel.
        opencode_log_forwarder::register_forwarder(
            self.runtime.conversation_id().to_string(),
            self.http_client.clone(),
            base_url.clone(),
            auth_header.clone(),
        );

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
            // Supervised reconnect loop (C02). Each pass runs one SSE reader;
            // on any non-cancel exit (network error, EOF, or heartbeat timeout)
            // we mark the connection `Reconnecting`, back off, and retry. The
            // backoff resets once a fresh `server.connected` is observed.
            let mut backoff = RECONNECT_DELAY_MIN;
            loop {
                // M08: close the sub-agent gap before (re)attaching the stream.
                // On the first pass this rehydrates children of a resumed
                // in-progress session; on reconnect it catches any spawned
                // during the gap. Best-effort and deduped by the registry.
                this.backfill_child_sessions().await;

                let exit = run_event_reader(&this, &client, &event_url, auth.as_deref(), &conversation_id).await;
                // If the pass reached `Connected`, reset the backoff so the next
                // transient drop retries fast. Read BEFORE the arms below mutate
                // status to `Reconnecting`.
                if this.state.read().await.connection_status == RemoteAgentStatus::Connected {
                    backoff = RECONNECT_DELAY_MIN;
                }
                match exit {
                    ReaderExit::ServerDisposed => {
                        // `server.instance.disposed` is emitted both on a real
                        // shutdown AND on a server-side hot-reload (e.g. a
                        // `PATCH /global/config` makes OpenCode dispose the old
                        // app instance and immediately spin up a new one).
                        // Treating it as terminal stranded every live
                        // conversation's stream after a config edit (no
                        // streaming until app restart), so we reconnect with
                        // backoff just like a transport drop: if the instance
                        // came back (reload) the next pass resubscribes and a
                        // fresh `server.connected` resets the backoff; if the
                        // server is truly gone the pass returns `ConnectFailed`
                        // and we keep backing off harmlessly until it returns.
                        //
                        // Release any MCP turn slot we hold first: the disposed
                        // instance will never emit this turn's terminal
                        // `Finish`, so without this an in-flight turn would pin
                        // the per-base-url slot until `TURN_WAIT_TIMEOUT`,
                        // blocking every other conversation on this server.
                        this.release_turn_slot().await;
                        {
                            let mut state = this.state.write().await;
                            if state.connection_status != RemoteAgentStatus::Error {
                                state.connection_status = RemoteAgentStatus::Reconnecting;
                            }
                        }
                        info!(
                            conversation_id = %conversation_id,
                            backoff_ms = backoff.as_millis() as u64,
                            "OpenCode server instance disposed (shutdown or config hot-reload); reconnecting"
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(RECONNECT_DELAY_MAX);
                    }
                    ReaderExit::ConnectFailed
                    | ReaderExit::Eof
                    | ReaderExit::StreamError
                    | ReaderExit::HeartbeatTimeout => {
                        {
                            let mut state = this.state.write().await;
                            // Don't clobber a terminal Error set elsewhere.
                            if state.connection_status != RemoteAgentStatus::Error {
                                state.connection_status = RemoteAgentStatus::Reconnecting;
                            }
                        }
                        info!(
                            conversation_id = %conversation_id,
                            reason = ?exit,
                            backoff_ms = backoff.as_millis() as u64,
                            "OpenCode SSE reader exited; reconnecting"
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(RECONNECT_DELAY_MAX);
                    }
                }
            }
        });

        *self._reader_handle.lock().await = Some(reader_handle);

        Ok(())
    }

    /// Backfill direct child sessions of the current parent session via
    /// `GET /session/{id}/children` (M08). Sub-agents are normally registered
    /// reactively from `session.created` SSE events, but any child spawned
    /// before this manager subscribed (resume of an in-progress session) or
    /// during a reconnect gap would otherwise be missed — its progress chip
    /// never appears. This gap-closer registers each previously-unseen direct
    /// child and emits `OpencodeSubtask::Started` so the renderer rehydrates
    /// the chip. New children continue to arrive via the reactive path.
    ///
    /// Scoped to **direct** children only — matching the reactive dispatcher,
    /// which also only registers sessions whose `parentID` is our own. Pure
    /// additive read; all failures are swallowed (empty fetch → no-op).
    async fn backfill_child_sessions(&self) {
        if !is_opencode_protocol(&self.remote_config.protocol) {
            return;
        }
        let parent = { self.state.read().await.opencode_session_id.clone() };
        let Some(parent) = parent else {
            return;
        };

        let base_url = normalize_base_url(&self.remote_config.url);
        let auth_header = build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());
        let children = fetch_child_sessions(&self.http_client, &base_url, auth_header.as_deref(), &parent).await;
        if children.is_empty() {
            return;
        }

        let mut newly_registered = 0usize;
        for child in &children {
            let now = now_ms();
            let registered = {
                let mut state = self.state.write().await;
                subagent::try_register_from_session_created(child, &parent, &mut state.child_sessions, now)
            };
            if let Some(child_session) = registered {
                subagent::emit_started(&self.runtime, &parent, &child_session);
                newly_registered += 1;
            }
        }

        if newly_registered > 0 {
            info!(
                conversation_id = %self.runtime.conversation_id(),
                parent_session = %parent,
                backfilled = newly_registered,
                total_children = children.len(),
                "M08: backfilled previously-unseen child sessions"
            );
        }
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
                if let Some(ref sid) = session_id {
                    let (status, reason) = opencode_status(status_type);
                    self.runtime
                        .emit(AgentStreamEvent::SessionStatus(SessionStatusEventData {
                            session_id: sid.clone(),
                            status: status.to_string(),
                            reason,
                        }));
                }
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
                            // Arm the turn: the root session is now producing.
                            // Only a Finish that follows a root `busy` is real
                            // (see `root_turn_active`). Child `busy` must not arm
                            // the parent turn. Also: a stray root `busy` arriving
                            // AFTER this turn's Finish has already been emitted
                            // must not re-arm — OpenCode can fire a second
                            // `busy → idle` burst as part of its post-completion
                            // finalization, and re-arming would let that stray
                            // `idle` emit a second Finish that lands on the next
                            // user turn's relay (see `finished_current_user_turn`).
                            if !is_child && !state.finished_current_user_turn {
                                state.root_turn_active = true;
                            }
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
                            self.emit_root_turn_finish(session_id.clone()).await;
                        }
                    }
                    _ => {}
                }
            }
            "session.idle" => {
                if let Some(ref sid) = session_id {
                    self.runtime.emit(AgentStreamEvent::SessionIdle(SessionIdleEventData {
                        session_id: sid.clone(),
                        reason: opencode_idle_reason(props),
                        at: props.get("at").and_then(|v| v.as_i64()).unwrap_or_else(now_ms),
                    }));
                }
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
                    self.emit_root_turn_finish(session_id.clone()).await;
                }
            }
            "session.error" => {
                // OpenCode sends errors as { name: "...", data: { message: "..." } }
                // in the "error" field of properties.
                let typed_error = typed_error_from_props(props);
                let message = typed_error.message.as_str();
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
                    if typed_error.recoverable {
                        let message_id = props
                            .get("messageID")
                            .or_else(|| props.get("messageId"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        let part_id = props
                            .get("partID")
                            .or_else(|| props.get("partId"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        if !message_id.is_empty() && !part_id.is_empty() {
                            self.runtime.emit(AgentStreamEvent::SessionErrorRecovered(
                                SessionErrorRecoveredEventData {
                                    message_id: message_id.to_string(),
                                    part_id: part_id.to_string(),
                                    error: typed_error.clone(),
                                    recovery_action: props
                                        .get("recoveryAction")
                                        .or_else(|| props.get("recovery_action"))
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("retry")
                                        .to_string(),
                                },
                            ));
                            return;
                        }
                    }
                    warn!(
                        conversation_id = %self.runtime.conversation_id(),
                        error = message,
                        "OpenCode session error"
                    );
                    self.runtime
                        .emit(AgentStreamEvent::Error(crate::protocol::events::ErrorEventData {
                            message: message.to_string(),
                            code: None,
                            kind: Some(typed_error.kind),
                            metadata: typed_error.metadata,
                            recoverable: Some(typed_error.recoverable),
                        }));
                    self.runtime.transition_to(ConversationStatus::Finished);
                }
            }
            "session.next.retried" => {
                let error = props.get("error").unwrap_or(&Value::Null);
                let message_id = props
                    .get("messageID")
                    .or_else(|| props.get("messageId"))
                    .or_else(|| error.get("messageID"))
                    .or_else(|| error.get("messageId"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let part_id = props
                    .get("partID")
                    .or_else(|| props.get("partId"))
                    .or_else(|| error.get("partID"))
                    .or_else(|| error.get("partId"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if message_id.is_empty() || part_id.is_empty() {
                    debug!(
                        conversation_id = %self.runtime.conversation_id(),
                        "session.next.retried missing message/part correlation"
                    );
                    return;
                }
                let reason = retry_reason(
                    props
                        .get("reason")
                        .or_else(|| error.get("reason"))
                        .or_else(|| error.get("type"))
                        .and_then(|v| v.as_str()),
                );
                self.runtime.emit(AgentStreamEvent::Retry(RetryEventData {
                    message_id: message_id.to_string(),
                    part_id: part_id.to_string(),
                    attempt: props.get("attempt").and_then(|v| v.as_u64()).unwrap_or(1),
                    reason,
                    retry_after: props
                        .get("retryAfter")
                        .or_else(|| props.get("retry_after"))
                        .and_then(|v| v.as_u64()),
                    provider_hint: props
                        .get("providerHint")
                        .or_else(|| props.get("provider_hint"))
                        .or_else(|| error.get("providerID"))
                        .or_else(|| error.get("providerId"))
                        .and_then(|v| v.as_str())
                        .map(String::from),
                    replay: None,
                }));
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
                let message_id = props.get("messageID").and_then(|v| v.as_str()).unwrap_or("");
                let part_id = props.get("partID").and_then(|v| v.as_str()).unwrap_or("");
                let is_reasoning = self.state.read().await.reasoning_parts.contains(part_id);
                // E03: queue the delta into the per-part accumulator instead of
                // emitting directly. Coalesced at ~60 Hz; force-flushed on
                // `message.part.updated` for this part and on root-turn finish.
                self.delta_batcher
                    .push(message_id, part_id, field, delta, is_reasoning)
                    .await;
            }
            "message.part.updated" => {
                if let Some(part) = props.get("part") {
                    // E03: drain any deltas accumulated for this part before
                    // forwarding the update. The server has finalized this
                    // part — we want what we've buffered to land on screen now,
                    // not 16 ms later. Safe no-op for tool/other types that
                    // never accumulate deltas.
                    if let Some(part_id) = part.get("id").and_then(|v| v.as_str()) {
                        self.delta_batcher.flush_part(part_id).await;
                    }
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

                            self.emit_root_turn_finish(session_id.clone()).await;
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
            "question.asked" => {
                // The model is asking the user a clarifying question via the
                // `ask` tool (M09). Map each question to an Approvals card so it
                // rides the same queue as permission prompts; buffer the request
                // so `confirm()` can accumulate answers and POST one reply.
                let parsed = match opencode_question::parse_question_request(props) {
                    Some(p) => p,
                    None => {
                        warn!(
                            conversation_id = %self.runtime.conversation_id(),
                            "question.asked missing id/questions; cannot prompt user"
                        );
                        return;
                    }
                };

                // Dedup: drop a re-emitted question we already answered, and
                // don't double-queue one already pending.
                {
                    let state = self.state.read().await;
                    if state.recently_replied_questions.contains_key(&parsed.request_id)
                        || state.pending_questions.contains_key(&parsed.request_id)
                    {
                        return;
                    }
                }

                let confirmations = opencode_question::build_question_confirmations(&parsed);
                {
                    let mut state = self.state.write().await;
                    state.pending_questions.insert(
                        parsed.request_id.clone(),
                        opencode_question::PendingQuestion::new(&parsed),
                    );
                    for conf in &confirmations {
                        state.confirmations.retain(|c| c.call_id != conf.call_id);
                        state.confirmations.push(conf.clone());
                    }
                }

                info!(
                    conversation_id = %self.runtime.conversation_id(),
                    request_id = %parsed.request_id,
                    question_count = confirmations.len(),
                    "queued OpenCode question for UI prompt"
                );

                for conf in confirmations {
                    self.runtime
                        .emit(AgentStreamEvent::AcpPermission(AcpPermissionEventData::Confirmation(
                            conf,
                        )));
                }
            }
            "question.replied" | "question.rejected" => {
                // The question was answered/closed — possibly by another client
                // pointed at the same server, or the echo of our own POST.
                // Reconcile: drop the buffered request and any still-pending
                // cards for it, and stamp the dedup map so a late local reply is
                // suppressed. Mirrors the `permission.replied` reconciliation.
                let request_id = props
                    .get("requestID")
                    .and_then(|v| v.as_str())
                    .or_else(|| props.get("id").and_then(|v| v.as_str()))
                    .map(String::from);
                if let Some(id) = request_id {
                    let mut state = self.state.write().await;
                    let was_pending = state.pending_questions.remove(&id).is_some();
                    state.confirmations.retain(|c| {
                        opencode_question::parse_question_call_id(&c.call_id)
                            .map(|(rid, _)| rid != id)
                            .unwrap_or(true)
                    });
                    state.recently_replied_questions.insert(id.clone(), now_ms());
                    prune_replied_map(&mut state.recently_replied_questions, QUESTION_DEDUP_CAP);
                    if was_pending {
                        debug!(
                            conversation_id = %self.runtime.conversation_id(),
                            request_id = %id,
                            event_type = event_type,
                            "OpenCode question resolved upstream; cleared pending question"
                        );
                    }
                }
            }
            "models-dev.refreshed" | "catalog.model.updated" => {
                // The server's provider/model catalog changed upstream (a
                // models.dev refresh or a per-model update). Drop the cached
                // `model_id -> context_window` map so the next synthesized
                // `acp_context_usage` lookup re-fetches from
                // `GET /config/providers` rather than reporting a stale window.
                // (E02)
                let mut state = self.state.write().await;
                if state.model_context_limits.take().is_some() {
                    debug!(
                        conversation_id = %self.runtime.conversation_id(),
                        event_type = event_type,
                        "OpenCode model catalog refreshed; invalidated context-window cache"
                    );
                }
            }
            "permission.replied" => {
                // A permission this server tracks was answered. The reply may
                // have come from another client (TUI/desktop) pointed at the
                // same server, or be the echo of our own `confirm()`. Reconcile
                // local state either way: drop any still-pending confirmation
                // for that request id so it can't linger in the Approvals tab
                // (or be re-surfaced by a `get_confirmations()` poll / reconnect
                // backfill), and stamp the dedup map so a late local reply for
                // the same id is suppressed (mirrors the `responded` Map in
                // OpenCode's own `permission.tsx`). (E02)
                let request_id = props
                    .get("requestID")
                    .and_then(|v| v.as_str())
                    .or_else(|| props.get("id").and_then(|v| v.as_str()))
                    .map(String::from);
                if let Some(id) = request_id {
                    let mut state = self.state.write().await;
                    let was_pending = state.confirmations.iter().any(|c| c.call_id == id);
                    state.confirmations.retain(|c| c.call_id != id);
                    state.recently_replied_permissions.insert(id.clone(), now_ms());
                    if was_pending {
                        debug!(
                            conversation_id = %self.runtime.conversation_id(),
                            request_id = %id,
                            "OpenCode permission replied upstream; cleared pending confirmation"
                        );
                    }
                }
            }
            "installation.updated" | "installation.update-available" => {
                // The server reported a new opencode version is installed or
                // available. Surface at info for production troubleshooting; we
                // deliberately do NOT auto-update and do NOT fan a user-facing
                // banner out of every per-conversation manager (the event is
                // global and would multiply N-fold across open conversations).
                // A dedicated, de-duplicated update banner is a renderer-side
                // follow-up. (E02 §3.2)
                let version = props.get("version").and_then(|v| v.as_str()).unwrap_or("");
                info!(
                    conversation_id = %self.runtime.conversation_id(),
                    event_type = event_type,
                    version = %version,
                    "OpenCode server reported an installation update"
                );
            }
            "skill.updated" => {
                self.state.write().await.opencode_skills = None;
                debug!(
                    conversation_id = %self.runtime.conversation_id(),
                    "M10: OpenCode skill catalog invalidated by skill.updated"
                );
            }
            "session.compacted" => {
                let summary = props.get("summary").and_then(|v| v.as_str()).unwrap_or("");
                let tokens_reclaimed = props.get("tokensReclaimed").and_then(|v| v.as_u64()).unwrap_or(0);
                let original_start = props
                    .get("originalRange")
                    .and_then(|r| r.get("startMessageId"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let original_end = props
                    .get("originalRange")
                    .and_then(|r| r.get("endMessageId"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                info!(
                    conversation_id = %self.runtime.conversation_id(),
                    tokens_reclaimed,
                    "OpenCode session compacted (M22)"
                );
                self.runtime.emit(AgentStreamEvent::OpencodeSessionCompacted(
                    crate::protocol::events::OpencodeSessionCompactedData {
                        summary: summary.to_string(),
                        tokens_reclaimed,
                        original_start_message_id: original_start.to_string(),
                        original_end_message_id: original_end.to_string(),
                    },
                ));
            }
            other => {
                // Full event coverage (E02 §3.4): a known-but-intentionally-
                // unhandled event is acknowledged at `trace` so the noisy global
                // stream stays quiet, while a genuinely-new event type is logged
                // at `debug` with a non-sensitive property-key fingerprint so it
                // becomes visible in diagnostics without code changes.
                if is_known_ignored_event(other) {
                    trace!(
                        conversation_id = %self.runtime.conversation_id(),
                        event_type = other,
                        is_child = is_child,
                        "Recognized OpenCode event with no client-side action"
                    );
                } else {
                    debug!(
                        conversation_id = %self.runtime.conversation_id(),
                        event_type = other,
                        is_child = is_child,
                        prop_fingerprint = %event_property_fingerprint(props),
                        prop_count = props.as_object().map(|m| m.len()).unwrap_or(0),
                        "Unhandled OpenCode event"
                    );
                }
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

    /// Fire-and-forget `POST /question/{requestID}/reply` (M09) with the full
    /// `answers` matrix. Same fire-and-forget contract as
    /// [`Self::spawn_permission_response`]: returns immediately, logs the
    /// outcome, never propagates errors.
    fn spawn_question_reply(&self, request_id: String, answers: Vec<Vec<String>>) {
        if !is_opencode_protocol(&self.remote_config.protocol) {
            return;
        }
        let base_url = normalize_base_url(&self.remote_config.url);
        let auth_header = build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());
        let http_client = self.http_client.clone();
        let conversation_id = self.runtime.conversation_id().to_string();
        tokio::spawn(async move {
            let (url, body) = opencode_question::build_question_reply_request(&base_url, &request_id, &answers);
            let mut req = http_client.post(&url).json(&body).timeout(Duration::from_secs(10));
            if let Some(h) = auth_header {
                req = req.header(AUTHORIZATION, h);
            }
            match req.send().await {
                Ok(resp) if resp.status().is_success() => {
                    debug!(%conversation_id, %request_id, endpoint = %url, "OpenCode question reply sent");
                }
                Ok(resp) => {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    warn!(%conversation_id, %request_id, %status, %body, endpoint = %url, "OpenCode question reply returned non-success");
                }
                Err(e) => {
                    warn!(%conversation_id, %request_id, error = %e, endpoint = %url, "OpenCode question reply request failed");
                }
            }
        });
    }

    /// Fire-and-forget `POST /question/{requestID}/reject` (M09 — no body).
    fn spawn_question_reject(&self, request_id: String) {
        if !is_opencode_protocol(&self.remote_config.protocol) {
            return;
        }
        let base_url = normalize_base_url(&self.remote_config.url);
        let auth_header = build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());
        let http_client = self.http_client.clone();
        let conversation_id = self.runtime.conversation_id().to_string();
        tokio::spawn(async move {
            let url = opencode_question::build_question_reject_url(&base_url, &request_id);
            let mut req = http_client.post(&url).timeout(Duration::from_secs(10));
            if let Some(h) = auth_header {
                req = req.header(AUTHORIZATION, h);
            }
            match req.send().await {
                Ok(resp) if resp.status().is_success() => {
                    debug!(%conversation_id, %request_id, endpoint = %url, "OpenCode question rejected");
                }
                Ok(resp) => {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    warn!(%conversation_id, %request_id, %status, %body, endpoint = %url, "OpenCode question reject returned non-success");
                }
                Err(e) => {
                    warn!(%conversation_id, %request_id, error = %e, endpoint = %url, "OpenCode question reject request failed");
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

    /// Populate the cached OpenCode primary-agent catalog from `GET /agent`.
    /// Hidden/internal agents and subagents are not selectable as session modes.
    async fn ensure_opencode_agents(&self) -> Vec<AgentModeOption> {
        {
            let guard = self.state.read().await;
            if let Some(ref cached) = guard.opencode_agents {
                return cached.clone();
            }
        }

        let base_url = normalize_base_url(&self.remote_config.url);
        let auth_header = build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());
        let mut req = self
            .http_client
            .get(format!("{base_url}/agent"))
            .timeout(Duration::from_secs(10));
        if let Some(ref h) = auth_header {
            req = req.header(AUTHORIZATION, h.as_str());
        }

        let fetched = match req.send().await {
            Ok(resp) if resp.status().is_success() => match resp.json::<Value>().await {
                Ok(body) => parse_opencode_agent_modes(&body),
                Err(e) => {
                    warn!(error = %e, "M10: failed to parse OpenCode agent catalog");
                    default_opencode_agent_modes()
                }
            },
            Ok(resp) => {
                warn!(status = %resp.status(), "M10: OpenCode agent catalog request failed");
                default_opencode_agent_modes()
            }
            Err(e) => {
                warn!(error = %e, "M10: failed to fetch OpenCode agent catalog");
                default_opencode_agent_modes()
            }
        };

        debug!(
            conversation_id = %self.runtime.conversation_id(),
            agent_count = fetched.len(),
            "M10: populated OpenCode agent mode cache"
        );
        let mut guard = self.state.write().await;
        guard.opencode_agents = Some(fetched.clone());
        fetched
    }

    /// Populate the cached OpenCode skill catalog from `GET /skill` (M10).
    /// Returns an empty vec on fetch failure so we don't retry every query.
    /// Cache is invalidated by `skill.updated` SSE events.
    async fn ensure_opencode_skills(&self) -> Vec<RemoteSkillInfo> {
        {
            let guard = self.state.read().await;
            if let Some(ref cached) = guard.opencode_skills {
                return cached.clone();
            }
        }

        let base_url = normalize_base_url(&self.remote_config.url);
        let auth_header = build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());
        let mut req = self
            .http_client
            .get(format!("{base_url}/skill"))
            .timeout(Duration::from_secs(10));
        if let Some(ref h) = auth_header {
            req = req.header(AUTHORIZATION, h.as_str());
        }

        let fetched = match req.send().await {
            Ok(resp) if resp.status().is_success() => match resp.json::<Value>().await {
                Ok(body) => parse_opencode_skills(&body),
                Err(e) => {
                    warn!(error = %e, "M10: failed to parse OpenCode skill catalog");
                    Vec::new()
                }
            },
            Ok(resp) => {
                warn!(status = %resp.status(), "M10: OpenCode skill catalog request failed");
                Vec::new()
            }
            Err(e) => {
                warn!(error = %e, "M10: failed to fetch OpenCode skill catalog");
                Vec::new()
            }
        };

        debug!(
            conversation_id = %self.runtime.conversation_id(),
            skill_count = fetched.len(),
            "M10: populated OpenCode skill cache"
        );
        let mut guard = self.state.write().await;
        guard.opencode_skills = Some(fetched.clone());
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

    /// M10: server-side skill catalog exposed via `IAgentTask::get_skills`
    /// for the Remote variant. Empty for non-opencode protocols.
    pub async fn get_skills_impl(&self) -> Result<Vec<RemoteSkillInfo>, AppError> {
        if !is_opencode_protocol(&self.remote_config.protocol) {
            return Ok(Vec::new());
        }
        Ok(self.ensure_opencode_skills().await)
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

    /// Emit the root turn's terminal `Finish` exactly once. No-op unless the
    /// turn was armed by a root `session.status busy` (see `root_turn_active`).
    /// This collapses OpenCode's redundant per-turn terminal trio
    /// (`message.updated finish=stop` + `session.status idle` + `session.idle`)
    /// into a single `Finish`, and — critically — ignores a stray trailing
    /// `idle` from the previous turn that can arrive just after the next turn's
    /// stream relay subscribes, which otherwise terminated the new turn
    /// instantly (the "2nd message never gets a reply" bug).
    async fn emit_root_turn_finish(&self, session_id: Option<String>) {
        {
            let mut state = self.state.write().await;
            if !state.root_turn_active {
                return;
            }
            state.root_turn_active = false;
            // Lock out subsequent root `busy` events for this user turn so
            // OpenCode's post-completion `busy → idle` finalization burst
            // can't re-arm the gate and emit a phantom second Finish.
            // Cleared in `send_message` when the user submits the next prompt.
            state.finished_current_user_turn = true;
        }
        // E03: drain any accumulated SSE deltas before the terminal `Finish`.
        // The stream relay treats `Finish` as a hard terminator, so a delta
        // emitted afterwards would never reach the renderer.
        self.delta_batcher.flush_all().await;
        self.runtime
            .emit(AgentStreamEvent::Finish(FinishEventData { session_id }));
        self.runtime.transition_to(ConversationStatus::Finished);
        self.release_turn_slot().await;
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

    /// Resolve the active OpenCode session id, erroring if none exists yet.
    /// Shared by the M07 edit/delete operations.
    async fn require_opencode_session(&self) -> Result<String, AppError> {
        if !is_opencode_protocol(&self.remote_config.protocol) {
            return Err(AppError::BadRequest(
                "Message edit/delete is only supported for OpenCode remote conversations".into(),
            ));
        }
        self.state
            .read()
            .await
            .opencode_session_id
            .clone()
            .ok_or_else(|| AppError::BadRequest("No active OpenCode session for this conversation".into()))
    }

    /// M07: delete an entire message (`DELETE /session/{id}/message/{messageID}`).
    /// The server emits `message.removed`, which our SSE handler reconciles into
    /// the local store.
    pub async fn opencode_delete_message(&self, message_id: &str) -> Result<(), AppError> {
        let session_id = self.require_opencode_session().await?;
        let base_url = normalize_base_url(&self.remote_config.url);
        let url = format!("{base_url}/session/{session_id}/message/{message_id}");
        self.opencode_delete(&url, "message").await
    }

    /// M07: delete a single part (`DELETE /session/{id}/message/{messageID}/part/{partID}`).
    pub async fn opencode_delete_message_part(&self, message_id: &str, part_id: &str) -> Result<(), AppError> {
        let session_id = self.require_opencode_session().await?;
        let base_url = normalize_base_url(&self.remote_config.url);
        let url = format!("{base_url}/session/{session_id}/message/{message_id}/part/{part_id}");
        self.opencode_delete(&url, "part").await
    }

    /// Shared DELETE helper for [`Self::opencode_delete_message`] /
    /// [`Self::opencode_delete_message_part`].
    async fn opencode_delete(&self, url: &str, what: &str) -> Result<(), AppError> {
        let auth_header = build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());
        let mut req = self.http_client.delete(url).timeout(Duration::from_secs(15));
        if let Some(ref h) = auth_header {
            req = req.header(AUTHORIZATION, h.as_str());
        }
        let resp = req
            .send()
            .await
            .map_err(|e| AppError::BadGateway(format!("OpenCode delete {what} failed: {e}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body_text = resp.text().await.unwrap_or_default();
            return Err(AppError::BadGateway(format!(
                "OpenCode delete {what} returned {status}: {body_text}"
            )));
        }
        Ok(())
    }

    /// M07: edit the text of a single text part. OpenCode's `PATCH .../part/{partID}`
    /// requires the full `Part` object, so we GET the message, mutate the target
    /// part's `text`, and PATCH it back. The server emits `message.part.updated`,
    /// reconciled by the existing SSE handler.
    pub async fn opencode_edit_message_part(
        &self,
        message_id: &str,
        part_id: &str,
        new_text: &str,
    ) -> Result<(), AppError> {
        let session_id = self.require_opencode_session().await?;
        let base_url = normalize_base_url(&self.remote_config.url);
        let auth_header = build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());

        // 1. Fetch the message to obtain the full part object.
        let get_url = format!("{base_url}/session/{session_id}/message/{message_id}");
        let mut get_req = self.http_client.get(&get_url).timeout(Duration::from_secs(15));
        if let Some(ref h) = auth_header {
            get_req = get_req.header(AUTHORIZATION, h.as_str());
        }
        let get_resp = get_req
            .send()
            .await
            .map_err(|e| AppError::BadGateway(format!("OpenCode get message failed: {e}")))?;
        if !get_resp.status().is_success() {
            let status = get_resp.status();
            return Err(AppError::BadGateway(format!("OpenCode get message returned {status}")));
        }
        let message: Value = get_resp
            .json()
            .await
            .map_err(|e| AppError::BadGateway(format!("OpenCode message response was not JSON: {e}")))?;

        // 2. Locate the target part and replace its text.
        let parts = message
            .get("parts")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut target = parts
            .into_iter()
            .find(|p| p.get("id").and_then(|v| v.as_str()) == Some(part_id))
            .ok_or_else(|| AppError::NotFound(format!("part '{part_id}' not found on message '{message_id}'")))?;
        if target.get("type").and_then(|v| v.as_str()) != Some("text") {
            return Err(AppError::BadRequest("Only text parts can be edited".into()));
        }
        target["text"] = json!(new_text);

        // 3. PATCH the full part back.
        let patch_url = format!("{base_url}/session/{session_id}/message/{message_id}/part/{part_id}");
        let mut patch_req = self
            .http_client
            .patch(&patch_url)
            .json(&target)
            .timeout(Duration::from_secs(15));
        if let Some(ref h) = auth_header {
            patch_req = patch_req.header(AUTHORIZATION, h.as_str());
        }
        let patch_resp = patch_req
            .send()
            .await
            .map_err(|e| AppError::BadGateway(format!("OpenCode patch part failed: {e}")))?;
        if !patch_resp.status().is_success() {
            let status = patch_resp.status();
            let body_text = patch_resp.text().await.unwrap_or_default();
            return Err(AppError::BadGateway(format!(
                "OpenCode patch part returned {status}: {body_text}"
            )));
        }
        Ok(())
    }

    /// Shared request helper for OpenCode session-scoped actions (M01–M05).
    /// Issues `<method> /session/{id}{subpath}` with optional JSON body and
    /// returns the parsed JSON response (or `Null` when the body is empty).
    async fn opencode_session_request(
        &self,
        method: reqwest::Method,
        subpath: &str,
        body: Option<Value>,
        timeout_secs: u64,
    ) -> Result<Value, AppError> {
        let session_id = self.require_opencode_session().await?;
        let base_url = normalize_base_url(&self.remote_config.url);
        let url = format!("{base_url}/session/{session_id}{subpath}");
        let auth_header = build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());

        let mut req = self
            .http_client
            .request(method, &url)
            .timeout(Duration::from_secs(timeout_secs));
        if let Some(ref b) = body {
            req = req.json(b);
        }
        if let Some(ref h) = auth_header {
            req = req.header(AUTHORIZATION, h.as_str());
        }
        let resp = req
            .send()
            .await
            .map_err(|e| AppError::BadGateway(format!("OpenCode session request failed: {e}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body_text = resp.text().await.unwrap_or_default();
            return Err(AppError::BadGateway(format!(
                "OpenCode session request returned {status}: {body_text}"
            )));
        }
        let text = resp.text().await.unwrap_or_default();
        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&text)
            .map_err(|e| AppError::BadGateway(format!("OpenCode session response was not JSON: {e}")))
    }

    /// M01: fork the session (optionally from a specific message). Returns the
    /// new server-side session id.
    ///
    /// OpenCode's `POST /session/{id}/fork` with `messageID` is **exclusive** —
    /// it keeps only the messages *strictly before* that message (verified
    /// against the live server: forking at the first message yields an empty
    /// session). "Fork from here" must be **inclusive** of the message the user
    /// clicked, so we fork at the message that *follows* it. When the selected
    /// message is the last one (or none is given, i.e. a header/session-level
    /// fork), we omit `messageID` entirely and OpenCode copies the whole
    /// transcript.
    pub async fn opencode_fork(&self, message_id: Option<&str>) -> Result<String, AppError> {
        let fork_at = match message_id.filter(|m| m.starts_with("msg")) {
            Some(m) => self.opencode_message_after(m).await?,
            None => None,
        };
        let body = fork_at
            .as_deref()
            .map(|m| json!({ "messageID": m }))
            .unwrap_or_else(|| json!({}));
        let resp = self
            .opencode_session_request(reqwest::Method::POST, "/fork", Some(body), 30)
            .await?;
        resp.get("id")
            .and_then(|v| v.as_str())
            .map(String::from)
            .ok_or_else(|| AppError::BadGateway(format!("OpenCode fork response missing id: {resp}")))
    }

    /// Return the id of the OpenCode message immediately following `message_id`
    /// in the current session's transcript, or `None` when `message_id` is the
    /// last message (so the caller forks from the tip and includes everything).
    /// Also returns `None` if the message can't be located — forking from the
    /// tip is the safe, non-lossy fallback.
    async fn opencode_message_after(&self, message_id: &str) -> Result<Option<String>, AppError> {
        let resp = self
            .opencode_session_request(reqwest::Method::GET, "/message", None, 30)
            .await?;
        Ok(next_opencode_message_id(&resp, message_id))
    }

    /// M02: revert the session to a message (and optionally a specific part).
    pub async fn opencode_revert(&self, message_id: &str, part_id: Option<&str>) -> Result<(), AppError> {
        let mut body = json!({ "messageID": message_id });
        if let Some(pid) = part_id.filter(|p| p.starts_with("prt")) {
            body["partID"] = json!(pid);
        }
        self.opencode_session_request(reqwest::Method::POST, "/revert", Some(body), 30)
            .await
            .map(|_| ())
    }

    /// M02: restore all reverted messages.
    pub async fn opencode_unrevert(&self) -> Result<(), AppError> {
        self.opencode_session_request(reqwest::Method::POST, "/unrevert", None, 30)
            .await
            .map(|_| ())
    }

    /// M04: summarize/compact the session. Uses the session's current desired
    /// model; errors if none is selected yet.
    pub async fn opencode_summarize(&self) -> Result<(), AppError> {
        let (provider_id, model_id) = {
            let state = self.state.read().await;
            let m = state
                .desired_model
                .as_ref()
                .ok_or_else(|| AppError::BadRequest("Select a model before summarizing".into()))?;
            let provider = m.get("providerID").and_then(|v| v.as_str()).unwrap_or("opencode-go");
            let model = m
                .get("modelID")
                .or_else(|| m.get("id"))
                .and_then(|v| v.as_str())
                .ok_or_else(|| AppError::BadRequest("Current model id is unavailable".into()))?;
            (provider.to_string(), model.to_string())
        };
        let body = json!({ "providerID": provider_id, "modelID": model_id });
        self.opencode_session_request(reqwest::Method::POST, "/summarize", Some(body), 60)
            .await
            .map(|_| ())
    }

    /// M22 Phase 3: V2 compact the session. Tries V2 `/api/session/{id}/compact`
    /// first; on 404 falls back to V1 `opencode_summarize`. The V2 endpoint does
    /// not require `providerID`/`modelID` — the server uses the session's model.
    pub async fn opencode_compact(&self, instructions: Option<&str>) -> Result<(), AppError> {
        if !is_opencode_protocol(&self.remote_config.protocol) {
            return Err(AppError::BadRequest(
                "Compact is only available for OpenCode remote connections".into(),
            ));
        }
        let session_id = self.require_opencode_session().await?;
        let base_url = normalize_base_url(&self.remote_config.url);
        let auth_header = build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());
        match super::opencode_v2::v2_compact(
            &self.http_client,
            &base_url,
            auth_header.as_deref(),
            &session_id,
            instructions,
        )
        .await
        {
            Ok(()) => Ok(()),
            Err(AppError::BadGateway(msg)) if msg.contains("404") => {
                debug!("V2 compact not available, falling back to V1 summarize");
                self.opencode_summarize().await
            }
            Err(e) => Err(e),
        }
    }

    /// M22 Phase 3: get the session's active context window (all messages
    /// after the last compaction). Returns raw JSON array of `SessionMessage`.
    pub async fn opencode_get_context(&self) -> Result<Value, AppError> {
        if !is_opencode_protocol(&self.remote_config.protocol) {
            return Err(AppError::BadRequest(
                "Context is only available for OpenCode remote connections".into(),
            ));
        }
        let session_id = self.require_opencode_session().await?;
        let base_url = normalize_base_url(&self.remote_config.url);
        let auth_header = build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());
        super::opencode_v2::v2_get_context(&self.http_client, &base_url, auth_header.as_deref(), &session_id).await
    }

    /// M22 Phase 2: V2 prompt path. Sends via `POST /api/session/{id}/prompt`
    /// instead of V1 `/session/{id}/prompt_async`. The V2 endpoint returns a
    /// `SessionMessage` synchronously, and streaming still arrives via SSE.
    pub async fn opencode_send_v2(
        &self,
        content: &str,
        model: Option<&Value>,
        agent: Option<&str>,
        inject_skills: &[String],
    ) -> Result<(), AppError> {
        let session_id = self.require_opencode_session().await?;
        let base_url = normalize_base_url(&self.remote_config.url);
        let auth_header = build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());
        super::opencode_v2::v2_prompt(
            &self.http_client,
            &base_url,
            auth_header.as_deref(),
            &session_id,
            content,
            model,
            agent,
            inject_skills,
            Some("immediate"),
        )
        .await
        .map(|_| ())
    }

    /// M22: get V2 session messages with cursor-based pagination.
    pub async fn opencode_v2_messages(&self, limit: Option<u32>, cursor: Option<&str>) -> Result<Value, AppError> {
        if !is_opencode_protocol(&self.remote_config.protocol) {
            return Err(AppError::BadRequest(
                "V2 messages is only available for OpenCode remote connections".into(),
            ));
        }
        let session_id = self.require_opencode_session().await?;
        let base_url = normalize_base_url(&self.remote_config.url);
        let auth_header = build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());
        super::opencode_v2::v2_get_messages(
            &self.http_client,
            &base_url,
            auth_header.as_deref(),
            &session_id,
            limit,
            cursor,
        )
        .await
    }

    /// M20 Phase 1: fetch sync history since the given aggregate sequences.
    /// Used after SSE reconnect to replay events missed during the gap.
    pub async fn fetch_sync_history(
        &self,
        since: &HashMap<String, u64>,
    ) -> Result<Vec<super::opencode_sync::SyncEvent>, AppError> {
        if !is_opencode_protocol(&self.remote_config.protocol) {
            return Err(AppError::BadRequest(
                "Sync is only available for OpenCode remote connections".into(),
            ));
        }
        let base_url = normalize_base_url(&self.remote_config.url);
        let auth_header = build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());
        super::opencode_sync::fetch_sync_history(&self.http_client, &base_url, auth_header.as_deref(), since).await
    }

    /// M22 Phase 1: fetch V2 model list from the server. Returns raw JSON.
    pub async fn fetch_v2_model_list(&self) -> Result<Value, AppError> {
        if !is_opencode_protocol(&self.remote_config.protocol) {
            return Err(AppError::BadRequest(
                "V2 models is only available for OpenCode remote connections".into(),
            ));
        }
        let base_url = normalize_base_url(&self.remote_config.url);
        let auth_header = build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());
        super::opencode_v2::fetch_v2_models(&self.http_client, &base_url, auth_header.as_deref()).await
    }

    /// M22 Phase 1: fetch V2 provider list from the server. Returns raw JSON.
    pub async fn fetch_v2_provider_list(&self) -> Result<Value, AppError> {
        if !is_opencode_protocol(&self.remote_config.protocol) {
            return Err(AppError::BadRequest(
                "V2 providers is only available for OpenCode remote connections".into(),
            ));
        }
        let base_url = normalize_base_url(&self.remote_config.url);
        let auth_header = build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());
        super::opencode_v2::fetch_v2_providers(&self.http_client, &base_url, auth_header.as_deref()).await
    }

    /// M03: create a shareable link for the session. Returns the share URL.
    pub async fn opencode_share(&self) -> Result<String, AppError> {
        let resp = self
            .opencode_session_request(reqwest::Method::POST, "/share", None, 15)
            .await?;
        resp.get("share")
            .and_then(|s| s.get("url"))
            .and_then(|v| v.as_str())
            .map(String::from)
            .ok_or_else(|| AppError::BadGateway(format!("OpenCode share response missing share.url: {resp}")))
    }

    /// M03: revoke the session's shareable link.
    pub async fn opencode_unshare(&self) -> Result<(), AppError> {
        self.opencode_session_request(reqwest::Method::DELETE, "/share", None, 15)
            .await
            .map(|_| ())
    }

    /// M05: fetch the session's file diff snapshot (optionally for a specific
    /// message). Returns the `SnapshotFileDiff[]` array as JSON.
    pub async fn opencode_session_diff(&self, message_id: Option<&str>) -> Result<Value, AppError> {
        let subpath = match message_id.filter(|m| m.starts_with("msg")) {
            Some(mid) => format!("/diff?messageID={mid}"),
            None => "/diff".to_string(),
        };
        self.opencode_session_request(reqwest::Method::GET, &subpath, None, 30)
            .await
    }

    /// M19: read the server's global configuration tree (`GET /global/config`).
    /// Returns the full effective config as JSON. This endpoint is **not**
    /// session-scoped — the config is shared by every conversation pointed at
    /// the same server, so this lives on the manager's transport rather than
    /// the session path.
    pub async fn opencode_get_global_config(&self) -> Result<Value, AppError> {
        if !is_opencode_protocol(&self.remote_config.protocol) {
            return Err(AppError::BadRequest(
                "Global config is only available for OpenCode remote connections".into(),
            ));
        }
        self.opencode_config_request("/global/config", reqwest::Method::GET, None)
            .await
    }

    /// M19 (Option A): read the server's **effective** configuration tree
    /// (`GET /config`). Unlike `/global/config`, this is the merged, resolved
    /// view the engine actually runs — including project-level and agent-file
    /// definitions that override the global layer. The renderer diffs a save
    /// against this to flag edits that were persisted to the global layer but
    /// are shadowed by a higher-precedence layer (so they never take effect).
    pub async fn opencode_get_effective_config(&self) -> Result<Value, AppError> {
        if !is_opencode_protocol(&self.remote_config.protocol) {
            return Err(AppError::BadRequest(
                "Global config is only available for OpenCode remote connections".into(),
            ));
        }
        self.opencode_config_request("/config", reqwest::Method::GET, None)
            .await
    }

    /// M19: shallow-merge a partial config object into the server's global
    /// configuration (`PATCH /global/config`) and return the new effective
    /// config. Read-only keys (e.g. `version`) rejected by the server surface
    /// as a `BadGateway` carrying the server's error body, so the renderer can
    /// show a friendly message and the caller's stashed "last good" config
    /// stays intact.
    pub async fn opencode_patch_global_config(&self, partial: Value) -> Result<Value, AppError> {
        if !is_opencode_protocol(&self.remote_config.protocol) {
            return Err(AppError::BadRequest(
                "Global config is only available for OpenCode remote connections".into(),
            ));
        }
        if !partial.is_object() {
            return Err(AppError::BadRequest("Global config patch must be a JSON object".into()));
        }
        self.opencode_config_request("/global/config", reqwest::Method::PATCH, Some(partial))
            .await
    }

    /// Shared transport for the M19 config calls (`/global/config` and
    /// `/config`). Mirrors [`Self::opencode_session_request`] but targets a
    /// server-global endpoint (no session id in the path). Empty 2xx bodies map
    /// to `Value::Null`; non-2xx responses carry the server body so callers can
    /// surface the server's own validation error.
    /// M15 — read the OpenCode server's LSP server statuses (`GET /lsp`).
    /// Returns `Vec<LSPStatus>` (`[{id, name, root, status:"connected"|"error"}]`)
    /// as raw JSON; the renderer can render the small badge directly off
    /// `length` and the count of `status == "connected"` entries.
    pub async fn opencode_get_lsp_status(&self) -> Result<Value, AppError> {
        if !is_opencode_protocol(&self.remote_config.protocol) {
            return Err(AppError::BadRequest(
                "LSP status is only available for OpenCode remote connections".into(),
            ));
        }
        self.opencode_config_request("/lsp", reqwest::Method::GET, None).await
    }

    /// M16 — read the OpenCode server's VCS info (`GET /vcs`). Returns
    /// `VcsInfo { branch?, default_branch? }`. An empty object is returned when
    /// the server's working tree isn't a git repo, so the renderer can hide
    /// the source pill cleanly.
    pub async fn opencode_get_vcs_info(&self) -> Result<Value, AppError> {
        if !is_opencode_protocol(&self.remote_config.protocol) {
            return Err(AppError::BadRequest(
                "VCS is only available for OpenCode remote connections".into(),
            ));
        }
        self.opencode_config_request("/vcs", reqwest::Method::GET, None).await
    }

    /// M16 — read the porcelain-equivalent working-tree status
    /// (`GET /vcs/status`). Returns `Vec<VcsFileStatus>` with
    /// `{file, additions, deletions, status:"added"|"deleted"|"modified"}` per
    /// changed file. The renderer counts the array length for the "N changes"
    /// pill and renders the file list inside the modal.
    pub async fn opencode_get_vcs_status(&self) -> Result<Value, AppError> {
        if !is_opencode_protocol(&self.remote_config.protocol) {
            return Err(AppError::BadRequest(
                "VCS is only available for OpenCode remote connections".into(),
            ));
        }
        self.opencode_config_request("/vcs/status", reqwest::Method::GET, None)
            .await
    }

    /// M16 — read the structured working-tree diff (`GET /vcs/diff?mode=git`).
    /// `mode` is required by the server: `"git"` (default) shows the working
    /// tree against HEAD; `"branch"` shows the current branch against the
    /// default branch. Returns `Vec<VcsFileDiff>` with the per-file `patch`
    /// (unified diff) string so the modal can render it without a second
    /// round-trip to `/vcs/diff/raw`.
    pub async fn opencode_get_vcs_diff(&self, mode: &str) -> Result<Value, AppError> {
        if !is_opencode_protocol(&self.remote_config.protocol) {
            return Err(AppError::BadRequest(
                "VCS is only available for OpenCode remote connections".into(),
            ));
        }
        let normalized = match mode {
            "git" | "branch" => mode,
            _ => "git",
        };
        let path = format!("/vcs/diff?mode={normalized}");
        self.opencode_config_request(&path, reqwest::Method::GET, None).await
    }

    async fn opencode_config_request(
        &self,
        path: &str,
        method: reqwest::Method,
        body: Option<Value>,
    ) -> Result<Value, AppError> {
        let base_url = normalize_base_url(&self.remote_config.url);
        let url = format!("{base_url}{path}");
        let auth_header = build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());

        let mut req = self.http_client.request(method, &url).timeout(Duration::from_secs(15));
        if let Some(ref b) = body {
            req = req.json(b);
        }
        if let Some(ref h) = auth_header {
            req = req.header(AUTHORIZATION, h.as_str());
        }
        let resp = req
            .send()
            .await
            .map_err(|e| AppError::BadGateway(format!("OpenCode global-config request failed: {e}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body_text = resp.text().await.unwrap_or_default();
            return Err(AppError::BadGateway(format!(
                "OpenCode global-config request returned {status}: {body_text}"
            )));
        }
        let text = resp.text().await.unwrap_or_default();
        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&text)
            .map_err(|e| AppError::BadGateway(format!("OpenCode global-config response was not JSON: {e}")))
    }

    async fn opencode_create_session(&self, base_url: &str) -> Result<String, AppError> {
        let url = format!("{base_url}/session");
        let auth_header = build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());

        // C04 tool-host mode. In the default "local" mode we inject the
        // client-side fs MCP and deny the server's built-in tools, forcing the
        // model to operate on the user's local files via `aionui-local-fs_*`.
        // In "server" mode we do neither: no MCP registration, no pre-deny —
        // the agent uses the OpenCode server's own tools against the server's
        // working tree, and permission prompts flow through the existing
        // `permission.asked` handler.
        let server_tools = is_server_tool_host(&self.remote_config);

        let session_body = if server_tools {
            info!(
                conversation_id = %self.runtime.conversation_id(),
                "creating OpenCode session in server-tools mode (no local-fs MCP, no tool pre-deny)"
            );
            // Omit `permission` entirely so the server applies its own defaults
            // and emits permission prompts for sensitive operations.
            json!({})
        } else {
            // Register the client-side fs MCP with the remote OpenCode before
            // creating the session, so any tool the agent emits on its first
            // turn already sees our tools advertised. Best-effort: failure is
            // logged but does not block session create — the agent will still
            // function, just without client-side fs (matching prior behavior).
            self.ensure_local_fs_mcp(base_url, auth_header.as_deref()).await;

            json!({
                "permission": [
                    { "permission": "bash",  "pattern": "*", "action": "deny" },
                    { "permission": "read",  "pattern": "*", "action": "deny" },
                    { "permission": "edit",  "pattern": "*", "action": "deny" },
                    { "permission": "glob",  "pattern": "*", "action": "deny" },
                    { "permission": "grep",  "pattern": "*", "action": "deny" }
                ]
            })
        };

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
    async fn opencode_send(
        &self,
        content: &str,
        opencode_message_id: Option<&str>,
        inject_skills: &[String],
    ) -> Result<(), AppError> {
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

        let result = self
            .opencode_send_after_acquire(content, &base_url, opencode_message_id, inject_skills)
            .await;
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
    async fn opencode_send_after_acquire(
        &self,
        content: &str,
        base_url: &str,
        opencode_message_id: Option<&str>,
        inject_skills: &[String],
    ) -> Result<(), AppError> {
        let base_url = base_url.to_string();
        // Re-confirm ownership of the OpenCode `aionui-local-fs` slot
        // before every prompt. If another conversation prompted last on
        // the same OpenCode instance, the slot now points at *their* MCP
        // server (rooted at *their* workspace). Re-registering here puts
        // it back on our server so the tool calls the model emits this
        // turn land on the right project. No-op when we already own the
        // slot. Best-effort — failures are logged inside, never thrown.
        //
        // C04: server-tools mode never uses the local-fs MCP, so skip the
        // per-prompt re-registration entirely.
        if !is_server_tool_host(&self.remote_config) {
            let auth_header_for_mcp =
                build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());
            self.ensure_local_fs_mcp(&base_url, auth_header_for_mcp.as_deref())
                .await;
        }

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
        // F02: persist the freshly-created session id to
        // `conversation.extra.sessionKey` immediately (not only at turn
        // completion). The 60s `remote_session_sync` loop dedups by
        // `extra.sessionKey`; if a sync tick runs mid-turn before the key is
        // persisted, it sees our just-created server session as "new" and
        // mirrors it into a duplicate Chisl conversation. Writing it here, at
        // creation time, closes that window. Best-effort: failure is logged and
        // never blocks the prompt (the completion-path persist still runs).
        if session_just_created {
            self.persist_session_key_now(&session_id).await;
        }
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

        // C04: in server-tools mode the model uses the OpenCode server's own
        // tools against the server's working tree, so we do NOT inject the
        // local-fs tool instructions or enumerate the (irrelevant) local
        // workspace tree. We let the server apply its own system prompt by
        // omitting `system` from the body below.
        let server_tools = is_server_tool_host(&self.remote_config);
        let system_hint: Option<String> = if server_tools {
            None
        } else {
            let workspace = self.runtime.workspace().to_string();
            let tree = {
                let root = std::path::PathBuf::from(&workspace);
                tokio::task::spawn_blocking(move || render_project_tree_default(&root))
                    .await
                    .unwrap_or_else(|_| String::from("(failed to enumerate project)"))
            };
            let shell_hint = super::local_fs_mcp::shell::shell_hint();
            Some(format!(
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
            ))
        };

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
        });
        // M07: when the caller owns the OpenCode message id (`^msg…`), send it
        // so the user message is addressable for later edit/delete. ONLY valid
        // on a freshly created session — sending `body.messageID` on a session
        // that already has prior turns makes OpenCode silently skip the model
        // invocation (the user message is created but no assistant response is
        // generated, emitting only `session.status busy → idle`). This was the
        // root of the "2nd message returns nothing" bug.
        if session_just_created && let Some(mid) = opencode_message_id.filter(|m| m.starts_with("msg")) {
            body["messageID"] = json!(mid);
        }
        if let Some(hint) = system_hint {
            body["system"] = json!(hint);
        }
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
        // M10: pass selected server-side skills into the prompt body so the
        // model can load the matching SKILL.md content. OpenCode's
        // `prompt_async` accepts `skills: string[]` (skill names matching the
        // `GET /skill` catalog). Empty array omitted to avoid wire noise.
        if !inject_skills.is_empty() {
            body["skills"] = json!(inject_skills);
        }

        // Surface the silent failure mode where the system hint instructs the
        // model to use `aionui-local-fs_*` tools but no local fs MCP is
        // registered. The user-visible symptom is "Unable to connect" from the
        // model; without this log there is nothing in production logs to
        // explain why. Best-effort observability — never blocks the prompt.
        // (Suppressed in server-tools mode, where running without a local-fs
        // MCP is the intended configuration.)
        if !server_tools && self.local_fs_mcp.lock().await.is_none() {
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
    /// Persist the OpenCode session id into `conversation.extra.sessionKey`
    /// right after the session is created (F02). Mirrors
    /// `aionui_conversation::service::persist_session_key`, duplicated here
    /// (rather than shared) to avoid a dependency cycle: this crate is below
    /// `aionui-conversation`. Best-effort and idempotent — no-op when no repo
    /// is wired (test constructors) or the key is already current.
    async fn persist_session_key_now(&self, session_key: &str) {
        let Some(repo) = self.conversation_repo.as_ref() else {
            return;
        };
        let conv_id = self.runtime.conversation_id().to_string();
        let row = match repo.get(&conv_id).await {
            Ok(Some(r)) => r,
            _ => return,
        };
        let mut extra: Value = serde_json::from_str(&row.extra).unwrap_or_else(|_| json!({}));
        if extra.get("sessionKey").and_then(|v| v.as_str()) == Some(session_key) {
            return;
        }
        extra["sessionKey"] = Value::String(session_key.to_owned());
        let extra_json = match serde_json::to_string(&extra) {
            Ok(j) => j,
            Err(e) => {
                warn!(conversation_id = %conv_id, error = %e, "F02: failed to serialize extra for early session-key persist");
                return;
            }
        };
        let update = aionui_db::ConversationRowUpdate {
            extra: Some(extra_json),
            updated_at: Some(now_ms()),
            ..Default::default()
        };
        if let Err(e) = repo.update(&conv_id, &update).await {
            warn!(conversation_id = %conv_id, error = %e, "F02: failed to persist session key at creation time");
        } else {
            debug!(conversation_id = %conv_id, "F02: persisted session key to conversation.extra at creation time");
        }
    }

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
        let available = self.ensure_opencode_agents().await;
        if !available.iter().any(|m| m.id == normalized) {
            return Err(AppError::BadRequest(format!(
                "Unsupported OpenCode mode '{normalized}'"
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
        let available_modes = self.ensure_opencode_agents().await;
        let guard = self.state.read().await;
        match guard.desired_agent.as_deref() {
            Some(m) => Ok(aionui_api_types::AgentModeResponse {
                mode: m.to_owned(),
                initialized: true,
                available_modes: Some(available_modes),
            }),
            None => Ok(aionui_api_types::AgentModeResponse {
                mode: "build".into(),
                initialized: false,
                available_modes: Some(available_modes),
            }),
        }
    }

    async fn fetch_opencode_models(&self) -> Result<Vec<aionui_api_types::ModelInfoEntry>, AppError> {
        let base_url = normalize_base_url(&self.remote_config.url);
        let auth_header = build_auth_header(&self.remote_config.auth_type, self.remote_config.auth_token.as_deref());

        // NOTE: this in-conversation picker must show exactly the same models as
        // the Guid (New Chat) page, which uses V1 `/provider` via
        // `services/remote.rs::fetch_opencode_model_info`. The V2 `/api/model`
        // endpoint carries a per-model `enabled` flag that is stricter than V1's
        // "all models of connected providers" semantics (it drops deprecated /
        // alpha / non-allowlisted models), so migrating this path to V2 made
        // models silently disappear from the thread picker while the Guid page
        // still showed them. We keep this on V1 for parity; the richer V2 data
        // (status / cost / capabilities) is exposed separately via the
        // `/opencode/v2-models` route for a future V2-aware model-card UI.
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
            // Clear the post-Finish lockout so the new user turn's root `busy`
            // can arm `root_turn_active` again. See `finished_current_user_turn`.
            state.finished_current_user_turn = false;
            first
        };
        self.runtime.transition_to(ConversationStatus::Running);

        if is_opencode_protocol(&self.remote_config.protocol) {
            if is_first {
                self.emit_model_info().await;
            }
            self.opencode_send(&data.content, data.opencode_message_id.as_deref(), &data.inject_skills)
                .await
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

            // Release the per-base-url MCP turn slot now. The turn is over, but
            // its terminal `Finish` (which normally releases the slot via
            // `emit_root_turn_finish`) may never arrive — e.g. the SSE stream
            // is mid-reconnect after a server hot-reload, or the abort races
            // ahead of the idle event. Without an explicit release here, a
            // cancelled turn pins the slot until `TURN_WAIT_TIMEOUT` (600s),
            // leaving every other conversation on this server stuck in
            // "Processing…". `release_turn` is owner-checked and idempotent, so
            // a later `Finish` that also releases is a harmless no-op.
            self.release_turn_slot().await;
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

        // M14: drop the log-forwarder registration before further teardown so
        // any tracing emitted during this kill path doesn't queue against a
        // server we're about to lose contact with.
        opencode_log_forwarder::unregister_forwarder(self.runtime.conversation_id());

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
            // Pending `/question` buffers (M09) are dropped too; the turn is
            // ending so there's no one to answer them.
            state.pending_questions.clear();
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

            // OpenCode `/question` reply (M09)? `call_id` "question-{reqID}-{i}".
            // The chosen value is the option *label* (or the reject sentinel),
            // so read `raw_reply` (the verbatim value) rather than the
            // permission-normalized `reply`.
            if opencode_question::is_question_call_id(call_id)
                && let Some((request_id, index)) = opencode_question::parse_question_call_id(call_id)
            {
                {
                    // Drop the card the user just acted on.
                    state.confirmations.retain(|c| c.call_id != call_id);
                    let chosen = raw_reply.clone().unwrap_or_default();

                    if chosen == opencode_question::QUESTION_REJECT_VALUE {
                        // Reject closes the whole request: drop the buffer and
                        // every sibling card, then POST the reject.
                        state.pending_questions.remove(&request_id);
                        state.confirmations.retain(|c| {
                            opencode_question::parse_question_call_id(&c.call_id)
                                .map(|(rid, _)| rid != request_id)
                                .unwrap_or(true)
                        });
                        state.recently_replied_questions.insert(request_id.clone(), now_ms());
                        prune_replied_map(&mut state.recently_replied_questions, QUESTION_DEDUP_CAP);
                        drop(state);
                        self.spawn_question_reject(request_id);
                        return Ok(());
                    }

                    // Record this question's answer; only POST once every
                    // question in the request has been answered.
                    let answers = match state.pending_questions.get_mut(&request_id) {
                        Some(p) => {
                            p.record(index, vec![chosen]);
                            if p.is_complete() { Some(p.collected()) } else { None }
                        }
                        None => None,
                    };
                    if let Some(answers) = answers {
                        state.pending_questions.remove(&request_id);
                        state.recently_replied_questions.insert(request_id.clone(), now_ms());
                        prune_replied_map(&mut state.recently_replied_questions, QUESTION_DEDUP_CAP);
                        drop(state);
                        self.spawn_question_reply(request_id, answers);
                    }
                    return Ok(());
                }
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
            // for diagnostics only. Question cards (`question-…`, M09) are NOT
            // permissions — they must be rejected via `/question/{id}/reject`,
            // so they're excluded here and collected separately below.
            let opencode_call_ids: Vec<(String, Option<String>)> = state
                .confirmations
                .iter()
                .filter(|c| !local_call_ids.contains(&c.call_id))
                .filter(|c| !opencode_question::is_question_call_id(&c.call_id))
                .map(|c| (c.call_id.clone(), c.session_id.clone()))
                .collect();

            // Distinct question requestIDs still pending — reject each once.
            let question_request_ids: HashSet<String> = state.pending_questions.keys().cloned().collect();
            state.pending_questions.clear();
            for id in &question_request_ids {
                state.recently_replied_questions.insert(id.clone(), now_ms());
            }
            prune_replied_map(&mut state.recently_replied_questions, QUESTION_DEDUP_CAP);

            state.confirmations.clear();
            for id in question_request_ids {
                self.spawn_question_reject(id);
            }
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

    // ---- M01: inclusive fork message resolution ----------------------------

    fn sample_transcript() -> Value {
        serde_json::json!([
            { "info": { "id": "msg_u1", "role": "user" }, "parts": [] },
            { "info": { "id": "msg_a1", "role": "assistant" }, "parts": [] },
            { "info": { "id": "msg_a2", "role": "assistant" }, "parts": [] },
            { "info": { "id": "msg_u2", "role": "user" }, "parts": [] },
        ])
    }

    #[test]
    fn next_opencode_message_id_returns_following_message() {
        let t = sample_transcript();
        // Forking "from here" at msg_u1 must include msg_u1, so we fork at the
        // next message (msg_a1) since OpenCode's fork is exclusive.
        assert_eq!(next_opencode_message_id(&t, "msg_u1"), Some("msg_a1".to_string()));
        assert_eq!(next_opencode_message_id(&t, "msg_a2"), Some("msg_u2".to_string()));
    }

    #[test]
    fn next_opencode_message_id_last_message_is_none() {
        let t = sample_transcript();
        // Last message → None → caller forks from the tip (copies everything).
        assert_eq!(next_opencode_message_id(&t, "msg_u2"), None);
    }

    #[test]
    fn next_opencode_message_id_unknown_or_malformed_is_none() {
        let t = sample_transcript();
        assert_eq!(next_opencode_message_id(&t, "msg_missing"), None);
        // Non-array payloads must not panic.
        assert_eq!(next_opencode_message_id(&serde_json::json!({}), "msg_u1"), None);
    }

    // ---- E02: event-coverage classifier -----------------------------------

    #[test]
    fn known_ignored_event_recognizes_global_and_mirror_events() {
        // Server/global-scoped.
        assert!(is_known_ignored_event("server.connected"));
        assert!(is_known_ignored_event("tui.toast.show"));
        assert!(is_known_ignored_event("project.updated"));
        // V2 streaming mirror of the message.part.* path.
        assert!(is_known_ignored_event("session.next.text.delta"));
        assert!(is_known_ignored_event("session.next.tool.success"));
        // Session-scoped feature stub delegated to a later plan.
        assert!(is_known_ignored_event("session.diff"));
        assert!(is_known_ignored_event("session.updated"));
    }

    #[test]
    fn known_ignored_event_excludes_handled_and_unknown_events() {
        // Explicitly handled in the dispatcher — must NOT be in the quiet set.
        assert!(!is_known_ignored_event("session.idle"));
        assert!(!is_known_ignored_event("permission.asked"));
        assert!(!is_known_ignored_event("permission.replied"));
        assert!(!is_known_ignored_event("models-dev.refreshed"));
        assert!(!is_known_ignored_event("installation.updated"));
        assert!(!is_known_ignored_event("session.next.tool.progress"));
        // Genuinely unknown.
        assert!(!is_known_ignored_event("weird.thing"));
        assert!(!is_known_ignored_event(""));
    }

    #[test]
    fn property_fingerprint_is_stable_and_key_order_independent() {
        let a = json!({ "sessionID": "ses_1", "reply": "once", "requestID": "perm_1" });
        let b = json!({ "requestID": "perm_1", "reply": "once", "sessionID": "ses_1" });
        // Same key set, different declaration order → identical fingerprint.
        assert_eq!(event_property_fingerprint(&a), event_property_fingerprint(&b));
        assert_eq!(event_property_fingerprint(&a).len(), 16);
    }

    #[test]
    fn property_fingerprint_depends_only_on_keys_not_values() {
        // Different values, same keys → same fingerprint (no payload leakage).
        let secret = json!({ "version": "9.9.9-supersecret" });
        let plain = json!({ "version": "1.0.0" });
        assert_eq!(event_property_fingerprint(&secret), event_property_fingerprint(&plain));
        // A different key set → different fingerprint.
        let other = json!({ "diff": "..." });
        assert_ne!(event_property_fingerprint(&plain), event_property_fingerprint(&other));
    }

    #[test]
    fn property_fingerprint_handles_non_object_payloads() {
        // Must not panic on absent/empty/non-object properties.
        assert_eq!(event_property_fingerprint(&json!({})).len(), 16);
        assert_eq!(event_property_fingerprint(&Value::Null).len(), 16);
        // Empty object and JSON null share the empty-key-set fingerprint.
        assert_eq!(
            event_property_fingerprint(&json!({})),
            event_property_fingerprint(&Value::Null)
        );
    }

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
    fn auth_header_basic_uses_supplied_credentials() {
        let h = build_auth_header("basic", Some("user:secret"));
        let expected = format!("Basic {}", BASE64.encode("user:secret"));
        assert_eq!(h, Some(expected));
    }

    #[test]
    fn parse_opencode_agent_modes_keeps_primary_visible_agents() {
        let modes = parse_opencode_agent_modes(&json!([
            { "name": "build", "mode": "primary", "description": "Default" },
            { "name": "review", "mode": "primary", "description": "Review code" },
            { "name": "ui-expert", "mode": "all", "native": false, "description": "Custom UI agent" },
            { "name": "explore", "mode": "subagent", "description": "Subagent" },
            { "name": "compaction", "mode": "primary", "hidden": true }
        ]));

        assert_eq!(
            modes.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
            vec!["build", "plan", "review", "ui-expert"]
        );
        assert_eq!(modes[2].description.as_deref(), Some("Review code"));
        assert_eq!(modes[3].description.as_deref(), Some("Custom UI agent"));
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
    fn server_tool_host_only_for_opencode_server_value() {
        let mut cfg = RemoteAgentConfig {
            remote_agent_id: "ra".into(),
            protocol: "opencode".into(),
            url: "http://h".into(),
            auth_type: "none".into(),
            auth_token: None,
            allow_insecure: false,
            tool_host: "server".into(),
        };
        assert!(is_server_tool_host(&cfg));
        cfg.tool_host = "local".into();
        assert!(!is_server_tool_host(&cfg));
        cfg.tool_host = "".into();
        assert!(!is_server_tool_host(&cfg));
        // Non-opencode protocol never uses server tool-host, even if set.
        cfg.protocol = "openclaw".into();
        cfg.tool_host = "server".into();
        assert!(!is_server_tool_host(&cfg));
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
            tool_host: "local".to_string(),
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
            tool_host: "local".to_string(),
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

        // Arm the turn: OpenCode always sends `session.status busy` before any
        // assistant output. `Finish` is only emitted for a turn that was armed
        // by a root `busy` (see `root_turn_active` / `emit_root_turn_finish`).
        let busy_event = json!({
            "type": "session.status",
            "properties": { "sessionID": "sess_1", "status": { "type": "busy" } }
        })
        .to_string();
        agent.handle_opencode_sse_event(&busy_event).await;

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
    async fn stray_idle_before_busy_does_not_emit_finish() {
        // Regression: a trailing `session.idle`/`finish=stop` from the previous
        // turn can be delivered just as the next turn's stream relay subscribes.
        // Without a preceding root `busy` it must NOT emit `Finish`, otherwise
        // the new turn is terminated instantly and the user never gets a reply.
        let agent = opencode_test_agent().await;
        let mut rx = agent.runtime.subscribe();

        let idle_status = json!({
            "type": "session.status",
            "properties": { "sessionID": "sess_1", "status": { "type": "idle" } }
        })
        .to_string();
        let session_idle = json!({
            "type": "session.idle",
            "properties": { "sessionID": "sess_1" }
        })
        .to_string();
        let finish_stop = json!({
            "type": "message.updated",
            "properties": { "sessionID": "sess_1", "info": { "id": "msg_stale", "role": "assistant", "finish": "stop" } }
        })
        .to_string();
        agent.handle_opencode_sse_event(&idle_status).await;
        agent.handle_opencode_sse_event(&session_idle).await;
        agent.handle_opencode_sse_event(&finish_stop).await;

        let events = drain_events(&mut rx);
        assert!(
            !events.iter().any(|e| matches!(e, AgentStreamEvent::Finish(_))),
            "stray terminal events before a root `busy` must not emit Finish"
        );
    }

    #[tokio::test]
    async fn stray_busy_after_finish_does_not_rearm_for_phantom_second_finish() {
        // Regression for the "2nd message returns nothing" bug. OpenCode emits
        // a `busy → idle` finalization burst AFTER `message.updated finish=stop`
        // — without the `finished_current_user_turn` lockout, the trailing
        // `busy` re-armed `root_turn_active` and the next `idle` fired a
        // phantom Finish that landed on the NEXT user turn's stream relay,
        // terminating it instantly with text_len=0.
        let agent = opencode_test_agent().await;
        let mut rx = agent.runtime.subscribe();

        // Turn 1: real flow + finalization burst that used to re-arm the gate.
        for ev in [
            json!({"type":"session.status","properties":{"sessionID":"sess_1","status":{"type":"busy"}}}),
            json!({"type":"message.updated","properties":{"sessionID":"sess_1","info":{"id":"msg_1","role":"assistant","finish":"stop"}}}),
            // Post-completion finalization burst — these used to emit a 2nd Finish.
            json!({"type":"session.status","properties":{"sessionID":"sess_1","status":{"type":"busy"}}}),
            json!({"type":"session.status","properties":{"sessionID":"sess_1","status":{"type":"idle"}}}),
            json!({"type":"session.idle","properties":{"sessionID":"sess_1"}}),
        ] {
            agent.handle_opencode_sse_event(&ev.to_string()).await;
        }

        let events = drain_events(&mut rx);
        let finishes = events
            .iter()
            .filter(|e| matches!(e, AgentStreamEvent::Finish(_)))
            .count();
        assert_eq!(
            finishes, 1,
            "stray `busy` after the turn's Finish must not re-arm the gate and emit a second Finish; got {finishes}"
        );
    }

    #[tokio::test]
    async fn armed_turn_emits_exactly_one_finish_for_terminal_trio() {
        // A single real turn fires OpenCode's terminal trio (finish=stop +
        // session.status idle + session.idle). After arming with `busy`, the
        // gate must collapse them into exactly one `Finish`.
        let agent = opencode_test_agent().await;
        let mut rx = agent.runtime.subscribe();

        for ev in [
            json!({"type":"session.status","properties":{"sessionID":"sess_1","status":{"type":"busy"}}}),
            json!({"type":"message.updated","properties":{"sessionID":"sess_1","info":{"id":"msg_1","role":"assistant","finish":"stop"}}}),
            json!({"type":"session.status","properties":{"sessionID":"sess_1","status":{"type":"idle"}}}),
            json!({"type":"session.idle","properties":{"sessionID":"sess_1"}}),
        ] {
            agent.handle_opencode_sse_event(&ev.to_string()).await;
        }

        let events = drain_events(&mut rx);
        let finishes = events
            .iter()
            .filter(|e| matches!(e, AgentStreamEvent::Finish(_)))
            .count();
        assert_eq!(
            finishes, 1,
            "expected exactly one Finish for the terminal trio, got {finishes}"
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
        // E03: text deltas are coalesced on a ~16 ms frame; sleep past the
        // flush window so the accumulator drains before we assert.
        tokio::time::sleep(Duration::from_millis(30)).await;
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
            tool_host: "local".to_string(),
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
            tool_host: "local".to_string(),
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

    /// E03: a burst of `message.part.delta` events for the same part is
    /// coalesced into a single `Text` event after the 16 ms flush window.
    #[tokio::test]
    async fn delta_batcher_coalesces_burst_into_single_text_event() {
        let agent = opencode_test_agent().await;
        let mut rx = agent.runtime.subscribe();

        for chunk in ["Hel", "lo, ", "wor", "ld"] {
            let ev = json!({
                "type": "message.part.delta",
                "properties": {
                    "sessionID": "sess_1",
                    "messageID": "msg_batch_1",
                    "partID": "prt_batch_1",
                    "field": "text",
                    "delta": chunk,
                }
            })
            .to_string();
            agent.handle_opencode_sse_event(&ev).await;
        }

        // Before the flush window elapses, no Text event should have been
        // emitted yet.
        assert!(
            drain_events(&mut rx)
                .iter()
                .all(|e| !matches!(e, AgentStreamEvent::Text(_))),
            "deltas must not emit individually inside the flush window"
        );

        // After the window elapses, exactly one Text event with the
        // concatenated content.
        tokio::time::sleep(Duration::from_millis(40)).await;
        let texts: Vec<String> = drain_events(&mut rx)
            .into_iter()
            .filter_map(|e| match e {
                AgentStreamEvent::Text(d) => Some(d.content),
                _ => None,
            })
            .collect();
        assert_eq!(texts.len(), 1, "expected one coalesced Text, got {:?}", texts);
        assert_eq!(texts[0], "Hello, world");
    }

    /// E03: when `message.part.updated` arrives for a part with deltas still
    /// pending, the accumulator is flushed synchronously — the user sees the
    /// already-streamed text rather than losing it past the part boundary.
    #[tokio::test]
    async fn delta_batcher_flushes_on_part_updated() {
        let agent = opencode_test_agent().await;
        let mut rx = agent.runtime.subscribe();

        for chunk in ["foo", "bar"] {
            let ev = json!({
                "type": "message.part.delta",
                "properties": {
                    "sessionID": "sess_1",
                    "messageID": "msg_flush_1",
                    "partID": "prt_flush_1",
                    "field": "text",
                    "delta": chunk,
                }
            })
            .to_string();
            agent.handle_opencode_sse_event(&ev).await;
        }

        let updated = json!({
            "type": "message.part.updated",
            "properties": {
                "sessionID": "sess_1",
                "time": 0,
                "part": { "id": "prt_flush_1", "type": "text" }
            }
        })
        .to_string();
        agent.handle_opencode_sse_event(&updated).await;

        // No sleep — the flush should have happened synchronously on the
        // `message.part.updated` boundary.
        let texts: Vec<String> = drain_events(&mut rx)
            .into_iter()
            .filter_map(|e| match e {
                AgentStreamEvent::Text(d) => Some(d.content),
                _ => None,
            })
            .collect();
        assert_eq!(
            texts,
            vec!["foobar".to_string()],
            "deltas must be flushed when their part finalizes"
        );
    }

    /// E03: pending deltas drain before the terminal `Finish` so streamed
    /// text never lingers past the end of the turn.
    #[tokio::test]
    async fn delta_batcher_flushes_on_root_turn_finish() {
        let agent = opencode_test_agent().await;
        let mut rx = agent.runtime.subscribe();

        // Arm the root turn.
        let busy = json!({
            "type": "session.status",
            "properties": { "sessionID": "sess_1", "status": { "type": "busy" } }
        })
        .to_string();
        agent.handle_opencode_sse_event(&busy).await;

        let delta = json!({
            "type": "message.part.delta",
            "properties": {
                "sessionID": "sess_1",
                "messageID": "msg_finish_1",
                "partID": "prt_finish_1",
                "field": "text",
                "delta": "tail-end ",
            }
        })
        .to_string();
        agent.handle_opencode_sse_event(&delta).await;

        // Send a `finish=stop` immediately — without flush_all, the
        // accumulator's pending "tail-end " would be lost behind Finish.
        let stop = json!({
            "type": "message.updated",
            "properties": {
                "sessionID": "sess_1",
                "info": { "id": "msg_finish_1", "role": "assistant", "finish": "stop" }
            }
        })
        .to_string();
        agent.handle_opencode_sse_event(&stop).await;

        let events = drain_events(&mut rx);
        let text_idx = events
            .iter()
            .position(|e| matches!(e, AgentStreamEvent::Text(d) if d.content == "tail-end "))
            .expect("pending delta must be flushed before Finish");
        let finish_idx = events
            .iter()
            .position(|e| matches!(e, AgentStreamEvent::Finish(_)))
            .expect("Finish must still be emitted");
        assert!(text_idx < finish_idx, "Text must be emitted before Finish");
    }

    /// E03: deltas for parts flagged as `reasoning` flush as `Thinking`
    /// events, not user-visible `Text` — matching the pre-batching behavior.
    #[tokio::test]
    async fn delta_batcher_routes_reasoning_parts_to_thinking() {
        let agent = opencode_test_agent().await;
        let mut rx = agent.runtime.subscribe();

        // Register the part as reasoning before any deltas — this mirrors
        // OpenCode's actual ordering (`part.updated type=reasoning` lands
        // before its deltas).
        agent
            .state
            .write()
            .await
            .reasoning_parts
            .insert("prt_reasoning_1".to_string());

        for chunk in ["thinking ", "out loud"] {
            let ev = json!({
                "type": "message.part.delta",
                "properties": {
                    "sessionID": "sess_1",
                    "messageID": "msg_reasoning_1",
                    "partID": "prt_reasoning_1",
                    "field": "text",
                    "delta": chunk,
                }
            })
            .to_string();
            agent.handle_opencode_sse_event(&ev).await;
        }

        tokio::time::sleep(Duration::from_millis(40)).await;
        let thinking: Vec<String> = drain_events(&mut rx)
            .into_iter()
            .filter_map(|e| match e {
                AgentStreamEvent::Thinking(d) => Some(d.content),
                _ => None,
            })
            .collect();
        assert_eq!(thinking, vec!["thinking out loud".to_string()]);
    }
}
