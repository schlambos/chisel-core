//! OpenCode SSE protocol conformance helpers.
//!
//! This crate is **test-only**. It is not wired into any runtime path of the
//! Chisl backend; its job is to pin the JSON shape of every SSE event the
//! remote-OpenCode adapter recognises so upstream protocol drift fails CI at
//! PR time instead of surfacing as silent rendering bugs in production.
//!
//! The companion `PROTOCOL.md` at
//! `crates/aionui-ai-agent/src/manager/remote/PROTOCOL.md` enumerates every
//! event with its handling site and status; the recorded fixtures under
//! `fixtures/` capture one or more live samples per event type; and the
//! integration test in `tests/event_parsing.rs` exercises this library against
//! those fixtures.
//!
//! ## Forward-compatibility contract
//!
//! 1. **Required-field removal must fail.** If a known event loses a field the
//!    adapter relies on, [`classify_event`] returns
//!    [`ClassifyError::MissingRequiredField`].
//! 2. **Unknown event types must NOT fail.** New event types upstream are
//!    surfaced as [`ClassifyOutcome::Unknown`] and reported as warnings — the
//!    test treats them as informational, not failures.
//! 3. **Unknown fields on known events must NOT fail.** The classifier reads
//!    only the required-field paths it needs; everything else passes through.
//!
//! Mirrors the adapter's `unwrap_event` (`agent.rs:233`) and the dispatch arms
//! in `handle_opencode_sse_event` (`agent.rs:1430`). When the dispatcher
//! changes, update [`EventKind`] and the matching `validate_*` helper.

use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Result of classifying a single event after `unwrap_event` normalisation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClassifyOutcome {
    /// A known event type with all required fields present.
    Handled(EventKind),
    /// A known event type listed in the adapter's `KNOWN_IGNORED_EVENTS` set.
    /// The dispatcher recognises it but takes no action; the JSON shape is
    /// pinned here so a future promotion does not surprise the runtime.
    Ignored(IgnoredKind),
    /// The V2 sync mirror wrapper (`{type:"sync", syncEvent:{…}, id}`). Not
    /// consumed by the current dispatcher but pinned for the eventual cursor-
    /// replay promotion.
    Sync { mirror_type: String, seq: i64 },
    /// An unrecognised event type. The classifier records the discriminator
    /// and a non-sensitive property-key fingerprint so the test can log it as
    /// a warning. Forward-compatible: this case never causes a test failure.
    Unknown {
        event_type: String,
        prop_fingerprint: String,
    },
}

/// Reasons [`classify_event`] returns an error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClassifyError {
    /// The top-level value is not a JSON object, or `type` is not a string.
    MalformedEnvelope,
    /// A required field on a known event is missing or has the wrong type.
    /// `event_type` is the recognised discriminator; `field_path` is the
    /// dotted JSON path that was expected to resolve.
    MissingRequiredField {
        event_type: String,
        field_path: &'static str,
    },
}

impl std::fmt::Display for ClassifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MalformedEnvelope => write!(f, "malformed event envelope (missing or non-string `type`)"),
            Self::MissingRequiredField { event_type, field_path } => {
                write!(f, "event `{event_type}` missing required field `{field_path}`")
            }
        }
    }
}

impl std::error::Error for ClassifyError {}

/// Known event types with handled dispatch arms in the adapter.
///
/// Each variant maps to one `match` arm in `handle_opencode_sse_event`
/// (`agent.rs:1430`) or to the SSE-reader lifecycle handling in
/// `run_event_reader` (`agent.rs:455`). The string variants for tool input
/// phases preserve the legacy `_input.` aliases the adapter still recognises.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventKind {
    ServerConnected,
    ServerInstanceDisposed,
    InstallationUpdated,
    InstallationUpdateAvailable,
    CatalogModelUpdated,
    ModelsDevRefreshed,
    SessionStatus,
    SessionIdle,
    SessionError,
    SessionCompacted,
    SessionNextAgentSwitched,
    SessionNextModelSwitched,
    SessionNextToolInputStarted,
    SessionNextToolInputDelta,
    SessionNextToolInputEnded,
    SessionNextToolProgress,
    MessageUpdated,
    MessagePartUpdated,
    MessagePartDelta,
    TodoUpdated,
    PermissionAsked,
    PermissionReplied,
    QuestionAsked,
    QuestionReplied,
    QuestionRejected,
    SkillUpdated,
    /// `session.created` is dispatched as a child-registration trigger when
    /// the payload's `parentID` matches the owning session; the root case is
    /// quietly acknowledged. Both reach the gate, so it is "handled" from a
    /// conformance perspective.
    SessionCreated,
}

/// Event types listed in the adapter's `KNOWN_IGNORED_EVENTS` set
/// (`agent.rs:272`). The dispatcher recognises them but takes no action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IgnoredKind {
    ServerHeartbeat,
    GlobalDisposed,
    AccountAdded,
    AccountRemoved,
    AccountSwitched,
    FileEdited,
    FileWatcherUpdated,
    CommandExecuted,
    LspUpdated,
    LspClientDiagnostics,
    McpToolsChanged,
    McpBrowserOpenFailed,
    ProjectUpdated, // dispatcher falls through; pinned because it has no
    // `KNOWN_IGNORED_EVENTS` entry today — see PROTOCOL.md note
    VcsBranchUpdated,
    WorkspaceFailed,
    WorkspaceReady,
    WorkspaceStatus,
    WorktreeFailed,
    WorktreeReady,
    PtyCreated,
    PtyUpdated,
    PtyExited,
    PtyDeleted,
    TuiCommandExecute,
    TuiPromptAppend,
    TuiSessionSelect,
    TuiToastShow,
    SessionNextPrompted,
    SessionNextSynthetic,
    SessionNextRetried,
    SessionNextStepStarted,
    SessionNextStepEnded,
    SessionNextStepFailed,
    SessionNextTextStarted,
    SessionNextTextDelta,
    SessionNextTextEnded,
    SessionNextReasoningStarted,
    SessionNextReasoningDelta,
    SessionNextReasoningEnded,
    SessionNextShellStarted,
    SessionNextShellEnded,
    SessionNextToolCalled,
    SessionNextToolSuccess,
    SessionNextToolFailed,
    SessionNextCompactionStarted,
    SessionNextCompactionDelta,
    SessionNextCompactionEnded,
    SessionUpdated,
    SessionDeleted,
    SessionDiff,
    MessageRemoved,
    MessagePartRemoved,
}

/// Mirror of the adapter's `unwrap_event` (`agent.rs:233`): strips the outer
/// `{payload}` wrapper that `/global/event` adds, leaving the inner event
/// object. A no-op for the legacy `/event` raw shape.
pub fn unwrap_event(raw: Value) -> Value {
    match raw {
        Value::Object(mut map) => match map.remove("payload") {
            Some(payload) => payload,
            None => Value::Object(map),
        },
        other => other,
    }
}

/// Classify an already-`unwrap_event`-ed JSON value.
///
/// Returns:
/// - `Ok(Handled)` when the type matches a known dispatch arm AND every
///   required field for that arm resolves.
/// - `Ok(Ignored)` when the type is in `KNOWN_IGNORED_EVENTS`.
/// - `Ok(Sync)` when the value is the V2 sync mirror wrapper.
/// - `Ok(Unknown)` when the type is a string but not recognised. Carries the
///   discriminator and a property-key fingerprint for diagnostics.
/// - `Err(MalformedEnvelope)` when the envelope itself is broken (missing
///   `type`, non-object root, etc.).
/// - `Err(MissingRequiredField)` when a known event's required-field path
///   does not resolve.
///
/// Mirrors `handle_opencode_sse_event` (`agent.rs:1430`). The required-field
/// checks intentionally cover only fields the adapter relies on; everything
/// else is tolerated (forward-compatibility contract).
pub fn classify_event(value: &Value) -> Result<ClassifyOutcome, ClassifyError> {
    let obj = value.as_object().ok_or(ClassifyError::MalformedEnvelope)?;
    let event_type = obj
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or(ClassifyError::MalformedEnvelope)?;

    if event_type == "sync" {
        let sync = obj.get("syncEvent").and_then(|v| v.as_object());
        let mirror_type = sync
            .and_then(|s| s.get("type"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let seq = sync.and_then(|s| s.get("seq")).and_then(|v| v.as_i64()).unwrap_or(-1);
        return Ok(ClassifyOutcome::Sync { mirror_type, seq });
    }

    let props = obj.get("properties");

    // Known event types — order keeps the most-frequent arms near the top to
    // mirror the dispatcher's hot path.
    match event_type {
        // -- Session lifecycle ----------------------------------------------------
        "session.status" => {
            require_str(event_type, props, "sessionID")?;
            require_path(event_type, props, "status.type")?;
            Ok(ClassifyOutcome::Handled(EventKind::SessionStatus))
        }
        "session.idle" => {
            require_str(event_type, props, "sessionID")?;
            Ok(ClassifyOutcome::Handled(EventKind::SessionIdle))
        }
        "session.error" => {
            // sessionID is optional in the SDK v2 type (it carries `sessionID?`);
            // the adapter still tolerates its absence. Only verify the envelope.
            let _ = props;
            Ok(ClassifyOutcome::Handled(EventKind::SessionError))
        }
        "session.compacted" => {
            require_str(event_type, props, "sessionID")?;
            Ok(ClassifyOutcome::Handled(EventKind::SessionCompacted))
        }
        "session.created" => {
            require_str(event_type, props, "sessionID")?;
            // `info` is what the child-registration helper inspects for
            // `parentID`; treat it as required.
            require_obj(event_type, props, "info")?;
            Ok(ClassifyOutcome::Handled(EventKind::SessionCreated))
        }
        // -- Session.next lifecycle ----------------------------------------------
        "session.next.agent.switched" => {
            require_str(event_type, props, "sessionID")?;
            require_str(event_type, props, "agent")?;
            Ok(ClassifyOutcome::Handled(EventKind::SessionNextAgentSwitched))
        }
        "session.next.model.switched" => {
            require_str(event_type, props, "sessionID")?;
            require_str(event_type, props, "model.id")?;
            Ok(ClassifyOutcome::Handled(EventKind::SessionNextModelSwitched))
        }
        // -- Tool input + progress (canonical + legacy aliases) ------------------
        "session.next.tool.input.started" | "session.next.tool_input.started" | "message.next.tool.input.started" => {
            require_str(event_type, props, "sessionID")?;
            require_tool_input_correlation(event_type, props)?;
            Ok(ClassifyOutcome::Handled(EventKind::SessionNextToolInputStarted))
        }
        "session.next.tool.input.delta" | "session.next.tool_input.delta" | "message.next.tool.input.delta" => {
            require_str(event_type, props, "sessionID")?;
            require_tool_input_correlation(event_type, props)?;
            Ok(ClassifyOutcome::Handled(EventKind::SessionNextToolInputDelta))
        }
        "session.next.tool.input.ended" | "session.next.tool_input.ended" | "message.next.tool.input.ended" => {
            require_str(event_type, props, "sessionID")?;
            require_tool_input_correlation(event_type, props)?;
            Ok(ClassifyOutcome::Handled(EventKind::SessionNextToolInputEnded))
        }
        "session.next.tool.progress" | "session.next.tool_progress" | "message.next.tool.progress" => {
            require_str(event_type, props, "sessionID")?;
            require_tool_input_correlation(event_type, props)?;
            Ok(ClassifyOutcome::Handled(EventKind::SessionNextToolProgress))
        }
        // -- Message lifecycle ---------------------------------------------------
        "message.updated" => {
            require_str(event_type, props, "sessionID")?;
            require_obj(event_type, props, "info")?;
            Ok(ClassifyOutcome::Handled(EventKind::MessageUpdated))
        }
        "message.part.updated" => {
            require_str(event_type, props, "sessionID")?;
            require_str(event_type, props, "part.type")?;
            Ok(ClassifyOutcome::Handled(EventKind::MessagePartUpdated))
        }
        "message.part.delta" => {
            require_str(event_type, props, "sessionID")?;
            require_str(event_type, props, "messageID")?;
            require_str(event_type, props, "partID")?;
            require_str(event_type, props, "field")?;
            // `delta` may be a string or any JSON scalar in practice; require presence only.
            require_path(event_type, props, "delta")?;
            Ok(ClassifyOutcome::Handled(EventKind::MessagePartDelta))
        }
        // -- Permission / question -----------------------------------------------
        "permission.asked" => {
            require_str(event_type, props, "id")?;
            require_str(event_type, props, "sessionID")?;
            Ok(ClassifyOutcome::Handled(EventKind::PermissionAsked))
        }
        "permission.replied" => {
            // The adapter accepts both `requestID` and `id` as the correlation key
            // (`agent.rs:2217-2221`), so either is sufficient.
            let p = props.and_then(|v| v.as_object());
            let has_request_id = p
                .map(|o| o.contains_key("requestID") || o.contains_key("id"))
                .unwrap_or(false);
            if !has_request_id {
                return Err(ClassifyError::MissingRequiredField {
                    event_type: event_type.to_string(),
                    field_path: "requestID|id",
                });
            }
            require_str(event_type, props, "reply")?;
            Ok(ClassifyOutcome::Handled(EventKind::PermissionReplied))
        }
        "question.asked" => {
            require_str(event_type, props, "id")?;
            Ok(ClassifyOutcome::Handled(EventKind::QuestionAsked))
        }
        "question.replied" => Ok(ClassifyOutcome::Handled(EventKind::QuestionReplied)),
        "question.rejected" => Ok(ClassifyOutcome::Handled(EventKind::QuestionRejected)),
        // -- Todo -----------------------------------------------------------------
        "todo.updated" => {
            require_str(event_type, props, "sessionID")?;
            require_array(event_type, props, "todos")?;
            Ok(ClassifyOutcome::Handled(EventKind::TodoUpdated))
        }
        // -- Skill catalog --------------------------------------------------------
        "skill.updated" => Ok(ClassifyOutcome::Handled(EventKind::SkillUpdated)),
        // -- Server / installation / catalog (global) ----------------------------
        "server.connected" => Ok(ClassifyOutcome::Handled(EventKind::ServerConnected)),
        "server.instance.disposed" => Ok(ClassifyOutcome::Handled(EventKind::ServerInstanceDisposed)),
        "installation.updated" => Ok(ClassifyOutcome::Handled(EventKind::InstallationUpdated)),
        "installation.update-available" => Ok(ClassifyOutcome::Handled(EventKind::InstallationUpdateAvailable)),
        "catalog.model.updated" => Ok(ClassifyOutcome::Handled(EventKind::CatalogModelUpdated)),
        "models-dev.refreshed" => Ok(ClassifyOutcome::Handled(EventKind::ModelsDevRefreshed)),
        // -- Known-ignored events (KNOWN_IGNORED_EVENTS in agent.rs:272) ---------
        "server.heartbeat" => Ok(ClassifyOutcome::Ignored(IgnoredKind::ServerHeartbeat)),
        "global.disposed" => Ok(ClassifyOutcome::Ignored(IgnoredKind::GlobalDisposed)),
        "account.added" => Ok(ClassifyOutcome::Ignored(IgnoredKind::AccountAdded)),
        "account.removed" => Ok(ClassifyOutcome::Ignored(IgnoredKind::AccountRemoved)),
        "account.switched" => Ok(ClassifyOutcome::Ignored(IgnoredKind::AccountSwitched)),
        "file.edited" => Ok(ClassifyOutcome::Ignored(IgnoredKind::FileEdited)),
        "file.watcher.updated" => Ok(ClassifyOutcome::Ignored(IgnoredKind::FileWatcherUpdated)),
        "command.executed" => Ok(ClassifyOutcome::Ignored(IgnoredKind::CommandExecuted)),
        "lsp.updated" => Ok(ClassifyOutcome::Ignored(IgnoredKind::LspUpdated)),
        "lsp.client.diagnostics" => Ok(ClassifyOutcome::Ignored(IgnoredKind::LspClientDiagnostics)),
        "mcp.tools.changed" => Ok(ClassifyOutcome::Ignored(IgnoredKind::McpToolsChanged)),
        "mcp.browser.open.failed" => Ok(ClassifyOutcome::Ignored(IgnoredKind::McpBrowserOpenFailed)),
        "project.updated" => Ok(ClassifyOutcome::Ignored(IgnoredKind::ProjectUpdated)),
        "vcs.branch.updated" => Ok(ClassifyOutcome::Ignored(IgnoredKind::VcsBranchUpdated)),
        "workspace.failed" => Ok(ClassifyOutcome::Ignored(IgnoredKind::WorkspaceFailed)),
        "workspace.ready" => Ok(ClassifyOutcome::Ignored(IgnoredKind::WorkspaceReady)),
        "workspace.status" => Ok(ClassifyOutcome::Ignored(IgnoredKind::WorkspaceStatus)),
        "worktree.failed" => Ok(ClassifyOutcome::Ignored(IgnoredKind::WorktreeFailed)),
        "worktree.ready" => Ok(ClassifyOutcome::Ignored(IgnoredKind::WorktreeReady)),
        "pty.created" => Ok(ClassifyOutcome::Ignored(IgnoredKind::PtyCreated)),
        "pty.updated" => Ok(ClassifyOutcome::Ignored(IgnoredKind::PtyUpdated)),
        "pty.exited" => Ok(ClassifyOutcome::Ignored(IgnoredKind::PtyExited)),
        "pty.deleted" => Ok(ClassifyOutcome::Ignored(IgnoredKind::PtyDeleted)),
        "tui.command.execute" => Ok(ClassifyOutcome::Ignored(IgnoredKind::TuiCommandExecute)),
        "tui.prompt.append" => Ok(ClassifyOutcome::Ignored(IgnoredKind::TuiPromptAppend)),
        "tui.session.select" => Ok(ClassifyOutcome::Ignored(IgnoredKind::TuiSessionSelect)),
        "tui.toast.show" => Ok(ClassifyOutcome::Ignored(IgnoredKind::TuiToastShow)),
        "session.next.prompted" => Ok(ClassifyOutcome::Ignored(IgnoredKind::SessionNextPrompted)),
        "session.next.synthetic" => Ok(ClassifyOutcome::Ignored(IgnoredKind::SessionNextSynthetic)),
        "session.next.retried" => Ok(ClassifyOutcome::Ignored(IgnoredKind::SessionNextRetried)),
        "session.next.step.started" => Ok(ClassifyOutcome::Ignored(IgnoredKind::SessionNextStepStarted)),
        "session.next.step.ended" => Ok(ClassifyOutcome::Ignored(IgnoredKind::SessionNextStepEnded)),
        "session.next.step.failed" => Ok(ClassifyOutcome::Ignored(IgnoredKind::SessionNextStepFailed)),
        "session.next.text.started" => Ok(ClassifyOutcome::Ignored(IgnoredKind::SessionNextTextStarted)),
        "session.next.text.delta" => Ok(ClassifyOutcome::Ignored(IgnoredKind::SessionNextTextDelta)),
        "session.next.text.ended" => Ok(ClassifyOutcome::Ignored(IgnoredKind::SessionNextTextEnded)),
        "session.next.reasoning.started" => Ok(ClassifyOutcome::Ignored(IgnoredKind::SessionNextReasoningStarted)),
        "session.next.reasoning.delta" => Ok(ClassifyOutcome::Ignored(IgnoredKind::SessionNextReasoningDelta)),
        "session.next.reasoning.ended" => Ok(ClassifyOutcome::Ignored(IgnoredKind::SessionNextReasoningEnded)),
        "session.next.shell.started" => Ok(ClassifyOutcome::Ignored(IgnoredKind::SessionNextShellStarted)),
        "session.next.shell.ended" => Ok(ClassifyOutcome::Ignored(IgnoredKind::SessionNextShellEnded)),
        "session.next.tool.called" => Ok(ClassifyOutcome::Ignored(IgnoredKind::SessionNextToolCalled)),
        "session.next.tool.success" => Ok(ClassifyOutcome::Ignored(IgnoredKind::SessionNextToolSuccess)),
        "session.next.tool.failed" => Ok(ClassifyOutcome::Ignored(IgnoredKind::SessionNextToolFailed)),
        "session.next.compaction.started" => Ok(ClassifyOutcome::Ignored(IgnoredKind::SessionNextCompactionStarted)),
        "session.next.compaction.delta" => Ok(ClassifyOutcome::Ignored(IgnoredKind::SessionNextCompactionDelta)),
        "session.next.compaction.ended" => Ok(ClassifyOutcome::Ignored(IgnoredKind::SessionNextCompactionEnded)),
        "session.updated" => Ok(ClassifyOutcome::Ignored(IgnoredKind::SessionUpdated)),
        "session.deleted" => Ok(ClassifyOutcome::Ignored(IgnoredKind::SessionDeleted)),
        "session.diff" => Ok(ClassifyOutcome::Ignored(IgnoredKind::SessionDiff)),
        "message.removed" => Ok(ClassifyOutcome::Ignored(IgnoredKind::MessageRemoved)),
        "message.part.removed" => Ok(ClassifyOutcome::Ignored(IgnoredKind::MessagePartRemoved)),
        // -- Unknown event type --------------------------------------------------
        other => Ok(ClassifyOutcome::Unknown {
            event_type: other.to_string(),
            prop_fingerprint: property_key_fingerprint(props),
        }),
    }
}

/// Stable, non-sensitive fingerprint of an event's `properties` keys.
/// Mirrors `event_property_fingerprint` in `agent.rs:350` (16-hex digest).
pub fn property_key_fingerprint(props: Option<&Value>) -> String {
    let mut keys: Vec<&str> = match props.and_then(|v| v.as_object()) {
        Some(map) => map.keys().map(String::as_str).collect(),
        None => Vec::new(),
    };
    keys.sort_unstable();
    let mut hasher = DefaultHasher::new();
    keys.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

// -- Helpers ----------------------------------------------------------------

fn resolve_path<'a>(props: Option<&'a Value>, path: &'static str) -> Option<&'a Value> {
    let mut cursor = props?;
    for segment in path.split('.') {
        cursor = cursor.as_object()?.get(segment)?;
    }
    Some(cursor)
}

fn require_path(event_type: &str, props: Option<&Value>, path: &'static str) -> Result<(), ClassifyError> {
    match resolve_path(props, path) {
        Some(_) => Ok(()),
        None => Err(ClassifyError::MissingRequiredField {
            event_type: event_type.to_string(),
            field_path: path,
        }),
    }
}

fn require_str(event_type: &str, props: Option<&Value>, path: &'static str) -> Result<(), ClassifyError> {
    match resolve_path(props, path).and_then(|v| v.as_str()) {
        Some(_) => Ok(()),
        None => Err(ClassifyError::MissingRequiredField {
            event_type: event_type.to_string(),
            field_path: path,
        }),
    }
}

fn require_obj(event_type: &str, props: Option<&Value>, path: &'static str) -> Result<(), ClassifyError> {
    match resolve_path(props, path).and_then(|v| v.as_object()) {
        Some(_) => Ok(()),
        None => Err(ClassifyError::MissingRequiredField {
            event_type: event_type.to_string(),
            field_path: path,
        }),
    }
}

fn require_array(event_type: &str, props: Option<&Value>, path: &'static str) -> Result<(), ClassifyError> {
    match resolve_path(props, path).and_then(|v| v.as_array()) {
        Some(_) => Ok(()),
        None => Err(ClassifyError::MissingRequiredField {
            event_type: event_type.to_string(),
            field_path: path,
        }),
    }
}

/// Tool-input + tool-progress events use one of two correlation shapes:
/// the canonical v2 `callID` (alone is sufficient), or the legacy
/// `partID + messageID` pair from `/event`. Either is accepted; absence of
/// both is a failure (the adapter cannot route the chunk without one).
fn require_tool_input_correlation(event_type: &str, props: Option<&Value>) -> Result<(), ClassifyError> {
    let p = match props.and_then(|v| v.as_object()) {
        Some(o) => o,
        None => {
            return Err(ClassifyError::MissingRequiredField {
                event_type: event_type.to_string(),
                field_path: "callID|(messageID+partID)",
            });
        }
    };
    let has_call_id =
        p.get("callID").and_then(|v| v.as_str()).is_some() || p.get("call_id").and_then(|v| v.as_str()).is_some();
    let has_legacy = (p.get("messageID").and_then(|v| v.as_str()).is_some()
        || p.get("message_id").and_then(|v| v.as_str()).is_some())
        && (p.get("partID").and_then(|v| v.as_str()).is_some() || p.get("part_id").and_then(|v| v.as_str()).is_some());
    if has_call_id || has_legacy {
        Ok(())
    } else {
        Err(ClassifyError::MissingRequiredField {
            event_type: event_type.to_string(),
            field_path: "callID|(messageID+partID)",
        })
    }
}

// -- Inline unit tests ------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn unwrap_event_strips_payload() {
        let wrapped = json!({"payload": {"id": "evt_1", "type": "server.connected", "properties": {}}});
        let inner = unwrap_event(wrapped);
        assert_eq!(inner.get("type").and_then(|v| v.as_str()), Some("server.connected"));
    }

    #[test]
    fn unwrap_event_passthrough_when_no_payload() {
        let raw = json!({"id": "evt_1", "type": "server.heartbeat", "properties": {}});
        let inner = unwrap_event(raw.clone());
        assert_eq!(inner, raw);
    }

    #[test]
    fn classify_session_idle_with_required_field() {
        let v = json!({"id": "evt_1", "type": "session.idle", "properties": {"sessionID": "ses_1"}});
        assert_eq!(
            classify_event(&v).unwrap(),
            ClassifyOutcome::Handled(EventKind::SessionIdle)
        );
    }

    #[test]
    fn classify_session_idle_missing_sessionid_fails() {
        let v = json!({"id": "evt_1", "type": "session.idle", "properties": {}});
        let err = classify_event(&v).unwrap_err();
        assert!(
            matches!(err, ClassifyError::MissingRequiredField { ref event_type, field_path: "sessionID" } if event_type == "session.idle")
        );
    }

    #[test]
    fn classify_unknown_event_type_is_warning_not_failure() {
        let v = json!({"id": "evt_1", "type": "brand.new.event", "properties": {"foo": 1, "bar": 2}});
        match classify_event(&v).unwrap() {
            ClassifyOutcome::Unknown {
                event_type,
                prop_fingerprint,
            } => {
                assert_eq!(event_type, "brand.new.event");
                assert_eq!(prop_fingerprint.len(), 16);
            }
            other => panic!("expected Unknown, got {other:?}"),
        }
    }

    #[test]
    fn classify_unknown_field_on_known_event_passes() {
        let v = json!({
            "id": "evt_1",
            "type": "session.idle",
            "properties": {"sessionID": "ses_1", "unknownFutureField": 42}
        });
        assert_eq!(
            classify_event(&v).unwrap(),
            ClassifyOutcome::Handled(EventKind::SessionIdle)
        );
    }

    #[test]
    fn classify_known_ignored_event() {
        let v = json!({"id": "evt_1", "type": "server.heartbeat", "properties": {}});
        assert_eq!(
            classify_event(&v).unwrap(),
            ClassifyOutcome::Ignored(IgnoredKind::ServerHeartbeat)
        );
    }

    #[test]
    fn classify_sync_wrapper() {
        let v = json!({
            "type": "sync",
            "id": "evt_1",
            "syncEvent": {
                "type": "session.created.1",
                "id": "evt_1",
                "seq": 7,
                "aggregateID": "ses_1",
                "data": {"sessionID": "ses_1"}
            }
        });
        match classify_event(&v).unwrap() {
            ClassifyOutcome::Sync { mirror_type, seq } => {
                assert_eq!(mirror_type, "session.created.1");
                assert_eq!(seq, 7);
            }
            other => panic!("expected Sync, got {other:?}"),
        }
    }

    #[test]
    fn classify_tool_input_canonical_callid() {
        let v = json!({
            "id": "evt_1",
            "type": "session.next.tool.input.started",
            "properties": {"sessionID": "ses_1", "callID": "call_1", "name": "bash", "timestamp": 0}
        });
        assert_eq!(
            classify_event(&v).unwrap(),
            ClassifyOutcome::Handled(EventKind::SessionNextToolInputStarted)
        );
    }

    #[test]
    fn classify_tool_input_legacy_part_message_pair() {
        let v = json!({
            "id": "evt_1",
            "type": "session.next.tool.input.delta",
            "properties": {"sessionID": "ses_1", "messageID": "msg_1", "partID": "part_1", "inputDelta": "{\"a\":"}
        });
        assert_eq!(
            classify_event(&v).unwrap(),
            ClassifyOutcome::Handled(EventKind::SessionNextToolInputDelta)
        );
    }

    #[test]
    fn classify_tool_input_missing_correlation_fails() {
        let v = json!({
            "id": "evt_1",
            "type": "session.next.tool.input.started",
            "properties": {"sessionID": "ses_1"}
        });
        let err = classify_event(&v).unwrap_err();
        assert!(matches!(err, ClassifyError::MissingRequiredField { .. }));
    }

    #[test]
    fn classify_malformed_envelope() {
        assert_eq!(
            classify_event(&json!({"properties": {}})).unwrap_err(),
            ClassifyError::MalformedEnvelope
        );
        assert_eq!(
            classify_event(&json!(42)).unwrap_err(),
            ClassifyError::MalformedEnvelope
        );
    }

    #[test]
    fn property_key_fingerprint_is_deterministic_and_value_independent() {
        let a = json!({"sessionID": "ses_1", "todos": [1, 2]});
        let b = json!({"todos": [3, 4, 5], "sessionID": "ses_2"});
        assert_eq!(property_key_fingerprint(Some(&a)), property_key_fingerprint(Some(&b)));
    }
}
