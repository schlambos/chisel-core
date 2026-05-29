//! OpenCode sub-agent (child session) tracking + canonical event emission.
//!
//! When the parent OpenCode session delegates work to a sub-agent (`explore`,
//! `general`, etc.), OpenCode spawns a **child `Session`** whose `parentID`
//! references the parent. Sub-agent activity (tool calls, permission requests,
//! tool-input streaming, tool-progress streaming) all fire on the same global
//! `/global/event` SSE stream, but with the child session's `sessionID`.
//!
//! The default behaviour of [`super::agent`]'s SSE dispatcher is to *drop*
//! events whose `sessionID` does not match `opencode_session_id` (the manager's
//! own parent session). Without sub-agent tracking the entire delegated turn is
//! invisible: the user sees a `Task` tool-part on the parent and then minutes
//! of `server.heartbeat` until the child reports back.
//!
//! This module owns the side-state and the helper API that lets the dispatcher
//! whitelist + route those events. It deliberately holds no other manager
//! state — `agent.rs` retains the `Arc<RwLock<RemoteState>>` and threads a
//! `&mut ChildSessionRegistry` (via the read/write guard) into the helpers.
//!
//! Lifecycle:
//!   - `session.created` whose `parentID` matches our own → [`register_child`]
//!     inserts a [`ChildSession`] and the emitter fires `OpencodeSubtask::Started`.
//!   - Each child event ticks [`note_event`], updating `last_event_at` and the
//!     running tool counter; a debounced `OpencodeSubtask::Progress` event
//!     surfaces the rolling summary to the renderer.
//!   - `session.idle` for a child → [`mark_completed`] flips the status and
//!     emits `OpencodeSubtask::Completed`.

use std::collections::HashMap;

use serde_json::Value;
use tracing::{debug, info};

use crate::agent_runtime::AgentRuntime;
use crate::protocol::events::{
    AgentStreamEvent, OpencodeSubtaskEventData, OpencodeSubtaskLiveSummary, OpencodeSubtaskPhase, OpencodeSubtaskStatus,
};

/// Tracked metadata for one OpenCode child session.
#[derive(Debug, Clone)]
pub struct ChildSession {
    pub child_session_id: String,
    pub agent_name: Option<String>,
    pub started_at_ms: i64,
    pub completed_at_ms: Option<i64>,
    pub tool_calls_count: u32,
    pub current_tool_name: Option<String>,
    pub last_event_at_ms: i64,
    /// Distinct `partID`s seen for this child so we can count unique tool
    /// invocations rather than every cumulative `message.part.updated` tick.
    pub seen_part_ids: HashMap<String, ()>,
    pub status: Option<OpencodeSubtaskStatus>,
}

/// Per-manager registry of OpenCode child sessions. Owned by `RemoteState` and
/// always accessed under its `RwLock`.
///
/// `Clone` is implemented so callers can snapshot the registry under the
/// state lock and then drop the lock before doing expensive lineage walks
/// (`session_or_ancestor_blessed`). The clone copies the underlying
/// `HashMap<ChildSession>` — cheap for the small (≤dozens) sub-agent counts
/// we see in practice.
#[derive(Debug, Default, Clone)]
pub struct ChildSessionRegistry {
    by_id: HashMap<String, ChildSession>,
}

impl ChildSessionRegistry {
    pub fn contains(&self, child_id: &str) -> bool {
        self.by_id.contains_key(child_id)
    }

    pub fn get(&self, child_id: &str) -> Option<&ChildSession> {
        self.by_id.get(child_id)
    }

    pub fn get_mut(&mut self, child_id: &str) -> Option<&mut ChildSession> {
        self.by_id.get_mut(child_id)
    }

    pub fn insert(&mut self, session: ChildSession) {
        self.by_id.insert(session.child_session_id.clone(), session);
    }

    /// Iterate all known child sessions (e.g. for backfill on reconnect).
    pub fn iter(&self) -> impl Iterator<Item = &ChildSession> {
        self.by_id.values()
    }
}

/// Inspect a `session.created` SSE payload and, if it represents a sub-agent of
/// `own_parent_session`, register the child and return the new session. Returns
/// `None` when the event is for the parent itself or for an unrelated session.
pub fn try_register_from_session_created(
    props: &Value,
    own_parent_session: &str,
    registry: &mut ChildSessionRegistry,
    now_ms: i64,
) -> Option<ChildSession> {
    let info = props.get("info").or(Some(props))?;
    let child_id = info.get("id").and_then(|v| v.as_str())?.to_string();
    let parent_id = info.get("parentID").and_then(|v| v.as_str())?.to_string();

    if parent_id != own_parent_session {
        return None;
    }

    if registry.contains(&child_id) {
        return None;
    }

    let agent_name = info
        .get("agent")
        .and_then(|v| v.as_str())
        .or_else(|| info.get("agentName").and_then(|v| v.as_str()))
        .map(String::from);

    let started_at_ms = info
        .get("time")
        .and_then(|t| t.get("created"))
        .and_then(|v| v.as_i64())
        .unwrap_or(now_ms);

    let session = ChildSession {
        child_session_id: child_id.clone(),
        agent_name,
        started_at_ms,
        completed_at_ms: None,
        tool_calls_count: 0,
        current_tool_name: None,
        last_event_at_ms: now_ms,
        seen_part_ids: HashMap::new(),
        status: None,
    };
    registry.insert(session.clone());
    Some(session)
}

/// Fold a `message.part.updated` tick into the child's rolling summary. Returns
/// `true` when the counter or active tool name changed (so the caller knows to
/// emit a `Progress` event), `false` when the tick is a duplicate update for an
/// already-seen `partID`.
pub fn note_tool_part(
    registry: &mut ChildSessionRegistry,
    child_id: &str,
    part_id: &str,
    tool_name: Option<&str>,
    now_ms: i64,
) -> bool {
    let Some(child) = registry.get_mut(child_id) else {
        return false;
    };
    let changed_active = match (&child.current_tool_name, tool_name) {
        (None, Some(n)) => {
            child.current_tool_name = Some(n.to_string());
            true
        }
        (Some(existing), Some(n)) if existing != n => {
            child.current_tool_name = Some(n.to_string());
            true
        }
        _ => false,
    };
    let first_for_part = child.seen_part_ids.insert(part_id.to_string(), ()).is_none();
    if first_for_part {
        child.tool_calls_count = child.tool_calls_count.saturating_add(1);
    }
    child.last_event_at_ms = now_ms;
    first_for_part || changed_active
}

/// Mark a child completed and return the populated [`ChildSession`] so the
/// caller can emit the terminal `OpencodeSubtask::Completed` event. Returns
/// `None` for unknown child ids (which can happen if `session.idle` arrives
/// before `session.created` due to upstream reordering).
pub fn mark_completed(
    registry: &mut ChildSessionRegistry,
    child_id: &str,
    status: OpencodeSubtaskStatus,
    summary: Option<String>,
    now_ms: i64,
) -> Option<ChildSession> {
    let child = registry.get_mut(child_id)?;
    if child.status.is_some() {
        // Already marked completed once — OpenCode re-emits idle on
        // reconnect; suppress duplicate terminal events.
        return None;
    }
    child.status = Some(status);
    child.completed_at_ms = Some(now_ms);
    child.last_event_at_ms = now_ms;
    let mut snapshot = child.clone();
    snapshot.status = Some(status);
    snapshot.completed_at_ms = Some(now_ms);
    if summary.is_some() {
        // Don't persist the summary in the registry itself (it could be
        // arbitrarily large); the caller threads it directly into the event.
    }
    let _ = summary;
    Some(snapshot)
}

/// Emit a `OpencodeSubtask::Started` event on the runtime.
pub fn emit_started(runtime: &AgentRuntime, parent_session_id: &str, child: &ChildSession) {
    let event = OpencodeSubtaskEventData {
        parent_session_id: parent_session_id.to_string(),
        child_session_id: child.child_session_id.clone(),
        phase: OpencodeSubtaskPhase::Started,
        agent_name: child.agent_name.clone(),
        live_summary: Some(live_summary_of(child)),
        status: None,
        summary: None,
        started_at: Some(child.started_at_ms),
        completed_at: None,
    };
    info!(
        parent_session = parent_session_id,
        child_session = %child.child_session_id,
        agent = ?child.agent_name,
        "OpenCode sub-agent started"
    );
    runtime.emit(AgentStreamEvent::OpencodeSubtask(event));
}

/// Emit a `OpencodeSubtask::Progress` event with the current rolling summary.
pub fn emit_progress(runtime: &AgentRuntime, parent_session_id: &str, child: &ChildSession) {
    let event = OpencodeSubtaskEventData {
        parent_session_id: parent_session_id.to_string(),
        child_session_id: child.child_session_id.clone(),
        phase: OpencodeSubtaskPhase::Progress,
        agent_name: child.agent_name.clone(),
        live_summary: Some(live_summary_of(child)),
        status: None,
        summary: None,
        started_at: Some(child.started_at_ms),
        completed_at: None,
    };
    debug!(
        parent_session = parent_session_id,
        child_session = %child.child_session_id,
        tool_calls = child.tool_calls_count,
        current_tool = ?child.current_tool_name,
        "OpenCode sub-agent progress"
    );
    runtime.emit(AgentStreamEvent::OpencodeSubtask(event));
}

/// Emit a `OpencodeSubtask::Completed` event.
pub fn emit_completed(runtime: &AgentRuntime, parent_session_id: &str, child: &ChildSession, summary: Option<String>) {
    let event = OpencodeSubtaskEventData {
        parent_session_id: parent_session_id.to_string(),
        child_session_id: child.child_session_id.clone(),
        phase: OpencodeSubtaskPhase::Completed,
        agent_name: child.agent_name.clone(),
        live_summary: Some(live_summary_of(child)),
        status: child.status,
        summary,
        started_at: Some(child.started_at_ms),
        completed_at: child.completed_at_ms,
    };
    info!(
        parent_session = parent_session_id,
        child_session = %child.child_session_id,
        status = ?child.status,
        tool_calls = child.tool_calls_count,
        "OpenCode sub-agent completed"
    );
    runtime.emit(AgentStreamEvent::OpencodeSubtask(event));
}

fn live_summary_of(child: &ChildSession) -> OpencodeSubtaskLiveSummary {
    OpencodeSubtaskLiveSummary {
        tool_calls_count: child.tool_calls_count,
        current_tool_name: child.current_tool_name.clone(),
        last_event_at: child.last_event_at_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn register_only_for_matching_parent() {
        let mut registry = ChildSessionRegistry::default();
        let props = json!({
            "info": {
                "id": "child-1",
                "parentID": "parent-A",
                "agent": "explore",
                "time": { "created": 1700000000000_i64 }
            }
        });
        // Wrong parent — no registration.
        assert!(try_register_from_session_created(&props, "parent-B", &mut registry, 1700000000000).is_none());
        assert!(!registry.contains("child-1"));

        // Right parent — registers.
        let s = try_register_from_session_created(&props, "parent-A", &mut registry, 1700000000000).expect("ok");
        assert_eq!(s.child_session_id, "child-1");
        assert_eq!(s.agent_name.as_deref(), Some("explore"));
        assert!(registry.contains("child-1"));

        // Idempotent on second call.
        assert!(try_register_from_session_created(&props, "parent-A", &mut registry, 1700000000000).is_none());
    }

    #[test]
    fn note_tool_part_dedupes_by_part_id() {
        let mut registry = ChildSessionRegistry::default();
        registry.insert(ChildSession {
            child_session_id: "child-1".into(),
            agent_name: None,
            started_at_ms: 0,
            completed_at_ms: None,
            tool_calls_count: 0,
            current_tool_name: None,
            last_event_at_ms: 0,
            seen_part_ids: HashMap::new(),
            status: None,
        });
        // First tick for partA increments + flips active tool.
        assert!(note_tool_part(&mut registry, "child-1", "partA", Some("read"), 100));
        assert_eq!(registry.get("child-1").unwrap().tool_calls_count, 1);
        // Second tick for partA is a duplicate — count unchanged, active unchanged.
        assert!(!note_tool_part(&mut registry, "child-1", "partA", Some("read"), 200));
        assert_eq!(registry.get("child-1").unwrap().tool_calls_count, 1);
        // partB is a new tool call.
        assert!(note_tool_part(&mut registry, "child-1", "partB", Some("bash"), 300));
        assert_eq!(registry.get("child-1").unwrap().tool_calls_count, 2);
        assert_eq!(
            registry.get("child-1").unwrap().current_tool_name.as_deref(),
            Some("bash")
        );
    }

    #[test]
    fn mark_completed_suppresses_duplicates() {
        let mut registry = ChildSessionRegistry::default();
        registry.insert(ChildSession {
            child_session_id: "child-1".into(),
            agent_name: None,
            started_at_ms: 0,
            completed_at_ms: None,
            tool_calls_count: 0,
            current_tool_name: None,
            last_event_at_ms: 0,
            seen_part_ids: HashMap::new(),
            status: None,
        });
        let first = mark_completed(&mut registry, "child-1", OpencodeSubtaskStatus::Completed, None, 500);
        assert!(first.is_some());
        // Second call returns None — already terminal.
        let second = mark_completed(&mut registry, "child-1", OpencodeSubtaskStatus::Completed, None, 600);
        assert!(second.is_none());
    }
}
