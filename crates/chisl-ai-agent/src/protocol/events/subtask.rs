use serde::{Deserialize, Serialize};

/// Data for the `OpencodeSubtask` event — a sub-agent (child session) lifecycle
/// update emitted whenever the parent OpenCode session spawns, progresses, or
/// finishes a delegated child session.
///
/// The renderer locates the right bubble by `(parent_session_id, child_session_id)`
/// and renders a drill-in chip whose nested transcript is the child's own
/// transcript.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpencodeSubtaskEventData {
    /// The parent session id that delegated this child (the conversation's own
    /// top-level OpenCode session).
    pub parent_session_id: String,
    /// The child session id assigned by OpenCode when it spawned the sub-agent.
    pub child_session_id: String,
    /// Lifecycle phase. `started` is emitted on `session.created` whose
    /// `parentID` matches our own; `progress` ticks on tool-count / current-tool
    /// updates; `completed` fires when the child session enters idle/finished.
    pub phase: OpencodeSubtaskPhase,
    /// The sub-agent's display name (e.g. `"explore"`, `"general"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
    /// Optional rolling summary: tool-call count + the currently active tool
    /// name, so the collapsed chip can show "3 toolcalls · reading src/foo.ts"
    /// without the user expanding the sub-agent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub live_summary: Option<OpencodeSubtaskLiveSummary>,
    /// Terminal status, present on `completed`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<OpencodeSubtaskStatus>,
    /// Optional final summary string captured from the child's last assistant
    /// message, present on `completed`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    /// Milliseconds since epoch when the child started.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<i64>,
    /// Milliseconds since epoch when the child finished.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<i64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OpencodeSubtaskPhase {
    Started,
    Progress,
    Completed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OpencodeSubtaskStatus {
    Completed,
    Failed,
    Aborted,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OpencodeSubtaskLiveSummary {
    /// Total tool calls observed in the child session so far.
    pub tool_calls_count: u32,
    /// Name of the most recently active tool call (if any). Used by the
    /// collapsed chip to surface what the sub-agent is doing right now.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_tool_name: Option<String>,
    /// Milliseconds since epoch of the most recent event observed for this
    /// child. Lets the UI tick a heartbeat indicator independent of the
    /// underlying counter.
    pub last_event_at: i64,
}
