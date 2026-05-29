//! OpenCode tool-input + tool-progress SSE event translation.
//!
//! Three OpenCode event families are mapped into Chisl's canonical
//! [`AgentStreamEvent`] variants:
//!
//! - `session.next.tool.input.started` → [`AgentStreamEvent::ToolInput`] with
//!   `phase = started` and `tool_name`.
//! - `session.next.tool.input.delta`   → `phase = delta` with `input_delta`.
//! - `session.next.tool.input.ended`   → `phase = ended` with `final_input`.
//! - `session.next.tool.progress`      → [`AgentStreamEvent::ToolProgress`] with
//!   the normalized `progress` payload.
//!
//! Event names follow OpenCode's "session.next.*" prefix observed in production
//! SSE traces (e.g. `session.next.model.switched`). The exact wire shape is
//! defended by extracting fields defensively so a partial upstream payload
//! never crashes the dispatcher.
//!
//! `parent_session_id` is set when the event came from a child (sub-agent)
//! session so the renderer can attach the streamed tool I/O to the right
//! nested transcript.

use serde_json::Value;

use crate::protocol::events::{
    AgentStreamEvent, ToolInputEventData, ToolInputPhase, ToolProgressEventData, normalize_progress_payload,
};

/// Translate the `properties` object of a `session.next.tool.input.*` SSE event
/// into a canonical [`AgentStreamEvent::ToolInput`]. Returns `None` when the
/// payload is missing required fields (`partID`, `messageID`) — those events
/// are dropped with a `debug` log by the caller.
pub fn translate_tool_input(
    event_type: &str,
    props: &Value,
    session_id: &str,
    parent_session_id: Option<String>,
) -> Option<AgentStreamEvent> {
    let phase = phase_for_event_type(event_type)?;
    let message_id = props
        .get("messageID")
        .and_then(|v| v.as_str())
        .or_else(|| props.get("message_id").and_then(|v| v.as_str()))?
        .to_string();
    let part_id = props
        .get("partID")
        .and_then(|v| v.as_str())
        .or_else(|| props.get("part_id").and_then(|v| v.as_str()))?
        .to_string();

    let tool_name = props
        .get("toolName")
        .or_else(|| props.get("tool_name"))
        .and_then(|v| v.as_str())
        .map(String::from);
    let input_delta = props
        .get("inputDelta")
        .or_else(|| props.get("input_delta"))
        .and_then(|v| v.as_str())
        .map(String::from);
    let final_input = props.get("finalInput").or_else(|| props.get("final_input")).cloned();
    let started_at = props
        .get("startedAt")
        .or_else(|| props.get("started_at"))
        .and_then(|v| v.as_i64());
    let completed_at = props
        .get("completedAt")
        .or_else(|| props.get("completed_at"))
        .and_then(|v| v.as_i64());

    Some(AgentStreamEvent::ToolInput(ToolInputEventData {
        session_id: session_id.to_string(),
        parent_session_id,
        message_id,
        part_id,
        phase,
        tool_name,
        input_delta,
        final_input,
        started_at,
        completed_at,
    }))
}

/// Translate the `properties` object of a `session.next.tool.progress` SSE
/// event into a canonical [`AgentStreamEvent::ToolProgress`].
pub fn translate_tool_progress(
    props: &Value,
    session_id: &str,
    parent_session_id: Option<String>,
    now_ms: i64,
) -> Option<AgentStreamEvent> {
    let message_id = props
        .get("messageID")
        .and_then(|v| v.as_str())
        .or_else(|| props.get("message_id").and_then(|v| v.as_str()))?
        .to_string();
    let part_id = props
        .get("partID")
        .and_then(|v| v.as_str())
        .or_else(|| props.get("part_id").and_then(|v| v.as_str()))?
        .to_string();
    let tool_name = props
        .get("toolName")
        .or_else(|| props.get("tool_name"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let raw_progress = props.get("progress").cloned().unwrap_or(Value::Null);
    let progress = normalize_progress_payload(&tool_name, &raw_progress);
    let at = props.get("at").and_then(|v| v.as_i64()).unwrap_or(now_ms);

    Some(AgentStreamEvent::ToolProgress(ToolProgressEventData {
        session_id: session_id.to_string(),
        parent_session_id,
        message_id,
        part_id,
        tool_name,
        progress,
        at,
    }))
}

fn phase_for_event_type(event_type: &str) -> Option<ToolInputPhase> {
    match event_type {
        "session.next.tool.input.started" | "session.next.tool_input.started" | "message.next.tool.input.started" => {
            Some(ToolInputPhase::Started)
        }
        "session.next.tool.input.delta" | "session.next.tool_input.delta" | "message.next.tool.input.delta" => {
            Some(ToolInputPhase::Delta)
        }
        "session.next.tool.input.ended" | "session.next.tool_input.ended" | "message.next.tool.input.ended" => {
            Some(ToolInputPhase::Ended)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn input_started_with_tool_name_serializes() {
        let props = json!({
            "messageID": "msg-1",
            "partID": "part-1",
            "toolName": "write",
            "startedAt": 1700000000000_i64,
        });
        let event = translate_tool_input("session.next.tool.input.started", &props, "sess-1", None).expect("event");
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["type"], "tool_input");
        assert_eq!(json["data"]["session_id"], "sess-1");
        assert_eq!(json["data"]["phase"], "started");
        assert_eq!(json["data"]["tool_name"], "write");
        assert_eq!(json["data"]["part_id"], "part-1");
    }

    #[test]
    fn input_delta_with_parent_session() {
        let props = json!({
            "messageID": "msg-1",
            "partID": "part-1",
            "inputDelta": "{\"path\":",
        });
        let event = translate_tool_input(
            "session.next.tool.input.delta",
            &props,
            "sess-child",
            Some("sess-parent".into()),
        )
        .expect("event");
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["data"]["phase"], "delta");
        assert_eq!(json["data"]["input_delta"], "{\"path\":");
        assert_eq!(json["data"]["parent_session_id"], "sess-parent");
    }

    #[test]
    fn input_without_partid_returns_none() {
        let props = json!({ "messageID": "msg-1", "toolName": "write" });
        assert!(translate_tool_input("session.next.tool.input.started", &props, "sess-1", None).is_none());
    }

    #[test]
    fn progress_serializes_with_normalized_payload() {
        let props = json!({
            "messageID": "msg-1",
            "partID": "part-1",
            "toolName": "bash",
            "progress": { "stdoutChunk": "hello\n" },
            "at": 1700000000123_i64,
        });
        let event = translate_tool_progress(&props, "sess-1", None, 0).expect("event");
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["type"], "tool_progress");
        assert_eq!(json["data"]["tool_name"], "bash");
        assert_eq!(json["data"]["progress"]["stdoutChunk"], "hello\n");
        assert_eq!(json["data"]["at"], 1700000000123_i64);
    }

    #[test]
    fn progress_apply_patch_strips_bodies() {
        let props = json!({
            "messageID": "msg-1",
            "partID": "part-1",
            "toolName": "apply_patch",
            "progress": {
                "status": "applying",
                "patch": "DO NOT LEAK",
                "newText": "file contents",
                "filesChanged": 1,
            },
            "at": 1700000000123_i64,
        });
        let event = translate_tool_progress(&props, "sess-1", None, 0).expect("event");
        let json = serde_json::to_value(&event).unwrap();
        let progress = &json["data"]["progress"];
        assert!(progress.get("status").is_some());
        assert!(progress.get("filesChanged").is_some());
        assert!(progress.get("patch").is_none());
        assert!(progress.get("newText").is_none());
    }

    #[test]
    fn unknown_event_type_returns_none() {
        let props = json!({ "messageID": "m", "partID": "p" });
        assert!(translate_tool_input("session.next.tool.input.bogus", &props, "sess-1", None).is_none());
    }
}
