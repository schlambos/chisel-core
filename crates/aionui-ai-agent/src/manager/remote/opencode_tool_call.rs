//! OpenCode tool-call SSE → `AcpToolCall` event translation.
//!
//! OpenCode streams tool execution through repeated `message.part.updated`
//! SSE events keyed on `part.callID`. Each event carries the *cumulative*
//! state of the tool (`state.status`, `state.input`, `state.metadata.output`,
//! and on completion `state.output` + exit code). We map this onto the same
//! `AcpToolCallEventData` the frontend already renders for ACP/OpenClaw
//! backends so the inline tool-call UI works for the OpenCode HTTP path
//! with no frontend changes.
//!
//! Lives in its own module because `agent.rs` is already past its line
//! budget (per workspace rules) and tool-translation logic is self-
//! contained enough to test in isolation.

use serde_json::Value;

use crate::protocol::events::{
    AcpToolCallContentItem, AcpToolCallEventData, AcpToolCallKind, AcpToolCallSessionUpdateKind, AcpToolCallStatus,
    AcpToolCallTextBlock, AcpToolCallTextBlockType, AcpToolCallUpdateData,
};

/// Translate a `message.part.updated` SSE payload (the `properties` object)
/// into an `AcpToolCallEventData` ready to be emitted on the agent runtime.
///
/// Returns `None` for:
/// - non-tool parts (the caller still needs to handle `reasoning` separately),
/// - `todowrite` (OpenCode also emits a dedicated `todo.updated` event that
///   is routed to `AgentStreamEvent::Plan`; emitting both would render the
///   todo list twice in the chat),
/// - payloads missing the required `callID`.
///
/// `session_id` is threaded through from the SSE envelope so the renderer
/// can correlate the update with its conversation. `props` is the
/// `properties` object of the SSE event, *not* the whole event.
///
/// `parent_session_id` is set when the event came from a sub-agent (OpenCode
/// child session) — it threads up to the renderer so the tool-call bubble lands
/// inside the right sub-agent transcript. `None` for parent-session calls.
pub fn translate_message_part_updated(
    props: &Value,
    session_id: Option<String>,
    parent_session_id: Option<String>,
) -> Option<AcpToolCallEventData> {
    let part = props.get("part")?;
    if part.get("type").and_then(|v| v.as_str()) != Some("tool") {
        return None;
    }

    let tool_name = part.get("tool").and_then(|v| v.as_str()).unwrap_or("");

    // `todowrite` is rendered via `Plan` (see the `todo.updated` arm of the
    // SSE dispatcher). Suppressing the tool card avoids a duplicate list.
    if tool_name.eq_ignore_ascii_case("todowrite") {
        return None;
    }

    let call_id = part.get("callID").and_then(|v| v.as_str())?.to_string();
    let state = part.get("state");

    let status_str = state
        .and_then(|s| s.get("status"))
        .and_then(|v| v.as_str())
        .unwrap_or("pending");
    let exit_code = state
        .and_then(|s| s.get("metadata"))
        .and_then(|m| m.get("exit"))
        .and_then(|v| v.as_i64());

    let status = match status_str {
        "pending" => AcpToolCallStatus::Pending,
        "running" => AcpToolCallStatus::InProgress,
        "completed" => {
            // OpenCode marks the call itself "completed" even when the
            // underlying shell process exited non-zero. Surface that as a
            // failure so the UI shows the red badge.
            if matches!(exit_code, Some(c) if c != 0) {
                AcpToolCallStatus::Failed
            } else {
                AcpToolCallStatus::Completed
            }
        }
        "error" | "failed" => AcpToolCallStatus::Failed,
        _ => AcpToolCallStatus::Pending,
    };

    let kind = classify_tool_kind(tool_name);

    // Title preference order:
    //   1. `state.title`            (server-supplied human label, set on completion)
    //   2. `state.metadata.description` (model-supplied description, present while running)
    //   3. the bare tool name       (always available)
    let title = state
        .and_then(|s| s.get("title"))
        .and_then(|v| v.as_str())
        .map(String::from)
        .or_else(|| {
            state
                .and_then(|s| s.get("metadata"))
                .and_then(|m| m.get("description"))
                .and_then(|v| v.as_str())
                .map(String::from)
        })
        .or_else(|| Some(display_tool_name(tool_name).to_string()));

    let raw_input = state
        .and_then(|s| s.get("input"))
        .filter(|v| !v.is_null() && !is_empty_object(v))
        .cloned();

    // Cumulative output snapshot:
    //   - Terminal `state.output` once the tool completes.
    //   - Otherwise the growing `state.metadata.output` while running.
    // Both are full snapshots — the frontend's shallow `mergeAcpToolCallContent`
    // replaces the previous `content` array on each update, so emitting the
    // full text every tick is correct and append-free.
    let output_text = state
        .and_then(|s| s.get("output"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(String::from)
        .or_else(|| {
            state
                .and_then(|s| s.get("metadata"))
                .and_then(|m| m.get("output"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(String::from)
        });

    let content = output_text.map(|text| {
        vec![AcpToolCallContentItem::Content {
            content: AcpToolCallTextBlock {
                block_type: AcpToolCallTextBlockType::Text,
                text,
            },
        }]
    });

    Some(AcpToolCallEventData {
        session_id: session_id.unwrap_or_default(),
        parent_session_id,
        update: AcpToolCallUpdateData {
            // The frontend merges by `tool_call_id` regardless of `sessionUpdate`
            // kind, so we always emit `ToolCallUpdate` — the first hit becomes
            // the initial card, every subsequent hit folds into it.
            session_update: AcpToolCallSessionUpdateKind::ToolCallUpdate,
            tool_call_id: call_id,
            status: Some(status),
            title,
            kind: Some(kind),
            raw_input,
            raw_output: None,
            content,
            locations: None,
        },
        meta: None,
    })
}

/// Categorize an OpenCode tool name so the UI picks the right icon
/// (`Read` / `Edit` / `Execute`). MCP-routed tools are advertised with a
/// `mcp__<server>__<tool>` prefix; we look at the bare suffix.
fn classify_tool_kind(tool: &str) -> AcpToolCallKind {
    let bare = strip_mcp_prefix(tool).to_ascii_lowercase();

    match bare.as_str() {
        "read" | "grep" | "glob" | "list" | "ls" | "find" | "ripgrep" | "webfetch" | "websearch" => {
            AcpToolCallKind::Read
        }
        "write" | "edit" | "multiedit" | "patch" | "delete" | "mv" | "move" => AcpToolCallKind::Edit,
        _ => AcpToolCallKind::Execute,
    }
}

/// Strip the `mcp__<server>__` prefix OpenCode prepends to MCP-routed tools
/// (e.g. `mcp__aionui-local-fs-conv1__read` → `read`) for clean display.
fn display_tool_name(tool: &str) -> &str {
    strip_mcp_prefix(tool)
}

fn strip_mcp_prefix(tool: &str) -> &str {
    tool.strip_prefix("mcp__")
        .and_then(|rest| rest.split_once("__").map(|(_, tail)| tail))
        .unwrap_or(tool)
}

fn is_empty_object(v: &Value) -> bool {
    v.as_object().map(|o| o.is_empty()).unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn extract(
        event: AcpToolCallEventData,
    ) -> (
        String,
        AcpToolCallStatus,
        AcpToolCallKind,
        Option<String>,
        Option<String>,
    ) {
        let u = event.update;
        let text = u.content.and_then(|items| {
            items.into_iter().find_map(|item| match item {
                AcpToolCallContentItem::Content { content } => Some(content.text),
                _ => None,
            })
        });
        let input = u.raw_input.map(|v| v.to_string());
        (u.tool_call_id, u.status.unwrap(), u.kind.unwrap(), text, input)
    }

    #[test]
    fn translates_pending_tool_part() {
        let props = json!({
            "sessionID": "ses_abc",
            "part": {
                "type": "tool",
                "tool": "bash",
                "callID": "call_1",
                "state": { "status": "pending", "input": {}, "raw": "" }
            }
        });
        let event = translate_message_part_updated(&props, Some("ses_abc".into()), None).expect("event");
        let (call_id, status, kind, text, input) = extract(event);
        assert_eq!(call_id, "call_1");
        assert!(matches!(status, AcpToolCallStatus::Pending));
        assert!(matches!(kind, AcpToolCallKind::Execute));
        assert!(text.is_none(), "no output yet for pending tool");
        assert!(input.is_none(), "empty input object is filtered out");
    }

    #[test]
    fn translates_running_with_partial_output() {
        let props = json!({
            "sessionID": "ses_abc",
            "part": {
                "type": "tool",
                "tool": "bash",
                "callID": "call_1",
                "state": {
                    "status": "running",
                    "input": {"command": "echo hi"},
                    "metadata": {"output": "hi", "description": "Prints hi"}
                }
            }
        });
        let event = translate_message_part_updated(&props, None, None).expect("event");
        let (_, status, kind, text, input) = extract(event);
        assert!(matches!(status, AcpToolCallStatus::InProgress));
        assert!(matches!(kind, AcpToolCallKind::Execute));
        assert_eq!(text.as_deref(), Some("hi"));
        assert!(input.unwrap().contains("echo hi"));
    }

    #[test]
    fn translates_completed_tool_with_zero_exit() {
        let props = json!({
            "part": {
                "type": "tool",
                "tool": "bash",
                "callID": "call_1",
                "state": {
                    "status": "completed",
                    "input": {"command": "echo hi"},
                    "output": "hi\n",
                    "metadata": {"output": "hi\n", "exit": 0},
                    "title": "Prints hi"
                }
            }
        });
        let event = translate_message_part_updated(&props, None, None).expect("event");
        let (_, status, _kind, text, _) = extract(event);
        assert!(matches!(status, AcpToolCallStatus::Completed));
        assert_eq!(text.as_deref(), Some("hi\n"));
    }

    #[test]
    fn nonzero_exit_is_failed_even_if_completed() {
        let props = json!({
            "part": {
                "type": "tool",
                "tool": "bash",
                "callID": "call_1",
                "state": {
                    "status": "completed",
                    "output": "boom\n",
                    "metadata": {"exit": 1}
                }
            }
        });
        let event = translate_message_part_updated(&props, None, None).expect("event");
        let (_, status, _, _, _) = extract(event);
        assert!(matches!(status, AcpToolCallStatus::Failed));
    }

    #[test]
    fn explicit_error_status_maps_to_failed() {
        let props = json!({
            "part": {
                "type": "tool",
                "tool": "bash",
                "callID": "call_1",
                "state": { "status": "error" }
            }
        });
        let event = translate_message_part_updated(&props, None, None).expect("event");
        let (_, status, _, _, _) = extract(event);
        assert!(matches!(status, AcpToolCallStatus::Failed));
    }

    #[test]
    fn classifies_read_edit_execute_kinds() {
        let cases = [
            ("read", AcpToolCallKind::Read),
            ("grep", AcpToolCallKind::Read),
            ("glob", AcpToolCallKind::Read),
            ("write", AcpToolCallKind::Edit),
            ("edit", AcpToolCallKind::Edit),
            ("multiedit", AcpToolCallKind::Edit),
            ("bash", AcpToolCallKind::Execute),
            ("task", AcpToolCallKind::Execute),
        ];
        for (tool, expected) in cases {
            let props = json!({
                "part": {
                    "type": "tool",
                    "tool": tool,
                    "callID": "call_1",
                    "state": {"status": "running"}
                }
            });
            let event = translate_message_part_updated(&props, None, None).expect("event");
            let actual = event.update.kind.unwrap();
            assert!(
                std::mem::discriminant(&actual) == std::mem::discriminant(&expected),
                "{tool}: expected {expected:?}, got {actual:?}"
            );
        }
    }

    #[test]
    fn strips_mcp_prefix_in_kind_classification() {
        let props = json!({
            "part": {
                "type": "tool",
                "tool": "mcp__aionui-local-fs-conv1__read",
                "callID": "call_1",
                "state": {"status": "running"}
            }
        });
        let event = translate_message_part_updated(&props, None, None).expect("event");
        assert!(matches!(event.update.kind.unwrap(), AcpToolCallKind::Read));
    }

    #[test]
    fn skips_todowrite_to_avoid_double_rendering() {
        let props = json!({
            "part": {
                "type": "tool",
                "tool": "todowrite",
                "callID": "call_1",
                "state": {"status": "running"}
            }
        });
        assert!(translate_message_part_updated(&props, None, None).is_none());
    }

    #[test]
    fn ignores_non_tool_parts() {
        let props = json!({
            "part": { "type": "reasoning", "id": "prt_1" }
        });
        assert!(translate_message_part_updated(&props, None, None).is_none());

        let props = json!({
            "part": { "type": "text", "text": "hello" }
        });
        assert!(translate_message_part_updated(&props, None, None).is_none());
    }

    #[test]
    fn requires_call_id() {
        let props = json!({
            "part": {
                "type": "tool",
                "tool": "bash",
                "state": {"status": "running"}
            }
        });
        assert!(translate_message_part_updated(&props, None, None).is_none());
    }

    #[test]
    fn prefers_terminal_output_over_metadata_when_both_present() {
        let props = json!({
            "part": {
                "type": "tool",
                "tool": "bash",
                "callID": "call_1",
                "state": {
                    "status": "completed",
                    "output": "final\n",
                    "metadata": {"output": "stale", "exit": 0}
                }
            }
        });
        let event = translate_message_part_updated(&props, None, None).expect("event");
        let (_, _, _, text, _) = extract(event);
        assert_eq!(text.as_deref(), Some("final\n"));
    }
}
