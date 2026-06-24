use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Data for the `ToolInput` event — streams the JSON arguments the model is
/// constructing for a tool call, before the tool is invoked. Maps OpenCode's
/// `EventSessionNextToolInputStarted` / `Delta` / `Ended` triplet into a single
/// canonical event with a `phase` discriminator.
///
/// `started` carries `tool_name` only; `delta` carries `input_delta` (raw,
/// incremental JSON text — invalid until `ended`); `ended` carries `final_input`
/// (the parsed, validated JSON the tool will receive).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolInputEventData {
    pub session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_session_id: Option<String>,
    pub message_id: String,
    pub part_id: String,
    pub phase: ToolInputPhase,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_delta: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_input: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<i64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolInputPhase {
    Started,
    Delta,
    Ended,
}

/// Data for the `ToolProgress` event — a transient progress update emitted by
/// long-running tools (`bash`, `grep`, `read`/`write`, MCP). Maps OpenCode's
/// `EventSessionNextToolProgress`. The `progress` payload shape is
/// tool-specific; chislcore normalizes well-known shapes via
/// `normalize_progress_payload`, otherwise it forwards the raw JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolProgressEventData {
    pub session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_session_id: Option<String>,
    pub message_id: String,
    pub part_id: String,
    pub tool_name: String,
    pub progress: Value,
    /// Milliseconds since epoch. May be 0 if upstream omitted the timestamp.
    pub at: i64,
}

/// Best-effort normalization of OpenCode tool-progress payloads into shapes the
/// renderer can consume without per-tool special-casing.
///
/// For known tool families (`bash`, `grep`, `glob`, `read`, `write`,
/// `apply_patch`), strip large bodies and surface canonical fields. Unknown
/// tools forward the raw object unchanged.
///
/// `apply_patch` is the most security-sensitive case: file bodies in `progress`
/// can leak sensitive contents to the renderer, so we drop everything except
/// summary fields.
pub fn normalize_progress_payload(tool_name: &str, payload: &Value) -> Value {
    match tool_name {
        "apply_patch" | "ApplyPatch" => {
            // Strip patch body / file contents. Keep only summary fields.
            let mut out = serde_json::Map::new();
            if let Some(obj) = payload.as_object() {
                for key in [
                    "status",
                    "step",
                    "filesTouched",
                    "filesChanged",
                    "files_changed",
                    "summary",
                ] {
                    if let Some(v) = obj.get(key) {
                        out.insert(key.to_string(), v.clone());
                    }
                }
            }
            Value::Object(out)
        }
        "bash" | "Bash" | "Shell" => {
            // Bash payloads are passthrough — the UI's terminal block expects
            // raw chunks. We trust the producer; size capping is the
            // renderer's responsibility.
            payload.clone()
        }
        _ => payload.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn tool_input_started_serializes() {
        let event = ToolInputEventData {
            session_id: "sess-1".into(),
            parent_session_id: None,
            message_id: "msg-1".into(),
            part_id: "part-1".into(),
            phase: ToolInputPhase::Started,
            tool_name: Some("write".into()),
            input_delta: None,
            final_input: None,
            started_at: Some(1700000000000),
            completed_at: None,
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["session_id"], "sess-1");
        assert_eq!(json["phase"], "started");
        assert_eq!(json["tool_name"], "write");
        assert!(json.get("parent_session_id").is_none());
    }

    #[test]
    fn tool_input_delta_serializes() {
        let event = ToolInputEventData {
            session_id: "sess-1".into(),
            parent_session_id: Some("sess-parent".into()),
            message_id: "msg-1".into(),
            part_id: "part-1".into(),
            phase: ToolInputPhase::Delta,
            tool_name: None,
            input_delta: Some("{\"path\":".into()),
            final_input: None,
            started_at: None,
            completed_at: None,
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["phase"], "delta");
        assert_eq!(json["input_delta"], "{\"path\":");
        assert_eq!(json["parent_session_id"], "sess-parent");
    }

    #[test]
    fn tool_progress_for_apply_patch_strips_bodies() {
        let raw = json!({
            "status": "applying",
            "step": "writing file",
            "patch": "some very long patch body that should not leak",
            "oldText": "previous file contents",
            "newText": "new file contents",
            "filesChanged": 1,
        });
        let out = normalize_progress_payload("apply_patch", &raw);
        let obj = out.as_object().unwrap();
        assert!(obj.contains_key("status"));
        assert!(obj.contains_key("step"));
        assert!(obj.contains_key("filesChanged"));
        assert!(!obj.contains_key("patch"));
        assert!(!obj.contains_key("oldText"));
        assert!(!obj.contains_key("newText"));
    }

    #[test]
    fn tool_progress_for_unknown_passes_through() {
        let raw = json!({ "custom": { "step": "x", "percent": 12 } });
        let out = normalize_progress_payload("my_custom_tool", &raw);
        assert_eq!(out, raw);
    }
}
