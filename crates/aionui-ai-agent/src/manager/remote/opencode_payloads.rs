//! Typed outbound payloads for the OpenCode remote API.
//!
//! Every `reqwest::Client::json(&body)` call on the Chisl → OpenCode wire
//! should go through a struct defined here. This module replaces the ad-hoc
//! `serde_json::json!({...})` blobs that were previously the source of truth
//! for the wire shape: with typed payloads, the OpenCode SDK's expected
//! `PromptInput`, `SessionCreateInput`, `PermissionReply`, etc. surface as
//! real Rust types and the `aionui-opencode-conformance` suite can pin the
//! exact field set.
//!
//! ## Wire-format rules
//!
//! All payloads follow three rules that preserve the exact byte shape Chisl
//! has been sending on the wire:
//!
//! 1. **Optional fields use `#[serde(skip_serializing_if = "Option::is_none")]`**.
//!    This keeps `body["skills"]` from being serialized as `"skills": null`
//!    when no skills are being injected, which would otherwise be silently
//!    rejected by strict OpenCode SDK schemas.
//! 2. **CamelCase field names with explicit `#[serde(rename = "...")]` where
//!    they would otherwise be lowercase**. OpenCode's SDK uses `messageID`,
//!    `providerID`, `modelID`, `partID` (capitalised acronyms) — these are
//!    not Rust's default snake_case, so we rename them explicitly.
//! 3. **Untyped blobs (`Value`) are only allowed for fields OpenCode's SDK
//!    types as `Record<string, any>`** (e.g. metadata, headers, per-tool
//!    inputs). All "shape" fields are real types.
//!
//! ## Adding a new endpoint
//!
//! 1. Add a struct here, deriving `serde::Serialize` (and `Debug, Clone` for
//!    ergonomics in the request helpers).
//! 2. Build it at the call site and pass it to `req.json(&payload)`.
//! 3. Add a `to_wire_value` unit test in this file's `#[cfg(test)]` block
//!    that pins the byte-level shape (key set, casing, presence of optional
//!    fields). The conformance suite will also exercise it.

use std::collections::HashMap;

use serde::Serialize;

// -- V1 /session/{id}/prompt_async ----------------------------------------

/// Body of `POST /session/{id}/prompt_async`.
///
/// Mirrors OpenCode v1's `PromptInput` schema. `parts` is required (the
/// server rejects an empty body); the other fields are sent only when set so
/// the wire shape matches what Chisl has always produced.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OpencodePromptRequest {
    pub parts: Vec<PromptPart>,

    /// Optional message id (only valid on a freshly created session — see
    /// `opencode_send` comment in `agent.rs` for the "2nd message returns
    /// nothing" bug rationale).
    #[serde(rename = "messageID", skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,

    /// Free-form system hint injected by the local-fs MCP subsystem.
    /// Omitted in server-tools mode (no client-side tools).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system: Option<String>,

    /// Per-prompt model override.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<PromptModel>,

    /// Per-prompt agent override (`build` / `plan` / ...).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,

    /// Skill names to surface (matches `GET /skill` catalog).
    /// Empty slice is omitted from the wire.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub skills: Vec<String>,
}

impl OpencodePromptRequest {
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            parts: vec![PromptPart::text(text)],
            message_id: None,
            system: None,
            model: None,
            agent: None,
            skills: Vec::new(),
        }
    }
}

/// A single part of a prompt body. Only `text` is wired today; the SDK
/// supports `file` / `agent` / `subtask` parts but the V1 prompt path does
/// not emit them.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum PromptPart {
    Text {
        text: String,
    },
    #[serde(other)]
    Other,
}

impl PromptPart {
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text { text: text.into() }
    }
}

/// Per-prompt model selection.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PromptModel {
    #[serde(rename = "providerID")]
    pub provider_id: String,
    #[serde(rename = "modelID")]
    pub model_id: String,
}

// -- V1 /session create ---------------------------------------------------

/// Body of `POST /session`.
///
/// The V1 server accepts an empty body (server-tools mode omits `permission`)
/// and a body that pre-denies the built-in tools (client-side fs MCP mode).
/// Each variant is its own struct so the call site does not have to remember
/// to "remove the `permission` field" when flipping modes.
#[derive(Debug, Clone, Serialize, PartialEq, Eq, Default)]
pub struct OpencodeSessionCreate {
    /// Server-side permission rules. Chisl emits a deny-all set for the
    /// built-in tools when running in local-fs MCP mode so the model is
    /// forced onto the client-side tools.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub permission: Vec<PermissionRule>,
}

/// One rule in the session-create `permission` array.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PermissionRule {
    pub permission: String,
    pub pattern: String,
    pub action: String,
}

impl OpencodeSessionCreate {
    /// Pre-deny the OpenCode built-in tools (`bash` / `read` / `edit` /
    /// `glob` / `grep`). Used in local-fs MCP mode so the model is forced
    /// through `aionui-local-fs_*` instead of the server's own filesystem.
    pub fn deny_builtin_tools() -> Self {
        Self {
            permission: vec![
                PermissionRule::deny("bash"),
                PermissionRule::deny("read"),
                PermissionRule::deny("edit"),
                PermissionRule::deny("glob"),
                PermissionRule::deny("grep"),
            ],
        }
    }
}

impl PermissionRule {
    pub fn deny(tool: impl Into<String>) -> Self {
        Self {
            permission: tool.into(),
            pattern: "*".to_string(),
            action: "deny".to_string(),
        }
    }
}

// -- V1 /session/{id}/fork, /revert, /summarize, /share -------------------

/// Body of `POST /session/{id}/fork`.
///
/// Fork-at semantics: the SDK keeps only messages *strictly before* the
/// given id, so the caller has already translated the user's "include this
/// message" intent into "fork at the message after it". Omitting `messageID`
/// forks from the tip (whole transcript).
#[derive(Debug, Clone, Serialize, Default, PartialEq, Eq)]
pub struct OpencodeForkRequest {
    #[serde(rename = "messageID", skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
}

/// Body of `POST /session/{id}/revert`.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OpencodeRevertRequest {
    #[serde(rename = "messageID")]
    pub message_id: String,
    #[serde(rename = "partID", skip_serializing_if = "Option::is_none")]
    pub part_id: Option<String>,
}

/// Body of `POST /session/{id}/summarize` (V1 compact).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OpencodeSummarizeRequest {
    #[serde(rename = "providerID")]
    pub provider_id: String,
    #[serde(rename = "modelID")]
    pub model_id: String,
}

// -- V1 /permission/{id}/reply --------------------------------------------

/// Body of `POST /permission/{id}/reply`.
///
/// `reply` is one of `once` | `always` | `reject` (wire-canonical — Chisl
/// maps `allow_dir` / `allow_session` to `once` upstream of this struct).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OpencodePermissionReply {
    pub reply: String,
}

impl OpencodePermissionReply {
    pub fn new(reply: impl Into<String>) -> Self {
        Self { reply: reply.into() }
    }
}

// -- V1 /question/{id}/reply, /question/{id}/reject -----------------------

/// Body of `POST /question/{id}/reply`. `answers[i][j]` is the option label
/// the user picked for question `i` (column-major in the SDK).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OpencodeQuestionReply {
    pub answers: Vec<Vec<String>>,
}

// -- V1 /session/{id}/command ---------------------------------------------

/// Body of `POST /session/{id}/command`.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OpencodeCommandRequest {
    pub command: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

// -- V2 /api/session/{id}/prompt, /compact --------------------------------

/// Body of `POST /api/session/{id}/prompt`.
///
/// Per the V2 SDK `Prompt` class, only `text` is accepted in `prompt` —
/// per-prompt `model` / `agent` / `skills` are silently stripped server-side.
/// Callers that need overrides must route through the V1 `prompt_async`
/// path. `delivery` (`"immediate"` | `"deferred"`) controls whether the
/// server runs the agent loop synchronously.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OpencodeV2PromptRequest {
    pub prompt: OpencodeV2PromptText,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OpencodeV2PromptText {
    pub text: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reference: Option<serde_json::Value>,
}

/// Body of `POST /api/session/{id}/compact` (V2).
#[derive(Debug, Clone, Serialize, Default, PartialEq, Eq)]
pub struct OpencodeV2CompactRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
}

// -- V1 /mcp (mcp.add) ----------------------------------------------------

/// Body of `POST /mcp`. OpenCode's "remote" transport is what the deployed
/// server supports today; see `opencode_mcp::register_mcp` for the keep-
/// using-remote rationale.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OpencodeMcpAddRequest {
    pub name: String,
    pub config: OpencodeMcpRemoteConfig,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OpencodeMcpRemoteConfig {
    #[serde(rename = "type")]
    pub transport_type: String,
    pub url: String,
    pub enabled: bool,
    pub oauth: bool,
    pub headers: HashMap<String, String>,
    pub timeout: u64,
}

impl OpencodeMcpRemoteConfig {
    pub fn remote(url: impl Into<String>, bearer_token: impl AsRef<str>, timeout_ms: u64) -> Self {
        let mut headers = HashMap::new();
        headers.insert("Authorization".to_string(), format!("Bearer {}", bearer_token.as_ref()));
        Self {
            transport_type: "remote".to_string(),
            url: url.into(),
            enabled: true,
            oauth: false,
            headers,
            timeout: timeout_ms,
        }
    }
}

// -- V1 /auth/{providerID}, OAuth -----------------------------------------

/// Body of `PUT /auth/{providerID}`. The OpenCode `Auth` union is
/// `{ type, ...payload }`; the variants below cover the three members Chisl
/// writes today.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum OpencodeAuthBody {
    /// `{ type: "api", key }` — simplest credentials.
    Api { key: String },
    /// `{ type: "wellknown", key, token }`.
    WellKnown { key: String, token: String },
    /// OAuth / custom — passes through unchanged.
    #[serde(untagged)]
    Other(serde_json::Value),
}

impl OpencodeAuthBody {
    pub fn api(key: impl Into<String>) -> Self {
        Self::Api { key: key.into() }
    }

    pub fn wellknown(key: impl Into<String>, token: impl Into<String>) -> Self {
        Self::WellKnown { key: key.into(), token: token.into() }
    }
}

/// Body of `POST /provider/{providerID}/oauth/authorize`.
///
/// `method` is the index into the provider's auth-methods array from
/// `GET /provider/auth`. `inputs` are method-specific form fields (e.g.
/// API key, base URL).
#[derive(Debug, Clone, Serialize, Default, PartialEq, Eq)]
pub struct OpencodeOAuthAuthorizeRequest {
    pub method: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inputs: Option<HashMap<String, String>>,
}

/// Body of `POST /provider/{providerID}/oauth/callback`.
#[derive(Debug, Clone, Serialize, Default, PartialEq, Eq)]
pub struct OpencodeOAuthCallbackRequest {
    pub method: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
}

// -- V1 /sync/steal, /sync/history ----------------------------------------

/// Body of `POST /sync/steal` (M20).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OpencodeSyncStealRequest {
    #[serde(rename = "sessionID")]
    pub session_id: String,
}

/// Body of `POST /sync/history` (M20).
///
/// `aggregate_id -> last_known_seq`. Aggregates not in the map receive
/// their full history.
#[derive(Debug, Clone, Serialize, Default, PartialEq, Eq)]
pub struct OpencodeSyncHistoryRequest(pub HashMap<String, u64>);

// -- V1 /log (M14 log forwarder) ------------------------------------------

/// Body of `POST /log`. The forwarder is a `tracing_subscriber::Layer` that
/// fans out INFO/WARN/ERROR events to the server's `/log` sink.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OpencodeLogEntry {
    pub service: String,
    pub level: String,
    pub message: String,
    pub extra: serde_json::Value,
}

// -- Internal model state -------------------------------------------------

/// Shape of `RemoteState::desired_model` — the model the next prompt will
/// be sent with, regardless of which path (V1 / V2) actually fires.
///
/// Not a wire payload itself (it's not sent; it's read on the way out and
/// fed into `OpencodePromptRequest::model` or `OpencodeSummarizeRequest`),
/// but it is the source of truth for the model-pair (`providerID`,
/// `modelID`) and the only place "variant" is recorded. Defined here so
/// the field set is colocated with the wire types.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DesiredModel {
    #[serde(rename = "modelID")]
    pub model_id: String,
    #[serde(rename = "providerID")]
    pub provider_id: String,
    #[serde(default = "default_variant")]
    pub variant: String,
}

use serde::Deserialize;

fn default_variant() -> String {
    "default".to_string()
}

impl DesiredModel {
    pub fn new(provider_id: impl Into<String>, model_id: impl Into<String>) -> Self {
        Self {
            model_id: model_id.into(),
            provider_id: provider_id.into(),
            variant: default_variant(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn to_value<S: Serialize>(s: &S) -> Value {
        serde_json::to_value(s).expect("typed payload should serialize")
    }

    #[test]
    fn prompt_request_minimal_only_parts() {
        let req = OpencodePromptRequest::text("hello");
        let v = to_value(&req);
        assert_eq!(v, json!({ "parts": [{ "type": "text", "text": "hello" }] }));
    }

    #[test]
    fn prompt_request_full_field_set() {
        let req = OpencodePromptRequest {
            parts: vec![PromptPart::text("body")],
            message_id: Some("msg_1".into()),
            system: Some("hint".into()),
            model: Some(PromptModel {
                provider_id: "anthropic".into(),
                model_id: "claude-sonnet-4-5".into(),
            }),
            agent: Some("build".into()),
            skills: vec!["s1".into()],
        };
        let v = to_value(&req);
        // Acronym casing matters: messageID, providerID, modelID.
        assert_eq!(
            v,
            json!({
                "parts": [{ "type": "text", "text": "body" }],
                "messageID": "msg_1",
                "system": "hint",
                "model": { "providerID": "anthropic", "modelID": "claude-sonnet-4-5" },
                "agent": "build",
                "skills": ["s1"]
            })
        );
    }

    #[test]
    fn prompt_request_omits_empty_skills() {
        // Empty `skills` array must not be emitted as `"skills": []` —
        // prior code already had `if !inject_skills.is_empty()` guarding
        // the field, and this must hold across the typed-payload migration.
        let req = OpencodePromptRequest {
            parts: vec![PromptPart::text("body")],
            message_id: None,
            system: None,
            model: None,
            agent: None,
            skills: vec![],
        };
        let v = to_value(&req);
        assert!(v.get("skills").is_none(), "empty skills array must be omitted");
    }

    #[test]
    fn session_create_default_is_empty_object() {
        // Server-tools mode: no `permission` field at all.
        let req = OpencodeSessionCreate::default();
        let v = to_value(&req);
        assert_eq!(v, json!({}));
    }

    #[test]
    fn session_create_deny_builtin_tools_shape() {
        let req = OpencodeSessionCreate::deny_builtin_tools();
        let v = to_value(&req);
        let perms = v.get("permission").and_then(|p| p.as_array()).expect("permission array");
        assert_eq!(perms.len(), 5);
        for entry in perms {
            assert_eq!(entry.get("pattern").and_then(|s| s.as_str()), Some("*"));
            assert_eq!(entry.get("action").and_then(|s| s.as_str()), Some("deny"));
        }
        let tools: Vec<&str> = perms
            .iter()
            .map(|e| e.get("permission").and_then(|s| s.as_str()).unwrap())
            .collect();
        assert_eq!(tools, vec!["bash", "read", "edit", "glob", "grep"]);
    }

    #[test]
    fn fork_request_omits_message_id_when_at_tip() {
        let req = OpencodeForkRequest::default();
        assert_eq!(to_value(&req), json!({}));
        let req = OpencodeForkRequest { message_id: Some("msg_after".into()) };
        assert_eq!(to_value(&req), json!({ "messageID": "msg_after" }));
    }

    #[test]
    fn revert_request_required_message_id_optional_part_id() {
        let req = OpencodeRevertRequest { message_id: "msg_1".into(), part_id: None };
        assert_eq!(to_value(&req), json!({ "messageID": "msg_1" }));
        let req = OpencodeRevertRequest { message_id: "msg_1".into(), part_id: Some("prt_2".into()) };
        assert_eq!(to_value(&req), json!({ "messageID": "msg_1", "partID": "prt_2" }));
    }

    #[test]
    fn summarize_request_shape() {
        let req = OpencodeSummarizeRequest { provider_id: "anthropic".into(), model_id: "claude-sonnet-4-5".into() };
        assert_eq!(
            to_value(&req),
            json!({ "providerID": "anthropic", "modelID": "claude-sonnet-4-5" })
        );
    }

    #[test]
    fn permission_reply_shape() {
        let req = OpencodePermissionReply::new("once");
        assert_eq!(to_value(&req), json!({ "reply": "once" }));
    }

    #[test]
    fn question_reply_matrix_shape() {
        let req = OpencodeQuestionReply {
            answers: vec![vec!["Postgres".into()], vec!["North".into(), "East".into()]],
        };
        let v = to_value(&req);
        // 2 questions, 1+2 answers, column-major.
        let arr = v.get("answers").and_then(|a| a.as_array()).unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0], json!(["Postgres"]));
        assert_eq!(arr[1], json!(["North", "East"]));
    }

    #[test]
    fn command_request_minimal_only_command() {
        let req = OpencodeCommandRequest { command: "/init".into(), agent: None, model: None };
        assert_eq!(to_value(&req), json!({ "command": "/init" }));
    }

    #[test]
    fn command_request_with_overrides() {
        let req = OpencodeCommandRequest {
            command: "/review src/main.rs".into(),
            agent: Some("build".into()),
            model: Some("claude-sonnet-4".into()),
        };
        let v = to_value(&req);
        assert_eq!(v.get("agent").and_then(|s| s.as_str()), Some("build"));
        assert_eq!(v.get("model").and_then(|s| s.as_str()), Some("claude-sonnet-4"));
    }

    #[test]
    fn v2_prompt_request_minimal_text_only() {
        let req = OpencodeV2PromptRequest {
            prompt: OpencodeV2PromptText { text: "hello".into(), files: vec![], agent: None, reference: None },
            delivery: Some("immediate".into()),
        };
        let v = to_value(&req);
        assert_eq!(v, json!({ "prompt": { "text": "hello" }, "delivery": "immediate" }));
    }

    #[test]
    fn v2_compact_request_optional_instructions() {
        let req = OpencodeV2CompactRequest::default();
        assert_eq!(to_value(&req), json!({}));
        let req = OpencodeV2CompactRequest { instructions: Some("summarize".into()) };
        assert_eq!(to_value(&req), json!({ "instructions": "summarize" }));
    }

    #[test]
    fn mcp_add_request_shape() {
        let req = OpencodeMcpAddRequest {
            name: "aionui-local-fs".into(),
            config: OpencodeMcpRemoteConfig::remote("http://127.0.0.1:7117/mcp", "tok-abc", 30_000),
        };
        let v = to_value(&req);
        assert_eq!(v.get("name").and_then(|s| s.as_str()), Some("aionui-local-fs"));
        let cfg = v.get("config").unwrap();
        assert_eq!(cfg.get("type").and_then(|s| s.as_str()), Some("remote"));
        assert_eq!(cfg.get("url").and_then(|s| s.as_str()), Some("http://127.0.0.1:7117/mcp"));
        assert_eq!(cfg.get("enabled").and_then(|b| b.as_bool()), Some(true));
        assert_eq!(cfg.get("oauth").and_then(|b| b.as_bool()), Some(false));
        let headers = cfg.get("headers").and_then(|h| h.as_object()).unwrap();
        assert_eq!(
            headers.get("Authorization").and_then(|s| s.as_str()),
            Some("Bearer tok-abc")
        );
        assert!(cfg.get("timeout").and_then(|n| n.as_u64()).is_some());
    }

    #[test]
    fn auth_body_api() {
        let body = OpencodeAuthBody::api("sk-abc");
        let v = to_value(&body);
        assert_eq!(v, json!({ "type": "api", "key": "sk-abc" }));
    }

    #[test]
    fn auth_body_wellknown() {
        let body = OpencodeAuthBody::wellknown("zen", "tk-xyz");
        let v = to_value(&body);
        assert_eq!(v, json!({ "type": "wellknown", "key": "zen", "token": "tk-xyz" }));
    }

    #[test]
    fn oauth_authorize_minimal_method_only() {
        let req = OpencodeOAuthAuthorizeRequest { method: 0, inputs: None };
        assert_eq!(to_value(&req), json!({ "method": 0 }));
    }

    #[test]
    fn oauth_authorize_with_inputs() {
        let mut inputs = HashMap::new();
        inputs.insert("api_key".to_string(), "sk-abc".to_string());
        let req = OpencodeOAuthAuthorizeRequest { method: 0, inputs: Some(inputs) };
        let v = to_value(&req);
        assert_eq!(v.get("method").and_then(|n| n.as_u64()), Some(0));
        assert_eq!(
            v.get("inputs").and_then(|i| i.get("api_key")).and_then(|s| s.as_str()),
            Some("sk-abc")
        );
    }

    #[test]
    fn oauth_callback_with_code() {
        let req = OpencodeOAuthCallbackRequest { method: 0, code: Some("auth-code".into()) };
        let v = to_value(&req);
        assert_eq!(v.get("code").and_then(|s| s.as_str()), Some("auth-code"));
    }

    #[test]
    fn oauth_callback_without_code() {
        let req = OpencodeOAuthCallbackRequest::default();
        let v = to_value(&req);
        assert!(v.get("code").is_none());
    }

    #[test]
    fn sync_steal_request_shape() {
        let req = OpencodeSyncStealRequest { session_id: "ses_1".into() };
        assert_eq!(to_value(&req), json!({ "sessionID": "ses_1" }));
    }

    #[test]
    fn sync_history_request_aggregate_seqs() {
        let mut since = HashMap::new();
        since.insert("ses_1".to_string(), 42u64);
        since.insert("ses_2".to_string(), 0u64);
        let req = OpencodeSyncHistoryRequest(since);
        let v = to_value(&req);
        assert_eq!(v.get("ses_1").and_then(|n| n.as_u64()), Some(42));
        assert_eq!(v.get("ses_2").and_then(|n| n.as_u64()), Some(0));
    }

    #[test]
    fn log_entry_shape() {
        let mut extra = serde_json::Map::new();
        extra.insert("conversation_id".to_string(), Value::String("c1".into()));
        let req = OpencodeLogEntry {
            service: "aionui".into(),
            level: "info".into(),
            message: "prompt sent".into(),
            extra: Value::Object(extra),
        };
        let v = to_value(&req);
        assert_eq!(v.get("service").and_then(|s| s.as_str()), Some("aionui"));
        assert_eq!(v.get("level").and_then(|s| s.as_str()), Some("info"));
        assert_eq!(v.get("message").and_then(|s| s.as_str()), Some("prompt sent"));
        assert_eq!(
            v.get("extra").and_then(|e| e.get("conversation_id")).and_then(|s| s.as_str()),
            Some("c1")
        );
    }

    #[test]
    fn desired_model_default_variant() {
        let m = DesiredModel::new("anthropic", "claude-sonnet-4-5");
        let v = to_value(&m);
        assert_eq!(
            v,
            json!({ "modelID": "claude-sonnet-4-5", "providerID": "anthropic", "variant": "default" })
        );
    }

    #[test]
    fn desired_model_round_trip() {
        let m = DesiredModel::new("anthropic", "claude-sonnet-4-5");
        let s = serde_json::to_string(&m).unwrap();
        let back: DesiredModel = serde_json::from_str(&s).unwrap();
        assert_eq!(back, m);
    }
}
