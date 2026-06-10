//! Wire types for the OpenCode bridge plugin channel.
//!
//! The first-party OpenCode plugin (`@chisl/chisl-opencode-plugin`) running
//! on a remote OpenCode instance dials back into AionCore's plugin
//! webserver over HTTP. These types are the on-the-wire shape for that
//! channel — they MUST stay in lock-step with the plugin's published
//! schema (see `packages/chisl-opencode-plugin`).
//!
//! Design notes:
//! - All payloads are `#[serde(rename_all = "camelCase")]` to match the
//!   plugin's TypeScript conventions; the renderer-facing REST API
//!   continues to use snake_case per the existing convention.
//! - The plugin -> AionCore direction is represented by
//!   [`PluginHelloRequest`], [`PluginResultRequest`], and
//!   [`RunShellStreamingRequest`].
//! - AionCore -> plugin direction (the SSE event stream) is the
//!   [`PluginPushEvent`] carried over the broadcast channel and emitted
//!   through the `/plugin/events` route.
//! - [`PluginAuditRecord`] is the in-memory audit log shape; production
//!   log lines must NOT carry the underlying args/output — only the
//!   `summary` field (≤2048 chars) is safe to record.

use serde::{Deserialize, Serialize};

/// Current plugin-channel protocol version. Bumped on any
/// backwards-incompatible wire change; the plugin sends its own
/// supported version in [`PluginHelloRequest::protocol_version`] and the
/// server picks the highest mutually supported (or rejects on no
/// overlap).
pub const PROTOCOL_VERSION: u32 = 1;

// ── Hello handshake ──────────────────────────────────────────────

/// Plugin's first message after connecting. Records the plugin's own
/// version, the OpenCode version it was loaded into, and the hook
/// surface it has registered (used for UI status + future policy
/// decisions). `project` is the OpenCode-reported working tree, used to
/// disambiguate the agent row when one host serves multiple projects.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginHelloRequest {
    pub protocol_version: u32,
    pub plugin_version: String,
    #[serde(default)]
    pub opencode_version: Option<String>,
    #[serde(default)]
    pub hooks: Vec<String>,
    #[serde(default)]
    pub project: Option<PluginProjectInfo>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginProjectInfo {
    pub directory: String,
    #[serde(default)]
    pub worktree: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginHelloResponse {
    pub ok: bool,
    /// Server's own supported protocol version. The plugin must compare
    /// this to its own and treat mismatch as a soft-degrade (continue
    /// with documented fields only) rather than disconnecting.
    pub protocol_version: u32,
}

// ── Hook result reporting ────────────────────────────────────────

/// Tagged enum of everything the plugin can hand back to the host.
///
/// Tagged on the `kind` field, not the default `tag`/`content` pair, so
/// the shape lines up with the plugin's own `discriminator: 'kind'`
/// convention used elsewhere in the OpenCode plugin ecosystem.
///
/// `rename_all_fields = "camelCase"` is required (on top of
/// `rename_all = "camelCase"` for the variant names) because serde
/// doesn't apply the variant-level rename to the struct fields of an
/// internally-tagged enum automatically.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum PluginResultRequest {
    ToolBefore {
        tool: String,
        session_id: String,
        call_id: String,
        args: serde_json::Value,
    },
    ToolAfter {
        tool: String,
        session_id: String,
        call_id: String,
        args: serde_json::Value,
        #[serde(default)]
        title: Option<String>,
        #[serde(default)]
        output_len: Option<u64>,
        #[serde(default)]
        output_preview: Option<String>,
        #[serde(default)]
        metadata: Option<serde_json::Value>,
    },
    /// Raw OpenCode event JSON. The plugin already filters to the small
    /// set we care about (`file.watcher.updated`, `session.idle`,
    /// `message.part.updated`) so the host doesn't need to re-implement
    /// the OpenCode event grammar.
    Event { event: serde_json::Value },
    /// Tool is asking the host whether the user has approved the action.
    /// MVP: respond with `status: "ask"` and let OpenCode's own
    /// permission flow take over. A richer policy engine (auto-allow
    /// lists, per-tool rules, etc.) is a documented follow-up.
    PermissionAsk { permission: serde_json::Value },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginResultResponse {
    pub ok: bool,
    /// Only set on `PermissionAsk` — `"allow"`, `"deny"`, or `"ask"`
    /// (passthrough — the native OpenCode flow continues). Other kinds
    /// leave this `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

// ── Streaming shell ──────────────────────────────────────────────

/// Body of `POST /tools/run_shell_streaming`. `cwd` is the user's
/// workspace; if `None`, the host falls back to the agent's recorded
/// workspace root. `timeout_secs` is a per-call cap bounded by the
/// server (see shell_stream.rs).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunShellStreamingRequest {
    pub command: String,
    #[serde(default)]
    pub cwd: Option<String>,
    pub session_id: String,
    #[serde(default)]
    pub call_id: Option<String>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
}

// ── Audit record (in-memory) ─────────────────────────────────────

/// One entry in the per-agent audit ring buffer. Lives in
/// [`crate::manager::remote::plugin::registry::PluginRegistry`] only —
/// never written to production logs. The `summary` field is the
/// redacted, ≤2048-char rendering of the event suitable for surfacing
/// in the UI; raw `args` / `output` are NOT stored.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginAuditRecord {
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub call_id: Option<String>,
    pub at_ms: u64,
    pub summary: String,
}

// ── Push events (server → plugin over SSE) ───────────────────────

/// Event payload the server pushes to the plugin over the
/// `/plugin/events` SSE stream. `event` is the SSE event name (e.g.
/// `"ping"`, `"agentStatusChanged"`, `"shellStreamStart"`) and `data` is
/// an opaque JSON value the plugin interprets per its own schema.
#[derive(Debug, Clone, Serialize)]
pub struct PluginPushEvent {
    pub event: String,
    pub data: serde_json::Value,
}
