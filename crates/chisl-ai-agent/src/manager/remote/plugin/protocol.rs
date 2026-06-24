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

// ── Background process management ───────────────────────────────

/// Lifecycle status of a background process. `running` is the
/// active state; `exited` means the child terminated under its own
/// steam (exit code set, `ended_at_ms` set); `killed` is the catch-all
/// for stop / timeout / shutdown paths where we initiated the kill.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BgStatus {
    Running,
    Exited,
    Killed,
}

/// Snapshot of a background process's state. Surfaces in the plugin
/// tool's start/stop/list/read responses and in the renderer-facing
/// REST listing. Mirrors the audit log's `bg.*` kinds so the UI can
/// reconstruct the same timeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BgProcessInfo {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub command: String,
    pub cwd: String,
    pub session_id: String,
    pub status: BgStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i64>,
    pub started_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ended_at_ms: Option<u64>,
    /// Total bytes that have been appended to the ring buffer for
    /// this process. Equal to `output.len()` when `truncated == false`;
    /// when `truncated == true`, the ring has evicted older bytes and
    /// this counts what the child emitted in total.
    pub output_bytes: u64,
    /// True once the ring buffer has wrapped at least once for this
    /// process — i.e. callers asking for an `offset` below
    /// `output_bytes - <ring_cap>` no longer get the original bytes.
    pub truncated: bool,
}

/// Plugin → server request body for `POST /tools/bg`. Internally
/// tagged on `op` so the wire shape is one flat JSON object with a
/// discriminator field, matching the convention the
/// [`PluginResultRequest`] enum uses for the same channel.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "op", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum BgRequest {
    /// Start a long-running process. `command` runs under the user's
    /// native shell (mirrors the streaming shell tool's
    /// `resolve_shell` + `Builder::clean_cli` pipeline) and streams
    /// into a 512 KiB ring buffer. The plugin is expected to supply a
    /// `session_id` for the `McpRequestContext` the approver sees.
    Start {
        command: String,
        #[serde(default)]
        cwd: Option<String>,
        session_id: String,
        #[serde(default)]
        call_id: Option<String>,
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        timeout_secs: Option<u64>,
    },
    /// Stop a running process. Idempotent — stopping an already
    /// terminal process is a no-op that returns the existing record.
    Stop { process_id: String, session_id: String },
    /// List every known process for the agent row (running + terminal,
    /// capped at 16 terminal records per agent).
    List { session_id: String },
    /// Read output from `offset` to the current end of the ring
    /// buffer. `offset` defaults to 0 when the caller is asking for
    /// the full buffer; pass a previous `next_offset` to resume.
    Read {
        process_id: String,
        session_id: String,
        #[serde(default)]
        offset: Option<u64>,
    },
}

/// Body of `POST /tools/bg_tail`. Tail replays the ring buffer from
/// `from_offset` and then follows live output as new chunks arrive.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BgTailRequest {
    pub process_id: String,
    pub session_id: String,
    #[serde(default)]
    pub from_offset: Option<u64>,
}

/// Response body for the `Start` and `Stop` ops. `process` is the
/// full snapshot after the operation completed.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BgProcessResponse {
    pub ok: bool,
    pub process: BgProcessInfo,
}

/// Response body for the `List` op.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BgListResponse {
    pub ok: bool,
    pub processes: Vec<BgProcessInfo>,
}

/// Response body for the `Read` op. `output` is the slice of the
/// ring buffer between `offset` and the current write head;
/// `next_offset` is what to pass on the next `read` call to keep
/// streaming.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BgReadResponse {
    pub ok: bool,
    pub output: String,
    pub next_offset: u64,
    pub process: BgProcessInfo,
}

/// `ok: false` payload for ops that fail. `code` is a stable string
/// the plugin can switch on (`"no_approver"`, `"not_found"`,
/// `"limit_exceeded"`, `"denied"`, `"timeout"`, `"invalid"`).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BgErrorResponse {
    pub ok: bool,
    pub code: String,
    pub message: String,
}
