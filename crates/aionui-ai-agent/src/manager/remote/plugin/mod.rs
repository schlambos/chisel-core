//! OpenCode bridge plugin channel — the control-plane companion to
//! [`super::local_fs_mcp`] (the data plane).
//!
//! The first-party OpenCode plugin (`@chisl/chisl-opencode-plugin`)
//! running on a remote OpenCode instance dials back into this
//! AionCore process over HTTP. The plugin webserver in this module
//! answers:
//!
//! - `POST /plugin/hello` — handshake, records the plugin's versions
//!   and hook surface for the agent's row.
//! - `GET  /plugin/events` — server-sent events stream the host uses
//!   to push messages to the plugin.
//! - `POST /plugin/result` — fire-and-forget hook telemetry
//!   (`tool.before`, `tool.after`, raw events, permission asks). All
//!   payloads land in the per-agent audit ring buffer; production
//!   logs never see the raw args/output.
//! - `POST /tools/run_shell_streaming` — SSE stream of a shell command
//!   the plugin wants executed locally on the user's machine. Gated
//!   through the same [`ShellApprover`] the local fs MCP uses.
//!
//! The server is a process-wide singleton bound on first call to
//! [`ensure_plugin_server`]; subsequent calls reuse the existing
//! listener and validator. One process can host many agents; each
//! agent has its own token, connection state, audit log, and push
//! channel (see [`registry::PluginRegistry`]).

pub mod auth;
mod port;
pub mod bg;
pub mod protocol;
pub mod registry;
pub mod server;
pub mod shell_stream;
pub mod ui_push;

pub use bg::{
    BgError, BgProcessManager, DEFAULT_BG_TIMEOUT_SECS, MAX_BG_PROCESSES_PER_AGENT, MAX_BG_TIMEOUT_SECS, bg_global,
    bg_info_to_ui, kill_all_bg_processes,
};
pub use port::PLUGIN_PORT_ENV;
pub use protocol::{
    BgErrorResponse, BgListResponse, BgProcessInfo, BgProcessResponse, BgReadResponse, BgRequest, BgStatus,
    BgTailRequest, PROTOCOL_VERSION, PluginAuditRecord, PluginHelloRequest, PluginHelloResponse, PluginProjectInfo,
    PluginPushEvent, PluginResultRequest, PluginResultResponse, RunShellStreamingRequest,
};
pub use auth::{DbPluginTokenValidator, db_token_validator, global_validator, set_global_validator};
pub use registry::{
    PluginConnectionState, PluginRegistry, PluginTokenValidator, STICKY_VOICE_MODE_CAP, global as global_registry,
};
pub use server::{PluginServer, ensure_plugin_server, plugin_listen_addr};
#[cfg(any(test, feature = "test-support"))]
pub use ui_push::install_for_test as install_ui_notifier_for_test;
pub use ui_push::{notify as notify_ui, set_ui_notifier};
