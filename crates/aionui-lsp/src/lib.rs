//! Language Server Protocol bridge for the Monaco-based editor.
//!
//! Exposes:
//! * `GET /api/lsp/servers` — list of supported language servers + install state
//! * `POST /api/lsp/sessions` — start a new LSP session for (workspace, language)
//! * `POST /api/lsp/sessions/stop` — stop a session by id
//! * `GET /api/lsp/ws/:session_id` — bidirectional JSON-RPC transport
//!
//! The HTTP routes (state-changing) go through the standard auth middleware
//! installed by `aionui-app`. The WebSocket upgrade route is CSRF-exempt
//! like `/ws` and authenticates via the standard `ws_upgrade_handler`
//! token-extractor convention (re-used here through a thin wrapper).
//!
//! Subprocess spawning goes exclusively through `aionui_runtime::Builder`,
//! per AGENTS.md.

pub mod languages;
pub mod routes;
pub mod service;
pub mod state;
pub mod transport;

pub use routes::{lsp_routes, lsp_ws_routes};
pub use service::LspService;
pub use state::LspRouterState;
