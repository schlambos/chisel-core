//! Terminal session management for AionUi.
//!
//! Exposes:
//! * `POST /api/terminal/sessions` — create a new terminal session
//! * `GET /api/terminal/sessions` — list all terminal sessions
//! * `POST /api/terminal/sessions/kill` — kill a terminal session
//! * `POST /api/terminal/sessions/resize` — resize a terminal session
//! * `GET /api/terminal/ws/{session_id}` — bidirectional terminal transport
//!
//! The HTTP routes (state-changing) go through the standard auth middleware
//! installed by `chisl-app`. The WebSocket upgrade route is CSRF-exempt
//! like `/ws` and authenticates via the standard `ws_upgrade_handler`
//! token-extractor convention (re-used here through a thin wrapper).
//!
//! Terminal spawning goes exclusively through `portable-pty` for
//! cross-platform PTY support.

pub mod pty;
pub mod routes;
pub mod service;
pub mod state;
pub mod transport;

pub use routes::{terminal_routes, terminal_ws_routes};
pub use service::TerminalService;
pub use state::TerminalRouterState;
