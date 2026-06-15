//! RouterState for the terminal module. Holds the session manager via Arc so the
//! HTTP and WebSocket route handlers share the same set of live sessions.

use std::sync::Arc;

use aionui_auth::JwtService;
use crate::service::TerminalService;

#[derive(Clone)]
pub struct TerminalRouterState {
    pub service: Arc<TerminalService>,
    pub jwt_service: Arc<JwtService>,
    pub local: bool,
}
