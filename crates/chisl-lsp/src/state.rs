//! RouterState for the LSP module. Holds the session manager via Arc so the
//! HTTP and WebSocket route handlers share the same set of live sessions.

use std::sync::Arc;

use crate::service::LspService;

#[derive(Clone)]
pub struct LspRouterState {
    pub service: Arc<LspService>,
}
