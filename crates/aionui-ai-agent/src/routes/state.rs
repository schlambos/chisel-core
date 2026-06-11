use std::sync::Arc;

use aionui_db::IConversationRepository;
use aionui_realtime::EventBroadcaster;

use crate::manager::local_opencode::LocalOpenCodeManager;
use crate::{AgentRegistry, AgentService, RemoteAgentService};

/// Router state for remote agent routes.
///
/// `conversation_repo` and `broadcaster` are carried here (rather than on
/// a separate router) so the Phase 4b `backfill-remote-history` route can
/// persist messages and emit `conversation.listChanged(updated)` without
/// adding a second axum extractor.
#[derive(Clone)]
pub struct RemoteAgentRouterState {
    pub service: Arc<RemoteAgentService>,
    pub conversation_repo: Arc<dyn IConversationRepository>,
    pub broadcaster: Arc<dyn EventBroadcaster>,
}

#[derive(Clone)]
pub struct AgentRouterState {
    pub agent_registry: Arc<AgentRegistry>,
    pub service: Arc<AgentService>,
}

/// Router state for local OpenCode management routes (Phase 4).
///
/// The renderer hits `/api/local-opencode/...` to spin up and
/// manage `opencode serve` processes on the host. The manager
/// is the process-global singleton so the graceful-shutdown
/// hook in `commands/server.rs` sees the same instances the
/// route handlers do.
#[derive(Clone)]
pub struct LocalOpenCodeRouterState {
    pub manager: Arc<LocalOpenCodeManager>,
}
