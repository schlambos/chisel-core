use std::sync::Arc;

use crate::service::ConversationService;
use chisl_ai_agent::IWorkerTaskManager;

/// Shared state for conversation route handlers.
#[derive(Clone)]
pub struct ConversationRouterState {
    pub service: ConversationService,
    pub task_manager: Arc<dyn IWorkerTaskManager>,
}
