//! Conversation and message CRUD with streaming relay and event emission.
mod convert;
pub mod response_middleware;
pub mod routes;
pub mod routes_aux;
pub mod service;
mod service_ops;
pub mod skill_resolver;
pub mod skill_snapshot;
pub mod snapshot_deps;
pub mod state;
pub mod stream_relay;
pub mod task_options;

pub use response_middleware::{
    CronCommand, CronCommandResult, CronCreateParams, CronUpdateParams, ICronService, MessageMiddleware,
    MiddlewareResult, detect_cron_commands, has_cron_commands, strip_cron_commands, strip_think_tags,
};
pub use routes::conversation_routes;
pub use routes_aux::conversation_ops_routes;
pub use service::{ConversationService, RevertToolCallResponse};
pub use snapshot_deps::{SnapshotDeps, get as snapshot_deps_get, set as snapshot_deps_set};
pub use state::ConversationRouterState;

#[cfg(test)]
#[path = "service_test.rs"]
mod service_test;
