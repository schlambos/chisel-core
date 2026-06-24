//! Client-side MCP server vending filesystem tools scoped to one project.
//!
//! Lifecycle: bound to a single OpenCode session. Tear down with
//! `LocalFsMcpServer::shutdown` when the session ends. Auth tokens are
//! per-server and should be rotated per session.

pub mod project_tree;
pub mod protocol;
pub mod server;
pub mod shell;
pub mod snapshot_deps;
pub mod tools;

pub use server::{ContactProbe, LocalFsMcpServer};
pub use shell::{
    ElicitationHandler, ElicitationOutcome, ElicitationRequest, McpRequestContext, ShellApproval, ShellApprover,
};
pub use snapshot_deps::{SnapshotDeps, get as snapshot_deps_get, set as snapshot_deps_set};
pub use tools::SnapshotHook;
