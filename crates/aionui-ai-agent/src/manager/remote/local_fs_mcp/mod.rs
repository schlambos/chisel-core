//! Client-side MCP server vending filesystem tools scoped to one project.
//!
//! Lifecycle: bound to a single OpenCode session. Tear down with
//! `LocalFsMcpServer::shutdown` when the session ends. Auth tokens are
//! per-server and should be rotated per session.

pub mod project_tree;
pub mod protocol;
pub mod server;
pub mod tools;

pub use server::LocalFsMcpServer;
