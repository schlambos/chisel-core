//! UI-facing types for the background-process REST surface.
//!
//! Mirrors the plugin webserver's [`BgProcessInfo`](
//! crate::manager::remote::plugin::BgProcessInfo) but in
//! snake_case to match the rest of the renderer REST surface
//! (`ApiResponse`, etc.). The plugin webserver uses camelCase
//! per its TypeScript convention; the renderer convention is
//! snake_case, so the two are deliberately different types.
//!
//! Lives in its own module so it doesn't widen the
//! `remote_agent` re-export and stay decoupled from the
//! remote-agent CRUD surface.

use serde::{Deserialize, Serialize};

/// Lifecycle status of a background process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BgProcessStatus {
    Running,
    Exited,
    Killed,
}

/// UI-facing snapshot of a background process owned by a
/// remote agent. Read via `GET /api/remote-agents/{id}/bg-processes`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BgProcessUiInfo {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub command: String,
    pub cwd: String,
    pub session_id: String,
    pub status: BgProcessStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i64>,
    pub started_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ended_at_ms: Option<u64>,
    pub output_bytes: u64,
    pub truncated: bool,
}

/// Response body for `GET /api/remote-agents/{id}/bg-processes`.
#[derive(Debug, Serialize)]
pub struct BgProcessListResponse {
    pub processes: Vec<BgProcessUiInfo>,
}

/// Response body for `GET /api/remote-agents/{id}/bg-processes/{pid}/output?offset=N`.
#[derive(Debug, Serialize)]
pub struct BgProcessOutputResponse {
    pub output: String,
    pub next_offset: u64,
    pub process: BgProcessUiInfo,
}
