//! Process-global registry for the snapshot deps the
//! `revert-tool-call` route needs (Task 14.3). Mirrors the
//! `aionui-ai-agent::SnapshotDepsRegistry` so the composition root can
//! wire (or skip) wiring without changing `ConversationService`'s
//! struct shape.
//!
//! ## Two-tier resolution
//!
//! 1. The per-service `with_snapshot_service` / `with_tool_snapshot_repo`
//!    setters take precedence — tests that want a service-local
//!    override can set it after construction.
//! 2. When the per-service slot is empty, the service falls back to
//!    [`get`], which returns whatever the composition root installed
//!    via [`set`] at startup. This is the production path.
//! 3. When both are `None`, the `revert_tool_call` route returns a
//!    500-style "service not configured" error so a partially-wired
//!    deployment surfaces a clear failure.

use std::sync::{Arc, OnceLock};

use chisl_db::IOpencodeToolSnapshotRepository;
use chisl_file::SnapshotService;

#[derive(Clone)]
pub struct SnapshotDeps {
    pub snapshot_service: Arc<SnapshotService>,
    pub tool_snapshot_repo: Arc<dyn IOpencodeToolSnapshotRepository>,
}

static REGISTRY: OnceLock<SnapshotDeps> = OnceLock::new();

/// Install the process-wide deps. Composition root (aionui-app) should
/// call this once at startup. First-wins semantics — see
/// `aionui-ai-agent::snapshot_deps` for the rationale.
pub fn set(deps: SnapshotDeps) {
    let _ = REGISTRY.set(deps);
}

/// Read the current process-wide deps, or `None` if the composition
/// root hasn't called [`set`] yet.
pub fn get() -> Option<SnapshotDeps> {
    REGISTRY.get().cloned()
}
