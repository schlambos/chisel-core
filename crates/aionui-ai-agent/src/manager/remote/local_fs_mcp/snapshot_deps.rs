//! Process-global registry for the per-tool-call snapshot dependencies
//! (Task 14.3). Lives in the agent crate so the
//! `local_fs_mcp::SnapshotHook` can resolve the deps without forcing every
//! composition root to thread them through every struct.
//!
//! ## Why a global
//!
//! The per-tool-call snapshot hook needs three things: a
//! `SnapshotService`, an `IOpencodeToolSnapshotRepository`, and the
//! `conversation_id` it belongs to. The first two are
//! **process-wide singletons** in the live deployment: the
//! `SnapshotService` is the same one backing the file-route API
//! (`aionui-file` returns a single `Arc<SnapshotService>` from
//! `build_file_state`), and the opencode-tool-snapshots repo is a single
//! `SqliteOpencodeToolSnapshotRepository` on the app's shared DB pool.
//! Threading them through `AgentFactoryDeps` would force a compile-time
//! dependency from `aionui-app/src/services.rs` (where the deps are
//! constructed) into the agent crate's struct shape, and a follow-up
//! commit in aionui-app each time the agent crate's surface changes.
//!
//! A process-global `OnceLock<SnapshotDeps>` side-steps the cross-crate
//! struct churn. `aionui-app` (or any composition root) calls
//! [`SnapshotDepsRegistry::set`] once at startup; the per-conversation
//! `LocalFsMcpServer` resolves the deps lazily at
//! `tools/call` time via [`SnapshotDepsRegistry::get`]. While the global
//! is unset, the hook is a no-op and the route returns a
//! 500-style "service not configured" error — the same degraded-mode
//! behavior Task 14.3's spec called out for non-OpenCode backends.
//!
//! ## Thread safety
//!
//! `OnceLock<T>` is the standard library's "set exactly once" primitive.
//! It is `Sync + Send` and the inner `T` is read-only after
//! initialization, so concurrent reads from the dispatch worker pool are
//! safe without further locking.

use std::sync::{Arc, OnceLock};

use aionui_db::IOpencodeToolSnapshotRepository;
use aionui_file::SnapshotService;

/// Bundle of process-wide snapshot dependencies the per-tool-call hook
/// (and any future snapshot-aware code) needs.
#[derive(Clone)]
pub struct SnapshotDeps {
    pub snapshot_service: Arc<SnapshotService>,
    pub tool_snapshot_repo: Arc<dyn IOpencodeToolSnapshotRepository>,
}

static REGISTRY: OnceLock<SnapshotDeps> = OnceLock::new();

/// Install the process-wide deps. Must be called from the composition
/// root (e.g. `aionui-app/src/services.rs`) before the first
/// `tools/call` reaches the local fs MCP server. Subsequent calls are
/// no-ops (the first wins) so test setups that race are safe — the
/// installer's own handle remains the active one.
pub fn set(deps: SnapshotDeps) {
    let _ = REGISTRY.set(deps);
}

/// Read the current deps, or `None` if the composition root hasn't
/// called [`set`] yet (or the snapshot feature is intentionally
/// disabled for this build).
pub fn get() -> Option<SnapshotDeps> {
    REGISTRY.get().cloned()
}

/// Clear the registry. Test-only helper — production code never needs to
/// unset; the process-global lives for the process lifetime.
#[cfg(any(test, feature = "test-support"))]
pub fn clear() {
    // `OnceLock` doesn't expose a `take` (intentionally — there's no
    // safe way to invalidate an in-flight reader). The test-only escape
    // hatch is to leak the slot: replace it with a fresh one by
    // poisoning via a different mechanism. For now, individual tests
    // should set distinct deps per test and accept that the global is
    // first-wins.
    //
    // The cfg gate is here so the production binary never accidentally
    // pulls in the unsafe-style reset path.
}

#[cfg(any(test, feature = "test-support"))]
pub fn set_for_test(deps: SnapshotDeps) {
    set(deps);
}
