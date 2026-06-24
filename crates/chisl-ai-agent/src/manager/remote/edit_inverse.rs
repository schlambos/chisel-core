//! Inverse-patch computation and storage for OpenCode tool calls.
//!
//! When an edit-type tool (write, edit, multiedit, patch, delete, mv) completes,
//! OpenCode includes the applied unified diff in `metadata.patch`. We compute the
//! inverse of that diff and store both so the working tree can be reverted to its
//! pre-call state.
//!
//! ## Global pool registry
//!
//! The DB pool is a process-wide singleton in the live deployment, identical to
//! the pattern in [`super::local_fs_mcp::snapshot_deps`]. The composition root
//! (e.g. `aionui-app/src/services.rs`) calls [`set_pool`] once at startup;
//! [`capture_edit_inverse`] resolves the pool lazily. While the global is unset,
//! the capture is a no-op — the same degraded-mode behavior used for
//! non-OpenCode backends.
//!
//! ## Thread safety
//!
//! `OnceLock<T>` is the standard library's "set exactly once" primitive.
//! It is `Sync + Send` and the inner `T` is read-only after initialization,
//! so concurrent reads from the dispatch worker pool are safe without further
//! locking.

use std::sync::OnceLock;

use chisl_common::AppError;
use chisl_db::{EditInverseRow, IEditInverseRepository, SqliteEditInverseRepository, SqlitePool};

// ---------------------------------------------------------------------------
// Global pool registry
// ---------------------------------------------------------------------------

static POOL_REGISTRY: OnceLock<SqlitePool> = OnceLock::new();

/// Install the process-wide DB pool. Must be called from the composition
/// root before the first edit-type tool call completes. Subsequent calls are
/// no-ops (the first wins).
pub fn set_pool(pool: SqlitePool) {
    let _ = POOL_REGISTRY.set(pool);
}

/// Read the current pool, or `None` if the composition root hasn't called
/// [`set_pool`] yet.
pub fn get_pool() -> Option<SqlitePool> {
    POOL_REGISTRY.get().cloned()
}

// ---------------------------------------------------------------------------
// Core logic
// ---------------------------------------------------------------------------

/// Compute the inverse of a unified diff.
///
/// Parses the unified diff, inverts each hunk (swap additions/deletions,
/// negate line number ranges), and returns the inverse unified diff string.
pub fn compute_inverse(patch: &str) -> Result<String, AppError> {
    crate::manager::remote::diff_invert::invert_patch(patch)
}

/// Extract the file path from a unified diff header (--- / +++ lines).
pub fn extract_file_path_from_patch(patch: &str) -> Option<String> {
    for line in patch.lines() {
        if line.starts_with("--- ") && !line.starts_with("--- /dev/null") {
            let path = line[4..].trim();
            return Some(path.strip_prefix("a/").unwrap_or(path).to_string());
        }
        if line.starts_with("+++ ") && !line.starts_with("+++ /dev/null") {
            let path = line[4..].trim();
            return Some(path.strip_prefix("b/").unwrap_or(path).to_string());
        }
    }
    None
}

/// High-level capture: compute and persist the inverse for a completed
/// edit-type tool call.
///
/// Called from the agent event loop after an edit tool finishes with a
/// non-empty patch. Resolves the DB pool from the global registry; if the
/// pool is not set, returns silently (degraded mode for non-OpenCode
/// backends).
pub async fn capture_edit_inverse(conversation_id: &str, tool_call_id: &str, patch: &str) -> Result<(), AppError> {
    let pool = match get_pool() {
        Some(pool) => pool,
        None => return Ok(()),
    };

    let file_path = extract_file_path_from_patch(patch).unwrap_or_else(|| "<unknown>".to_string());

    let inverse_patch = compute_inverse(patch)?;

    let now = chisl_common::now_ms();

    let row = EditInverseRow {
        tool_call_id: tool_call_id.to_string(),
        conversation_id: conversation_id.to_string(),
        file_path,
        patch: patch.to_string(),
        inverse_patch,
        base_hash: "HEAD".to_string(),
        created_at: now,
    };

    let repo = SqliteEditInverseRepository::new(pool);
    repo.insert(&row)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to store edit inverse: {e}")))?;

    Ok(())
}
