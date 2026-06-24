use chisl_common::TimestampMs;
use serde::{Deserialize, Serialize};

/// Row mapping for the `edit_inverses` table.
///
/// One row per edit-type tool call that mutated the working tree. `patch` is
/// the unified diff the tool applied; `inverse_patch` is its inverse (swap
/// add/del lines, negate line numbers) so the working tree can be restored
/// to its pre-call state. `base_hash` is the Git HEAD at the time of capture
/// (placeholder "HEAD" when pre-call hash is unavailable).
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct EditInverseRow {
    pub tool_call_id: String,
    pub conversation_id: String,
    pub file_path: String,
    pub patch: String,
    pub inverse_patch: String,
    pub base_hash: String,
    pub created_at: TimestampMs,
}
