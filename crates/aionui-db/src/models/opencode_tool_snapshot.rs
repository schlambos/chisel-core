use aionui_common::TimestampMs;
use serde::{Deserialize, Serialize};

/// Row mapping for the `opencode_tool_snapshots` table.
///
/// One row per tool call that mutated the working tree. `commit_sha` is the
/// post-mutation Git HEAD captured by the snapshot service; `files_changed_json`
/// is a JSON array of paths touched by the tool (empty array when the tool
/// reported no diff).
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct OpencodeToolSnapshotRow {
    pub tool_call_id: String,
    pub conversation_id: String,
    pub commit_sha: String,
    pub files_changed_json: String,
    pub created_at: TimestampMs,
}
