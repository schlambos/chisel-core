use crate::error::DbError;
use crate::models::OpencodeToolSnapshotRow;

/// Data access abstraction for the `opencode_tool_snapshots` ledger.
///
/// One row per tool call that mutated the working tree. The `tool_call_id`
/// is the primary key, so a second write for the same call is rejected by
/// the unique constraint and surfaced as `DbError::Conflict`.
///
/// Object-safe via `async_trait` to support `Arc<dyn IOpencodeToolSnapshotRepository>`.
#[async_trait::async_trait]
pub trait IOpencodeToolSnapshotRepository: Send + Sync {
    /// Inserts a new snapshot row. Returns `DbError::Conflict` if a row with
    /// the same `tool_call_id` already exists.
    async fn insert(&self, row: &OpencodeToolSnapshotRow) -> Result<(), DbError>;

    /// Returns the snapshot for a given tool call, or `None` if no row exists.
    async fn get_by_tool_call_id(&self, tool_call_id: &str) -> Result<Option<OpencodeToolSnapshotRow>, DbError>;

    /// Returns all snapshots for a conversation, ordered by `created_at` ASC
    /// (oldest first — the natural order for replaying or walking the ledger).
    async fn list_by_conversation(&self, conversation_id: &str) -> Result<Vec<OpencodeToolSnapshotRow>, DbError>;
}
