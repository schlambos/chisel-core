use crate::error::DbError;
use crate::models::EditInverseRow;

/// Data access abstraction for the `edit_inverses` ledger.
///
/// One row per edit-type tool call that mutated the working tree. The
/// `tool_call_id` is the primary key, so a second write for the same call
/// is rejected by the unique constraint and surfaced as `DbError::Conflict`.
///
/// Object-safe via `async_trait` to support `Arc<dyn IEditInverseRepository>`.
#[async_trait::async_trait]
pub trait IEditInverseRepository: Send + Sync {
    /// Inserts a new edit-inverse row. Returns `DbError::Conflict` if a row
    /// with the same `tool_call_id` already exists.
    async fn insert(&self, row: &EditInverseRow) -> Result<(), DbError>;

    /// Returns the edit-inverse for a given tool call, or `None` if no row
    /// exists.
    async fn get_by_tool_call_id(&self, tool_call_id: &str) -> Result<Option<EditInverseRow>, DbError>;

    /// Returns all edit-inverses for a conversation, ordered by `created_at`
    /// ASC (oldest first — the natural order for replaying or walking the
    /// ledger).
    async fn list_by_conversation(&self, conversation_id: &str) -> Result<Vec<EditInverseRow>, DbError>;

    /// Deletes the edit-inverse row for a given tool call. Used after a
    /// successful revert to prevent stale rows from reappearing on refresh.
    async fn delete_by_tool_call_id(&self, tool_call_id: &str) -> Result<(), DbError>;
}
