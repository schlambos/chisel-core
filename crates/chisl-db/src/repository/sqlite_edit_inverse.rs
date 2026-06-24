use sqlx::SqlitePool;

use crate::error::DbError;
use crate::models::EditInverseRow;
use crate::repository::edit_inverse::IEditInverseRepository;

/// SQLite-backed implementation of [`IEditInverseRepository`].
#[derive(Clone, Debug)]
pub struct SqliteEditInverseRepository {
    pool: SqlitePool,
}

impl SqliteEditInverseRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

/// Heuristic check for SQLite UNIQUE constraint violations on the
/// `tool_call_id` PRIMARY KEY. Mirrors the pattern used in other
/// repositories (e.g. `sqlite_opencode_tool_snapshot`).
fn is_tool_call_unique_violation(err: &sqlx::Error) -> bool {
    let msg = err.to_string().to_ascii_lowercase();
    msg.contains("unique constraint failed: edit_inverses.tool_call_id")
}

#[async_trait::async_trait]
impl IEditInverseRepository for SqliteEditInverseRepository {
    async fn insert(&self, row: &EditInverseRow) -> Result<(), DbError> {
        sqlx::query(
            "INSERT INTO edit_inverses \
                (tool_call_id, conversation_id, file_path, patch, inverse_patch, base_hash, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&row.tool_call_id)
        .bind(&row.conversation_id)
        .bind(&row.file_path)
        .bind(&row.patch)
        .bind(&row.inverse_patch)
        .bind(&row.base_hash)
        .bind(row.created_at)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            if is_tool_call_unique_violation(&e) {
                DbError::Conflict(format!(
                    "Edit inverse for tool_call_id '{}' already exists",
                    row.tool_call_id
                ))
            } else {
                DbError::Query(e)
            }
        })?;
        Ok(())
    }

    async fn get_by_tool_call_id(&self, tool_call_id: &str) -> Result<Option<EditInverseRow>, DbError> {
        let row = sqlx::query_as::<_, EditInverseRow>("SELECT * FROM edit_inverses WHERE tool_call_id = ?")
            .bind(tool_call_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row)
    }

    async fn list_by_conversation(&self, conversation_id: &str) -> Result<Vec<EditInverseRow>, DbError> {
        let rows = sqlx::query_as::<_, EditInverseRow>(
            "SELECT * FROM edit_inverses \
             WHERE conversation_id = ? \
             ORDER BY created_at ASC",
        )
        .bind(conversation_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn delete_by_tool_call_id(&self, tool_call_id: &str) -> Result<(), DbError> {
        sqlx::query("DELETE FROM edit_inverses WHERE tool_call_id = ?")
            .bind(tool_call_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::init_database_memory;

    async fn setup() -> (SqliteEditInverseRepository, crate::Database) {
        let db = init_database_memory().await.unwrap();
        let pool = db.pool();

        sqlx::query(
            "INSERT INTO users (id, username, password_hash, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind("test_user")
        .bind("tester")
        .bind("")
        .bind(0i64)
        .bind(0i64)
        .execute(pool)
        .await
        .unwrap();

        let repo = SqliteEditInverseRepository::new(pool.clone());
        (repo, db)
    }

    async fn insert_conversation(pool: &SqlitePool, id: &str) {
        sqlx::query(
            "INSERT INTO conversations \
                (id, user_id, name, type, extra, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id)
        .bind("test_user")
        .bind("test conv")
        .bind("acp")
        .bind("{}")
        .bind(0i64)
        .bind(0i64)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn insert_then_get_round_trips() {
        let (repo, db) = setup().await;
        insert_conversation(db.pool(), "conv-1").await;

        let row = EditInverseRow {
            tool_call_id: "tc-1".to_string(),
            conversation_id: "conv-1".to_string(),
            file_path: "src/main.rs".to_string(),
            patch: "--- a/src/main.rs\n+++ b/src/main.rs\n@@ -1 +1 @@\n-old\n+new\n".to_string(),
            inverse_patch: "--- a/src/main.rs\n+++ b/src/main.rs\n@@ -1 +1 @@\n-new\n+old\n".to_string(),
            base_hash: "HEAD".to_string(),
            created_at: 1_700_000_000_000,
        };
        repo.insert(&row).await.unwrap();

        let fetched = repo.get_by_tool_call_id("tc-1").await.unwrap().unwrap();
        assert_eq!(fetched.tool_call_id, "tc-1");
        assert_eq!(fetched.conversation_id, "conv-1");
        assert_eq!(fetched.file_path, "src/main.rs");
        assert_eq!(
            fetched.patch,
            "--- a/src/main.rs\n+++ b/src/main.rs\n@@ -1 +1 @@\n-old\n+new\n"
        );
        assert_eq!(
            fetched.inverse_patch,
            "--- a/src/main.rs\n+++ b/src/main.rs\n@@ -1 +1 @@\n-new\n+old\n"
        );
        assert_eq!(fetched.base_hash, "HEAD");
        assert_eq!(fetched.created_at, 1_700_000_000_000);
    }

    #[tokio::test]
    async fn get_unknown_tool_call_returns_none() {
        let (repo, _db) = setup().await;
        let result = repo.get_by_tool_call_id("nope").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn duplicate_insert_returns_conflict() {
        let (repo, db) = setup().await;
        insert_conversation(db.pool(), "conv-1").await;

        let row = EditInverseRow {
            tool_call_id: "tc-dup".to_string(),
            conversation_id: "conv-1".to_string(),
            file_path: "a.rs".to_string(),
            patch: "p1".to_string(),
            inverse_patch: "ip1".to_string(),
            base_hash: "HEAD".to_string(),
            created_at: 100,
        };
        repo.insert(&row).await.unwrap();

        let dup = EditInverseRow {
            tool_call_id: "tc-dup".to_string(),
            conversation_id: "conv-1".to_string(),
            file_path: "b.rs".to_string(),
            patch: "p2".to_string(),
            inverse_patch: "ip2".to_string(),
            base_hash: "HEAD".to_string(),
            created_at: 200,
        };
        let err = repo.insert(&dup).await.unwrap_err();
        assert!(matches!(err, DbError::Conflict(_)), "expected Conflict, got {err:?}");
    }

    #[tokio::test]
    async fn list_by_conversation_orders_by_created_at() {
        let (repo, db) = setup().await;
        insert_conversation(db.pool(), "conv-A").await;
        insert_conversation(db.pool(), "conv-B").await;

        for (tc, conv, ts) in [
            ("tc-A-1", "conv-A", 300i64),
            ("tc-A-2", "conv-A", 100),
            ("tc-A-3", "conv-A", 200),
            ("tc-B-1", "conv-B", 50),
        ] {
            repo.insert(&EditInverseRow {
                tool_call_id: tc.to_string(),
                conversation_id: conv.to_string(),
                file_path: "f.rs".to_string(),
                patch: "p".to_string(),
                inverse_patch: "ip".to_string(),
                base_hash: "HEAD".to_string(),
                created_at: ts,
            })
            .await
            .unwrap();
        }

        let conv_a = repo.list_by_conversation("conv-A").await.unwrap();
        assert_eq!(conv_a.len(), 3);
        assert_eq!(
            conv_a.iter().map(|r| r.tool_call_id.as_str()).collect::<Vec<_>>(),
            vec!["tc-A-2", "tc-A-3", "tc-A-1"]
        );

        let conv_b = repo.list_by_conversation("conv-B").await.unwrap();
        assert_eq!(conv_b.len(), 1);
        assert_eq!(conv_b[0].tool_call_id, "tc-B-1");
    }

    #[tokio::test]
    async fn insert_referencing_missing_conversation_fails_fk() {
        let (repo, _db) = setup().await;
        let row = EditInverseRow {
            tool_call_id: "tc-orphan".to_string(),
            conversation_id: "no-such-conv".to_string(),
            file_path: "f.rs".to_string(),
            patch: "p".to_string(),
            inverse_patch: "ip".to_string(),
            base_hash: "HEAD".to_string(),
            created_at: 0,
        };
        let err = repo.insert(&row).await.unwrap_err();
        assert!(matches!(err, DbError::Query(_)), "expected FK violation, got {err:?}");
    }

    #[tokio::test]
    async fn delete_by_tool_call_id_removes_row() {
        let (repo, db) = setup().await;
        insert_conversation(db.pool(), "conv-del").await;

        let row = EditInverseRow {
            tool_call_id: "tc-del".to_string(),
            conversation_id: "conv-del".to_string(),
            file_path: "f.rs".to_string(),
            patch: "p".to_string(),
            inverse_patch: "ip".to_string(),
            base_hash: "HEAD".to_string(),
            created_at: 1,
        };
        repo.insert(&row).await.unwrap();

        // Row exists before delete.
        assert!(repo.get_by_tool_call_id("tc-del").await.unwrap().is_some());

        repo.delete_by_tool_call_id("tc-del").await.unwrap();

        // Row is gone after delete.
        assert!(repo.get_by_tool_call_id("tc-del").await.unwrap().is_none());
    }
}
