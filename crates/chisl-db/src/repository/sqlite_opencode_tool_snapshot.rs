use sqlx::SqlitePool;

use crate::error::DbError;
use crate::models::OpencodeToolSnapshotRow;
use crate::repository::opencode_tool_snapshot::IOpencodeToolSnapshotRepository;

/// SQLite-backed implementation of [`IOpencodeToolSnapshotRepository`].
#[derive(Clone, Debug)]
pub struct SqliteOpencodeToolSnapshotRepository {
    pool: SqlitePool,
}

impl SqliteOpencodeToolSnapshotRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

/// Heuristic check for SQLite UNIQUE constraint violations on the
/// `tool_call_id` PRIMARY KEY. Mirrors the pattern used in other
/// repositories (e.g. `sqlite_channel`) — we match on the textual message
/// because sqlx does not surface the structured error code on this path.
fn is_tool_call_unique_violation(err: &sqlx::Error) -> bool {
    let msg = err.to_string().to_ascii_lowercase();
    msg.contains("unique constraint failed: opencode_tool_snapshots.tool_call_id")
}

#[async_trait::async_trait]
impl IOpencodeToolSnapshotRepository for SqliteOpencodeToolSnapshotRepository {
    async fn insert(&self, row: &OpencodeToolSnapshotRow) -> Result<(), DbError> {
        sqlx::query(
            "INSERT INTO opencode_tool_snapshots \
                (tool_call_id, conversation_id, commit_sha, files_changed_json, created_at) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&row.tool_call_id)
        .bind(&row.conversation_id)
        .bind(&row.commit_sha)
        .bind(&row.files_changed_json)
        .bind(row.created_at)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            if is_tool_call_unique_violation(&e) {
                DbError::Conflict(format!(
                    "Opencode tool snapshot for tool_call_id '{}' already exists",
                    row.tool_call_id
                ))
            } else {
                DbError::Query(e)
            }
        })?;
        Ok(())
    }

    async fn get_by_tool_call_id(&self, tool_call_id: &str) -> Result<Option<OpencodeToolSnapshotRow>, DbError> {
        let row = sqlx::query_as::<_, OpencodeToolSnapshotRow>(
            "SELECT * FROM opencode_tool_snapshots WHERE tool_call_id = ?",
        )
        .bind(tool_call_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn list_by_conversation(&self, conversation_id: &str) -> Result<Vec<OpencodeToolSnapshotRow>, DbError> {
        let rows = sqlx::query_as::<_, OpencodeToolSnapshotRow>(
            "SELECT * FROM opencode_tool_snapshots \
             WHERE conversation_id = ? \
             ORDER BY created_at ASC",
        )
        .bind(conversation_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::init_database_memory;

    async fn setup() -> (SqliteOpencodeToolSnapshotRepository, crate::Database) {
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

        let repo = SqliteOpencodeToolSnapshotRepository::new(pool.clone());
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

        let row = OpencodeToolSnapshotRow {
            tool_call_id: "tc-1".to_string(),
            conversation_id: "conv-1".to_string(),
            commit_sha: "abcdef1234567890".to_string(),
            files_changed_json: r#"["src/main.rs","README.md"]"#.to_string(),
            created_at: 1_700_000_000_000,
        };
        repo.insert(&row).await.unwrap();

        let fetched = repo.get_by_tool_call_id("tc-1").await.unwrap().unwrap();
        assert_eq!(fetched.tool_call_id, "tc-1");
        assert_eq!(fetched.conversation_id, "conv-1");
        assert_eq!(fetched.commit_sha, "abcdef1234567890");
        assert_eq!(fetched.files_changed_json, r#"["src/main.rs","README.md"]"#);
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

        let row = OpencodeToolSnapshotRow {
            tool_call_id: "tc-dup".to_string(),
            conversation_id: "conv-1".to_string(),
            commit_sha: "sha-1".to_string(),
            files_changed_json: "[]".to_string(),
            created_at: 100,
        };
        repo.insert(&row).await.unwrap();

        let dup = OpencodeToolSnapshotRow {
            tool_call_id: "tc-dup".to_string(),
            conversation_id: "conv-1".to_string(),
            commit_sha: "sha-2".to_string(),
            files_changed_json: "[]".to_string(),
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
            repo.insert(&OpencodeToolSnapshotRow {
                tool_call_id: tc.to_string(),
                conversation_id: conv.to_string(),
                commit_sha: format!("sha-{tc}"),
                files_changed_json: "[]".to_string(),
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
    async fn list_by_conversation_returns_empty_for_unknown() {
        let (repo, _db) = setup().await;
        let rows = repo.list_by_conversation("missing").await.unwrap();
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn insert_referencing_missing_conversation_fails_fk() {
        let (repo, _db) = setup().await;
        let row = OpencodeToolSnapshotRow {
            tool_call_id: "tc-orphan".to_string(),
            conversation_id: "no-such-conv".to_string(),
            commit_sha: "sha".to_string(),
            files_changed_json: "[]".to_string(),
            created_at: 0,
        };
        let err = repo.insert(&row).await.unwrap_err();
        assert!(matches!(err, DbError::Query(_)), "expected FK violation, got {err:?}");
    }
}
