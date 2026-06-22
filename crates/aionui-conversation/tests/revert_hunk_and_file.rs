//! Integration tests for `ConversationService::revert_hunk` and
//! `ConversationService::revert_file`.

use std::sync::Arc;

use aionui_ai_agent::IWorkerTaskManager;
use aionui_common::{AgentKillReason, AppError, TimestampMs};
use aionui_conversation::ConversationService;
use aionui_conversation::skill_resolver::SkillResolver;
use aionui_db::IEditInverseRepository;
use aionui_db::IOpencodeToolSnapshotRepository;
use aionui_db::models::{EditInverseRow, OpencodeToolSnapshotRow};
use aionui_db::{SqliteConversationRepository, SqliteEditInverseRepository, init_database_memory};
use aionui_file::{ISnapshotService, SnapshotService};
use aionui_realtime::EventBroadcaster;
use serde_json::json;
use std::sync::Mutex;
use tempfile::TempDir;

struct TestBroadcaster {
    events: Mutex<Vec<aionui_api_types::WebSocketMessage<serde_json::Value>>>,
}

impl TestBroadcaster {
    fn new() -> Self {
        Self {
            events: Mutex::new(vec![]),
        }
    }
}

impl EventBroadcaster for TestBroadcaster {
    fn broadcast(&self, event: aionui_api_types::WebSocketMessage<serde_json::Value>) {
        self.events.lock().unwrap().push(event);
    }
}

struct NoopTaskManager;

#[async_trait::async_trait]
impl IWorkerTaskManager for NoopTaskManager {
    fn get_task(&self, _: &str) -> Option<aionui_ai_agent::AgentInstance> {
        None
    }
    async fn get_or_build_task(
        &self,
        _: &str,
        _: aionui_ai_agent::types::BuildTaskOptions,
    ) -> Result<aionui_ai_agent::AgentInstance, AppError> {
        Err(AppError::Internal("noop".into()))
    }
    fn kill(&self, _: &str, _: Option<AgentKillReason>) -> Result<(), AppError> {
        Ok(())
    }
    fn kill_and_wait(
        &self,
        _: &str,
        _: Option<AgentKillReason>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        Box::pin(std::future::ready(()))
    }
    fn clear(&self) {}
    fn active_count(&self) -> usize {
        0
    }
    fn collect_idle(&self, _idle_threshold_ms: TimestampMs) -> Vec<String> {
        vec![]
    }
}

struct EmptySkillResolver;

#[async_trait::async_trait]
impl SkillResolver for EmptySkillResolver {
    async fn auto_inject_names(&self) -> Vec<String> {
        Vec::new()
    }
    async fn resolve_skills(&self, _names: &[String]) -> Vec<aionui_extension::ResolvedAgentSkill> {
        Vec::new()
    }
    async fn link_workspace_skills(
        &self,
        _workspace: &std::path::Path,
        _rel_dirs: &[&str],
        _skills: &[aionui_extension::ResolvedAgentSkill],
    ) -> usize {
        0
    }
}

async fn setup() -> (ConversationService, aionui_db::SqlitePool, TempDir) {
    let db = init_database_memory().await.unwrap();
    let pool = db.pool().clone();
    let conversation_repo: Arc<dyn aionui_db::IConversationRepository> =
        Arc::new(SqliteConversationRepository::new(pool.clone()));
    let agent_metadata_repo: Arc<dyn aionui_db::IAgentMetadataRepository> =
        Arc::new(aionui_db::SqliteAgentMetadataRepository::new(pool.clone()));
    let acp_session_repo: Arc<dyn aionui_db::IAcpSessionRepository> =
        Arc::new(aionui_db::SqliteAcpSessionRepository::new(pool.clone()));
    let task_mgr: Arc<dyn IWorkerTaskManager> = Arc::new(NoopTaskManager);
    let workspace = TempDir::new().unwrap();
    let svc = ConversationService::new(
        workspace.path().to_path_buf(),
        Arc::new(TestBroadcaster::new()),
        Arc::new(EmptySkillResolver),
        task_mgr,
        conversation_repo,
        agent_metadata_repo,
        acp_session_repo,
    );
    (svc, pool, workspace)
}

fn wire_edit_inverse_repo(svc: &ConversationService, pool: &aionui_db::SqlitePool) -> Arc<dyn IEditInverseRepository> {
    let repo: Arc<dyn IEditInverseRepository> = Arc::new(SqliteEditInverseRepository::new(pool.clone()));
    svc.with_edit_inverse_repo(repo.clone());
    repo
}

fn wire_snapshot_deps(
    svc: &ConversationService,
    pool: &aionui_db::SqlitePool,
) -> (Arc<SnapshotService>, Arc<dyn IOpencodeToolSnapshotRepository>) {
    let snapshot_service = Arc::new(SnapshotService::new());
    let tool_snapshot_repo: Arc<dyn IOpencodeToolSnapshotRepository> =
        Arc::new(aionui_db::SqliteOpencodeToolSnapshotRepository::new(pool.clone()));
    svc.with_snapshot_service(snapshot_service.clone());
    svc.with_tool_snapshot_repo(tool_snapshot_repo.clone());
    (snapshot_service, tool_snapshot_repo)
}

async fn seed_conversation(svc: &ConversationService) -> String {
    let req: aionui_api_types::CreateConversationRequest = serde_json::from_value(json!({
        "type": "acp",
        "extra": { "workspace": "/tmp/some-ws" }
    }))
    .unwrap();
    let resp = svc.create(OWNER_USER_ID, req).await.unwrap();
    resp.id
}

/// User ID used by `seed_conversation` — matches the owner of all test
/// conversations created through that helper.
const OWNER_USER_ID: &str = "system_default_user";

const TWO_HUNK_PATCH: &str = "--- a/test.txt\n+++ b/test.txt\n@@ -1,2 +1,2 @@\n-line1\n+LINE1\n line2\n@@ -3,2 +3,2 @@\n line3\n-line4\n+LINE4\n";

const SINGLE_HUNK_PATCH: &str = "--- a/test.txt\n+++ b/test.txt\n@@ -1 +1 @@\n-old\n+new\n";

const MODIFIED_CONTENT: &str = "LINE1\nline2\nline3\nLINE4\n";

#[tokio::test]
async fn revert_hunk_reverts_single_hunk_and_reports_remaining() {
    let (svc, pool, workspace) = setup().await;
    wire_edit_inverse_repo(&svc, &pool);
    let conv_id = seed_conversation(&svc).await;

    let file_path = workspace.path().join("test.txt");
    std::fs::write(&file_path, MODIFIED_CONTENT).unwrap();

    let edit_inverse_repo = SqliteEditInverseRepository::new(pool.clone());
    edit_inverse_repo
        .insert(&EditInverseRow {
            tool_call_id: "tc-hunk".to_string(),
            conversation_id: conv_id.clone(),
            file_path: "test.txt".to_string(),
            patch: TWO_HUNK_PATCH.to_string(),
            inverse_patch: String::new(),
            base_hash: "HEAD".to_string(),
            created_at: 1,
        })
        .await
        .unwrap();

    let resp = svc
        .revert_hunk(OWNER_USER_ID, &conv_id, "tc-hunk", 0)
        .await
        .expect("revert_hunk should succeed");
    assert!(resp.success);
    assert_eq!(resp.reverted_hunk_index, 0);
    assert_eq!(
        resp.remaining_hunks, 1,
        "one hunk should remain after reverting one of two"
    );

    let on_disk = std::fs::read_to_string(&file_path).unwrap();
    assert_eq!(
        on_disk, "line1\nline2\nline3\nLINE4\n",
        "only hunk 0 should be reverted"
    );

    // Revert hunk 1 on a fresh file.
    std::fs::write(&file_path, MODIFIED_CONTENT).unwrap();
    let resp = svc
        .revert_hunk(OWNER_USER_ID, &conv_id, "tc-hunk", 1)
        .await
        .expect("revert_hunk should succeed");
    assert_eq!(resp.reverted_hunk_index, 1);
    assert_eq!(resp.remaining_hunks, 1);

    let on_disk = std::fs::read_to_string(&file_path).unwrap();
    assert_eq!(
        on_disk, "LINE1\nline2\nline3\nline4\n",
        "only hunk 1 should be reverted"
    );
}

#[tokio::test]
async fn revert_hunk_single_hunk_deletes_row() {
    let (svc, pool, workspace) = setup().await;
    wire_edit_inverse_repo(&svc, &pool);
    let conv_id = seed_conversation(&svc).await;

    let file_path = workspace.path().join("test.txt");
    std::fs::write(&file_path, "new\n").unwrap();

    let edit_inverse_repo = SqliteEditInverseRepository::new(pool.clone());
    edit_inverse_repo
        .insert(&EditInverseRow {
            tool_call_id: "tc-single-hunk-del".to_string(),
            conversation_id: conv_id.clone(),
            file_path: "test.txt".to_string(),
            patch: SINGLE_HUNK_PATCH.to_string(),
            inverse_patch: String::new(),
            base_hash: "HEAD".to_string(),
            created_at: 1,
        })
        .await
        .unwrap();

    let resp = svc
        .revert_hunk(OWNER_USER_ID, &conv_id, "tc-single-hunk-del", 0)
        .await
        .expect("revert_hunk should succeed");
    assert!(resp.success);
    assert_eq!(resp.remaining_hunks, 0, "no hunks should remain");

    // The edit-inverse row should be deleted when the last hunk is reverted.
    assert!(
        edit_inverse_repo
            .get_by_tool_call_id("tc-single-hunk-del")
            .await
            .unwrap()
            .is_none(),
        "edit-inverse row should be deleted after reverting the only hunk"
    );
}

#[tokio::test]
async fn revert_hunk_multi_hunk_does_not_delete_row() {
    let (svc, pool, workspace) = setup().await;
    wire_edit_inverse_repo(&svc, &pool);
    let conv_id = seed_conversation(&svc).await;

    let file_path = workspace.path().join("test.txt");
    std::fs::write(&file_path, MODIFIED_CONTENT).unwrap();

    let edit_inverse_repo = SqliteEditInverseRepository::new(pool.clone());
    edit_inverse_repo
        .insert(&EditInverseRow {
            tool_call_id: "tc-multi-no-del".to_string(),
            conversation_id: conv_id.clone(),
            file_path: "test.txt".to_string(),
            patch: TWO_HUNK_PATCH.to_string(),
            inverse_patch: String::new(),
            base_hash: "HEAD".to_string(),
            created_at: 1,
        })
        .await
        .unwrap();

    let _resp = svc
        .revert_hunk(OWNER_USER_ID, &conv_id, "tc-multi-no-del", 0)
        .await
        .expect("revert_hunk should succeed");

    // The row should still exist — only one of two hunks was reverted.
    assert!(
        edit_inverse_repo
            .get_by_tool_call_id("tc-multi-no-del")
            .await
            .unwrap()
            .is_some(),
        "edit-inverse row should NOT be deleted when more hunks remain"
    );
}

#[tokio::test]
async fn revert_hunk_out_of_range_returns_not_found() {
    let (svc, pool, _workspace) = setup().await;
    wire_edit_inverse_repo(&svc, &pool);
    let conv_id = seed_conversation(&svc).await;

    let edit_inverse_repo = SqliteEditInverseRepository::new(pool.clone());
    edit_inverse_repo
        .insert(&EditInverseRow {
            tool_call_id: "tc-oob".to_string(),
            conversation_id: conv_id.clone(),
            file_path: "test.txt".to_string(),
            patch: TWO_HUNK_PATCH.to_string(),
            inverse_patch: String::new(),
            base_hash: "HEAD".to_string(),
            created_at: 1,
        })
        .await
        .unwrap();

    let err = svc
        .revert_hunk(OWNER_USER_ID, &conv_id, "tc-oob", 5)
        .await
        .expect_err("out-of-range hunk index should fail");
    assert!(matches!(err, AppError::NotFound(_)), "expected NotFound, got: {err:?}");
    assert!(
        err.to_string().contains("out of range"),
        "expected out of range, got: {err}"
    );
}

#[tokio::test]
async fn revert_hunk_cross_conversation_returns_not_found() {
    let (svc, pool, _workspace) = setup().await;
    wire_edit_inverse_repo(&svc, &pool);

    let conv_a = seed_conversation(&svc).await;
    let conv_b = seed_conversation(&svc).await;

    let edit_inverse_repo = SqliteEditInverseRepository::new(pool.clone());
    edit_inverse_repo
        .insert(&EditInverseRow {
            tool_call_id: "tc-cross".to_string(),
            conversation_id: conv_a.clone(),
            file_path: "test.txt".to_string(),
            patch: TWO_HUNK_PATCH.to_string(),
            inverse_patch: String::new(),
            base_hash: "HEAD".to_string(),
            created_at: 1,
        })
        .await
        .unwrap();

    let err = svc
        .revert_hunk(OWNER_USER_ID, &conv_b, "tc-cross", 0)
        .await
        .expect_err("cross-conversation attempt should fail");
    assert!(matches!(err, AppError::NotFound(_)), "expected NotFound, got: {err:?}");
}

#[tokio::test]
async fn revert_hunk_unknown_tool_call_returns_not_found() {
    let (svc, pool, _workspace) = setup().await;
    wire_edit_inverse_repo(&svc, &pool);
    let conv_id = seed_conversation(&svc).await;

    let err = svc
        .revert_hunk(OWNER_USER_ID, &conv_id, "nonexistent-tc", 0)
        .await
        .expect_err("unknown tool_call_id should fail");
    assert!(matches!(err, AppError::NotFound(_)));
}

#[tokio::test]
async fn revert_hunk_returns_internal_error_when_repo_not_wired() {
    let (svc, _pool, _workspace) = setup().await;
    let conv_id = seed_conversation(&svc).await;

    let err = svc
        .revert_hunk(OWNER_USER_ID, &conv_id, "any", 0)
        .await
        .expect_err("expected error when edit_inverse_repo not wired");
    let msg = err.to_string();
    assert!(
        msg.contains("edit_inverse_repo not configured"),
        "expected not configured error, got: {msg}"
    );
}

#[tokio::test]
async fn revert_hunk_wrong_user_returns_not_found() {
    let (svc, pool, _workspace) = setup().await;
    wire_edit_inverse_repo(&svc, &pool);
    let conv_id = seed_conversation(&svc).await;

    let edit_inverse_repo = SqliteEditInverseRepository::new(pool.clone());
    edit_inverse_repo
        .insert(&EditInverseRow {
            tool_call_id: "tc-wrong-user-hunk".to_string(),
            conversation_id: conv_id.clone(),
            file_path: "test.txt".to_string(),
            patch: TWO_HUNK_PATCH.to_string(),
            inverse_patch: String::new(),
            base_hash: "HEAD".to_string(),
            created_at: 1,
        })
        .await
        .unwrap();

    let err = svc
        .revert_hunk("impostor_user", &conv_id, "tc-wrong-user-hunk", 0)
        .await
        .expect_err("wrong-user request should fail");
    assert!(
        matches!(err, AppError::NotFound(_)),
        "expected NotFound for wrong user, got: {err:?}"
    );
}

#[tokio::test]
async fn revert_file_wrong_user_returns_not_found() {
    let (svc, pool, _workspace) = setup().await;
    wire_edit_inverse_repo(&svc, &pool);
    let conv_id = seed_conversation(&svc).await;

    let edit_inverse_repo = SqliteEditInverseRepository::new(pool.clone());
    edit_inverse_repo
        .insert(&EditInverseRow {
            tool_call_id: "tc-wrong-user-file".to_string(),
            conversation_id: conv_id.clone(),
            file_path: "test.txt".to_string(),
            patch: TWO_HUNK_PATCH.to_string(),
            inverse_patch: String::new(),
            base_hash: "HEAD".to_string(),
            created_at: 1,
        })
        .await
        .unwrap();

    let err = svc
        .revert_file("impostor_user", &conv_id, "tc-wrong-user-file")
        .await
        .expect_err("wrong-user request should fail");
    assert!(
        matches!(err, AppError::NotFound(_)),
        "expected NotFound for wrong user, got: {err:?}"
    );
}

#[tokio::test]
async fn list_edit_inverses_wrong_user_returns_not_found() {
    let (svc, pool, _workspace) = setup().await;
    wire_edit_inverse_repo(&svc, &pool);
    let conv_id = seed_conversation(&svc).await;

    let edit_inverse_repo = SqliteEditInverseRepository::new(pool.clone());
    edit_inverse_repo
        .insert(&EditInverseRow {
            tool_call_id: "tc-wrong-user-list".to_string(),
            conversation_id: conv_id.clone(),
            file_path: "test.txt".to_string(),
            patch: TWO_HUNK_PATCH.to_string(),
            inverse_patch: String::new(),
            base_hash: "HEAD".to_string(),
            created_at: 1,
        })
        .await
        .unwrap();

    let err = svc
        .list_edit_inverses("impostor_user", &conv_id)
        .await
        .expect_err("wrong-user request should fail");
    assert!(
        matches!(err, AppError::NotFound(_)),
        "expected NotFound for wrong user, got: {err:?}"
    );
}

#[tokio::test]
async fn revert_file_returns_internal_error_when_snapshot_not_wired() {
    let (svc, pool, _workspace) = setup().await;
    wire_edit_inverse_repo(&svc, &pool);
    let conv_id = seed_conversation(&svc).await;

    let edit_inverse_repo = SqliteEditInverseRepository::new(pool.clone());
    edit_inverse_repo
        .insert(&EditInverseRow {
            tool_call_id: "tc-no-snap".to_string(),
            conversation_id: conv_id.clone(),
            file_path: "test.txt".to_string(),
            patch: TWO_HUNK_PATCH.to_string(),
            inverse_patch: String::new(),
            base_hash: "HEAD".to_string(),
            created_at: 1,
        })
        .await
        .unwrap();

    let err = svc
        .revert_file(OWNER_USER_ID, &conv_id, "tc-no-snap")
        .await
        .expect_err("expected error when snapshot deps not wired");
    let msg = err.to_string();
    assert!(
        msg.contains("Snapshot service not configured"),
        "expected not configured error, got: {msg}"
    );
}

#[tokio::test]
async fn revert_file_returns_internal_error_when_no_snapshot_row() {
    let (svc, pool, _workspace) = setup().await;
    wire_edit_inverse_repo(&svc, &pool);
    wire_snapshot_deps(&svc, &pool);
    let conv_id = seed_conversation(&svc).await;

    let edit_inverse_repo = SqliteEditInverseRepository::new(pool.clone());
    edit_inverse_repo
        .insert(&EditInverseRow {
            tool_call_id: "tc-missing-snap".to_string(),
            conversation_id: conv_id.clone(),
            file_path: "test.txt".to_string(),
            patch: TWO_HUNK_PATCH.to_string(),
            inverse_patch: String::new(),
            base_hash: "HEAD".to_string(),
            created_at: 1,
        })
        .await
        .unwrap();

    let err = svc
        .revert_file(OWNER_USER_ID, &conv_id, "tc-missing-snap")
        .await
        .expect_err("expected error when no snapshot row exists");
    let msg = err.to_string();
    assert!(
        msg.contains("no snapshot available for file revert"),
        "expected no snapshot error, got: {msg}"
    );
}

#[tokio::test]
async fn revert_file_round_trip_restores_file_via_snapshot() {
    let (svc, pool, workspace) = setup().await;
    wire_edit_inverse_repo(&svc, &pool);
    let (snapshot_service, tool_snapshot_repo) = wire_snapshot_deps(&svc, &pool);
    let conv_id = seed_conversation(&svc).await;

    let root = workspace.path().canonicalize().unwrap();
    let target = root.join("target.txt");

    std::fs::write(&target, "v0-baseline").unwrap();
    snapshot_service.init(root.to_str().unwrap()).await.unwrap();
    snapshot_service
        .commit_tool_snapshot("seed-baseline", &["target.txt".to_string()])
        .await
        .unwrap();

    std::fs::write(&target, "v1-after-tool").unwrap();
    let tool_sha = snapshot_service
        .commit_tool_snapshot("call-rf", &["target.txt".to_string()])
        .await
        .unwrap();

    let edit_inverse_repo = SqliteEditInverseRepository::new(pool.clone());
    edit_inverse_repo
        .insert(&EditInverseRow {
            tool_call_id: "call-rf".to_string(),
            conversation_id: conv_id.clone(),
            file_path: "target.txt".to_string(),
            patch: TWO_HUNK_PATCH.to_string(),
            inverse_patch: String::new(),
            base_hash: "HEAD".to_string(),
            created_at: 1,
        })
        .await
        .unwrap();

    tool_snapshot_repo
        .insert(&OpencodeToolSnapshotRow {
            tool_call_id: "call-rf".to_string(),
            conversation_id: conv_id.clone(),
            commit_sha: tool_sha,
            files_changed_json: r#"["target.txt"]"#.to_string(),
            created_at: 1,
        })
        .await
        .unwrap();

    assert_eq!(std::fs::read_to_string(&target).unwrap(), "v1-after-tool");

    let resp = svc
        .revert_file(OWNER_USER_ID, &conv_id, "call-rf")
        .await
        .expect("revert_file should succeed");
    assert!(resp.success);
    assert_eq!(resp.file_path, "target.txt");

    assert_eq!(
        std::fs::read_to_string(&target).unwrap(),
        "v0-baseline",
        "revert_file should restore the pre-tool content via snapshot"
    );

    // The edit-inverse row should be deleted after successful revert_file.
    let edit_inverse_repo = SqliteEditInverseRepository::new(pool.clone());
    assert!(
        edit_inverse_repo
            .get_by_tool_call_id("call-rf")
            .await
            .unwrap()
            .is_none(),
        "edit-inverse row should be deleted after revert_file"
    );
}

#[tokio::test]
async fn revert_file_cross_conversation_returns_not_found() {
    let (svc, pool, workspace) = setup().await;
    wire_edit_inverse_repo(&svc, &pool);
    let (snapshot_service, tool_snapshot_repo) = wire_snapshot_deps(&svc, &pool);

    let conv_a = seed_conversation(&svc).await;
    let conv_b = seed_conversation(&svc).await;

    let root = workspace.path().canonicalize().unwrap();
    let target = root.join("x.txt");
    std::fs::write(&target, "content").unwrap();
    snapshot_service.init(root.to_str().unwrap()).await.unwrap();
    let sha = snapshot_service
        .commit_tool_snapshot("call-cross-file", &["x.txt".to_string()])
        .await
        .unwrap();

    let edit_inverse_repo = SqliteEditInverseRepository::new(pool.clone());
    edit_inverse_repo
        .insert(&EditInverseRow {
            tool_call_id: "call-cross-file".to_string(),
            conversation_id: conv_a.clone(),
            file_path: "x.txt".to_string(),
            patch: TWO_HUNK_PATCH.to_string(),
            inverse_patch: String::new(),
            base_hash: "HEAD".to_string(),
            created_at: 1,
        })
        .await
        .unwrap();

    tool_snapshot_repo
        .insert(&OpencodeToolSnapshotRow {
            tool_call_id: "call-cross-file".to_string(),
            conversation_id: conv_a.clone(),
            commit_sha: sha,
            files_changed_json: r#"["x.txt"]"#.to_string(),
            created_at: 1,
        })
        .await
        .unwrap();

    // The edit_inverse belongs to conv_a, so requesting from conv_b should fail.
    let err = svc
        .revert_file(OWNER_USER_ID, &conv_b, "call-cross-file")
        .await
        .expect_err("cross-conversation attempt should fail");
    assert!(matches!(err, AppError::NotFound(_)), "expected NotFound, got: {err:?}");
}
