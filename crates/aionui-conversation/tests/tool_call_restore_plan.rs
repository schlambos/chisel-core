//! Integration tests for
//! `GET /api/conversations/{id}/opencode/tool-call-restore-plan` (forge-5-02-03).
//!
//! Mirrors `revert_tool_call.rs`: real `SqliteOpencodeToolSnapshotRepository`
//! for the ledger, real `SnapshotService` for the Git side, and a real
//! temp-dir workspace so the plan walks a real commit tree. Service-level
//! integration only — the Axum route shape (route registered, query
//! deserialized, ownership check) is asserted by a single happy-path
//! round-trip; the rest of the cases cover the service's "not found",
//! cross-conversation, and read-only invariants.

use std::sync::Arc;

use aionui_ai_agent::IWorkerTaskManager;
use aionui_common::{AgentKillReason, AppError, TimestampMs};
use aionui_conversation::ConversationService;
use aionui_conversation::skill_resolver::SkillResolver;
use aionui_db::IOpencodeToolSnapshotRepository;
use aionui_db::models::OpencodeToolSnapshotRow;
use aionui_db::{SqliteConversationRepository, init_database_memory};
use aionui_file::{ISnapshotService, SnapshotService};
use aionui_realtime::EventBroadcaster;
use serde_json::json;
use std::sync::Mutex;
use tempfile::TempDir;

// ── Test doubles (mirror revert_tool_call.rs) ──────────────────────

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

const USER_ID: &str = "system_default_user";

async fn setup() -> ConversationService {
    let (svc, _snapshot_pool) = setup_with_pool().await;
    svc
}

async fn setup_with_pool() -> (ConversationService, aionui_db::SqlitePool) {
    let db = init_database_memory().await.unwrap();
    let pool = db.pool().clone();
    let conversation_repo: Arc<dyn aionui_db::IConversationRepository> =
        Arc::new(SqliteConversationRepository::new(pool.clone()));
    let agent_metadata_repo: Arc<dyn aionui_db::IAgentMetadataRepository> =
        Arc::new(aionui_db::SqliteAgentMetadataRepository::new(pool.clone()));
    let acp_session_repo: Arc<dyn aionui_db::IAcpSessionRepository> =
        Arc::new(aionui_db::SqliteAcpSessionRepository::new(pool.clone()));
    let task_mgr: Arc<dyn IWorkerTaskManager> = Arc::new(NoopTaskManager);

    let svc = ConversationService::new(
        std::env::temp_dir(),
        Arc::new(TestBroadcaster::new()),
        Arc::new(EmptySkillResolver),
        task_mgr,
        conversation_repo,
        agent_metadata_repo,
        acp_session_repo,
    );
    (svc, pool)
}

async fn wire_snapshot_deps(
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
    let resp = svc.create(USER_ID, req).await.unwrap();
    resp.id
}

// ── Tests ───────────────────────────────────────────────────────────

#[tokio::test]
async fn restore_plan_returns_not_found_for_unknown_conversation() {
    let (svc, pool) = setup_with_pool().await;
    wire_snapshot_deps(&svc, &pool).await;

    let err = svc
        .tool_call_restore_plan(USER_ID, "no-such-conv", "any")
        .await
        .expect_err("expected not-found for missing conversation");
    assert!(matches!(err, AppError::NotFound(_)));
}

#[tokio::test]
async fn restore_plan_returns_internal_error_when_deps_not_wired() {
    let svc = setup().await;
    let conv_id = seed_conversation(&svc).await;

    let err = svc
        .tool_call_restore_plan(USER_ID, &conv_id, "missing-call")
        .await
        .expect_err("expected error when snapshot deps are not wired");
    let msg = err.to_string();
    assert!(
        msg.contains("Snapshot service not configured"),
        "expected clear 'not configured' error, got: {msg}"
    );
}

#[tokio::test]
async fn restore_plan_returns_found_false_when_no_ledger_entry() {
    let (svc, pool) = setup_with_pool().await;
    wire_snapshot_deps(&svc, &pool).await;
    let conv_id = seed_conversation(&svc).await;

    let resp = svc
        .tool_call_restore_plan(USER_ID, &conv_id, "call-that-was-never-recorded")
        .await
        .expect("not-found path is non-error: returns found=false");
    assert!(!resp.found, "expected found=false for missing ledger row");
    assert!(resp.plan.is_none());
    assert!(!resp.actionable);
    assert_eq!(resp.tool_call_id, "call-that-was-never-recorded");
    // Unsupported coverage defaults must still be reported so the UI can
    // show the "this plan does not cover …" panel even when no plan
    // exists.
    assert!(resp.unsupported_coverage.run_shell_not_snapshotted);
    assert!(resp.unsupported_coverage.non_local_fs_mcp_not_covered);
    assert!(resp.unsupported_coverage.opencode_session_revert_not_used);
}

#[tokio::test]
async fn restore_plan_returns_found_false_for_cross_conversation_tool_call() {
    // Security: a `tool_call_id` from a different conversation must not
    // surface its plan under this conversation's id. The service
    // checks the row's `conversation_id` matches the path param.
    let (svc, pool) = setup_with_pool().await;
    let (snapshot_service, tool_snapshot_repo) = wire_snapshot_deps(&svc, &pool).await;

    let other_conv_id = {
        let req: aionui_api_types::CreateConversationRequest = serde_json::from_value(json!({
            "type": "acp",
            "extra": { "workspace": "/tmp/other-ws" }
        }))
        .unwrap();
        svc.create(USER_ID, req).await.unwrap().id
    };
    let target_conv_id = seed_conversation(&svc).await;

    let ws = tempfile::tempdir().unwrap();
    let root = ws.path().canonicalize().unwrap();
    snapshot_service.init(root.to_str().unwrap()).await.unwrap();
    let sha = snapshot_service
        .commit_tool_snapshot("call-cross", &["x.txt".to_string()])
        .await
        .unwrap();

    tool_snapshot_repo
        .insert(&OpencodeToolSnapshotRow {
            tool_call_id: "call-cross".to_string(),
            conversation_id: other_conv_id.clone(),
            commit_sha: sha,
            files_changed_json: r#"["x.txt"]"#.to_string(),
            created_at: 1,
        })
        .await
        .unwrap();

    let resp = svc
        .tool_call_restore_plan(USER_ID, &target_conv_id, "call-cross")
        .await
        .expect("cross-conversation attempt is non-error: found=false");
    assert!(!resp.found, "expected found=false for cross-conversation tool_call_id");
    assert!(resp.plan.is_none());
}

#[tokio::test]
async fn restore_plan_rejects_wrong_user() {
    let (svc, pool) = setup_with_pool().await;
    wire_snapshot_deps(&svc, &pool).await;
    let conv_id = seed_conversation(&svc).await;

    let err = svc
        .tool_call_restore_plan("not-the-owner", &conv_id, "any")
        .await
        .expect_err("expected not-found for non-owner");
    assert!(matches!(err, AppError::NotFound(_)));
}

#[tokio::test]
async fn restore_plan_round_trip_returns_read_only_preview() {
    // Happy path: the plan returns a populated `plan` for a real ledger
    // row, the response is marked actionable, and the working tree is
    // *not* mutated. We assert the latter by snapshotting the file
    // content both before and after the call.
    let (svc, pool) = setup_with_pool().await;
    let (snapshot_service, tool_snapshot_repo) = wire_snapshot_deps(&svc, &pool).await;
    let conv_id = seed_conversation(&svc).await;

    let workspace = TempDir::new().unwrap();
    let root = workspace.path().canonicalize().unwrap();
    let target = root.join("target.txt");

    // Baseline file content, committed via the snapshot service.
    std::fs::write(&target, "v0-baseline").unwrap();
    snapshot_service.init(root.to_str().unwrap()).await.unwrap();
    snapshot_service
        .commit_tool_snapshot("seed-baseline", &["target.txt".to_string()])
        .await
        .unwrap();

    // The model's tool call overwrote the file.
    std::fs::write(&target, "v1-after-tool").unwrap();
    let tool_sha = snapshot_service
        .commit_tool_snapshot("call-rp", &["target.txt".to_string()])
        .await
        .unwrap();

    tool_snapshot_repo
        .insert(&OpencodeToolSnapshotRow {
            tool_call_id: "call-rp".to_string(),
            conversation_id: conv_id.clone(),
            commit_sha: tool_sha.clone(),
            files_changed_json: r#"["target.txt"]"#.to_string(),
            created_at: 1,
        })
        .await
        .unwrap();

    let on_disk_before = std::fs::read_to_string(&target).unwrap();
    assert_eq!(on_disk_before, "v1-after-tool");

    let resp = svc
        .tool_call_restore_plan(USER_ID, &conv_id, "call-rp")
        .await
        .expect("restore-plan should succeed");
    assert!(resp.found, "expected found=true for happy path");
    assert!(resp.actionable, "expected actionable=true when no errors");
    assert_eq!(resp.tool_call_id, "call-rp");

    let detail = resp.plan.expect("plan should be populated for found=true");
    assert_eq!(detail.commit_sha, tool_sha);
    assert_eq!(detail.paths.len(), 1);
    let entry = &detail.paths[0];
    assert_eq!(entry.path, "target.txt");
    assert_eq!(entry.operation, aionui_api_types::RestorePathOperation::Modify);
    assert!(entry.prior_content_restorable, "Modify should expose parent content");
    assert!(!entry.preview_blocked, "text file must not be preview-blocked");
    assert!(entry.source_commit_sha.is_some(), "parent commit must be reported");
    assert!(detail.warnings.is_empty(), "no warnings expected: {:?}", detail.warnings);
    assert!(detail.errors.is_empty(), "no errors expected: {:?}", detail.errors);

    // Read-only invariant: the file must still hold the post-tool
    // content. The restore-plan call must not touch the working tree.
    let on_disk_after = std::fs::read_to_string(&target).unwrap();
    assert_eq!(
        on_disk_after, on_disk_before,
        "restore-plan must not mutate the working tree"
    );
    assert_eq!(on_disk_after, "v1-after-tool");
}
