//! Integration tests for `POST /api/conversations/{id}/opencode/revert-tool-call`.
//!
//! These tests exercise `ConversationService::revert_tool_call` end-to-end:
//! the real `SqliteOpencodeToolSnapshotRepository` for the ledger, the real
//! `SnapshotService` for the Git side, and a real temp-dir workspace so the
//! narrow-revert checkout operates against a real filesystem.
//!
//! Service-level integration only — the Axum route is covered by the route
//! shape (route registered, body deserialized, ownership check, error
//! mapping) inside `routes.rs`'s own wiring, and a single happy-path
//! round-trip here is enough to assert the per-call ledger write/commit/
//! revert cycle works.

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

// ── Test doubles (mirror conversation_crud.rs) ─────────────────────

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

/// Build a service with real `SqliteConversationRepository` (in-memory),
/// real `SqliteOpencodeToolSnapshotRepository`, and a real `SnapshotService`
/// (no deps wired for snapshot, to test the 500-style error path; callers
/// that need the happy path call `wire_snapshot_deps`).
async fn setup() -> ConversationService {
    let (svc, _snapshot_pool) = setup_with_pool().await;
    svc
}

/// Variant that also returns the underlying `SqlitePool` so tests that need
/// to seed the snapshot repo (the FK on `opencode_tool_snapshots` points at
/// `conversations(id)` in *the same* pool) can use the shared connection.
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
    // Share the service's pool so the FK on `opencode_tool_snapshots.conversation_id`
    // (which references `conversations(id)`) is satisfied.
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
async fn revert_tool_call_returns_internal_error_when_deps_not_wired() {
    // Service with no snapshot deps. Route should return a 500-style
    // "service not configured" error rather than silently doing nothing
    // or panicking — so a partially-wired production deployment surfaces
    // a clear failure mode the operator can act on.
    let svc = setup().await;
    let conv_id = seed_conversation(&svc).await;

    let err = svc
        .revert_tool_call(USER_ID, &conv_id, "missing-call")
        .await
        .expect_err("expected error when snapshot deps are not wired");
    let msg = err.to_string();
    assert!(
        msg.contains("Snapshot service not configured"),
        "expected clear 'not configured' error, got: {msg}"
    );
}

#[tokio::test]
async fn revert_tool_call_returns_not_found_for_unknown_tool_call() {
    let (svc, pool) = setup_with_pool().await;
    wire_snapshot_deps(&svc, &pool).await;
    let conv_id = seed_conversation(&svc).await;

    let err = svc
        .revert_tool_call(USER_ID, &conv_id, "call-that-was-never-recorded")
        .await
        .expect_err("expected not-found for missing tool_call_id");
    assert!(
        err.to_string().contains("No tool-call snapshot"),
        "expected not-found message, got: {err}"
    );
    assert!(matches!(err, AppError::NotFound(_)));
}

#[tokio::test]
async fn revert_tool_call_rejects_mismatched_conversation_id() {
    // Security: a `tool_call_id` from a different conversation must not
    // be usable to revert this conversation's working tree. The service
    // checks the row's `conversation_id` matches the path param.
    let (svc, pool) = setup_with_pool().await;
    let (snapshot_service, tool_snapshot_repo) = wire_snapshot_deps(&svc, &pool).await;

    // Seed two conversations in the service's pool. The seed in
    // `wire_snapshot_deps` uses its own pool, so we must also seed
    // users + conversations in *that* pool for the FK to allow our row.
    let other_conv_id = {
        let req: aionui_api_types::CreateConversationRequest = serde_json::from_value(json!({
            "type": "acp",
            "extra": { "workspace": "/tmp/other-ws" }
        }))
        .unwrap();
        svc.create(USER_ID, req).await.unwrap().id
    };
    let target_conv_id = seed_conversation(&svc).await;

    // Initialize the snapshot service for the other conversation's
    // workspace so we can produce a real commit.
    let ws = tempfile::tempdir().unwrap();
    let root = ws.path().canonicalize().unwrap();
    snapshot_service.init(root.to_str().unwrap()).await.unwrap();
    let sha = snapshot_service
        .commit_tool_snapshot("call-cross", &["x.txt".to_string()])
        .await
        .unwrap();

    // Insert a ledger row for `call-cross` attributing it to OTHER conv.
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

    // Asking the service to revert `call-cross` for the *target*
    // conversation must fail with NotFound (security gate).
    let err = svc
        .revert_tool_call(USER_ID, &target_conv_id, "call-cross")
        .await
        .expect_err("expected cross-conversation attempt to be denied");
    assert!(matches!(err, AppError::NotFound(_)));
    assert!(
        err.to_string().contains("in this conversation"),
        "expected cross-conversation guard message, got: {err}"
    );
}

#[tokio::test]
async fn revert_tool_call_rejects_wrong_user() {
    let (svc, pool) = setup_with_pool().await;
    wire_snapshot_deps(&svc, &pool).await;
    let conv_id = seed_conversation(&svc).await;

    let err = svc
        .revert_tool_call("not-the-owner", &conv_id, "any")
        .await
        .expect_err("expected not-found for non-owner");
    assert!(matches!(err, AppError::NotFound(_)));
}

#[tokio::test]
async fn revert_tool_call_round_trip_restores_file() {
    // Happy path: pre-seed a file, simulate a tool call that overwrote it
    // (the snapshot service captures the post-tool state in the commit,
    // and revert_to_tool_snapshot narrows-checkout from the parent tree
    // which has the pre-tool state).
    let (svc, pool) = setup_with_pool().await;
    let (snapshot_service, tool_snapshot_repo) = wire_snapshot_deps(&svc, &pool).await;
    let conv_id = seed_conversation(&svc).await;

    let workspace = TempDir::new().unwrap();
    let root = workspace.path().canonicalize().unwrap();
    let target = root.join("target.txt");

    // Baseline file content, committed via the snapshot service so HEAD
    // knows about it.
    std::fs::write(&target, "v0-baseline").unwrap();
    snapshot_service.init(root.to_str().unwrap()).await.unwrap();
    let pre_sha = snapshot_service
        .commit_tool_snapshot("seed-baseline", &["target.txt".to_string()])
        .await
        .unwrap();

    // The model's tool call overwrote the file.
    std::fs::write(&target, "v1-after-tool").unwrap();

    // The hook committed a per-tool-call snapshot capturing the post-state.
    let tool_sha = snapshot_service
        .commit_tool_snapshot("call-rt", &["target.txt".to_string()])
        .await
        .unwrap();
    assert_ne!(pre_sha, tool_sha);

    // Record the ledger row in the snapshot repo (the hook did this in
    // the live path; we do it manually here so the test stays focused on
    // the revert side).
    tool_snapshot_repo
        .insert(&OpencodeToolSnapshotRow {
            tool_call_id: "call-rt".to_string(),
            conversation_id: conv_id.clone(),
            commit_sha: tool_sha.clone(),
            files_changed_json: r#"["target.txt"]"#.to_string(),
            created_at: 1,
        })
        .await
        .unwrap();

    // File currently holds the post-tool state.
    assert_eq!(std::fs::read_to_string(&target).unwrap(), "v1-after-tool");

    // Revert.
    let resp = svc
        .revert_tool_call(USER_ID, &conv_id, "call-rt")
        .await
        .expect("revert should succeed");
    assert_eq!(resp.tool_call_id, "call-rt");
    assert_eq!(resp.commit_sha, tool_sha);
    assert_eq!(resp.files_reverted, 1);

    // File now matches the pre-tool state captured in `pre_sha`'s parent.
    assert_eq!(
        std::fs::read_to_string(&target).unwrap(),
        "v0-baseline",
        "revert should restore the pre-tool content"
    );
}
