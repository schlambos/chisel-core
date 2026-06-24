//! Integration tests for Remote (OpenCode) session persistence & resume.
//!
//! Covers the rebuild path: a persisted OpenCode session id (carried in
//! `conversation.extra.sessionKey`) is validated against the server on
//! `connect()` and either reused (resume server-side context) or discarded
//! (stale id → next send starts a fresh session). Uses a mock OpenCode server
//! so the assertions don't depend on a live LAN server.

use std::sync::Arc;

use chisl_ai_agent::manager::remote::{RemoteAgentConfig, RemoteAgentManager};
use tempfile::TempDir;
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn opencode_config(url: String) -> RemoteAgentConfig {
    RemoteAgentConfig {
        remote_agent_id: "ra_resume".to_string(),
        protocol: "opencode".to_string(),
        url,
        auth_type: "none".to_string(),
        auth_token: None,
        allow_insecure: false,
        tool_host: "local".to_string(),
    }
}

/// Minimal mocks for `connect_opencode`'s synchronous path: health check and
/// the eager command-catalog fetch. The background SSE reader (`GET /event`)
/// is intentionally left unmocked — it runs in a spawned task and does not
/// affect the resume assertions.
async fn mount_connect_basics(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/global/health"))
        .respond_with(ResponseTemplate::new(200))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/command"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
        .mount(server)
        .await;
}

#[tokio::test]
async fn resume_keeps_valid_persisted_session() {
    let server = MockServer::start().await;
    mount_connect_basics(&server).await;

    // The persisted session still exists server-side → validation succeeds.
    Mock::given(method("GET"))
        .and(path("/session/ses_valid"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({ "id": "ses_valid" })))
        .mount(&server)
        .await;
    // A rebuild that resumes must NOT create a new session.
    Mock::given(method("POST"))
        .and(path("/session"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;

    let manager = RemoteAgentManager::new(
        "conv_resume_valid".to_string(),
        "/tmp/ws".to_string(),
        opencode_config(server.uri()),
        Some("ses_valid".to_string()),
    )
    .await
    .unwrap();
    let arc = Arc::new(manager);
    arc.connect().await.unwrap();

    // Validated id is retained, so the next send reuses it (no new session).
    assert_eq!(arc.get_session_key().as_deref(), Some("ses_valid"));
    // `.expect(0)` on POST /session is asserted when `server` drops.
}

#[tokio::test]
async fn resume_registers_local_fs_mcp_for_valid_session() {
    // Regression: a resumed OpenCode session must re-register the client's
    // local fs MCP. The previous process's `LocalFsMcpServer` is gone (its
    // loopback/LAN port was process-scoped), but the resumed server-side
    // session still holds the stale registration. Without re-registering on
    // connect, the model's first `mcp__aionui-local-fs-*` tool call dials a
    // dead URL and surfaces "Unable to connect. Is the computer able to
    // access the url?" — exactly the production failure this test guards.
    let server = MockServer::start().await;
    mount_connect_basics(&server).await;

    // Keep the reachability verify loop fast: the mock never dials our MCP
    // server back, so every candidate "fails" verification after this
    // window before falling through to the best-guess registration.
    // SAFETY: process-global, but every test sets the same value.
    unsafe { std::env::set_var("AIONUI_LOCAL_FS_MCP_VERIFY_MS", "30") };

    Mock::given(method("GET"))
        .and(path("/session/ses_valid_mcp"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({ "id": "ses_valid_mcp" })))
        .mount(&server)
        .await;
    // The regression invariant: resume MUST register the client fs MCP
    // (mirroring the new-session path). With reachability verification the
    // exact count is an implementation detail — what matters is that
    // registration happens at all on resume.
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1..)
        .mount(&server)
        .await;
    // Verification's connect/disconnect probes — accept and ignore.
    Mock::given(method("POST"))
        .and(path_regex(r"^/mcp/.+/connect$"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path_regex(r"^/mcp/.+/disconnect$"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    // A rebuild that resumes must NOT create a new server-side session.
    Mock::given(method("POST"))
        .and(path("/session"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;

    // `LocalFsMcpServer::start` requires the workspace to be an existing
    // directory, so use a real tempdir rather than a fake path string —
    // otherwise registration would silently fall through to the
    // `ensure_local_fs_mcp` warn-and-return branch and `POST /mcp` would
    // never fire, masking the regression.
    let workspace = TempDir::new().unwrap();

    let manager = RemoteAgentManager::new(
        "conv_resume_register_mcp".to_string(),
        workspace.path().to_string_lossy().into_owned(),
        opencode_config(server.uri()),
        Some("ses_valid_mcp".to_string()),
    )
    .await
    .unwrap();
    let arc = Arc::new(manager);
    arc.connect().await.unwrap();

    assert_eq!(arc.get_session_key().as_deref(), Some("ses_valid_mcp"));
    // `.expect(1)` on POST /mcp is asserted when `server` drops.
}

#[tokio::test]
async fn resume_skips_mcp_registration_for_stale_session() {
    // The mirror of the regression above: when the persisted session id is
    // stale (server-side 404), the client should NOT register a fs MCP yet.
    // Registration is bound to a live session — registering against a dead
    // id leaks a port and confuses later teardown. The fresh session created
    // on the next send (`opencode_create_session`) handles registration.
    let server = MockServer::start().await;
    mount_connect_basics(&server).await;

    Mock::given(method("GET"))
        .and(path("/session/ses_stale_mcp"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(0)
        .mount(&server)
        .await;

    let workspace = TempDir::new().unwrap();
    let manager = RemoteAgentManager::new(
        "conv_resume_stale_mcp".to_string(),
        workspace.path().to_string_lossy().into_owned(),
        opencode_config(server.uri()),
        Some("ses_stale_mcp".to_string()),
    )
    .await
    .unwrap();
    let arc = Arc::new(manager);
    arc.connect().await.unwrap();

    assert_eq!(arc.get_session_key(), None);
    // `.expect(0)` on POST /mcp is asserted when `server` drops.
}

#[tokio::test]
async fn resume_discards_stale_persisted_session() {
    let server = MockServer::start().await;
    mount_connect_basics(&server).await;

    // The persisted session was deleted/expired server-side → 404.
    Mock::given(method("GET"))
        .and(path("/session/ses_stale"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let manager = RemoteAgentManager::new(
        "conv_resume_stale".to_string(),
        "/tmp/ws".to_string(),
        opencode_config(server.uri()),
        Some("ses_stale".to_string()),
    )
    .await
    .unwrap();
    let arc = Arc::new(manager);
    arc.connect().await.unwrap();

    // Stale id discarded up front — a fresh session is created on the next
    // send (rather than a failed first `prompt_async` against a dead id).
    assert_eq!(arc.get_session_key(), None);
}
