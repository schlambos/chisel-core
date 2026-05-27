//! Integration tests for Remote (OpenCode) session persistence & resume.
//!
//! Covers the rebuild path: a persisted OpenCode session id (carried in
//! `conversation.extra.sessionKey`) is validated against the server on
//! `connect()` and either reused (resume server-side context) or discarded
//! (stale id → next send starts a fresh session). Uses a mock OpenCode server
//! so the assertions don't depend on a live LAN server.

use std::sync::Arc;

use aionui_ai_agent::manager::remote::{RemoteAgentConfig, RemoteAgentManager};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn opencode_config(url: String) -> RemoteAgentConfig {
    RemoteAgentConfig {
        remote_agent_id: "ra_resume".to_string(),
        protocol: "opencode".to_string(),
        url,
        auth_type: "none".to_string(),
        auth_token: None,
        allow_insecure: false,
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
