//! Integration tests for OpenCode local-fs MCP registration round-trip.

use std::sync::Arc;

use aionui_ai_agent::manager::remote::local_fs_mcp::{ShellApproval, ShellApprover};
use aionui_ai_agent::manager::remote::opencode_mcp::{
    MCP_NAME, owns_slot_for_test, start_and_register, sweep_stale_registrations,
};
use async_trait::async_trait;
use serde_json::json;
use tempfile::TempDir;
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

struct AllowShell;
#[async_trait]
impl ShellApprover for AllowShell {
    async fn approve_shell(&self, _command: &str, _cwd: &str) -> ShellApproval {
        ShellApproval::Allow
    }
}

fn set_fast_verify() {
    unsafe { std::env::set_var("AIONUI_LOCAL_FS_MCP_VERIFY_MS", "50") };
}

fn clear_fast_verify() {
    unsafe { std::env::remove_var("AIONUI_LOCAL_FS_MCP_VERIFY_MS") };
}

async fn mount_connect_stubs(opencode: &MockServer) {
    Mock::given(method("POST"))
        .and(path_regex(r"^/mcp/.+/connect$"))
        .respond_with(ResponseTemplate::new(200))
        .mount(opencode)
        .await;
    Mock::given(method("POST"))
        .and(path_regex(r"^/mcp/.+/disconnect$"))
        .respond_with(ResponseTemplate::new(200))
        .mount(opencode)
        .await;
}

async fn mount_register_with_dial_back(opencode: &MockServer, http: reqwest::Client) {
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .respond_with(move |req: &wiremock::Request| {
            if let Ok(body) = serde_json::from_slice::<serde_json::Value>(&req.body) {
                if let Some(url) = body.get("config").and_then(|c| c.get("url")).and_then(|u| u.as_str()) {
                    let http = http.clone();
                    let url = url.to_string();
                    tokio::spawn(async move {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        let _ = http
                            .post(&url)
                            .json(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize"}))
                            .send()
                            .await;
                    });
                }
            }
            ResponseTemplate::new(200).set_body_json(json!({}))
        })
        .expect(1..)
        .mount(opencode)
        .await;
}

#[tokio::test]
async fn tc1_verified_registration_dial_back() {
    set_fast_verify();
    let opencode = MockServer::start().await;
    mount_connect_stubs(&opencode).await;
    let http = reqwest::Client::new();
    mount_register_with_dial_back(&opencode, http.clone()).await;

    let workspace = TempDir::new().unwrap();
    let conv_id = "conv_tc1_verify";
    let server = start_and_register(
        &http,
        &opencode.uri(),
        None,
        conv_id,
        &workspace.path().to_string_lossy(),
        None,
        None,
        None,
    )
    .await
    .expect("verified registration should succeed");

    assert!(server.was_contacted(), "OpenCode dial-back must reach MCP server");
    assert!(
        owns_slot_for_test(&opencode.uri(), conv_id, server.bind_addr().port()),
        "conversation must own the MCP slot"
    );
    server.shutdown().await;
    clear_fast_verify();
}

#[tokio::test]
async fn tc2_unverified_fallback_when_no_dial_back() {
    set_fast_verify();
    let opencode = MockServer::start().await;
    mount_connect_stubs(&opencode).await;
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .expect(2..)
        .mount(&opencode)
        .await;

    let http = reqwest::Client::new();
    let workspace = TempDir::new().unwrap();
    let server = start_and_register(
        &http,
        &opencode.uri(),
        None,
        "conv_tc2_unverified",
        &workspace.path().to_string_lossy(),
        None,
        None,
        None,
    )
    .await
    .expect("unverified fallback should still register best guess");

    assert!(
        !server.was_contacted(),
        "without dial-back, verification should not record contact"
    );
    server.shutdown().await;
    clear_fast_verify();
}

#[tokio::test]
async fn tc3_opencode_rejects_registration() {
    set_fast_verify();
    let opencode = MockServer::start().await;
    mount_connect_stubs(&opencode).await;
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&opencode)
        .await;

    let http = reqwest::Client::new();
    let workspace = TempDir::new().unwrap();
    let result = start_and_register(
        &http,
        &opencode.uri(),
        None,
        "conv_tc3_reject",
        &workspace.path().to_string_lossy(),
        None,
        None,
        None,
    )
    .await;

    assert!(result.is_err(), "OpenCode 500 must fail registration");
    let err = result.err().unwrap();
    assert!(err.contains("rejected"), "expected rejection error: {err}");
    clear_fast_verify();
}

#[tokio::test]
async fn tc4_stale_sweep_disconnects_legacy_slots() {
    let opencode = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/mcp"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "aionui-local-fs-old": { "status": "connected" },
            "github": { "status": "connected" }
        })))
        .expect(1)
        .mount(&opencode)
        .await;
    Mock::given(method("POST"))
        .and(path("/mcp/aionui-local-fs-old/disconnect"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&opencode)
        .await;

    let http = reqwest::Client::new();
    sweep_stale_registrations(&http, &opencode.uri(), None).await;
    sweep_stale_registrations(&http, &opencode.uri(), None).await;
}

#[tokio::test]
async fn tc5_run_shell_end_to_end_after_registration() {
    set_fast_verify();
    let opencode = MockServer::start().await;
    mount_connect_stubs(&opencode).await;
    let http = reqwest::Client::new();
    mount_register_with_dial_back(&opencode, http.clone()).await;

    let workspace = TempDir::new().unwrap();
    let approver: Arc<dyn ShellApprover> = Arc::new(AllowShell);
    let server = start_and_register(
        &http,
        &opencode.uri(),
        None,
        "conv_tc5_shell",
        &workspace.path().to_string_lossy(),
        Some(approver),
        None,
        None,
    )
    .await
    .expect("registration for shell e2e");

    let url = server.local_url();
    let token = server.auth_token();
    let resp = http
        .post(&url)
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({
            "jsonrpc": "2.0",
            "id": "shell-1",
            "method": "tools/call",
            "params": {
                "name": "run_shell",
                "arguments": { "command": "echo mcp_ok" }
            }
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let text = body["result"]["content"][0]["text"].as_str().unwrap_or("");
    assert!(text.contains("mcp_ok"), "run_shell output missing marker: {text}");
    assert_eq!(MCP_NAME, "aionui-local-fs");

    server.shutdown().await;
    clear_fast_verify();
}
