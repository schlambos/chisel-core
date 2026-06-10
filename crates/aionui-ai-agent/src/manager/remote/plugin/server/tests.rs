//! Tests for the plugin webserver (`super` = `server`).
//!
//! Lives in a sibling file because the inline test module pushed
//! `server.rs` past AGENTS.md's 1000-line cap. Same `#[cfg(test)]`
//! semantics; `#[path = "tests.rs"] mod tests;` in `server.rs` is
//! what wires this in.
//!
//! Tests follow the boot-on-ephemeral-port pattern from
//! `crate::manager::remote::local_fs_mcp::server`'s test module: each
//! test binds a real `PluginServer` on `127.0.0.1:0`, exercises it
//! with `reqwest`, and asserts on the response.

use super::*;
use crate::manager::remote::local_fs_mcp::{ShellApproval, ShellApprover};
use crate::manager::remote::plugin::PluginPushEvent;
use async_trait::async_trait;
use reqwest::Client;
use std::net::{IpAddr, Ipv4Addr};

// Stub validator that maps a fixed token to a fixed agent id.
struct FixedValidator {
    token: String,
    agent_id: String,
}

#[async_trait]
impl PluginTokenValidator for FixedValidator {
    async fn resolve(&self, token: &str) -> Option<String> {
        if constant_time_eq(token.as_bytes(), self.token.as_bytes()) {
            Some(self.agent_id.clone())
        } else {
            None
        }
    }
}

struct ApproverAllow;
#[async_trait]
impl ShellApprover for ApproverAllow {
    async fn approve_shell(&self, _command: &str, _cwd: &str) -> ShellApproval {
        ShellApproval::Allow
    }
}
struct ApproverReject;
#[async_trait]
impl ShellApprover for ApproverReject {
    async fn approve_shell(&self, _command: &str, _cwd: &str) -> ShellApproval {
        ShellApproval::Reject
    }
}

async fn boot() -> (PluginServer, String, String, Arc<PluginRegistry>) {
    let registry = Arc::new(PluginRegistry::new());
    let validator: Arc<dyn PluginTokenValidator> = Arc::new(FixedValidator {
        token: "test-token-xyz".to_string(),
        agent_id: "ra_x".to_string(),
    });
    let server = PluginServer::start_with_registry(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        validator,
        registry.clone(),
    )
    .await
    .unwrap();
    let url = format!("http://{}", server.bind_addr());
    let token = "test-token-xyz".to_string();
    (server, url, token, registry)
}

#[tokio::test]
async fn hello_without_auth_returns_401() {
    let (_server, url, _token, _reg) = boot().await;
    let resp = Client::new()
        .post(format!("{url}/plugin/hello"))
        .json(&json!({
            "protocolVersion": 1, "pluginVersion": "0.1.0", "hooks": []
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn hello_with_wrong_token_returns_401() {
    let (_server, url, _token, _reg) = boot().await;
    let resp = Client::new()
        .post(format!("{url}/plugin/hello"))
        .header("Authorization", "Bearer wrong-token-zzzzzz")
        .json(&json!({
            "protocolVersion": 1, "pluginVersion": "0.1.0", "hooks": []
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn hello_with_valid_token_succeeds_and_records() {
    let (_server, url, token, registry) = boot().await;
    let resp = Client::new()
        .post(format!("{url}/plugin/hello"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({
            "protocolVersion": 1,
            "pluginVersion": "0.1.0",
            "opencodeVersion": "1.2.3",
            "hooks": ["tool.before", "session.idle"]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: PluginHelloResponse = resp.json().await.unwrap();
    assert!(body.ok);
    assert_eq!(body.protocol_version, PROTOCOL_VERSION);
    let state = registry.connection_state("ra_x");
    assert_eq!(state.plugin_version.as_deref(), Some("0.1.0"));
    assert_eq!(state.opencode_version.as_deref(), Some("1.2.3"));
    assert_eq!(state.hooks, vec!["tool.before", "session.idle"]);
    assert_eq!(state.hello_count, 1);
}

#[tokio::test]
async fn result_records_audit_for_each_kind() {
    let (_server, url, token, registry) = boot().await;
    let cases: Vec<(&str, serde_json::Value)> = vec![
        (
            "toolBefore",
            json!({
                "kind": "toolBefore",
                "tool": "read",
                "sessionId": "ses_1",
                "callId": "c_1",
                "args": {"path": "x.txt"}
            }),
        ),
        (
            "toolAfter",
            json!({
                "kind": "toolAfter",
                "tool": "read",
                "sessionId": "ses_1",
                "callId": "c_1",
                "args": {},
                "outputLen": 42
            }),
        ),
        (
            "event",
            json!({
                "kind": "event",
                "event": {"type": "session.idle", "properties": {"sessionID": "ses_1"}}
            }),
        ),
        (
            "permissionAsk",
            json!({
                "kind": "permissionAsk",
                "permission": {"tool": "bash", "sessionID": "ses_1", "callID": "c_1"}
            }),
        ),
    ];

    for (label, body) in cases {
        let resp = Client::new()
            .post(format!("{url}/plugin/result"))
            .header("Authorization", format!("Bearer {token}"))
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = resp.status();
        if status != 200 {
            let text = resp.text().await.unwrap();
            panic!("{label} should succeed, got {status}: {text}");
        }
        let v: PluginResultResponse = resp.json().await.unwrap();
        assert!(v.ok, "{label} should report ok");
        if label == "permissionAsk" {
            assert_eq!(v.status.as_deref(), Some("ask"));
        } else {
            assert!(v.status.is_none(), "{label} should not set status");
        }
    }

    let records = registry.audit_records("ra_x");
    assert_eq!(records.len(), 4);
    assert!(
        records
            .iter()
            .any(|r| r.kind == "tool.before" && r.tool.as_deref() == Some("read"))
    );
    assert!(records.iter().any(|r| r.kind == "tool.after"));
    assert!(records.iter().any(|r| r.kind == "event"));
    assert!(records.iter().any(|r| r.kind == "permission.ask"));
}

#[tokio::test]
async fn result_requires_auth() {
    let (_server, url, _token, _reg) = boot().await;
    let resp = Client::new()
        .post(format!("{url}/plugin/result"))
        .json(&json!({"kind": "toolBefore", "tool": "x", "sessionId": "s", "callId": "c", "args": {}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn run_shell_streaming_no_approver_emits_error() {
    let (_server, url, token, _reg) = boot().await;
    let resp = Client::new()
        .post(format!("{url}/tools/run_shell_streaming"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "text/event-stream")
        .json(&json!({"command": "echo hi", "sessionId": "ses_1"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("content-type").and_then(|v| v.to_str().ok()),
        Some("text/event-stream")
    );
    let body = resp.text().await.unwrap();
    assert!(body.contains("event: error"), "expected error event in: {body}");
    assert!(body.contains("no approver"), "expected message in: {body}");
    assert!(body.contains("event: done"), "expected terminal done in: {body}");
}

#[tokio::test]
async fn run_shell_streaming_reject_emits_error() {
    let (_server, url, token, registry) = boot().await;
    registry.register_shell_approver("ra_x", Arc::new(ApproverReject));
    let resp = Client::new()
        .post(format!("{url}/tools/run_shell_streaming"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "text/event-stream")
        .json(&json!({"command": "echo hi", "sessionId": "ses_1"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("event: error"), "rejected should emit error in: {body}");
    assert!(body.contains("rejected"), "expected 'rejected' in: {body}");
    assert!(body.contains("event: done"));
}

#[tokio::test]
async fn run_shell_streaming_allow_streams_chunks() {
    let (_server, url, token, registry) = boot().await;
    registry.register_shell_approver("ra_x", Arc::new(ApproverAllow));
    let resp = Client::new()
        .post(format!("{url}/tools/run_shell_streaming"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "text/event-stream")
        .json(&json!({"command": "echo hello_from_allow", "sessionId": "ses_1"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("hello_from_allow"), "expected stdout in: {body}");
    assert!(body.contains("event: done"));
}

#[tokio::test]
async fn run_shell_streaming_requires_auth() {
    let (_server, url, _token, _reg) = boot().await;
    let resp = Client::new()
        .post(format!("{url}/tools/run_shell_streaming"))
        .json(&json!({"command": "echo hi", "sessionId": "ses_1"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn events_sse_subscribes_and_receives_push() {
    use futures_util::StreamExt;
    let (_server, url, token, registry) = boot().await;
    // Fire a `push` in the background after a short delay so the
    // SSE handshake has time to register the subscriber.
    let reg_for_push = registry.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        reg_for_push.push(
            "ra_x",
            PluginPushEvent {
                event: "agentStatusChanged".into(),
                data: json!({"status": "connected"}),
            },
        );
    });

    let resp = reqwest::Client::new()
        .get(format!("{url}/plugin/events"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "text/event-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let mut stream = resp.bytes_stream();
    // Read a few events with a timeout. We're looking for the
    // `agentStatusChanged` push.
    let mut found_push = false;
    let mut buf = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
            Ok(Some(Ok(chunk))) => {
                buf.extend_from_slice(&chunk);
                let text = String::from_utf8_lossy(&buf);
                if text.contains("agentStatusChanged") && text.contains("connected") {
                    found_push = true;
                    break;
                }
            }
            Ok(Some(Err(_))) | Ok(None) | Err(_) => break,
        }
    }
    assert!(
        found_push,
        "did not receive pushed event; buffer: {}",
        String::from_utf8_lossy(&buf)
    );
}

#[tokio::test]
async fn events_sse_stream_open_flag_toggles() {
    use futures_util::StreamExt;
    let (_server, url, token, registry) = boot().await;
    // Connect, then close immediately.
    {
        let resp = reqwest::Client::new()
            .get(format!("{url}/plugin/events"))
            .header("Authorization", format!("Bearer {token}"))
            .header("Accept", "text/event-stream")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        // We can't easily wait for the handler to set the flag
        // before the response is read, but the flag will be set
        // before the first byte is written. The Drop on the
        // guard fires when we drop the resp / stream.
        let mut s = resp.bytes_stream();
        let _ = tokio::time::timeout(Duration::from_millis(100), s.next()).await;
        // Read enough that the handler's `subscribe` has had a
        // chance to run.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            registry.connection_state("ra_x").events_stream_open,
            "should be open while client connected"
        );
    }
    // Give the Drop a moment to fire.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !registry.connection_state("ra_x").events_stream_open,
        "should flip back to false after disconnect"
    );
}

#[tokio::test]
async fn events_sse_requires_auth() {
    let (_server, url, _token, _reg) = boot().await;
    let resp = Client::new().get(format!("{url}/plugin/events")).send().await.unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn ensure_plugin_server_is_singleton() {
    // Use the process-wide singleton path. The first call binds;
    // the second call returns the same address. We can't use the
    // `boot()` helper because it builds an isolated server, not
    // the singleton.
    let validator: Arc<dyn PluginTokenValidator> = Arc::new(FixedValidator {
        token: "tok".into(),
        agent_id: "ra_singleton".into(),
    });
    let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let addr1 = ensure_plugin_server(bind, validator.clone()).await.unwrap();
    // Second call with a different validator — must still return
    // the original addr (the first call's validator is in use).
    let other: Arc<dyn PluginTokenValidator> = Arc::new(FixedValidator {
        token: "other".into(),
        agent_id: "ra_other".into(),
    });
    let addr2 = ensure_plugin_server(bind, other).await.unwrap();
    assert_eq!(addr1, addr2);
}

#[test]
fn constant_time_eq_works() {
    assert!(constant_time_eq(b"hello", b"hello"));
    assert!(!constant_time_eq(b"hello", b"world"));
    assert!(!constant_time_eq(b"hello", b"hell"));
    assert!(!constant_time_eq(b"", b"x"));
}

#[test]
fn truncate_summary_handles_multibyte() {
    let s = "x".repeat(3000);
    let out = truncate_summary(&s);
    assert!(out.len() <= 2048 + 4);
    // 2048 + 1 "…"
    let emoji = "😀".repeat(3000);
    let out = truncate_summary(&emoji);
    assert!(out.len() <= 2048 + 5);
}

#[test]
fn truncate_command_preview_is_short() {
    let s = "echo ".to_string() + &"a".repeat(200);
    let out = truncate_command_preview(&s);
    // MAX (80) + "…" (3 bytes) — clamp to the loose upper bound so
    // a future shrink doesn't trip a brittle assertion.
    assert!(out.len() <= 80 + 6, "preview too long: {out:?}");
}
