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
fn plugin_listen_addr_uses_fixed_default_port() {
    use crate::manager::remote::plugin::plugin_listen_addr;
    use crate::manager::remote::reachability::{Candidate, Plan};
    use aionui_common::constants::DEFAULT_PLUGIN_PORT;
    use std::net::{IpAddr, Ipv4Addr};

    let auto = Plan::Auto {
        candidates: vec![Candidate {
            ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            provider: "loopback",
        }],
    };
    let addr = plugin_listen_addr(&auto);
    assert_eq!(addr.port(), DEFAULT_PLUGIN_PORT);
    assert!(addr.ip().is_unspecified());

    let override_plan = Plan::Override {
        public_url: "https://tunnel.example/".into(),
    };
    let loopback = plugin_listen_addr(&override_plan);
    assert_eq!(loopback.port(), DEFAULT_PLUGIN_PORT);
    assert!(loopback.ip().is_loopback());
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

// ── /tools/bg + /tools/bg_tail tests ────────────────────────────

/// Spin up a server with a permissive approver already
/// registered so the `/tools/bg` route doesn't short-circuit
/// on `no_approver`.
async fn boot_with_approver() -> (PluginServer, String, String, Arc<PluginRegistry>) {
    let (server, url, token, registry) = boot().await;
    registry.register_shell_approver("ra_x", Arc::new(ApproverAllow));
    (server, url, token, registry)
}

#[tokio::test]
async fn bg_no_approver_returns_error() {
    let (_server, url, token, _reg) = boot().await;
    let resp = Client::new()
        .post(format!("{url}/tools/bg"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({
            "op": "start",
            "command": "echo hi",
            "sessionId": "ses_1"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["ok"], false);
    assert_eq!(v["code"], "no_approver");
}

#[tokio::test]
async fn bg_list_returns_processes_envelope() {
    let (_server, url, token, _reg) = boot_with_approver().await;
    // Start a short-lived process.
    let resp = Client::new()
        .post(format!("{url}/tools/bg"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({
            "op": "start",
            "command": "echo bg_list_test",
            "sessionId": "ses_1"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);
    let pid = body["process"]["id"].as_str().unwrap().to_string();

    // List.
    let resp = Client::new()
        .post(format!("{url}/tools/bg"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({"op": "list", "sessionId": "ses_1"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["ok"], true);
    let procs = v["processes"].as_array().unwrap();
    assert!(procs.iter().any(|p| p["id"] == pid));
    // Each entry carries the BgProcessInfo shape.
    for p in procs {
        assert!(p["status"].is_string());
        assert!(p["command"].is_string());
        assert!(p["outputBytes"].is_u64());
        assert!(p["truncated"].is_boolean());
    }
}

#[tokio::test]
async fn bg_stop_returns_process_envelope() {
    let (_server, url, token, _reg) = boot_with_approver().await;
    let resp = Client::new()
        .post(format!("{url}/tools/bg"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({
            "op": "start",
            "command": "sleep 30",
            "sessionId": "ses_1"
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let pid = body["process"]["id"].as_str().unwrap().to_string();

    let resp = Client::new()
        .post(format!("{url}/tools/bg"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({
            "op": "stop",
            "processId": pid,
            "sessionId": "ses_1"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["ok"], true);
    // Wire shape: { ok, process: BgProcessInfo }
    let p = &v["process"];
    assert_eq!(p["id"], pid);
    assert_ne!(p["status"], "running");
    assert_eq!(p["status"], "killed");
}

#[tokio::test]
async fn bg_read_returns_output_envelope() {
    let (_server, url, token, _reg) = boot_with_approver().await;
    let resp = Client::new()
        .post(format!("{url}/tools/bg"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({
            "op": "start",
            "command": "printf bg_read_marker",
            "sessionId": "ses_1"
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let pid = body["process"]["id"].as_str().unwrap().to_string();

    // Poll read until output arrives.
    let mut output = String::new();
    let mut next_offset: u64 = 0;
    for _ in 0..50 {
        let resp = Client::new()
            .post(format!("{url}/tools/bg"))
            .header("Authorization", format!("Bearer {token}"))
            .json(&json!({
                "op": "read",
                "processId": pid,
                "sessionId": "ses_1",
                "offset": next_offset
            }))
            .send()
            .await
            .unwrap();
        let v: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(v["ok"], true);
        output.push_str(v["output"].as_str().unwrap_or(""));
        next_offset = v["nextOffset"].as_u64().unwrap();
        if output.contains("bg_read_marker") {
            // Wire shape: { ok, output, nextOffset, process }
            assert!(v["process"].is_object());
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("bg read never produced the marker; got: {output}");
}

#[tokio::test]
async fn bg_tail_sse_replay_live_and_done() {
    use futures_util::StreamExt;
    let (_server, url, token, _reg) = boot_with_approver().await;
    // Start a process that emits a marker, sleeps, then exits.
    let resp = Client::new()
        .post(format!("{url}/tools/bg"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({
            "op": "start",
            "command": "printf bg_tail_marker;sleep 0.2",
            "sessionId": "ses_1"
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let pid = body["process"]["id"].as_str().unwrap().to_string();

    // Open the tail stream.
    let resp = reqwest::Client::new()
        .post(format!("{url}/tools/bg_tail"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "text/event-stream")
        .json(&json!({
            "processId": pid,
            "sessionId": "ses_1",
            "fromOffset": 0
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("content-type").and_then(|v| v.to_str().ok()),
        Some("text/event-stream")
    );

    let mut stream = resp.bytes_stream();
    let mut buf = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut saw_marker = false;
    let mut saw_done = false;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
            Ok(Some(Ok(chunk))) => {
                buf.extend_from_slice(&chunk);
                let text = String::from_utf8_lossy(&buf);
                if text.contains("bg_tail_marker") {
                    saw_marker = true;
                }
                if text.contains("event: done") {
                    saw_done = true;
                    break;
                }
            }
            Ok(Some(Err(_))) | Ok(None) | Err(_) => break,
        }
    }
    assert!(
        saw_marker,
        "did not see the tail marker; buffer: {}",
        String::from_utf8_lossy(&buf)
    );
    assert!(
        saw_done,
        "did not see the terminal `done` event; buffer: {}",
        String::from_utf8_lossy(&buf)
    );
}

#[tokio::test]
async fn bg_tail_unknown_process_returns_error_envelope() {
    let (_server, url, token, _reg) = boot_with_approver().await;
    let resp = Client::new()
        .post(format!("{url}/tools/bg_tail"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({
            "processId": "missing",
            "sessionId": "ses_1"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["ok"], false);
    assert_eq!(v["code"], "not_found");
}

// ── Phase 3: voice-mode + sticky replay + reactions tests ────────

/// Process-wide serialisation for the UI-notify tests in this
/// module. The notifier is a process-global static (see
/// `plugin/ui_push.rs`); a delayed `notify` from one test's
/// debounced reaction could otherwise land in another test's
/// captured log if the notifier got swapped between the
/// producer and the consumer. Serialisation uses the
/// process-wide `ui_push::test_serial()` lock — shared with the
/// bg and ui_push test modules, because module-local locks do
/// NOT protect against a *different* module swapping the global
/// notifier mid-test. The whole UI-notify section runs in a few
/// seconds, so serialisation is cheap.
struct ServerNotifyFixture {
    captured: Arc<std::sync::Mutex<Vec<(String, serde_json::Value)>>>,
    _guard: crate::manager::remote::plugin::ui_push::NotifierGuard,
    _serial: std::sync::RwLockWriteGuard<'static, ()>,
}

fn serialised_notify() -> ServerNotifyFixture {
    let serial = crate::manager::remote::plugin::ui_push::test_serial();
    let captured: Arc<std::sync::Mutex<Vec<(String, serde_json::Value)>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let cap_clone = captured.clone();
    let notifier: Arc<dyn Fn(&str, serde_json::Value) + Send + Sync> = Arc::new(move |name, payload| {
        cap_clone.lock().unwrap().push((name.to_string(), payload));
    });
    let guard = crate::manager::remote::plugin::ui_push::install_for_test(notifier);
    ServerNotifyFixture {
        captured,
        _guard: guard,
        _serial: serial,
    }
}

/// Build a one-shot PluginPushEvent that matches the wire shape
/// the plugin expects for a `voice_mode` SSE event. The plugin's
/// SSE consumer parses the `data` field as JSON and switches on
/// `type`; this helper keeps the exact shape pinned down so a
/// future refactor of the producer trips a test.
fn voice_mode_event(session_id: Option<&str>, enabled: bool) -> PluginPushEvent {
    PluginPushEvent {
        event: "voice_mode".into(),
        data: json!({
            "type": "voice_mode",
            "data": {
                "sessionID": session_id,
                "enabled": enabled,
            }
        }),
    }
}

#[tokio::test]
async fn events_sse_replays_sticky_voice_mode_on_new_connection() {
    use futures_util::StreamExt;
    let (_server, url, token, registry) = boot().await;
    // Seed two sticky voice-mode records (different sessions)
    // BEFORE the SSE client connects. The events stream should
    // emit the initial ping and then replay these in insertion
    // order.
    registry.set_sticky_voice_mode("ra_x", "ses_a".into(), voice_mode_event(Some("ses_a"), true));
    registry.set_sticky_voice_mode("ra_x", "ses_b".into(), voice_mode_event(Some("ses_b"), true));

    let resp = reqwest::Client::new()
        .get(format!("{url}/plugin/events"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "text/event-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let mut stream = resp.bytes_stream();
    let mut buf = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut seen_a = false;
    let mut seen_b = false;
    let mut seen_ping = false;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
            Ok(Some(Ok(chunk))) => {
                buf.extend_from_slice(&chunk);
                let text = String::from_utf8_lossy(&buf);
                if !seen_ping && text.contains("event: ping") {
                    seen_ping = true;
                }
                // Assert the exact wire JSON shape (matches what
                // context.update serializes).
                if !seen_a
                    && text.contains(r#""type":"voice_mode""#)
                    && text.contains(r#""sessionID":"ses_a""#)
                    && text.contains(r#""enabled":true"#)
                {
                    seen_a = true;
                }
                if !seen_b && text.contains(r#""sessionID":"ses_b""#) && text.contains(r#""enabled":true"#) {
                    seen_b = true;
                }
                if seen_a && seen_b {
                    break;
                }
            }
            _ => break,
        }
    }
    assert!(
        seen_ping,
        "initial ping missing; buffer: {}",
        String::from_utf8_lossy(&buf)
    );
    assert!(
        seen_a,
        "sticky voice_mode for ses_a missing; buffer: {}",
        String::from_utf8_lossy(&buf)
    );
    assert!(
        seen_b,
        "sticky voice_mode for ses_b missing; buffer: {}",
        String::from_utf8_lossy(&buf)
    );
}

#[tokio::test]
async fn events_sse_replay_then_live_push_delivered() {
    use futures_util::StreamExt;
    let (_server, url, token, registry) = boot().await;
    // Seed one sticky record before connect.
    registry.set_sticky_voice_mode("ra_x", "ses_a".into(), voice_mode_event(Some("ses_a"), true));

    let resp = reqwest::Client::new()
        .get(format!("{url}/plugin/events"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "text/event-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Fire a LIVE push after the SSE handshake has had a chance
    // to subscribe. The receiver should see both the sticky
    // replay AND the live event.
    let reg_for_push = registry.clone();
    let url_for_push = url.clone();
    let token_for_push = token.clone();
    tokio::spawn(async move {
        // Give the SSE handler a moment to register its receiver.
        tokio::time::sleep(Duration::from_millis(250)).await;
        reg_for_push.push(
            "ra_x",
            PluginPushEvent {
                event: "agentStatusChanged".into(),
                data: json!({"status": "connected"}),
            },
        );
    });

    let mut stream = resp.bytes_stream();
    let mut buf = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut seen_sticky = false;
    let mut seen_live = false;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
            Ok(Some(Ok(chunk))) => {
                buf.extend_from_slice(&chunk);
                let text = String::from_utf8_lossy(&buf);
                if !seen_sticky && text.contains(r#""sessionID":"ses_a""#) {
                    seen_sticky = true;
                }
                if !seen_live && text.contains("agentStatusChanged") && text.contains("connected") {
                    seen_live = true;
                }
                if seen_sticky && seen_live {
                    break;
                }
            }
            _ => break,
        }
    }
    assert!(
        seen_sticky,
        "sticky replay missing; buf={}",
        String::from_utf8_lossy(&buf)
    );
    assert!(seen_live, "live push missing; buf={}", String::from_utf8_lossy(&buf));
    // quiet the linter about unused captures.
    let _ = (url_for_push, token_for_push);
}

#[tokio::test]
async fn result_event_file_watcher_updated_fires_remote_workspace_changed() {
    let fix = serialised_notify();
    // Clear the static debouncer so a previous test's in-flight
    // window can't pollute our assertion.
    super::reset_workspace_debounce_for_test();
    let (_server, url, token, _reg) = boot().await;

    let resp = Client::new()
        .post(format!("{url}/plugin/result"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({
            "kind": "event",
            "event": {
                "type": "file.watcher.updated",
                "properties": {
                    "file": "src/lib.rs",
                    "event": "change"
                }
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Debounce is ~250ms; wait a bit longer than that to give
    // the timer a chance to fire.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let log = fix.captured.lock().unwrap();
    let workspace_events: Vec<_> = log.iter().filter(|(n, _)| n == "remote.workspaceChanged").collect();
    assert!(
        !workspace_events.is_empty(),
        "expected at least one workspace change notify, log: {log:?}"
    );
    let (name, payload) = workspace_events[0];
    assert_eq!(name, "remote.workspaceChanged");
    assert_eq!(payload["agent_id"], "ra_x");
    assert_eq!(payload["file"], "src/lib.rs");
    assert_eq!(payload["event"], "change");
}

#[tokio::test]
async fn result_event_file_watcher_updated_is_debounced() {
    let fix = serialised_notify();
    // Clear the static debouncer so a previous test's in-flight
    // window can't pollute our assertion.
    super::reset_workspace_debounce_for_test();
    let (_server, url, token, _reg) = boot().await;
    // Snapshot the captured log length BEFORE we start, so the
    // assertion is about the delta within this test rather than
    // the absolute count (the global notifier is process-shared
    // and a parallel test's stray notify could otherwise shift
    // the count by ±N).
    let baseline = fix.captured.lock().unwrap().len();

    // Fire two rapid file.watcher.updated events. The debouncer
    // should coalesce them into a single notify.
    for _ in 0..2 {
        let resp = Client::new()
            .post(format!("{url}/plugin/result"))
            .header("Authorization", format!("Bearer {token}"))
            .json(&json!({
                "kind": "event",
                "event": {
                    "type": "file.watcher.updated",
                    "properties": {"file": "x.rs", "event": "change"}
                }
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }
    // Wait past the debounce window.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let after = fix.captured.lock().unwrap();
    // Count only this test's workspace changes (defensive
    // against stray notifies from other tests landing in the
    // shared global notifier's log).
    let workspace_count = after
        .iter()
        .skip(baseline)
        .filter(|(n, _)| n == "remote.workspaceChanged")
        .count();
    assert_eq!(
        workspace_count, 1,
        "expected exactly 1 debounced workspace change notify, got {workspace_count}"
    );
}

#[tokio::test]
async fn result_event_session_idle_fires_remote_session_health() {
    let fix = serialised_notify();
    let (_server, url, token, _reg) = boot().await;

    let resp = Client::new()
        .post(format!("{url}/plugin/result"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({
            "kind": "event",
            "event": {
                "type": "session.idle",
                "properties": {"sessionID": "ses_42"}
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let log = fix.captured.lock().unwrap();
    let health_events: Vec<_> = log.iter().filter(|(n, _)| n == "remote.sessionHealth").collect();
    assert_eq!(
        health_events.len(),
        1,
        "expected exactly one sessionHealth notify, log: {log:?}"
    );
    let (name, payload) = health_events[0];
    assert_eq!(name, "remote.sessionHealth");
    assert_eq!(payload["kind"], "idle");
    assert_eq!(payload["session_id"], "ses_42");
    assert_eq!(payload["agent_id"], "ra_x");
}

#[tokio::test]
async fn result_event_session_error_fires_remote_session_health_with_message() {
    let fix = serialised_notify();
    let (_server, url, token, _reg) = boot().await;

    let resp = Client::new()
        .post(format!("{url}/plugin/result"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({
            "kind": "event",
            "event": {
                "type": "session.error",
                "properties": {
                    "sessionID": "ses_42",
                    "error": {"message": "rate limit hit"}
                }
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let log = fix.captured.lock().unwrap();
    let health_events: Vec<_> = log.iter().filter(|(n, _)| n == "remote.sessionHealth").collect();
    assert_eq!(health_events.len(), 1);
    let (name, payload) = health_events[0];
    assert_eq!(name, "remote.sessionHealth");
    assert_eq!(payload["kind"], "error");
    assert_eq!(payload["session_id"], "ses_42");
    assert_eq!(payload["message"], "rate limit hit");
}

#[tokio::test]
async fn result_event_session_error_caps_long_message() {
    let fix = serialised_notify();
    let (_server, url, token, _reg) = boot().await;

    let long_msg = "x".repeat(2000);
    let resp = Client::new()
        .post(format!("{url}/plugin/result"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({
            "kind": "event",
            "event": {
                "type": "session.error",
                "properties": {
                    "sessionID": "ses_42",
                    "error": {"message": long_msg}
                }
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let log = fix.captured.lock().unwrap();
    let health = log.iter().find(|(n, _)| n == "remote.sessionHealth").unwrap();
    let msg = health.1["message"].as_str().unwrap();
    assert!(msg.len() <= 512, "message should be capped at 512, got {}", msg.len());
}

#[tokio::test]
async fn result_event_message_part_updated_does_not_fire_reaction() {
    let fix = serialised_notify();
    let (_server, url, token, _reg) = boot().await;

    let resp = Client::new()
        .post(format!("{url}/plugin/result"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({
            "kind": "event",
            "event": {
                "type": "message.part.updated",
                "properties": {"sessionID": "ses_42"}
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let log = fix.captured.lock().unwrap();
    assert!(
        log.is_empty(),
        "message.part.updated must not fire any notify, got: {log:?}"
    );
}

#[tokio::test]
async fn result_event_malformed_payload_does_not_panic_or_notify() {
    let fix = serialised_notify();
    let (_server, url, token, _reg) = boot().await;

    // Snapshot the captured log length BEFORE we post the
    // malformed event. A parallel test's stray notify (the
    // global notifier is process-shared) could land in the
    // captured log; what we want to assert is that THIS
    // test's posts do not ADD any new entries.
    let baseline = fix.captured.lock().unwrap().len();

    // Missing `type`, missing `properties` — must not panic, must
    // not fire a reaction. Audit still records "event unknown".
    let resp = Client::new()
        .post(format!("{url}/plugin/result"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({
            "kind": "event",
            "event": {"foo": "bar"}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let after_first = fix.captured.lock().unwrap().len();
    assert_eq!(after_first, baseline, "malformed event must not fire any new notify");

    // `event` is a non-object.
    let resp = Client::new()
        .post(format!("{url}/plugin/result"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({
            "kind": "event",
            "event": "not-an-object"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let after_second = fix.captured.lock().unwrap().len();
    assert_eq!(after_second, baseline, "non-object event must not fire any new notify");
}

#[tokio::test]
async fn result_event_audit_still_records_when_notify_fails() {
    let _fix = serialised_notify();
    let (_server, url, token, registry) = boot().await;
    // Install a panicking notifier; the route must still return
    // 200 because the notifier is best-effort.
    let panicking: Arc<dyn Fn(&str, serde_json::Value) + Send + Sync> =
        Arc::new(|_, _| panic!("notifier should not fire"));
    let _panic_guard = crate::manager::remote::plugin::ui_push::install_for_test(panicking);
    let resp = Client::new()
        .post(format!("{url}/plugin/result"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({
            "kind": "event",
            "event": {
                "type": "file.watcher.updated",
                "properties": {"file": "x.rs"}
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    // Wait past the debounce — the notifier is called from the
    // debounce task, which is a separate task. The test asserts
    // the audit is recorded regardless of the notifier panic;
    // the panic would be in another task so it doesn't fail
    // this test (tokio swallows task panics by default).
    tokio::time::sleep(Duration::from_millis(400)).await;
    let records = registry.audit_records("ra_x");
    assert!(records.iter().any(|r| r.kind == "event"), "event audit missing");
}
