//! Tests for the bg-process lifecycle UI broadcasts.
//!
//! Lives in a sibling file because `bg.rs` is already past the
//! 1000-line cap. The `[path = "tests.rs"] mod tests;` shim in
//! `bg.rs` is what wires this in.
//!
//! ## Test isolation caveat
//!
//! The UI notifier is a process-global static (see
//! `plugin/ui_push.rs`). `install_for_test` swaps it for the
//! duration of the returned guard, but bg-process `start` and
//! `kill` notifiers fire from background tasks that may outlive
//! the test that started the process. Running these tests in
//! parallel would let one test's notifier be replaced while
//! another test's bg task is still in flight, dumping the
//! straggler notify into the wrong captured log.
//!
//! We serialise the tests in this module with a static mutex so
//! the global notifier is owned by exactly one test at a time.
//! The whole suite runs in seconds, so the serialisation cost is
//! trivial.

use super::*;
use crate::manager::remote::local_fs_mcp::{ShellApproval, ShellApprover};
use crate::manager::remote::plugin::protocol::BgRequest;
use crate::manager::remote::plugin::ui_push;
use async_trait::async_trait;
use std::sync::Mutex as StdMutex;

/// Test fixture: takes the process-wide `ui_push::test_serial()`
/// lock (shared with the server and ui_push test modules — a
/// module-local lock would not stop another module from swapping
/// the process-global notifier mid-test), installs a fresh
/// capturing notifier, and returns the captured list + the
/// notifier guard. Drop the guard at the end of the test to
/// release both.
struct NotifyFixture {
    captured: Arc<StdMutex<Vec<(String, serde_json::Value)>>>,
    _guard: ui_push::NotifierGuard,
    _serial: std::sync::RwLockWriteGuard<'static, ()>,
}

fn serialised() -> NotifyFixture {
    let serial = ui_push::test_serial();
    let captured: Arc<StdMutex<Vec<(String, serde_json::Value)>>> = Arc::new(StdMutex::new(Vec::new()));
    let cap_clone = captured.clone();
    let notifier: Arc<dyn Fn(&str, serde_json::Value) + Send + Sync> = Arc::new(move |name, payload| {
        cap_clone.lock().unwrap().push((name.to_string(), payload));
    });
    let guard = ui_push::install_for_test(notifier);
    NotifyFixture {
        captured,
        _guard: guard,
        _serial: serial,
    }
}

fn manager() -> Arc<BgProcessManager> {
    Arc::new(BgProcessManager::new())
}

fn registry() -> Arc<PluginRegistry> {
    Arc::new(PluginRegistry::new())
}

struct Allow;
#[async_trait]
impl ShellApprover for Allow {
    async fn approve_shell(&self, _: &str, _: &str) -> ShellApproval {
        ShellApproval::Allow
    }
}

fn approver() -> Arc<dyn ShellApprover> {
    Arc::new(Allow)
}

#[tokio::test]
async fn bg_start_fires_remote_bg_process_changed() {
    let fix = serialised();
    let mgr = manager();
    let reg = registry();

    let dir = tempfile::tempdir().unwrap();
    let info = mgr
        .start(
            "ra_bg_notify",
            &reg,
            BgRequest::Start {
                command: "echo bg_notify_marker".into(),
                cwd: None,
                session_id: "ses_1".into(),
                call_id: None,
                name: None,
                timeout_secs: None,
            },
            approver(),
            dir.path(),
        )
        .await
        .unwrap();

    // Wait for either the start broadcast (status=running) or
    // the terminal broadcast (status=exited, because `echo`
    // exits fast) to land. The first broadcast we see is what
    // counts; the test only requires the start event landed.
    let mut start_seen = false;
    for _ in 0..100 {
        let log = fix.captured.lock().unwrap();
        for (name, payload) in log.iter() {
            if name == "remote.bgProcessChanged"
                && payload["process"]["id"] == info.id
                && payload["process"]["status"] == "running"
            {
                start_seen = true;
                break;
            }
        }
        if start_seen {
            break;
        }
        drop(log);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let log_snapshot = fix.captured.lock().unwrap().clone();
    assert!(start_seen, "expected start broadcast, log: {log_snapshot:?}");

    // Clean up: stop the process so the monitor exits.
    let _ = mgr.stop("ra_bg_notify", &info.id).await;
}

#[tokio::test]
async fn bg_stop_fires_remote_bg_process_changed() {
    let fix = serialised();
    let mgr = manager();
    let reg = registry();
    let dir = tempfile::tempdir().unwrap();
    let info = mgr
        .start(
            "ra_bg_stop_notify",
            &reg,
            BgRequest::Start {
                command: "sleep 5".into(),
                cwd: None,
                session_id: "ses_1".into(),
                call_id: None,
                name: None,
                timeout_secs: None,
            },
            approver(),
            dir.path(),
        )
        .await
        .unwrap();

    // Snapshot baseline. Serialised() guarantees no other test is
    // running, but a previous test's bg task that escaped the
    // serial lock window could still land a stray notify before
    // we get a chance to act. We filter to OUR process id so
    // stray noise doesn't poison the assertion.
    let _ = mgr.stop("ra_bg_stop_notify", &info.id).await;

    // Wait for the monitor to mark terminal and fire the
    // broadcast. The post-stop snapshot's `status` should be
    // `killed`.
    let mut killed_seen = false;
    for _ in 0..200 {
        let log = fix.captured.lock().unwrap();
        for (name, payload) in log.iter() {
            if name == "remote.bgProcessChanged"
                && payload["agent_id"] == "ra_bg_stop_notify"
                && payload["process"]["id"] == info.id
                && payload["process"]["status"] == "killed"
            {
                killed_seen = true;
                break;
            }
        }
        if killed_seen {
            break;
        }
        drop(log);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let log_snapshot = fix.captured.lock().unwrap().clone();
    assert!(killed_seen, "expected killed broadcast, log: {log_snapshot:?}");
    // The payload should also carry the UI shape (snake_case
    // fields, BgProcessStatus enum string).
    let log = fix.captured.lock().unwrap();
    let last = log
        .iter()
        .rev()
        .find(|(n, p)| {
            n == "remote.bgProcessChanged"
                && p["agent_id"] == "ra_bg_stop_notify"
                && p["process"]["id"] == info.id
                && p["process"]["status"] == "killed"
        })
        .expect("killed broadcast present");
    assert_eq!(last.1["agent_id"], "ra_bg_stop_notify");
    assert!(last.1["process"]["output_bytes"].is_u64());
    assert!(last.1["process"]["truncated"].is_boolean());
}

#[tokio::test]
async fn bg_self_exit_fires_remote_bg_process_changed_with_exited_status() {
    let fix = serialised();
    let mgr = manager();
    let reg = registry();
    let dir = tempfile::tempdir().unwrap();

    let info = mgr
        .start(
            "ra_bg_exit_notify",
            &reg,
            BgRequest::Start {
                command: "true".into(),
                cwd: None,
                session_id: "ses_1".into(),
                call_id: None,
                name: None,
                timeout_secs: None,
            },
            approver(),
            dir.path(),
        )
        .await
        .unwrap();

    // Wait for the self-exit broadcast.
    let mut exited_seen = false;
    for _ in 0..200 {
        let log = fix.captured.lock().unwrap();
        for (name, payload) in log.iter() {
            if name == "remote.bgProcessChanged"
                && payload["process"]["id"] == info.id
                && payload["process"]["status"] == "exited"
            {
                exited_seen = true;
                break;
            }
        }
        if exited_seen {
            break;
        }
        drop(log);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let log_snapshot = fix.captured.lock().unwrap().clone();
    assert!(exited_seen, "expected exited broadcast, log: {log_snapshot:?}");
}
