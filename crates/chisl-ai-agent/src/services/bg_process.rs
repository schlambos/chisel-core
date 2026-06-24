//! UI-facing service for the background-process surface.
//!
//! Reads from the process-wide [`crate::manager::remote::plugin::bg::BgProcessManager`]
//! and translates the plugin webserver's camelCase [`BgProcessInfo`](
//! crate::manager::remote::plugin::BgProcessInfo) into the snake_case
//! [`BgProcessUiInfo`] the renderer REST surface uses. The translation
//! is a pure field-by-field copy plus an enum rename; no business
//! logic lives here — every method is a thin pass-through so the
//! state always reflects what the plugin webserver has.
//!
//! All methods take a `remote_agent_id` (a.k.a. the OpenCode remote
//! agent row's primary key). The manager's `list` / `stop` / `read`
//! work in the same key, so there's no ID mapping.

use chisl_api_types::{BgProcessOutputResponse, BgProcessUiInfo};

use crate::manager::remote::plugin::BgError;
use crate::manager::remote::plugin::bg::bg_global;

/// Read every known background process (running + terminal) for the
/// given remote agent. Cheap — acquires the manager's mutex and
/// returns a clone of the per-process snapshot.
pub fn list_bg_processes(remote_agent_id: &str) -> Vec<BgProcessUiInfo> {
    bg_global()
        .list(remote_agent_id)
        .into_iter()
        .map(bg_info_to_ui)
        .collect()
}

/// Stop a single background process. Returns the post-stop
/// snapshot. Translates the plugin `BgError` to an `AppError`-friendly
/// shape the route handler can convert to a 4xx/5xx response.
pub async fn stop_bg_process(remote_agent_id: &str, process_id: &str) -> Result<BgProcessUiInfo, BgError> {
    bg_global().stop(remote_agent_id, process_id).await.map(bg_info_to_ui)
}

/// Read the ring-buffer slice starting at `offset`. Returns the
/// slice as a lossy utf-8 string, the `next_offset` to pass on the
/// next call, and the latest process snapshot. The route handler
/// wraps this in [`BgProcessOutputResponse`].
pub fn read_bg_process(
    remote_agent_id: &str,
    process_id: &str,
    offset: u64,
) -> Result<(String, u64, BgProcessUiInfo), BgError> {
    bg_global()
        .read(remote_agent_id, process_id, offset)
        .map(|(out, next, info)| (out, next, bg_info_to_ui(info)))
}

/// Convert a plugin-webserver [`BgProcessInfo`] into the
/// snake_case UI shape. Re-exported from
/// [`crate::manager::remote::plugin::bg_info_to_ui`] so callers
/// who already import from the plugin module see the same
/// conversion; lives there to avoid a `services` ↔
/// `manager::remote::plugin::bg` cycle.
pub use crate::manager::remote::plugin::bg_info_to_ui;

#[allow(dead_code)]
fn _force_use_output_response_type() -> Option<BgProcessOutputResponse> {
    // Compile-time check that the wrapper type stays
    // reachable from this module so a future refactor that
    // drops the route's dependency on it gets caught.
    None
}

// ── Tests for the UI REST happy paths ────────────────────────────
//
// The routes in `routes/remote.rs` are thin pass-throughs to the
// three service functions in this module. Testing here covers the
// exact wire shape the renderer sees (`BgProcessUiInfo` /
// `BgProcessOutputResponse`), and the route layer's only added
// behaviour is the agent-row existence check via
// `state.service.get(&id)` (which lives in `services/remote.rs` and
// is exercised by the existing `remote_agent` integration tests).
#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::remote::local_fs_mcp::{ShellApproval, ShellApprover};
    use crate::manager::remote::plugin::BgStatus;
    use crate::manager::remote::plugin::registry::PluginRegistry;
    use chisl_api_types::BgProcessStatus;
    use async_trait::async_trait;
    use std::sync::Arc;
    use std::time::Duration;

    struct AllowAll;
    #[async_trait]
    impl ShellApprover for AllowAll {
        async fn approve_shell(&self, _command: &str, _cwd: &str) -> ShellApproval {
            ShellApproval::Allow
        }
    }

    /// Build a fresh isolated manager + registry pair so tests
    /// don't bleed into the process-wide singletons. The service
    /// helpers call `bg_global()` so we *do* exercise the same
    /// code path the routes use; we accept the cross-test state
    /// bleed because the in-process manager is mutex-guarded and
    /// each test uses a unique agent id.
    fn approver() -> Arc<dyn ShellApprover> {
        Arc::new(AllowAll)
    }

    /// Wait for the named process to reach a non-Running status.
    /// The bg manager's monitor task is async, so a freshly-spawned
    /// `sleep 0.05` may take a few ms to flip.
    async fn wait_terminal(agent_id: &str, process_id: &str) {
        for _ in 0..100 {
            let list = bg_global().list(agent_id);
            if let Some(p) = list.iter().find(|p| p.id == process_id)
                && p.status != BgStatus::Running
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn ui_list_returns_empty_for_unknown_agent() {
        let procs = list_bg_processes("ui_unknown");
        assert!(procs.is_empty());
    }

    #[tokio::test]
    async fn ui_list_returns_running_and_terminal_in_ui_shape() {
        let reg = Arc::new(PluginRegistry::new());
        let dir = tempfile::tempdir().unwrap();
        // Start a self-exiting process and a sleeping one.
        let self_exiting = bg_global()
            .start(
                "ui_list_agent",
                &reg,
                crate::manager::remote::plugin::protocol::BgRequest::Start {
                    command: "printf ui_list_marker".into(),
                    cwd: None,
                    session_id: "ses_1".into(),
                    call_id: None,
                    name: Some("self-exit".into()),
                    timeout_secs: None,
                },
                approver(),
                dir.path(),
            )
            .await
            .unwrap();
        let _sleeper = bg_global()
            .start(
                "ui_list_agent",
                &reg,
                crate::manager::remote::plugin::protocol::BgRequest::Start {
                    command: "sleep 5".into(),
                    cwd: None,
                    session_id: "ses_1".into(),
                    call_id: None,
                    name: Some("sleeper".into()),
                    timeout_secs: None,
                },
                approver(),
                dir.path(),
            )
            .await
            .unwrap();

        wait_terminal("ui_list_agent", &self_exiting.id).await;

        let procs = list_bg_processes("ui_list_agent");
        assert_eq!(procs.len(), 2);
        // Both entries are in the UI snake_case shape.
        for p in &procs {
            assert!(!p.id.is_empty());
            assert!(p.command.starts_with("printf ") || p.command.starts_with("sleep "));
            // Status is the snake_case enum, not the camelCase plugin shape.
            assert!(matches!(
                p.status,
                BgProcessStatus::Running | BgProcessStatus::Exited | BgProcessStatus::Killed
            ));
            assert_eq!(p.session_id, "ses_1");
        }
        let exited = procs.iter().find(|p| p.id == self_exiting.id).unwrap();
        assert_eq!(exited.status, BgProcessStatus::Exited);

        // Cleanup: kill the sleeper so the monitor task can exit.
        let _ = stop_bg_process("ui_list_agent", &_sleeper.id).await;
    }

    #[tokio::test]
    async fn ui_stop_returns_post_stop_snapshot() {
        let reg = Arc::new(PluginRegistry::new());
        let dir = tempfile::tempdir().unwrap();
        let info = bg_global()
            .start(
                "ui_stop_agent",
                &reg,
                crate::manager::remote::plugin::protocol::BgRequest::Start {
                    command: "sleep 30".into(),
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

        let stopped = stop_bg_process("ui_stop_agent", &info.id).await.unwrap();
        assert_eq!(stopped.id, info.id);
        assert_eq!(stopped.status, BgProcessStatus::Killed);
        assert!(stopped.ended_at_ms.is_some());
    }

    #[tokio::test]
    async fn ui_stop_unknown_process_returns_not_found() {
        let err = stop_bg_process("ui_stop_unknown", "missing").await.unwrap_err();
        assert!(matches!(err, BgError::NotFound(_)));
    }

    #[tokio::test]
    async fn ui_read_returns_offset_and_advances() {
        let reg = Arc::new(PluginRegistry::new());
        let dir = tempfile::tempdir().unwrap();
        let info = bg_global()
            .start(
                "ui_read_agent",
                &reg,
                crate::manager::remote::plugin::protocol::BgRequest::Start {
                    command: "printf ui_read_marker".into(),
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

        // Poll until the marker shows up.
        let mut output = String::new();
        let mut next_offset: u64 = 0;
        for _ in 0..50 {
            let (out, next, _info) = read_bg_process("ui_read_agent", &info.id, next_offset).unwrap();
            output.push_str(&out);
            next_offset = next;
            if output.contains("ui_read_marker") {
                assert!(next_offset > 0);
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("ui read never produced the marker; got: {output}");
    }

    #[tokio::test]
    async fn ui_read_unknown_process_returns_not_found() {
        let err = read_bg_process("ui_read_unknown", "missing", 0).unwrap_err();
        assert!(matches!(err, BgError::NotFound(_)));
    }

    /// Status enum wire-shape check: the routes serialize
    /// `BgProcessStatus` (snake_case) so the renderer's TypeScript
    /// types must match. Lock the wire encoding down so a future
    /// accidental rename of the enum variants trips a test.
    #[test]
    fn ui_status_serializes_as_snake_case_strings() {
        let cases = [
            (BgProcessStatus::Running, "\"running\""),
            (BgProcessStatus::Exited, "\"exited\""),
            (BgProcessStatus::Killed, "\"killed\""),
        ];
        for (s, want) in cases {
            let json = serde_json::to_string(&s).unwrap();
            assert_eq!(json, want, "status {s:?} should serialize as {want}");
        }
    }
}
