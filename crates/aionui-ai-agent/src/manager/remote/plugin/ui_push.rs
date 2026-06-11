//! Process-global UI notifier for the OpenCode bridge plugin.
//!
//! The plugin webserver ([`super::server`]) and the background-process
//! manager ([`super::bg`]) live inside `aionui-ai-agent` and have no
//! direct handle to the host's [`aionui_realtime::EventBroadcaster`]
//! (the broadcaster is constructed in `aionui-app` and lives behind an
//! `Arc<dyn EventBroadcaster>` on the router state). Reactions to
//! plugin-forwarded OpenCode events and bg-process lifecycle changes
//! need a way to surface WebSocket messages to the renderer without
//! the plugin pipeline importing `aionui-realtime` directly — a
//! dependency arrow that would create a cycle
//! (`aionui-realtime` already depends on `aionui-api-types`, and
//! `aionui-app` composes both).
//!
//! The notifier is a minimal closure: callers register a
//! `Fn(&str, serde_json::Value) + Send + Sync` once at startup; the
//! plugin pipeline calls [`notify`] with the event name + payload and
//! the closure does the `broadcaster.broadcast(...)` call. When the
//! notifier has not been installed (test code, the plugin used in
//! isolation) [`notify`] is a no-op so the plugin webserver remains
//! test-friendly.
//!
//! ## Test override
//!
//! Tests that need to assert on a notifier call use
//! [`install_for_test`], which swaps the notifier for the duration of
//! the returned guard. The guard's `Drop` restores the prior
//! notifier so cross-test bleed is impossible. The whole state is
//! behind a `std::sync::RwLock<Option<Arc<dyn Fn>>>` — a write lock
//! is held only across the closure swap, never across a `notify`
//! call. `notify` takes a read lock and clones the `Arc` for a
//! panic-free release; the notifier body itself runs lock-free.

use std::sync::{Arc, RwLock};

/// Type-erased notifier body. The signature is intentionally minimal:
/// `event_name` (the WebSocket `name` field) and an arbitrary JSON
/// payload. Implementations are expected to call
/// `broadcaster.broadcast(WebSocketMessage::new(name, payload))`.
type NotifierBody = dyn Fn(&str, serde_json::Value) + Send + Sync;

static UI_NOTIFIER: RwLock<Option<Arc<NotifierBody>>> = RwLock::new(None);

/// Install a notifier. The host (composition root in
/// `aionui-app::build_remote_agent_state`) calls this once at startup
/// with a closure that calls `broadcaster.broadcast(WebSocketMessage::new(...))`.
///
/// Replaces any prior notifier. The previous notifier is dropped after
/// the swap; outstanding `notify` calls in flight against the prior
/// closure complete first because they hold an `Arc` clone.
pub fn set_ui_notifier(f: Arc<NotifierBody>) {
    if let Ok(mut guard) = UI_NOTIFIER.write() {
        *guard = Some(f);
    }
}

/// Process-wide serialisation for tests that install a notifier.
///
/// `UI_NOTIFIER` is a single process-global slot. Tests in *different*
/// modules (server tests, bg tests, this module's tests) each install
/// their own capture notifier; if they serialise on module-local
/// mutexes they can interleave: test B's `install_for_test` captures
/// test A's notifier as "prior" and restores it while A's *async*
/// notify (e.g. the workspace-change debounce task, which fires up to
/// 250 ms later) is still in flight — A's notify then lands in B's
/// capture (or nowhere) and A's count assertion fails flakily.
///
/// Every test fixture that installs a notifier MUST hold this guard
/// for the test's full duration. `install_for_test` deliberately does
/// not take the lock itself (callers hold it while calling, which
/// would deadlock a non-reentrant mutex).
#[cfg(any(test, feature = "test-support"))]
static TEST_NOTIFY_SERIAL: RwLock<()> = RwLock::new(());

/// Acquire the process-wide notifier-test lock. See
/// [`TEST_NOTIFY_SERIAL`] for why this must be held for the whole
/// test whenever [`install_for_test`] is used.
#[cfg(any(test, feature = "test-support"))]
pub fn test_serial() -> std::sync::RwLockWriteGuard<'static, ()> {
    TEST_NOTIFY_SERIAL.write().unwrap_or_else(|e| e.into_inner())
}

/// Test-only: install a fresh notifier and return a guard that
/// restores the prior value on drop. The guard exists for the
/// duration of the test; subsequent tests start with no notifier
/// installed (or with the one the host already wired in).
///
/// Callers must hold [`test_serial`] for the duration of the test.
#[cfg(any(test, feature = "test-support"))]
pub fn install_for_test(f: Arc<NotifierBody>) -> NotifierGuard {
    let prior = {
        let mut guard = UI_NOTIFIER.write().expect("ui notifier lock poisoned");
        let prior = guard.take();
        *guard = Some(f);
        prior
    };
    NotifierGuard { prior }
}

/// Drop guard for [`install_for_test`]. Restores the prior notifier
/// (or leaves the slot empty) when dropped, so tests can't leak
/// closures into each other.
#[cfg(any(test, feature = "test-support"))]
pub struct NotifierGuard {
    prior: Option<Arc<NotifierBody>>,
}

#[cfg(any(test, feature = "test-support"))]
impl Drop for NotifierGuard {
    fn drop(&mut self) {
        let mut guard = UI_NOTIFIER.write().expect("ui notifier lock poisoned");
        *guard = self.prior.take();
    }
}

/// Push a UI event. No-op if no notifier has been installed (test
/// code, host not yet bootstrapped). Errors inside the notifier body
/// are NOT caught here — the notifier is expected to log / drop
/// failures itself, the same way the host's `EventBroadcaster`
/// implementation does.
///
/// Cheap: a read lock, an `Arc` clone, then a function call. The
/// caller (the plugin webserver's `handle_result` path) sits on a
/// hot path that must not block on chat streaming latency; this
/// function is the reason we use `std::sync::RwLock` instead of
/// `Mutex` — multiple readers can `notify` concurrently and a writer
/// is only acquired during the rare `set_ui_notifier` /
/// `install_for_test` swap.
pub fn notify(event_name: &str, payload: serde_json::Value) {
    let arc = {
        let guard = match UI_NOTIFIER.read() {
            Ok(g) => g,
            Err(_) => return,
        };
        guard.as_ref().cloned()
    };
    if let Some(f) = arc {
        f(event_name, payload);
    }
}

// ── Tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Mutex;

    /// All notifier tests serialise on the process-wide
    /// [`test_serial`] lock — shared with the server and bg test
    /// modules — so no other module can swap the notifier while a
    /// test here (or an async notify elsewhere) is in flight.
    fn serial() -> std::sync::RwLockWriteGuard<'static, ()> {
        test_serial()
    }

    #[test]
    fn notify_without_installation_is_noop() {
        let _serial = serial();
        // Ensure a clean state for this test.
        let _prior = UI_NOTIFIER.write().unwrap().take();
        // No panic, no observable effect.
        notify("test.event", json!({"k": 1}));
    }

    #[test]
    fn installed_notifier_receives_event_with_payload() {
        let _serial = serial();
        // Clear the static so this test's install_for_test sees
        // a clean prior.
        let _prior = UI_NOTIFIER.write().unwrap().take();
        let captured: Arc<Mutex<Vec<(String, serde_json::Value)>>> = Arc::new(Mutex::new(Vec::new()));
        let captured_clone = captured.clone();
        let notifier: Arc<NotifierBody> = Arc::new(move |name, payload| {
            captured_clone.lock().unwrap().push((name.to_string(), payload));
        });
        let _guard = install_for_test(notifier);
        notify("remote.bgProcessChanged", json!({"agent_id": "ra_x"}));
        notify("remote.sessionHealth", json!({"agent_id": "ra_x", "kind": "idle"}));
        let log = captured.lock().unwrap();
        assert_eq!(log.len(), 2);
        assert_eq!(log[0].0, "remote.bgProcessChanged");
        assert_eq!(log[0].1["agent_id"], "ra_x");
        assert_eq!(log[1].0, "remote.sessionHealth");
        assert_eq!(log[1].1["kind"], "idle");
    }

    #[test]
    fn install_guard_restores_prior_state_on_drop() {
        let _serial = serial();
        // No prior notifier — guard drop should leave None.
        let _prior = UI_NOTIFIER.write().unwrap().take();
        let captured: Arc<Mutex<Vec<(String, serde_json::Value)>>> = Arc::new(Mutex::new(Vec::new()));
        let cap = captured.clone();
        let notifier: Arc<NotifierBody> = Arc::new(move |n, p| cap.lock().unwrap().push((n.to_string(), p)));
        {
            let _g = install_for_test(notifier);
            notify("with_guard", json!({}));
            assert_eq!(captured.lock().unwrap().len(), 1);
        }
        // Guard dropped — notifier should be gone.
        notify("after_guard", json!({}));
        assert_eq!(
            captured.lock().unwrap().len(),
            1,
            "notifier must not fire after guard drop"
        );
    }

    #[test]
    fn install_guard_restores_prior_notifier() {
        let _serial = serial();
        let _prior = UI_NOTIFIER.write().unwrap().take();
        let first: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));
        let first_c = first.clone();
        let first_n: Arc<NotifierBody> = Arc::new(move |_, _| *first_c.lock().unwrap() += 1);
        set_ui_notifier(first_n);
        notify("a", json!({}));
        assert_eq!(*first.lock().unwrap(), 1);

        let second: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));
        let second_c = second.clone();
        let second_n: Arc<NotifierBody> = Arc::new(move |_, _| *second_c.lock().unwrap() += 1);
        {
            let _g = install_for_test(second_n);
            notify("b", json!({}));
            assert_eq!(*first.lock().unwrap(), 1, "first should NOT fire under second");
            assert_eq!(*second.lock().unwrap(), 1);
        }
        // Guard dropped — first notifier restored.
        notify("c", json!({}));
        assert_eq!(*first.lock().unwrap(), 2, "first should fire again after guard drop");
        assert_eq!(*second.lock().unwrap(), 1, "second should NOT fire after guard drop");
    }
}
