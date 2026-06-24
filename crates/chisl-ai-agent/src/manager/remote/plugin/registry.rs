//! Process-wide state for the OpenCode bridge plugin channel.
//!
//! Three concerns live here, all keyed by `remote_agent_id`:
//!
//! 1. **Per-agent connection state** — what the plugin told us at hello,
//!    whether the SSE event stream is currently open, last contact time.
//!    Drives `connected()` and surfaces in the `RemoteAgentPluginStatus`
//!    REST response.
//! 2. **Audit ring buffer** — small in-memory history (capped at 500
//!    entries per agent) for the renderer-side status panel. Production
//!    logs do NOT contain these records; only the redacted `summary`
//!    field is ever materialised into logs.
//! 3. **Push channel** — per-agent `tokio::sync::broadcast` so the SSE
//!    handler can publish events to whichever plugin subscriber is
//!    currently connected.
//!
//! Plus the [`PluginTokenValidator`] trait the server middleware uses to
//! resolve a bearer token back to the owning `remote_agent_id`; the
//! real implementation is wired in `services::remote` against
//! `IRemoteAgentRepository::find_by_plugin_token`.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;

use super::protocol::{PluginAuditRecord, PluginPushEvent};
use crate::manager::remote::local_fs_mcp::ShellApprover;

// ── Constants ─────────────────────────────────────────────────────

/// Soft cap on the audit ring buffer per agent. The renderer only
/// surfaces the first few entries; older ones are dropped silently on
/// overflow.
const AUDIT_RING_CAP: usize = 500;

/// Window during which a `hello` counts as "connected" even if the
/// SSE stream isn't currently open. Picked to outlast a short network
/// blip but still let the UI notice a truly gone plugin within a
/// minute.
const HELLO_CONNECTED_GRACE_MS: u64 = 60_000;

/// Capacity of each agent's push broadcast channel. The plugin
/// processes pushes near-instantly; a slow consumer (e.g. the user's
/// home network) shouldn't slow the dispatcher, so we use a moderate
/// cap and let `send` errors silently drop on overflow.
const PUSH_CHANNEL_CAP: usize = 256;

/// Soft cap on the per-agent sticky voice-mode state we remember for
/// replay when a plugin reconnects. The plugin only needs the most
/// recent toggle per session to recover state; the cap exists to keep
/// the in-memory footprint bounded for agents that run many short
/// sessions in sequence. LRU/insertion-order eviction — the oldest
/// entry is dropped when the cap is hit.
pub const STICKY_VOICE_MODE_CAP: usize = 64;

// ── Token validator trait ────────────────────────────────────────

/// Resolves a plugin-channel bearer token to its owning `remote_agent_id`.
///
/// The server middleware calls this once per request. The concrete
/// implementation lives in `services::remote` and is backed by
/// `IRemoteAgentRepository::find_by_plugin_token` — constant-time
/// comparison is the implementation's responsibility.
#[async_trait]
pub trait PluginTokenValidator: Send + Sync {
    async fn resolve(&self, token: &str) -> Option<String>;
}

// ── Connection state ─────────────────────────────────────────────

/// Per-agent liveness summary. Captures what we know about the
/// plugin's hello (versions, hook surface) plus two runtime signals
/// (events stream open, last hello timestamp). Read by
/// [`PluginRegistry::connected`] and by the `RemoteAgentPluginStatus`
/// renderer payload.
#[derive(Debug, Clone, Default)]
pub struct PluginConnectionState {
    pub last_hello_at_ms: Option<u64>,
    pub plugin_version: Option<String>,
    pub opencode_version: Option<String>,
    pub hooks: Vec<String>,
    pub events_stream_open: bool,
    pub hello_count: u64,
}

impl PluginConnectionState {
    /// True iff the events stream is currently open OR a `hello` was
    /// received within the grace window. See [`HELLO_CONNECTED_GRACE_MS`].
    pub fn is_connected(&self, now_ms: u64) -> bool {
        if self.events_stream_open {
            return true;
        }
        self.last_hello_at_ms
            .is_some_and(|t| now_ms.saturating_sub(t) <= HELLO_CONNECTED_GRACE_MS)
    }
}

// ── The registry itself ──────────────────────────────────────────

/// Per-agent state bundled together. Held under the registry's single
/// mutex — the registry is read/written at human (UI) and plugin
/// (network) speeds, not in a hot loop, so a coarse mutex is fine.
struct AgentEntry {
    state: PluginConnectionState,
    audit: VecDeque<PluginAuditRecord>,
    push_tx: tokio::sync::broadcast::Sender<PluginPushEvent>,
    /// Shell approver registered by the agent manager when the
    /// conversation becomes live. `None` when no conversation is
    /// currently bound to this agent row.
    shell_approver: Option<Arc<dyn ShellApprover>>,
    /// Sticky voice-mode state — the most recent toggle per session
    /// (or for the unsessioned case, keyed by the empty string).
    /// Replayed in insertion order when a plugin opens
    /// `/plugin/events`. Capped at [`STICKY_VOICE_MODE_CAP`].
    voice_sticky: VecDeque<StickyVoiceMode>,
}

/// One sticky voice-mode record. `session_key` is either the
/// session id the renderer set voice mode for, or the empty string
/// for the unsessioned case. The keying is what makes "latest wins"
/// per session work — re-setting the same `session_id` overwrites
/// the prior entry instead of growing the list.
#[derive(Debug, Clone)]
pub struct StickyVoiceMode {
    pub session_key: String,
    pub event: PluginPushEvent,
}

/// Process-wide singleton. `std::sync::Mutex` matches the local_fs_mcp
/// pattern (`tools.rs`); the lock is held briefly and the workload is
/// not latency-critical.
#[derive(Default)]
pub struct PluginRegistry {
    agents: Mutex<HashMap<String, AgentEntry>>,
}

static REGISTRY: OnceLock<Arc<PluginRegistry>> = OnceLock::new();

/// Get the process-wide registry. Installs one on first call so test
/// code (which doesn't go through `services::remote`'s bootstrap) can
/// still find something to talk to.
pub fn global() -> Arc<PluginRegistry> {
    REGISTRY.get_or_init(|| Arc::new(PluginRegistry::default())).clone()
}

/// Test-only: install a fresh registry (replaces any prior one). The
/// real production registry is installed at first access in
/// [`global`] and is never replaced.
#[cfg(any(test, feature = "test-support"))]
pub fn install_for_test(reg: Arc<PluginRegistry>) {
    // `OnceLock::set` only succeeds once per process; tests that need
    // a fresh registry should use the per-test constructor and the
    // `PluginServer` routes directly, instead of relying on the
    // process-wide singleton.
    let _ = REGISTRY.set(reg);
}

impl PluginRegistry {
    /// Construct a fresh, isolated registry. Tests use this to avoid
    /// leaking state between cases. Production code should use
    /// [`global`].
    pub fn new() -> Self {
        Self::default()
    }

    fn entry_mut<F, R>(&self, agent_id: &str, f: F) -> R
    where
        F: FnOnce(&mut AgentEntry) -> R,
    {
        let mut map = self.agents.lock().expect("PluginRegistry mutex poisoned");
        let entry = map.entry(agent_id.to_string()).or_insert_with(|| AgentEntry {
            state: PluginConnectionState::default(),
            audit: VecDeque::with_capacity(AUDIT_RING_CAP),
            push_tx: tokio::sync::broadcast::channel(PUSH_CHANNEL_CAP).0,
            shell_approver: None,
            voice_sticky: VecDeque::with_capacity(STICKY_VOICE_MODE_CAP),
        });
        f(entry)
    }

    fn entry(&self, agent_id: &str) -> Option<PluginConnectionState> {
        let map = self.agents.lock().expect("PluginRegistry mutex poisoned");
        map.get(agent_id).map(|e| e.state.clone())
    }

    // ── Connection state ────────────────────────────────────────

    /// Record a fresh `hello` for the given agent. Increments the
    /// counter, stores versions and hook list, and stamps the
    /// last-hello timestamp. Returns the new `hello_count` so callers
    /// can log it once per transition.
    pub fn record_hello(
        &self,
        agent_id: &str,
        plugin_version: String,
        opencode_version: Option<String>,
        hooks: Vec<String>,
    ) -> u64 {
        let now = chisl_common::now_ms();
        let is_probe = plugin_version == "0.0.0" && hooks.is_empty();
        self.entry_mut(agent_id, |e| {
            // Probe hellos (v0.0.0, no hooks) only update liveness timestamp —
            // they must not overwrite real plugin metadata from a prior hello.
            if !is_probe || e.state.plugin_version.is_none() {
                e.state.plugin_version = Some(plugin_version);
                e.state.opencode_version = opencode_version;
                e.state.hooks = hooks;
            }
            e.state.last_hello_at_ms = Some(now.max(0) as u64);
            e.state.hello_count += 1;
            e.state.hello_count
        })
    }

    pub fn set_events_stream_open(&self, agent_id: &str, open: bool) {
        self.entry_mut(agent_id, |e| {
            e.state.events_stream_open = open;
        });
    }

    pub fn connection_state(&self, agent_id: &str) -> PluginConnectionState {
        self.entry(agent_id).unwrap_or_default()
    }

    /// Connected = events stream open OR hello within the grace
    /// window. See [`PluginConnectionState::is_connected`].
    pub fn connected(&self, agent_id: &str) -> bool {
        let state = self.connection_state(agent_id);
        state.is_connected(chisl_common::now_ms().max(0) as u64)
    }

    // ── Audit ───────────────────────────────────────────────────

    /// Append a record to the ring buffer, dropping the oldest entry
    /// if we're at the cap. See [`AUDIT_RING_CAP`].
    pub fn record_audit(&self, agent_id: &str, record: PluginAuditRecord) {
        self.entry_mut(agent_id, |e| {
            if e.audit.len() == AUDIT_RING_CAP {
                e.audit.pop_front();
            }
            e.audit.push_back(record);
        });
    }

    /// Snapshot the audit log in chronological order (oldest first).
    /// Empty vec for unknown agents.
    pub fn audit_records(&self, agent_id: &str) -> Vec<PluginAuditRecord> {
        let map = self.agents.lock().expect("PluginRegistry mutex poisoned");
        map.get(agent_id)
            .map(|e| e.audit.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn audit_count(&self, agent_id: &str) -> u64 {
        let map = self.agents.lock().expect("PluginRegistry mutex poisoned");
        map.get(agent_id).map(|e| e.audit.len() as u64).unwrap_or(0)
    }

    // ── Shell approver ──────────────────────────────────────────

    /// Register (or replace) the shell approver an agent's
    /// conversation is using. The plugin webserver consults this when
    /// the plugin's `run_shell_streaming` tool fires; if `None`, the
    /// tool returns an SSE `error` event. A no-op for an empty
    /// `agent_id` (the caller should already guard, but we double
    /// check to keep the registry from accumulating orphan entries).
    pub fn register_shell_approver(&self, agent_id: &str, approver: Arc<dyn ShellApprover>) {
        if agent_id.is_empty() {
            return;
        }
        self.entry_mut(agent_id, |e| {
            e.shell_approver = Some(approver);
        });
    }

    pub fn unregister_shell_approver(&self, agent_id: &str) {
        if agent_id.is_empty() {
            return;
        }
        self.entry_mut(agent_id, |e| {
            e.shell_approver = None;
        });
    }

    pub fn shell_approver(&self, agent_id: &str) -> Option<Arc<dyn ShellApprover>> {
        let map = self.agents.lock().expect("PluginRegistry mutex poisoned");
        map.get(agent_id).and_then(|e| e.shell_approver.clone())
    }

    // ── Push channel ────────────────────────────────────────────

    /// Broadcast a push event to whichever plugin subscriber is
    /// currently connected. `send` errors are deliberately ignored:
    /// the plugin might be temporarily disconnected, in which case
    /// the next reconnection's hello handshake is what matters.
    pub fn push(&self, agent_id: &str, event: PluginPushEvent) {
        let map = self.agents.lock().expect("PluginRegistry mutex poisoned");
        if let Some(entry) = map.get(agent_id) {
            let _ = entry.push_tx.send(event);
        }
    }

    /// Subscribe to push events for an agent. The returned
    /// `Receiver` misses events sent before this call (broadcast
    /// channels don't replay); callers wanting initial state should
    /// follow up with a one-shot read of [`connection_state`].
    pub fn subscribe(&self, agent_id: &str) -> tokio::sync::broadcast::Receiver<PluginPushEvent> {
        // We have to take the lock to either look up an existing tx
        // or create a fresh entry. The entry creation is harmless
        // for an unknown agent — it just primes a slot so the
        // subscriber can attach before any `push` lands.
        let mut map = self.agents.lock().expect("PluginRegistry mutex poisoned");
        let entry = map.entry(agent_id.to_string()).or_insert_with(|| AgentEntry {
            state: PluginConnectionState::default(),
            audit: VecDeque::with_capacity(AUDIT_RING_CAP),
            push_tx: tokio::sync::broadcast::channel(PUSH_CHANNEL_CAP).0,
            shell_approver: None,
            voice_sticky: VecDeque::with_capacity(STICKY_VOICE_MODE_CAP),
        });
        entry.push_tx.subscribe()
    }

    // ── Sticky voice-mode (for SSE replay on reconnect) ───────

    /// Record the latest voice-mode toggle for the given session
    /// (empty string for the unsessioned case) as a sticky event.
    /// "Latest wins" per session — re-setting the same
    /// `session_key` overwrites the prior entry; the cap
    /// ([`STICKY_VOICE_MODE_CAP`]) only kicks in when a new
    /// `session_key` arrives and the list is full.
    pub fn set_sticky_voice_mode(&self, agent_id: &str, session_key: String, event: PluginPushEvent) {
        self.entry_mut(agent_id, |e| {
            if let Some(existing) = e.voice_sticky.iter_mut().find(|s| s.session_key == session_key) {
                existing.event = event;
                return;
            }
            if e.voice_sticky.len() >= STICKY_VOICE_MODE_CAP {
                e.voice_sticky.pop_front();
            }
            e.voice_sticky.push_back(StickyVoiceMode { session_key, event });
        });
    }

    /// Snapshot the sticky voice-mode events in insertion order
    /// (oldest first). Empty for an unknown agent. The SSE handler
    /// replays this list verbatim after the initial `ping` so a
    /// freshly-connected plugin recovers its state.
    pub fn sticky_voice_mode(&self, agent_id: &str) -> Vec<PluginPushEvent> {
        let map = self.agents.lock().expect("PluginRegistry mutex poisoned");
        map.get(agent_id)
            .map(|e| e.voice_sticky.iter().map(|s| s.event.clone()).collect())
            .unwrap_or_default()
    }

    /// Test-only: clear sticky voice-mode state for one agent. Used
    /// by tests that build an isolated registry and want a fresh
    /// slate between cases.
    #[cfg(any(test, feature = "test-support"))]
    pub fn clear_sticky_voice_mode(&self, agent_id: &str) {
        self.entry_mut(agent_id, |e| e.voice_sticky.clear());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::remote::local_fs_mcp::ShellApproval;

    fn registry() -> PluginRegistry {
        PluginRegistry::new()
    }

    #[test]
    fn fresh_agent_has_default_state() {
        let r = registry();
        let s = r.connection_state("ra_x");
        assert!(s.last_hello_at_ms.is_none());
        assert!(!s.events_stream_open);
        assert!(s.hooks.is_empty());
        assert_eq!(s.hello_count, 0);
    }

    #[test]
    fn record_hello_increments_and_stamps_state() {
        let r = registry();
        let n = r.record_hello(
            "ra_x",
            "0.1.0".to_string(),
            Some("1.2.3".to_string()),
            vec!["tool.before".into(), "session.idle".into()],
        );
        assert_eq!(n, 1);
        let s = r.connection_state("ra_x");
        assert_eq!(s.plugin_version.as_deref(), Some("0.1.0"));
        assert_eq!(s.opencode_version.as_deref(), Some("1.2.3"));
        assert_eq!(s.hooks.len(), 2);
        assert!(s.last_hello_at_ms.is_some());
    }

    #[test]
    fn connected_reflects_stream_or_recent_hello() {
        let r = registry();
        assert!(!r.connected("ra_x"), "no hello, no stream → not connected");
        r.record_hello("ra_x", "0.1.0".into(), None, vec![]);
        assert!(r.connected("ra_x"), "recent hello → connected");
        r.set_events_stream_open("ra_x", false);
        assert!(r.connected("ra_x"));
        r.set_events_stream_open("ra_x", true);
        assert!(r.connected("ra_x"));
    }

    #[test]
    fn events_stream_toggle_does_not_drop_hello_count() {
        let r = registry();
        r.record_hello("ra_x", "0.1.0".into(), None, vec![]);
        r.set_events_stream_open("ra_x", true);
        r.set_events_stream_open("ra_x", false);
        let s = r.connection_state("ra_x");
        assert_eq!(s.hello_count, 1);
        assert!(!s.events_stream_open);
    }

    #[tokio::test]
    async fn audit_ring_caps_at_500() {
        let r = registry();
        for i in 0..600 {
            r.record_audit(
                "ra_x",
                PluginAuditRecord {
                    kind: "tool.before".into(),
                    tool: Some("read".into()),
                    session_id: None,
                    call_id: None,
                    at_ms: i,
                    summary: format!("n={i}"),
                },
            );
        }
        let all = r.audit_records("ra_x");
        assert_eq!(all.len(), 500);
        // Oldest should be 100 (we dropped the first 100).
        assert_eq!(all.first().unwrap().at_ms, 100);
        assert_eq!(all.last().unwrap().at_ms, 599);
        assert_eq!(r.audit_count("ra_x"), 500);
    }

    #[test]
    fn unknown_agent_audit_is_empty() {
        let r = registry();
        assert!(r.audit_records("ra_unknown").is_empty());
        assert_eq!(r.audit_count("ra_unknown"), 0);
    }

    struct FixedApprover(ShellApproval);
    #[async_trait]
    impl ShellApprover for FixedApprover {
        async fn approve_shell(&self, _command: &str, _cwd: &str) -> ShellApproval {
            self.0
        }
    }

    #[test]
    fn shell_approver_register_and_unregister() {
        let r = registry();
        assert!(r.shell_approver("ra_x").is_none());
        let a: Arc<dyn ShellApprover> = Arc::new(FixedApprover(ShellApproval::Allow));
        r.register_shell_approver("ra_x", a);
        assert!(r.shell_approver("ra_x").is_some());
        r.unregister_shell_approver("ra_x");
        assert!(r.shell_approver("ra_x").is_none());
    }

    #[test]
    fn register_with_empty_agent_id_is_noop() {
        let r = registry();
        let a: Arc<dyn ShellApprover> = Arc::new(FixedApprover(ShellApproval::Allow));
        r.register_shell_approver("", a);
        // Should not create a phantom entry.
        assert!(r.shell_approver("").is_none());
        assert_eq!(r.audit_count(""), 0);
    }

    #[tokio::test]
    async fn push_delivers_to_subscriber() {
        let r = registry();
        let mut rx = r.subscribe("ra_x");
        r.push(
            "ra_x",
            PluginPushEvent {
                event: "ping".into(),
                data: serde_json::json!({"n": 1}),
            },
        );
        let got = rx.recv().await.unwrap();
        assert_eq!(got.event, "ping");
        assert_eq!(got.data["n"], 1);
    }

    #[tokio::test]
    async fn push_to_unknown_agent_is_silent_noop() {
        let r = registry();
        // No subscriber; push should not panic.
        r.push(
            "ra_unknown",
            PluginPushEvent {
                event: "x".into(),
                data: serde_json::Value::Null,
            },
        );
    }

    #[tokio::test]
    async fn subscribe_before_push_does_not_replay() {
        let r = registry();
        // Push first, then subscribe. Broadcast channel's send has
        // happened before any receiver exists, so the message is gone.
        r.push(
            "ra_x",
            PluginPushEvent {
                event: "early".into(),
                data: serde_json::Value::Null,
            },
        );
        let mut rx = r.subscribe("ra_x");
        // No "early" event should arrive.
        r.push(
            "ra_x",
            PluginPushEvent {
                event: "late".into(),
                data: serde_json::json!({}),
            },
        );
        let got = tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
            .await
            .expect("timeout")
            .expect("recv");
        assert_eq!(got.event, "late");
    }

    #[test]
    fn sticky_voice_mode_unknown_agent_is_empty() {
        let r = registry();
        assert!(r.sticky_voice_mode("ra_unknown").is_empty());
    }

    #[test]
    fn sticky_voice_mode_latest_wins_per_session() {
        let r = registry();
        // Two different sessions: A and B.
        r.set_sticky_voice_mode(
            "ra_x",
            "ses_a".into(),
            PluginPushEvent {
                event: "voice_mode".into(),
                data: serde_json::json!({"type": "voice_mode", "data": {"sessionID": "ses_a", "enabled": true}}),
            },
        );
        r.set_sticky_voice_mode(
            "ra_x",
            "ses_b".into(),
            PluginPushEvent {
                event: "voice_mode".into(),
                data: serde_json::json!({"type": "voice_mode", "data": {"sessionID": "ses_b", "enabled": true}}),
            },
        );
        // Re-set ses_a with a new payload — should overwrite, not append.
        r.set_sticky_voice_mode(
            "ra_x",
            "ses_a".into(),
            PluginPushEvent {
                event: "voice_mode".into(),
                data: serde_json::json!({"type": "voice_mode", "data": {"sessionID": "ses_a", "enabled": false}}),
            },
        );
        let entries = r.sticky_voice_mode("ra_x");
        assert_eq!(entries.len(), 2, "no duplicate for re-set session");
        // Insertion order: ses_a first (overwrite, kept in place), then ses_b.
        assert_eq!(entries[0].data["data"]["enabled"], false);
        assert_eq!(entries[0].data["data"]["sessionID"], "ses_a");
        assert_eq!(entries[1].data["data"]["sessionID"], "ses_b");
    }

    #[test]
    fn sticky_voice_mode_caps_at_limit() {
        let r = registry();
        for i in 0..(STICKY_VOICE_MODE_CAP + 5) {
            r.set_sticky_voice_mode(
                "ra_x",
                format!("ses_{i}"),
                PluginPushEvent {
                    event: "voice_mode".into(),
                    data: serde_json::json!({"n": i}),
                },
            );
        }
        let entries = r.sticky_voice_mode("ra_x");
        assert_eq!(entries.len(), STICKY_VOICE_MODE_CAP, "cap enforced");
        // The first 5 were evicted; the oldest survivor is ses_5.
        assert_eq!(entries[0].data["n"], 5);
        assert_eq!(entries.last().unwrap().data["n"], STICKY_VOICE_MODE_CAP as i64 + 4);
    }

    #[test]
    fn sticky_voice_mode_unsessioned_uses_empty_key() {
        let r = registry();
        r.set_sticky_voice_mode(
            "ra_x",
            String::new(),
            PluginPushEvent {
                event: "voice_mode".into(),
                data: serde_json::json!({"enabled": true}),
            },
        );
        let entries = r.sticky_voice_mode("ra_x");
        assert_eq!(entries.len(), 1);
    }
}
