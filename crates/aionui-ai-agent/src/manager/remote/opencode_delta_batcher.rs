//! Per-part SSE delta batcher for OpenCode `message.part.delta` events (E03).
//!
//! OpenCode emits a `message.part.delta` per streamed token. Forwarding each
//! one directly through `AgentRuntime::emit` saturates the IPC channel during
//! fast generation — every token causes a full round-trip through
//! serialization, ipcMain, ipcRenderer, store update, and re-render.
//!
//! This batcher coalesces deltas for the same `(messageID, partID, field)` on
//! a ~60 Hz frame (`FLUSH_FRAME_MS = 16`). The first delta for a key arms a
//! single-shot timer; subsequent deltas within the window are concatenated;
//! when the timer fires the accumulated text emits as one `Text`/`Thinking`
//! event. This drops the IPC frame count by roughly (tokens-per-second / 60)
//! while keeping streaming visually instantaneous.
//!
//! Forced flushes happen at two points:
//!
//! * `flush_part` on `message.part.updated` for the same part — the server has
//!   finalized that part, so we drain immediately rather than waiting for the
//!   timer.
//! * `flush_all` on root-turn finish — the terminal `Finish` event marks the
//!   relay shut, so any not-yet-flushed accumulator would be lost.
//!
//! `is_reasoning` is captured at the FIRST delta for a key and held for the
//! rest of that key's lifetime, mirroring the pre-batching behavior in which
//! the renderer-visible event kind (`Text` vs `Thinking`) was decided at emit
//! time from `state.reasoning_parts`. The reasoning flag for a given partID is
//! fixed at part creation (set by `message.part.updated` for `type=reasoning`),
//! so first-delta-wins is the correct invariant in practice.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::agent_runtime::AgentRuntime;
use crate::protocol::events::{AgentStreamEvent, TextEventData, ThinkingEventData};

/// 60 Hz frame interval. Coalesces token deltas within one render frame.
const FLUSH_FRAME_MS: u64 = 16;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct DeltaKey {
    message_id: String,
    part_id: String,
    field: String,
}

struct PendingDelta {
    buffer: String,
    is_reasoning: bool,
    flush_handle: Option<JoinHandle<()>>,
}

#[derive(Default)]
struct DeltaBatcherInner {
    pending: HashMap<DeltaKey, PendingDelta>,
}

/// Cloneable handle to the per-part delta accumulator. Cheap to clone — only
/// the shared `Arc<Mutex<…>>` and a `Clone` of the runtime are copied. Clones
/// share state, so a spawned flush timer can drain its own entry.
#[derive(Clone)]
pub(super) struct DeltaBatcherHandle {
    inner: Arc<Mutex<DeltaBatcherInner>>,
    runtime: AgentRuntime,
}

impl DeltaBatcherHandle {
    pub fn new(runtime: AgentRuntime) -> Self {
        Self {
            inner: Arc::new(Mutex::new(DeltaBatcherInner::default())),
            runtime,
        }
    }

    /// Queue a delta token for the given key. Schedules a flush timer if
    /// none is pending for that key.
    pub async fn push(&self, message_id: &str, part_id: &str, field: &str, delta: &str, is_reasoning: bool) {
        let key = DeltaKey {
            message_id: message_id.to_string(),
            part_id: part_id.to_string(),
            field: field.to_string(),
        };
        let needs_timer;
        {
            let mut guard = self.inner.lock().await;
            let entry = guard.pending.entry(key.clone()).or_insert_with(|| PendingDelta {
                buffer: String::new(),
                is_reasoning,
                flush_handle: None,
            });
            entry.buffer.push_str(delta);
            needs_timer = entry.flush_handle.is_none();
        }
        if needs_timer {
            let me = self.clone();
            let timer_key = key.clone();
            let handle = tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(FLUSH_FRAME_MS)).await;
                me.flush_key(&timer_key).await;
            });
            // Re-lock to attach the handle. If a concurrent flush_part /
            // flush_all has already drained the entry between the unlock
            // above and here, the entry is gone — the timer will wake up,
            // find nothing to flush, and exit cleanly.
            let mut guard = self.inner.lock().await;
            if let Some(entry) = guard.pending.get_mut(&key) {
                entry.flush_handle = Some(handle);
            } else {
                handle.abort();
            }
        }
    }

    async fn flush_key(&self, key: &DeltaKey) {
        let pending = {
            let mut guard = self.inner.lock().await;
            guard.pending.remove(key)
        };
        let Some(p) = pending else { return };
        // Detach (not abort) the timer handle — if we reached here from the
        // timer task itself, the future is already complete; if we reached
        // here from a forced flush, the still-pending timer will find the
        // entry gone and no-op.
        drop(p.flush_handle);
        if p.buffer.is_empty() {
            return;
        }
        if p.is_reasoning {
            self.runtime.emit(AgentStreamEvent::Thinking(ThinkingEventData {
                content: p.buffer,
                subject: None,
                duration: None,
                status: None,
            }));
        } else {
            self.runtime
                .emit(AgentStreamEvent::Text(TextEventData { content: p.buffer }));
        }
    }

    /// Drain and emit every pending entry whose `part_id` matches. Called
    /// when `message.part.updated` arrives for a part — the server has
    /// finalized that part, so we want what we've accumulated on-screen
    /// now rather than waiting for the timer.
    pub async fn flush_part(&self, part_id: &str) {
        let keys: Vec<DeltaKey> = {
            let guard = self.inner.lock().await;
            guard.pending.keys().filter(|k| k.part_id == part_id).cloned().collect()
        };
        for k in keys {
            self.flush_key(&k).await;
        }
    }

    /// Drain and emit every pending entry. Called on root-turn finish and
    /// on connection teardown so streamed text never lingers past the
    /// terminal event.
    pub async fn flush_all(&self) {
        let keys: Vec<DeltaKey> = {
            let guard = self.inner.lock().await;
            guard.pending.keys().cloned().collect()
        };
        for k in keys {
            self.flush_key(&k).await;
        }
    }
}
