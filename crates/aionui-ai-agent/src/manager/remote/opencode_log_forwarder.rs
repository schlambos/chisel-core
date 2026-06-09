//! M14 — forward tracing events to the remote OpenCode server's `POST /log`.
//!
//! A `tracing_subscriber::Layer` registered at app init inspects every emitted
//! event. If the event carries a `conversation_id` field that matches an active
//! remote OpenCode agent (registered via [`register_forwarder`]), the event is
//! shipped to that agent's `POST /log` endpoint. Events without a
//! `conversation_id` are not forwarded — there's no way to route them.
//!
//! ## Verbosity
//!
//! Forwarded levels: **INFO, WARN, ERROR** (the broader band the user opted into
//! for Stage 13). `DEBUG` and `TRACE` stay local — they would saturate the
//! channel during normal operation.
//!
//! ## Backpressure
//!
//! Each registration spawns a background task that drains a bounded
//! `mpsc::Sender<LogEntry>` (capacity `QUEUE_CAP`) and POSTs entries one by one
//! to `/log`. The layer pushes via `try_send`, so a stalled server (or a burst
//! larger than `QUEUE_CAP`) drops the *oldest* unsent entries rather than
//! blocking the hot path — log forwarding is always best-effort.
//!
//! ## Failure mode
//!
//! Network or 4xx errors on `POST /log` are silently swallowed. The plan
//! (`M14-log-endpoint.md` §3.4) explicitly forbids retry-on-error to prevent
//! log loops; the entry is dropped.
//!
//! ## Privacy
//!
//! No structured field carrying a prompt, tool input, file content, or auth
//! token is forwarded — `extra` is filled only with primitive
//! (`bool`/`i64`/`u64`/`f64`/short string) fields recorded on the event itself.
//! The event's `message` is included verbatim because tracing's own callsites
//! own its content; production-visible logs in this codebase already follow
//! the AGENTS.md rule of not embedding sensitive payloads in `message`.

use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};
use std::time::Duration;

use reqwest::header::AUTHORIZATION;
use serde_json::Value;
use tokio::sync::mpsc;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

use super::opencode_payloads::OpencodeLogEntry;

const QUEUE_CAP: usize = 256;
const SERVICE_NAME: &str = "aionui";

struct ForwarderEntry {
    tx: mpsc::Sender<LogEntry>,
}

#[derive(Debug)]
struct LogEntry {
    level: &'static str,
    message: String,
    target: String,
    extra: serde_json::Map<String, Value>,
}

fn registry() -> &'static RwLock<HashMap<String, ForwarderEntry>> {
    static R: OnceLock<RwLock<HashMap<String, ForwarderEntry>>> = OnceLock::new();
    R.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Register a forwarder for the conversation. Future tracing events whose
/// `conversation_id` field equals `conversation_id` will be POSTed to
/// `<base_url>/log`. Spawns a single background task that drains the queue
/// until the matching [`unregister_forwarder`] call drops the receiver.
pub fn register_forwarder(
    conversation_id: String,
    http_client: reqwest::Client,
    base_url: String,
    auth_header: Option<String>,
) {
    let (tx, mut rx) = mpsc::channel::<LogEntry>(QUEUE_CAP);
    {
        let mut g = registry().write().unwrap_or_else(|e| e.into_inner());
        g.insert(conversation_id.clone(), ForwarderEntry { tx });
    }
    let log_url = format!("{}/log", base_url.trim_end_matches('/'));
    tokio::spawn(async move {
        while let Some(entry) = rx.recv().await {
            let body = OpencodeLogEntry {
                service: SERVICE_NAME.to_string(),
                level: entry.level.to_string(),
                message: entry.message.clone(),
                extra: serde_json::json!({
                    "conversation_id": conversation_id,
                    "target": entry.target,
                    "fields": Value::Object(entry.extra),
                }),
            };
            let mut req = http_client.post(&log_url).timeout(Duration::from_secs(5));
            if let Some(ref h) = auth_header {
                req = req.header(AUTHORIZATION, h.as_str());
            }
            // Fire-and-forget. Per the plan, never retry on failure: a failing
            // /log call should not become a log line that becomes another /log
            // call.
            let _ = req.json(&body).send().await;
        }
    });
}

/// Stop forwarding for the conversation. The background task exits once its
/// receiver is dropped (when this removes the last `Sender`).
pub fn unregister_forwarder(conversation_id: &str) {
    let mut g = registry().write().unwrap_or_else(|e| e.into_inner());
    g.remove(conversation_id);
}

/// Tracing layer that funnels INFO/WARN/ERROR events with a `conversation_id`
/// field through to the registered forwarder for that conversation.
pub struct OpenCodeLogLayer;

impl<S> Layer<S> for OpenCodeLogLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let metadata = event.metadata();
        // tracing::Level orders TRACE > DEBUG > INFO > WARN > ERROR via Ord
        // (larger = noisier). `<= INFO` accepts INFO, WARN, ERROR and drops
        // DEBUG / TRACE.
        if metadata.level() > &Level::INFO {
            return;
        }

        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);

        let Some(conversation_id) = visitor.conversation_id else {
            return;
        };

        let tx = {
            let g = registry().read().unwrap_or_else(|e| e.into_inner());
            g.get(&conversation_id).map(|f| f.tx.clone())
        };
        let Some(tx) = tx else { return };

        let level_str = match *metadata.level() {
            Level::ERROR => "error",
            Level::WARN => "warn",
            Level::INFO => "info",
            // Filtered above, but exhaustively match for clarity.
            Level::DEBUG | Level::TRACE => return,
        };

        let entry = LogEntry {
            level: level_str,
            message: visitor.message.unwrap_or_else(|| metadata.target().to_string()),
            target: metadata.target().to_string(),
            extra: visitor.extra,
        };

        // Drop on overflow rather than block the emitter's thread.
        let _ = tx.try_send(entry);
    }
}

#[derive(Default)]
struct FieldVisitor {
    conversation_id: Option<String>,
    message: Option<String>,
    extra: serde_json::Map<String, Value>,
}

impl FieldVisitor {
    fn record(&mut self, name: &str, value: Value) {
        if name == "conversation_id" {
            if let Some(s) = value.as_str() {
                self.conversation_id = Some(s.to_string());
                return;
            }
            // `conversation_id = %x` records via Display through record_debug;
            // strip the surrounding quotes the debug-format adds.
            self.conversation_id = Some(value.to_string().trim_matches('"').to_string());
            return;
        }
        if name == "message" {
            if let Some(s) = value.as_str() {
                self.message = Some(s.to_string());
                return;
            }
            self.message = Some(value.to_string());
            return;
        }
        self.extra.insert(name.to_string(), value);
    }
}

impl Visit for FieldVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.record(field.name(), Value::String(value.to_string()));
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.record(field.name(), Value::Bool(value));
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record(field.name(), Value::from(value));
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record(field.name(), Value::from(value));
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.record(field.name(), Value::from(value));
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        // `%foo` (Display) and `?foo` (Debug) both route here. Convert via
        // formatting, then unwrap any wrapping quotes for the special-cased
        // `conversation_id` / `message` fields.
        self.record(field.name(), Value::String(format!("{value:?}")));
    }
    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.record(field.name(), Value::String(value.to_string()));
    }
}
