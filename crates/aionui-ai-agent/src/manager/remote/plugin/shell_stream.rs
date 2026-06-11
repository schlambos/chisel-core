//! Streaming shell execution for the OpenCode bridge plugin.
//!
//! Mirrors the local fs MCP's [`crate::manager::remote::local_fs_mcp::shell::run_shell`] but
//! streams the child process's stdout/stderr to the plugin over the
//! `/tools/run_shell_streaming` SSE response instead of buffering it. The
//! user-facing approval flow is the same — the plugin webserver hands the
//! command to the agent's [`ShellApprover`], which surfaces it to the
//! renderer's confirmation UI, and only runs the command once the user
//! has said yes.
//!
//! Production logs are intentionally quiet:
//!
//! - The command body never appears in any `info!` / `warn!` / `error!` line.
//! - The audit record that the registry stores carries the first 80 chars
//!   of the command in its `summary` field (caller-controlled) — anything
//!   longer is truncated, and the full body is never materialised into a
//!   log line.
//! - Exit codes, timeouts, byte caps, and the streaming cancel reason are
//!   logged at `debug!` so production runs can stay at `info`.
//!
//! Two caps are enforced:
//!
//! - **Timeout** — default 120 s, per-request override capped at 1 h.
//! - **Total bytes** — 4 MiB. Beyond that, the stream stops forwarding new
//!   chunks (we keep draining so the process doesn't block on a full pipe
//!   and we still observe the real exit code), and the final `done` event
//!   carries `truncated: true`.
//!
//! Output chunks are emitted as utf-8 lossy slices so a binary blob in a
//! compile step doesn't get a chunk event rejected for not being valid
//! text. SSE event names match the wire spec: `chunk` (with
//! `{stream, data}`), `done` (with `{exitCode, isError, truncated}`), and
//! `error` (with `{message}`).

use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::response::sse::Event;
use futures_util::Stream;
use serde_json::json;
use tokio::io::{AsyncReadExt, BufReader};
use tokio::process::Child;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use aionui_runtime::Builder;

use super::protocol::RunShellStreamingRequest;
use crate::manager::remote::local_fs_mcp::shell::{McpRequestContext, ShellApproval, ShellApprover};

/// Default wall-clock cap on a single command. Mirrors the
/// `local_fs_mcp::shell::SHELL_TIMEOUT` so the plugin's streaming tool
/// and the synchronous fs MCP tool can't disagree by accident.
const DEFAULT_TIMEOUT_SECS: u64 = 120;

/// Hard cap on per-request timeout. The plugin is expected to be on the
/// local network; an hour is more than any plausible build / test run.
const MAX_TIMEOUT_SECS: u64 = 3600;

/// 4 MiB of streamed body — `local_fs_mcp::shell::MAX_SHELL_OUTPUT` is
/// 1 MiB; we quadruple it for the streaming case because the SSE
/// transport can carry more without choking the renderer's chat view.
const STREAM_BYTE_CAP: usize = 4 * 1024 * 1024;

/// Chunk size for stdout / stderr reads. Small enough to keep
/// per-event latency low; large enough that we don't pay an
/// `AsyncReadExt` round-trip per byte.
const READ_CHUNK: usize = 8 * 1024;

/// Outcome of the streaming run, surfaceable in the final `done` event.
struct RunOutcome {
    exit_code: Option<i32>,
    is_error: bool,
    truncated: bool,
}

/// One wire event emitted by the streaming shell tool. We use a custom
/// enum (not axum's `Event`) inside the streaming pipeline so the
/// readers and approver paths can be unit-tested without
/// going through axum. The `to_sse_event` adapter is the only place
/// that touches axum's `Event`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ShellStreamEvent {
    Chunk {
        stream: String,
        data: String,
    },
    Done {
        exit_code: Option<i32>,
        is_error: bool,
        truncated: bool,
    },
    Error {
        message: String,
    },
}

impl ShellStreamEvent {
    fn to_sse_event(&self) -> Event {
        match self {
            ShellStreamEvent::Chunk { stream, data } => Event::default()
                .event("chunk")
                .data(json!({"stream": stream, "data": data}).to_string()),
            ShellStreamEvent::Done {
                exit_code,
                is_error,
                truncated,
            } => Event::default().event("done").data(
                json!({
                    "exitCode": exit_code,
                    "isError": is_error,
                    "truncated": truncated,
                })
                .to_string(),
            ),
            ShellStreamEvent::Error { message } => Event::default()
                .event("error")
                .data(json!({"message": message}).to_string()),
        }
    }
}

/// Stream the result of a `RunShellStreamingRequest` as SSE events. The
/// returned stream emits `chunk` events as bytes arrive, then a single
/// terminal `done` event with the exit code, is_error, and truncated
/// flag. A failure to even spawn the child is reported as a single
/// `error` event followed by `done {exitCode: null, isError: true,
/// truncated: false}`.
///
/// `default_cwd` is the workspace root the agent conversation is bound
/// to; the request's own `cwd` overrides it if it's a real directory
/// (canonicalised). `default_session_id` is used for the
/// `McpRequestContext` passed to the approver (so the host can route
/// the prompt to the right sub-agent surface) when the request omits a
/// session id — it always does today, but we keep the fallback
/// symmetric with `local_fs_mcp::shell`.
pub fn run_shell_streaming(
    request: RunShellStreamingRequest,
    approver: Arc<dyn ShellApprover>,
    default_cwd: &Path,
    default_session_id: &str,
) -> Pin<Box<dyn Stream<Item = Result<Event, std::convert::Infallible>> + Send>> {
    // Materialise channel + outcome into a single async task; the
    // returned stream is a thin `mpsc::Receiver → Event` adapter. This
    // keeps the streaming state (child + readers + timeout) confined
    // to one task and the SSE handler oblivious to tokio process
    // plumbing.
    let (tx, rx) = mpsc::channel::<ShellStreamEvent>(32);

    let default_cwd = default_cwd.to_path_buf();
    let approver = approver;
    let session_id = if request.session_id.is_empty() {
        default_session_id.to_string()
    } else {
        request.session_id.clone()
    };
    let command = request.command.clone();
    let cwd_override = request.cwd.clone();
    let timeout_secs = request.timeout_secs.unwrap_or(DEFAULT_TIMEOUT_SECS);

    // Run the actual work in a dedicated task and collect events on a
    // stream the SSE handler can await.
    tokio::spawn(async move {
        run_shell_inner(
            tx,
            approver,
            default_cwd,
            session_id,
            command,
            cwd_override,
            timeout_secs,
        )
        .await;
    });

    Box::pin(ChannelStream { rx })
}

/// Pure async body for the streaming shell task; separated so the
/// public entry point stays small and so we can test the inner
/// event-emission logic by collecting from a channel.
async fn run_shell_inner(
    tx: mpsc::Sender<ShellStreamEvent>,
    approver: Arc<dyn ShellApprover>,
    default_cwd: PathBuf,
    session_id: String,
    command: String,
    cwd_override: Option<String>,
    timeout_secs: u64,
) {
    let ctx = McpRequestContext {
        session_id: Some(session_id.clone()),
        parent_session_id: None,
    };
    let cwd = resolve_cwd(cwd_override.as_deref(), &default_cwd);

    let approval = approver
        .approve_shell_with_context(&command, &cwd.to_string_lossy(), &ctx)
        .await;
    match approval {
        ShellApproval::Allow => {}
        ShellApproval::Reject => {
            emit_and_done(
                &tx,
                ShellStreamEvent::Error {
                    message: "shell command rejected by user".to_string(),
                },
                RunOutcome {
                    exit_code: None,
                    is_error: true,
                    truncated: false,
                },
            )
            .await;
            return;
        }
        ShellApproval::TimedOut => {
            emit_and_done(
                &tx,
                ShellStreamEvent::Error {
                    message: "shell approval timed out".to_string(),
                },
                RunOutcome {
                    exit_code: None,
                    is_error: true,
                    truncated: false,
                },
            )
            .await;
            return;
        }
    }

    let spec = crate::manager::remote::local_fs_mcp::shell::resolve_shell();
    let (program, arg) = spec.parts();
    let mut builder = Builder::clean_cli(program);
    builder.arg(arg).arg(&command).current_dir(&cwd);

    let child = match builder.spawn() {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "failed to spawn shell for plugin streaming tool");
            emit_and_done(
                &tx,
                ShellStreamEvent::Error {
                    message: format!("failed to spawn shell: {e}"),
                },
                RunOutcome {
                    exit_code: None,
                    is_error: true,
                    truncated: false,
                },
            )
            .await;
            return;
        }
    };

    let outcome = drive_child(child, &tx, timeout_secs).await;
    let _ = tx
        .send(ShellStreamEvent::Done {
            exit_code: outcome.exit_code,
            is_error: outcome.is_error,
            truncated: outcome.truncated,
        })
        .await;
    tracing::trace!(
        ts_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
        exit_code = outcome.exit_code,
        "plugin_shell_done",
    );
}

/// Inner task: pull the child's stdout/stderr, forward `chunk` events
/// up to `STREAM_BYTE_CAP`, enforce `timeout_secs`, and report the final
/// exit code / truncation status.
async fn drive_child(mut child: Child, tx: &mpsc::Sender<ShellStreamEvent>, timeout_secs: u64) -> RunOutcome {
    let mut total_bytes: usize = 0;
    let mut truncated = false;
    let mut chunk_seq: u64 = 0;

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    let (out_tx, mut out_rx) = mpsc::channel::<ChunkEvent>(32);
    if let Some(out) = stdout {
        spawn_reader(out, "stdout".to_string(), out_tx.clone());
    }
    if let Some(err) = stderr {
        spawn_reader(err, "stderr".to_string(), out_tx.clone());
    }
    drop(out_tx);

    let timeout = Duration::from_secs(timeout_secs.clamp(1, MAX_TIMEOUT_SECS));
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        tokio::select! {
            biased;
            wait_result = child.wait() => {
                match wait_result {
                    Ok(status) => {
                        while let Ok(ev) = out_rx.try_recv() {
                            if forward_chunk(tx, ev, &mut total_bytes, &mut truncated, &mut chunk_seq)
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        let exit_code = status.code();
                        return RunOutcome {
                            exit_code,
                            is_error: !status.success(),
                            truncated,
                        };
                    }
                    Err(e) => {
                        warn!(error = %e, "shell wait failed in plugin streaming tool");
                        return RunOutcome { exit_code: None, is_error: true, truncated };
                    }
                }
            }
            maybe_chunk = out_rx.recv() => {
                match maybe_chunk {
                    Some(ev) => {
                        if forward_chunk(tx, ev, &mut total_bytes, &mut truncated, &mut chunk_seq)
                            .await
                            .is_err()
                        {
                            let _ = child.kill().await;
                            return RunOutcome { exit_code: None, is_error: true, truncated };
                        }
                    }
                    None => {
                        match child.wait().await {
                            Ok(status) => {
                                return RunOutcome {
                                    exit_code: status.code(),
                                    is_error: !status.success(),
                                    truncated,
                                };
                            }
                            Err(_) => {
                                return RunOutcome { exit_code: None, is_error: true, truncated };
                            }
                        }
                    }
                }
            }
            _ = tokio::time::sleep_until(deadline) => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                return RunOutcome { exit_code: None, is_error: true, truncated };
            }
            // Client disconnected (all SSE receivers dropped). A
            // command producing no output would otherwise sit here
            // until the timeout — up to an hour — with nobody
            // listening. Kill the child and return a synthetic
            // is_error outcome.
            _ = tx.closed() => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                return RunOutcome { exit_code: None, is_error: true, truncated };
            }
        }
    }
}

/// Single chunk from one of the readers.
struct ChunkEvent {
    stream: String,
    data: String,
}

fn spawn_reader<R: AsyncReadExt + Unpin + Send + 'static>(pipe: R, stream: String, tx: mpsc::Sender<ChunkEvent>) {
    tokio::spawn(async move {
        let mut reader = BufReader::with_capacity(READ_CHUNK, pipe);
        let mut buf = vec![0u8; READ_CHUNK];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    let text = String::from_utf8_lossy(&buf[..n]).into_owned();
                    if tx
                        .send(ChunkEvent {
                            stream: stream.clone(),
                            data: text,
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
}

async fn forward_chunk(
    tx: &mpsc::Sender<ShellStreamEvent>,
    ev: ChunkEvent,
    total_bytes: &mut usize,
    truncated: &mut bool,
    chunk_seq: &mut u64,
) -> Result<(), ()> {
    if *truncated {
        return Ok(());
    }
    let new_total = *total_bytes + ev.data.len();
    if new_total > STREAM_BYTE_CAP {
        *truncated = true;
        let remaining = STREAM_BYTE_CAP.saturating_sub(*total_bytes);
        if remaining > 0 {
            let truncated_text = truncate_lossy(&ev.data, remaining);
            *total_bytes += truncated_text.len();
            let stream_name = ev.stream.clone();
            let data_len = truncated_text.len();
            if tx
                .send(ShellStreamEvent::Chunk {
                    stream: ev.stream,
                    data: truncated_text,
                })
                .await
                .is_err()
            {
                return Err(());
            }
            *chunk_seq += 1;
            tracing::trace!(
                chunk_seq = *chunk_seq,
                ts_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
                stream = %stream_name,
                bytes = data_len,
                "plugin_shell_chunk_emit",
            );
        }
        debug!(
            total_bytes = *total_bytes,
            cap = STREAM_BYTE_CAP,
            "plugin streaming tool hit byte cap; truncating further output",
        );
        return Ok(());
    }
    *total_bytes = new_total;
    let stream_name = ev.stream.clone();
    let data_len = ev.data.len();
    if tx
        .send(ShellStreamEvent::Chunk {
            stream: ev.stream,
            data: ev.data,
        })
        .await
        .is_err()
    {
        return Err(());
    }
    *chunk_seq += 1;
    tracing::trace!(
        chunk_seq = *chunk_seq,
        ts_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
        stream = %stream_name,
        bytes = data_len,
        "plugin_shell_chunk_emit",
    );
    Ok(())
}

async fn emit_and_done(tx: &mpsc::Sender<ShellStreamEvent>, error_event: ShellStreamEvent, outcome: RunOutcome) {
    if tx.send(error_event).await.is_err() {
        return;
    }
    let _ = tx
        .send(ShellStreamEvent::Done {
            exit_code: outcome.exit_code,
            is_error: outcome.is_error,
            truncated: outcome.truncated,
        })
        .await;
}

fn resolve_cwd(request_cwd: Option<&str>, default_cwd: &Path) -> PathBuf {
    if let Some(req) = request_cwd {
        let p = PathBuf::from(req);
        if p.is_dir() {
            if let Ok(canonical) = p.canonicalize() {
                return canonical;
            }
            return p;
        }
    }
    default_cwd.to_path_buf()
}

fn truncate_lossy(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut cut = max;
    while cut > 0 && !s.is_char_boundary(cut) {
        cut -= 1;
    }
    s[..cut].to_string()
}

/// Adapter from `mpsc::Receiver<ShellStreamEvent>` to
/// `Stream<Item = Result<Event, Infallible>>`.
struct ChannelStream {
    rx: mpsc::Receiver<ShellStreamEvent>,
}

impl Stream for ChannelStream {
    type Item = Result<Event, std::convert::Infallible>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.rx.poll_recv(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some(Ok(item.to_sse_event()))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Internal accessor that lets tests run the streaming tool's event
/// loop without going through the axum SSE adapter. Returned as a
/// plain stream of `ShellStreamEvent` so test code can assert on the
/// enum directly.
#[cfg(test)]
async fn run_shell_streaming_events(
    request: RunShellStreamingRequest,
    approver: Arc<dyn ShellApprover>,
    default_cwd: &Path,
    default_session_id: &str,
) -> Vec<ShellStreamEvent> {
    let (tx, mut rx) = mpsc::channel::<ShellStreamEvent>(32);

    let default_cwd = default_cwd.to_path_buf();
    let session_id = if request.session_id.is_empty() {
        default_session_id.to_string()
    } else {
        request.session_id.clone()
    };
    let command = request.command.clone();
    let cwd_override = request.cwd.clone();
    let timeout_secs = request.timeout_secs.unwrap_or(DEFAULT_TIMEOUT_SECS);

    let handle = tokio::spawn(async move {
        run_shell_inner(
            tx,
            approver,
            default_cwd,
            session_id,
            command,
            cwd_override,
            timeout_secs,
        )
        .await;
    });

    let mut out = Vec::new();
    while let Some(ev) = rx.recv().await {
        out.push(ev);
    }
    let _ = handle.await;
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::remote::local_fs_mcp::shell::ShellApproval;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct AllowAll;
    #[async_trait]
    impl ShellApprover for AllowAll {
        async fn approve_shell(&self, _command: &str, _cwd: &str) -> ShellApproval {
            ShellApproval::Allow
        }
    }

    struct FixedApproval(ShellApproval);
    #[async_trait]
    impl ShellApprover for FixedApproval {
        async fn approve_shell(&self, _command: &str, _cwd: &str) -> ShellApproval {
            self.0
        }
    }

    struct CountingAllow {
        count: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl ShellApprover for CountingAllow {
        async fn approve_shell(&self, _command: &str, _cwd: &str) -> ShellApproval {
            self.count.fetch_add(1, Ordering::SeqCst);
            ShellApproval::Allow
        }
    }

    fn req(command: &str) -> RunShellStreamingRequest {
        RunShellStreamingRequest {
            command: command.to_string(),
            cwd: None,
            session_id: "ses_test".to_string(),
            call_id: None,
            timeout_secs: None,
        }
    }

    #[tokio::test]
    async fn streams_stdout_chunks_and_done() {
        let dir = tempfile::tempdir().unwrap();
        let approver: Arc<dyn ShellApprover> = Arc::new(AllowAll);
        let events =
            run_shell_streaming_events(req("echo hello_from_streaming"), approver, dir.path(), "ses_test").await;

        let chunks: Vec<&str> = events
            .iter()
            .filter_map(|e| match e {
                ShellStreamEvent::Chunk { data, .. } => Some(data.as_str()),
                _ => None,
            })
            .collect();
        let done = events.iter().find_map(|e| match e {
            ShellStreamEvent::Done {
                exit_code,
                is_error,
                truncated,
            } => Some((*exit_code, *is_error, *truncated)),
            _ => None,
        });
        assert!(
            chunks.iter().any(|c| c.contains("hello_from_streaming")),
            "missing stdout in chunks: {chunks:?}"
        );
        let (code, err, trunc) = done.expect("stream ended without a done event");
        assert_eq!(code, Some(0));
        assert!(!err);
        assert!(!trunc);
    }

    #[tokio::test]
    async fn rejection_emits_error_event() {
        let dir = tempfile::tempdir().unwrap();
        let approver: Arc<dyn ShellApprover> = Arc::new(FixedApproval(ShellApproval::Reject));
        let events = run_shell_streaming_events(req("echo should_not_run"), approver, dir.path(), "ses_test").await;

        let saw_error = events
            .iter()
            .any(|e| matches!(e, ShellStreamEvent::Error { message } if message.to_lowercase().contains("rejected")));
        let saw_done = events
            .iter()
            .any(|e| matches!(e, ShellStreamEvent::Done { is_error: true, .. }));
        assert!(
            saw_error && saw_done,
            "rejection path must emit both error and done, got: {events:?}"
        );
    }

    #[tokio::test]
    async fn timeout_kills_hung_command() {
        // 1-second cap via the per-request override.
        let dir = tempfile::tempdir().unwrap();
        let approver: Arc<dyn ShellApprover> = Arc::new(AllowAll);
        let mut r = req("sleep 30");
        r.timeout_secs = Some(1);
        let events = run_shell_streaming_events(r, approver, dir.path(), "ses_test").await;

        let done = events
            .iter()
            .find_map(|e| match e {
                ShellStreamEvent::Done {
                    exit_code, is_error, ..
                } => Some((*exit_code, *is_error)),
                _ => None,
            })
            .expect("timeout path must still emit done");
        assert_eq!(done.0, None, "timeout must yield null exitCode");
        assert!(done.1, "timeout path must report isError");
    }

    #[tokio::test]
    async fn approver_is_called_exactly_once() {
        let dir = tempfile::tempdir().unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let approver: Arc<dyn ShellApprover> = Arc::new(CountingAllow { count: count.clone() });
        let _events = run_shell_streaming_events(req("echo x"), approver, dir.path(), "ses_test").await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn resolve_cwd_prefers_request_when_valid() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("sub");
        std::fs::create_dir(&sub).unwrap();
        let resolved = resolve_cwd(Some(sub.to_str().unwrap()), dir.path());
        let canon = dir.path().canonicalize().unwrap_or_else(|_| dir.path().to_path_buf());
        assert!(resolved.starts_with(canon));
    }

    #[test]
    fn resolve_cwd_falls_back_when_request_is_garbage() {
        let dir = tempfile::tempdir().unwrap();
        let resolved = resolve_cwd(Some("/nonexistent/xyzzy"), dir.path());
        assert_eq!(resolved, dir.path().to_path_buf());
    }

    #[test]
    fn resolve_cwd_falls_back_when_request_is_none() {
        let dir = tempfile::tempdir().unwrap();
        let resolved = resolve_cwd(None, dir.path());
        assert_eq!(resolved, dir.path().to_path_buf());
    }

    #[test]
    fn truncate_lossy_does_not_split_utf8() {
        let s = "abcdef😀gh";
        let out = truncate_lossy(s, 7);
        assert!(out.len() <= 7);
        assert!(s.is_char_boundary(out.len()));
    }

    #[test]
    fn truncate_lossy_under_limit_is_noop() {
        let s = "hi";
        assert_eq!(truncate_lossy(s, 100), s);
    }

    /// When the SSE receiver goes away mid-command, `drive_child`
    /// must observe the close and kill the child — a long-running
    /// `sleep` with no output would otherwise sit inside `drive_child`
    /// until the per-request timeout (up to 1 h).
    #[tokio::test]
    async fn client_disconnect_kills_silent_child() {
        // Generous timeout: the test asserts we return *well under*
        // this. If `tx.closed()` is wired correctly, we should see
        // the child killed in milliseconds.
        let generous_timeout_secs = 60;

        let mut cmd = tokio::process::Command::new("sleep");
        cmd.arg("30")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let child = cmd.spawn().expect("spawn sleep");

        let (tx, rx) = mpsc::channel::<ShellStreamEvent>(8);

        let drive = tokio::spawn(async move {
            // Hold the only `Sender` here; the test drops `rx` so
            // `tx.closed()` resolves as soon as the spawn returns.
            drive_child(child, &tx, generous_timeout_secs).await
        });

        // Drop the receiver side — `tx.closed()` fires when all
        // receivers are gone.
        drop(rx);

        // Bound the wait well below the configured timeout so a
        // regression is caught.
        let outcome = tokio::time::timeout(Duration::from_secs(10), drive)
            .await
            .expect("drive_child did not return within 10s of client disconnect")
            .expect("drive_child task panicked");

        assert!(
            outcome.exit_code.is_none(),
            "killed child should have no exit code, got {:?}",
            outcome.exit_code
        );
        assert!(outcome.is_error, "killed child must be reported as is_error");
    }
}
