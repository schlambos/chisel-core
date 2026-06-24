//! JSON-RPC framing bridge between a WebSocket and a language-server child
//! process's stdio.
//!
//! Wire format
//! -----------
//! The LSP wire format frames each JSON-RPC payload with
//! `Content-Length: <n>\r\n\r\n<n bytes of utf-8 JSON>`.
//!
//! We strip the framing on the way out (child stdout → WS, sent as plain
//! JSON text frames) and re-apply it on the way in (WS text frame → child
//! stdin). The renderer-side `vscode-ws-jsonrpc` adapter does the inverse,
//! so end-to-end the LSP client sees a normal stdio LSP server.

use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{ChildStdin, ChildStdout};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

/// Spawned LSP child handle that the WebSocket loop borrows.
pub struct LspChild {
    pub stdin: ChildStdin,
    pub stdout: ChildStdout,
    pub kill: tokio::sync::oneshot::Sender<()>,
}

/// Drive the bidirectional bridge until either the socket or the child exits.
///
/// On exit, the child process is killed (via `kill_on_drop` on the `Child`
/// instance the caller still owns) and the WebSocket is closed by the
/// outer handler. Errors on either side log + break the loop.
pub async fn bridge(socket: WebSocket, child: LspChild) {
    let (ws_sink, mut ws_stream) = socket.split();
    let ws_sink = Arc::new(Mutex::new(ws_sink));

    let LspChild {
        mut stdin,
        mut stdout,
        kill,
    } = child;

    // child → ws: read framed messages from stdout, strip headers, send JSON
    // payload as a WS text frame.
    let ws_sink_for_reader = ws_sink.clone();
    let reader_task = tokio::spawn(async move {
        let mut buf: Vec<u8> = Vec::with_capacity(8192);
        let mut chunk = [0u8; 4096];
        loop {
            match stdout.read(&mut chunk).await {
                Ok(0) => {
                    debug!("lsp child stdout EOF");
                    break;
                }
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                Err(e) => {
                    warn!(error = %e, "lsp child stdout read error");
                    break;
                }
            }
            // Drain every complete frame the buffer currently holds.
            while let Some((header_len, body_len)) = parse_frame_lengths(&buf) {
                let total = header_len + body_len;
                if buf.len() < total {
                    break;
                }
                let body = buf[header_len..total].to_vec();
                buf.drain(..total);
                let payload = String::from_utf8_lossy(&body).into_owned();
                let mut sink = ws_sink_for_reader.lock().await;
                if sink.send(Message::Text(payload.into())).await.is_err() {
                    debug!("lsp ws sink closed");
                    return;
                }
            }
        }
    });

    // ws → child: receive text frames from the WebSocket, prepend
    // Content-Length headers, write to stdin.
    while let Some(msg) = ws_stream.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                let body = text.as_bytes();
                let header = format!("Content-Length: {}\r\n\r\n", body.len());
                if stdin.write_all(header.as_bytes()).await.is_err() {
                    warn!("lsp child stdin write error (header)");
                    break;
                }
                if stdin.write_all(body).await.is_err() {
                    warn!("lsp child stdin write error (body)");
                    break;
                }
                if stdin.flush().await.is_err() {
                    warn!("lsp child stdin flush error");
                    break;
                }
            }
            Ok(Message::Close(_)) => {
                debug!("lsp ws received close");
                break;
            }
            Ok(_) => {
                // Ignore binary / ping / pong frames.
            }
            Err(e) => {
                debug!(error = %e, "lsp ws recv error");
                break;
            }
        }
    }

    // Signal the spawner to kill the child; the spawner owns the `Child` and
    // will drop it (kill_on_drop=true).
    let _ = kill.send(());
    reader_task.abort();
    info!("lsp bridge exited");
}

/// Parse `Content-Length: N\r\n\r\n` (plus any other headers we ignore) at
/// the head of `buf`. Returns `(header_byte_count, body_byte_count)` once a
/// complete header section is present, or `None` if the buffer is still
/// short.
fn parse_frame_lengths(buf: &[u8]) -> Option<(usize, usize)> {
    let sep = b"\r\n\r\n";
    let header_end = buf.windows(sep.len()).position(|w| w == sep)?;
    let header = std::str::from_utf8(&buf[..header_end]).ok()?;
    let mut content_length: Option<usize> = None;
    for line in header.split("\r\n") {
        if let Some(rest) = line.strip_prefix("Content-Length:") {
            content_length = rest.trim().parse::<usize>().ok();
        }
    }
    let body_len = content_length?;
    Some((header_end + sep.len(), body_len))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_frame() {
        let raw = b"Content-Length: 5\r\n\r\nhello";
        let (h, b) = parse_frame_lengths(raw).unwrap();
        assert_eq!(h, raw.len() - 5);
        assert_eq!(b, 5);
    }

    #[test]
    fn parse_frame_with_extra_header() {
        let raw = b"Content-Length: 12\r\nContent-Type: application/vscode-jsonrpc\r\n\r\nXXXXXXXXXXXX";
        let (h, b) = parse_frame_lengths(raw).unwrap();
        assert_eq!(b, 12);
        assert_eq!(raw.len() - h, 12);
    }

    #[test]
    fn parse_partial_returns_none() {
        let raw = b"Content-Length: 5\r\n";
        assert!(parse_frame_lengths(raw).is_none());
    }

    #[test]
    fn parse_missing_length_returns_none() {
        let raw = b"Other: 5\r\n\r\n";
        assert!(parse_frame_lengths(raw).is_none());
    }
}
