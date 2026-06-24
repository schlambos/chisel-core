//! WebSocket transport bridge for terminal sessions.
//!
//! This module provides the WebSocket bridge that connects a WebSocket connection
//! to a terminal PTY session, handling bidirectional data flow.

use std::sync::Arc;
use std::sync::mpsc::TryRecvError;
use std::time::Duration;

use chisl_api_types::terminal::{
    TerminalMessageType, TerminalOutputMessage, TerminalOutputType, TerminalWebSocketMessage,
};
use axum::extract::ws::{Message, WebSocket};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt};
use serde_json;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::pty::SharedTerminalSession;

/// Drive the bidirectional bridge between WebSocket and terminal PTY.
///
/// This function runs in a tokio task and handles:
/// - Receiving messages from the WebSocket and sending them to the PTY
/// - Receiving output from the PTY and sending it to the WebSocket
/// - Handling WebSocket close and ping/pong messages
/// - Proper cleanup on exit
pub async fn bridge(socket: WebSocket, pty: SharedTerminalSession) {
    let (ws_sink, mut ws_stream) = socket.split();
    let ws_sink = Arc::new(Mutex::new(ws_sink));

    // PTY → WS: spawn a task to read from PTY and send to WebSocket
    let ws_sink_for_reader = ws_sink.clone();
    let pty_clone = pty.clone();
    let mut reader_task = tokio::spawn(async move {
        let mut coalescing_buffer = Vec::with_capacity(4096);
        let mut last_send_time = tokio::time::Instant::now();

        loop {
            // Try to receive output from PTY
            match pty_clone.try_recv_output() {
                Ok(data) => {
                    coalescing_buffer.extend(data);

                    // Check if we should flush the coalescing buffer
                    let now = tokio::time::Instant::now();
                    if now.duration_since(last_send_time) >= Duration::from_millis(8) || coalescing_buffer.len() >= 4096
                    {
                        if !coalescing_buffer.is_empty() {
                            let data = std::mem::take(&mut coalescing_buffer);
                            // Encode as base64 to preserve all bytes exactly (FIX 1: UTF-8 chunk corruption)
                            let base64_data = STANDARD.encode(&data);
                            let msg = TerminalOutputMessage {
                                message_type: TerminalOutputType::Output,
                                data: Some(base64_data),
                            };
                            let json_data = serde_json::to_string(&msg).unwrap_or_else(|_| {
                                // Fallback to empty string if serialization fails
                                String::new()
                            });
                            let mut sink = ws_sink_for_reader.lock().await;
                            if sink.send(Message::Text(json_data.into())).await.is_err() {
                                debug!("terminal ws sink closed");
                                return;
                            }
                            last_send_time = now;
                        }
                    }
                }
                Err(TryRecvError::Empty) => {
                    // Flush any pending buffered output if the coalescing interval has elapsed
                    let now = tokio::time::Instant::now();
                    if !coalescing_buffer.is_empty() && now.duration_since(last_send_time) >= Duration::from_millis(8) {
                        let data = std::mem::take(&mut coalescing_buffer);
                        // Encode as base64 to preserve all bytes exactly (FIX 1: UTF-8 chunk corruption)
                        let base64_data = STANDARD.encode(&data);
                        let msg = TerminalOutputMessage {
                            message_type: TerminalOutputType::Output,
                            data: Some(base64_data),
                        };
                        let json_data = serde_json::to_string(&msg).unwrap_or_else(|_| {
                            // Fallback to empty string if serialization fails
                            String::new()
                        });
                        let mut sink = ws_sink_for_reader.lock().await;
                        if sink.send(Message::Text(json_data.into())).await.is_err() {
                            debug!("terminal ws sink closed");
                            return;
                        }
                        last_send_time = now;
                    }
                    tokio::time::sleep(Duration::from_millis(16)).await;
                }
                Err(TryRecvError::Disconnected) => {
                    // PTY reader thread exited, terminal is closed
                    debug!("PTY reader thread exited");
                    // Send exit message before breaking
                    let msg = TerminalOutputMessage {
                        message_type: TerminalOutputType::Exit,
                        data: Some("0".to_string()),
                    };
                    let json_data = serde_json::to_string(&msg).unwrap_or_else(|_| String::new());
                    let mut sink = ws_sink_for_reader.lock().await;
                    let _ = sink.send(Message::Text(json_data.into())).await;
                    break;
                }
            }
        }
    });

    // WS → PTY: receive messages from WebSocket and send to PTY
    // Race against the reader task exiting
    let mut reader_task_finished = false;
    loop {
        tokio::select! {
            // PTY reader task exited (PTY is dead)
            _ = &mut reader_task => {
                reader_task_finished = true;
                debug!("PTY reader task exited, closing bridge");
                break;
            }
            // WebSocket message received
            msg = ws_stream.next() => {
                match msg {
                     Some(Ok(Message::Text(text))) => {
                         // Handle text messages as JSON control messages
                         debug!(byte_len = text.len(), "terminal ws received text message");
                         let text_str = text.to_string();

                         // FIX 3: Malformed JSON fallback - heuristic check
                         if text_str.starts_with('{') {
                             // Looks like JSON — parse strictly
                             match serde_json::from_str::<TerminalWebSocketMessage>(&text_str) {
                                 Ok(msg) => {
                                     match msg.message_type {
                                         TerminalMessageType::Input => {
                                             // Send input data to PTY
                                             if let Some(data) = msg.data {
                                                 if let Err(e) = pty.send_input(data.as_bytes().to_vec()) {
                                                     warn!(error = %e, "terminal failed to send input");
                                                     break;
                                                 }
                                             }
                                         }
                                         TerminalMessageType::Resize => {
                                             // Parse cols and rows from data field (format: "cols,rows")
                                             if let Some(data) = msg.data {
                                                 let parts: Vec<&str> = data.split(',').collect();
                                                 if parts.len() == 2 {
                                                     if let (Ok(cols), Ok(rows)) = (parts[0].parse::<u16>(), parts[1].parse::<u16>()) {
                                                         if let Err(e) = pty.resize(cols, rows) {
                                                             warn!(error = %e, "terminal failed to resize");
                                                             break;
                                                         }
                                                     } else {
                                                         warn!("Invalid resize data format");
                                                     }
                                                 } else {
                                                     warn!("Invalid resize data format");
                                                 }
                                             }
                                         }
                                          TerminalMessageType::Ping => {
                                              // Send pong response as JSON
                                              debug!("terminal ws received ping");
                                              let pong_msg = TerminalOutputMessage {
                                                  message_type: TerminalOutputType::Pong,
                                                  data: None,
                                              };
                                              let json_data = serde_json::to_string(&pong_msg).unwrap_or_else(|_| {
                                                  String::new()
                                              });
                                              let mut sink = ws_sink.lock().await;
                                              let _ = sink.send(Message::Text(json_data.into())).await;
                                          }
                                     }
                                 }
                                 Err(e) => {
                                     // Looks like JSON but failed to parse — send error, do NOT write to PTY
                                     warn!(error = %e, "malformed JSON message from client");
                                     let err_msg = TerminalOutputMessage {
                                         message_type: TerminalOutputType::Error,
                                         data: Some(format!("invalid message: {}", e)),
                                     };
                                     let json_data = serde_json::to_string(&err_msg).unwrap_or_else(|_| {
                                         String::new()
                                     });
                                     let mut sink = ws_sink.lock().await;
                                     let _ = sink.send(Message::Text(json_data.into())).await;
                                 }
                             }
                         } else {
                             // Not JSON — raw terminal input (backward compat)
                             debug!(byte_len = text_str.len(), "treating as raw input");
                             if let Err(e) = pty.send_input(text_str.as_bytes().to_vec()) {
                                 warn!(error = %e, "terminal failed to send input");
                                 break;
                             }
                         }
                    }
                     Some(Ok(Message::Binary(data))) => {
                         // Handle binary messages (raw terminal input) for backward compatibility
                         debug!(byte_len = data.len(), "terminal ws received binary message");
                         if let Err(e) = pty.send_input(data.to_vec()) {
                            warn!(error = %e, "terminal failed to send binary input");
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) => {
                        debug!("terminal ws received close");
                        break;
                    }
                      Some(Ok(Message::Ping(payload))) => {
                          // FIX 4: WebSocket protocol-level Ping — respond with Pong control frame
                          debug!("terminal ws received protocol-level ping");
                          let mut sink = ws_sink.lock().await;
                          let _ = sink.send(Message::Pong(payload)).await;
                      }
                    Some(Ok(Message::Pong(_))) => {
                        debug!("terminal ws received pong");
                    }
                    Some(Err(e)) => {
                        debug!(error = %e, "terminal ws recv error");
                        break;
                    }
                    None => {
                        // WebSocket stream ended
                        debug!("terminal ws stream ended");
                        break;
                    }
                }
            }
        }
    }

    // Clean up: abort reader task if it's still running
    if !reader_task_finished {
        reader_task.abort();
        let _ = reader_task.await;
    }

    // Send a close frame to the WebSocket if it's still open
    {
        let mut sink = ws_sink.lock().await;
        let _ = sink.send(Message::Close(None)).await;
    }

    info!("terminal bridge exited");
}

/// WebSocket upgrade handler.
///
/// This function is called when a WebSocket upgrade request is received.
/// Authentication is already validated in routes.rs before upgrade.
pub async fn ws_upgrade_handler(socket: WebSocket, pty: SharedTerminalSession) -> Result<(), String> {
    // Start the bridge
    bridge(socket, pty).await;

    Ok(())
}
