//! Request / response DTOs for `/api/terminal/*`.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminalCreateSessionRequest {
    /// Optional initial command to run in the terminal
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub command: Option<String>,
    /// Optional working directory for the terminal session
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub cwd: Option<String>,
    /// Terminal columns (default: 80)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub cols: Option<u16>,
    /// Terminal rows (default: 24)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub rows: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminalCreateSessionResponse {
    pub session_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminalKillSessionRequest {
    pub session_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminalResizeSessionRequest {
    pub session_id: String,
    pub cols: u16,
    pub rows: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminalSessionInfo {
    pub session_id: String,
    pub created_at: String,
    pub cols: u16,
    pub rows: u16,
    pub is_active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminalListSessionsResponse {
    pub sessions: Vec<TerminalSessionInfo>,
}

/// WebSocket message from client to server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminalWebSocketMessage {
    #[serde(rename = "type")]
    pub message_type: TerminalMessageType,
    /// Input data for the terminal (for "input" type)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub data: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TerminalMessageType {
    Input,
    Resize,
    Ping,
}

/// WebSocket message from server to client
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminalOutputMessage {
    #[serde(rename = "type")]
    pub message_type: TerminalOutputType,
    /// Output data from the terminal
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub data: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TerminalOutputType {
    Output,
    Error,
    Exit,
    Pong,
}
