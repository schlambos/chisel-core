//! Minimal MCP JSON-RPC types for the client-side filesystem MCP server.
//!
//! Trimmed copy of the JSON-RPC shape used by `aionui-team`'s MCP server.
//! Kept local to avoid cross-crate coupling for a small, stable surface.

use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION_LATEST: &str = "2025-03-26";
pub const PROTOCOL_VERSION_MIN: &str = "2024-11-05";
/// Alias kept for callers that still import the pre-negotiation name.
pub const PROTOCOL_VERSION: &str = PROTOCOL_VERSION_LATEST;

/// Pick the highest mutually supported MCP protocol version for `initialize`.
pub fn negotiate_protocol_version(client_version: Option<&str>) -> &'static str {
    match client_version {
        None => PROTOCOL_VERSION_LATEST,
        Some("2025-03-26") => PROTOCOL_VERSION_LATEST,
        Some("2024-11-05") => PROTOCOL_VERSION_MIN,
        Some(v) if v >= PROTOCOL_VERSION_LATEST => PROTOCOL_VERSION_LATEST,
        Some(v) if v >= PROTOCOL_VERSION_MIN => PROTOCOL_VERSION_MIN,
        Some(_) => PROTOCOL_VERSION_MIN,
    }
}
pub const SERVER_NAME: &str = "aionui-local-fs";
pub const SERVER_VERSION: &str = "0.1.0";

pub const METHOD_NOT_FOUND: i64 = -32601;
pub const INVALID_PARAMS: i64 = -32602;
pub const INTERNAL_ERROR: i64 = -32603;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<serde_json::Value>,
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, Clone, Serialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl JsonRpcResponse {
    pub fn success(id: Option<serde_json::Value>, result: serde_json::Value) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: Some(result),
            error: None,
        }
    }

    pub fn error(id: Option<serde_json::Value>, code: i64, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(JsonRpcError {
                code,
                message: message.into(),
                data: None,
            }),
        }
    }
}
