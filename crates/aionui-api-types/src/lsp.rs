//! Request / response DTOs for `/api/lsp/*`.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LspServerInfoResponse {
    pub language: String,
    pub installed: bool,
    pub command: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub install_hint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LspStartSessionRequest {
    pub language: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub workspace: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LspStartSessionResponse {
    pub session_id: String,
    pub language: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LspStopSessionRequest {
    pub session_id: String,
}
