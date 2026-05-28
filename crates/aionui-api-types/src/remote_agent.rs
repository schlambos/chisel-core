use aionui_common::{RemoteAgentAuthType, RemoteAgentProtocol, RemoteAgentStatus, TimestampMs};
use serde::{Deserialize, Serialize};

/// Request body for creating a remote agent.
#[derive(Debug, Deserialize)]
pub struct CreateRemoteAgentRequest {
    pub name: String,
    pub protocol: RemoteAgentProtocol,
    pub url: String,
    pub auth_type: RemoteAgentAuthType,
    #[serde(default)]
    pub auth_token: Option<String>,
    #[serde(default)]
    pub allow_insecure: bool,
    #[serde(default)]
    pub avatar: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

/// Request body for updating a remote agent (partial update).
#[derive(Debug, Deserialize)]
pub struct UpdateRemoteAgentRequest {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub protocol: Option<RemoteAgentProtocol>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub auth_type: Option<RemoteAgentAuthType>,
    /// `Some(Some("token"))` → set, `Some(None)` → clear, `None` → keep.
    /// serde maps `"authToken": null` to `Some(None)` and absent field to `None`.
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub auth_token: Option<Option<String>>,
    #[serde(default)]
    pub allow_insecure: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub avatar: Option<Option<String>>,
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub description: Option<Option<String>>,
}

/// Remote agent response for list endpoint (auth_token omitted).
#[derive(Debug, Serialize)]
pub struct RemoteAgentListItem {
    pub id: String,
    pub name: String,
    pub protocol: RemoteAgentProtocol,
    pub url: String,
    pub auth_type: RemoteAgentAuthType,
    pub allow_insecure: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avatar: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub status: RemoteAgentStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_connected_at: Option<TimestampMs>,
    pub created_at: TimestampMs,
    pub updated_at: TimestampMs,
}

/// Remote agent response for detail endpoint (auth_token masked, device keys visible).
#[derive(Debug, Serialize)]
pub struct RemoteAgentResponse {
    pub id: String,
    pub name: String,
    pub protocol: RemoteAgentProtocol,
    pub url: String,
    pub auth_type: RemoteAgentAuthType,
    /// Masked token: `***xxxx` (last 4 chars visible).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_token: Option<String>,
    pub allow_insecure: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avatar: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_public_key: Option<String>,
    pub status: RemoteAgentStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_connected_at: Option<TimestampMs>,
    pub created_at: TimestampMs,
    pub updated_at: TimestampMs,
}

/// Request body for testing a remote agent connection.
#[derive(Debug, Deserialize)]
pub struct TestRemoteAgentConnectionRequest {
    pub url: String,
    #[serde(default = "default_remote_agent_protocol")]
    pub protocol: RemoteAgentProtocol,
    #[serde(default)]
    pub auth_type: Option<RemoteAgentAuthType>,
    #[serde(default)]
    pub auth_token: Option<String>,
    #[serde(default)]
    pub allow_insecure: bool,
}

fn default_remote_agent_protocol() -> RemoteAgentProtocol {
    RemoteAgentProtocol::OpenClaw
}

/// Response for OpenClaw handshake.
#[derive(Debug, Serialize)]
pub struct HandshakeResponse {
    pub status: String,
}

/// Active session listed on a remote OpenCode agent.  Returned by
/// `GET /api/remote-agents/{id}/sessions`, which proxies the upstream
/// OpenCode `GET /session` call.  Drives the Phase 4 sidebar mirror:
/// the sync service materialises one Chisl conversation per row, the
/// renderer groups it by `directory`, and `id` becomes the persisted
/// `extra.sessionKey` for resume.
#[derive(Debug, Clone, Serialize)]
pub struct RemoteSessionInfo {
    /// OpenCode session id (`ses_...`).  Persisted to
    /// `conversation.extra.sessionKey` when the user attaches.
    pub id: String,
    /// Server-side title (often the first user prompt).  Optional —
    /// freshly created sessions may not have one yet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Working directory OpenCode reports for this session
    /// (`session.directory`). Used as the Chisl `extra.workspace`
    /// so sync-discovered sessions group under the right project
    /// node in the sidebar — even if the path doesn't exist on this
    /// machine.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub directory: Option<String>,
    /// Creation timestamp in milliseconds, lifted from OpenCode's
    /// `time.created`. Used as the Chisl row's `created_at` so
    /// sidebar ordering matches the server.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<TimestampMs>,
    /// Last-activity timestamp in milliseconds, lifted from
    /// OpenCode's `time.updated`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<TimestampMs>,
}

/// Deserialize `Option<Option<T>>`:
/// - JSON field absent → `None` (keep current value)
/// - JSON `null` → `Some(None)` (clear the value)
/// - JSON value → `Some(Some(value))` (set new value)
fn deserialize_optional_nullable<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    // When serde reaches this function, the field is present in JSON.
    // Deserialize the value: null → None, value → Some(value).
    let value: Option<T> = Option::deserialize(deserializer)?;
    Ok(Some(value))
}
