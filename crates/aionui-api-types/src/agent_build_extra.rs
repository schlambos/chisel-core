use serde::{Deserialize, Serialize};

use crate::{GuideMcpConfig, TeamMcpStdioConfig};

/// ACP-specific fields extracted from `extra` in build task options.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcpBuildExtra {
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub backend: Option<String>,
    #[serde(default)]
    pub cli_path: Option<String>,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub custom_agent_id: Option<String>,
    #[serde(default)]
    pub preset_context: Option<String>,
    #[serde(default)]
    pub skills: Vec<String>,
    #[serde(default)]
    pub preset_assistant_id: Option<String>,
    #[serde(default)]
    pub session_mode: Option<String>,
    #[serde(default)]
    pub current_model_id: Option<String>,
    #[serde(default)]
    pub cron_job_id: Option<String>,
    #[serde(default)]
    pub team_mcp_stdio_config: Option<TeamMcpStdioConfig>,
    #[serde(default)]
    pub guide_mcp_config: Option<GuideMcpConfig>,
    #[serde(default)]
    pub user_id: Option<String>,
}

/// OpenClaw gateway configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OpenClawGatewayConfig {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub token: Option<String>,
    pub password: Option<String>,
    #[serde(default)]
    pub use_external_gateway: bool,
    pub cli_path: Option<String>,
}

/// OpenClaw-specific fields extracted from `extra` in build task options.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenClawBuildExtra {
    #[serde(default)]
    pub backend: Option<String>,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub gateway: OpenClawGatewayConfig,
    #[serde(default)]
    pub skills: Vec<String>,
    #[serde(default)]
    pub preset_assistant_id: Option<String>,
    #[serde(default)]
    pub cron_job_id: Option<String>,
    #[serde(default, rename = "sessionKey")]
    pub session_key: Option<String>,
}

/// Remote agent-specific fields extracted from `extra` in build task options.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteBuildExtra {
    pub remote_agent_id: String,
    /// Initial model selection forwarded from the Guid (New Chat) page.
    /// Format matches what `RemoteAgentManager::set_model` expects:
    /// `"<providerID>::<modelID>"` for OpenCode.  Optional — when absent,
    /// the manager falls back to its existing default-discovery path on
    /// first send.
    #[serde(default)]
    pub current_model_id: Option<String>,
    /// Initial mode selection forwarded from the Guid (New Chat) page.
    /// For OpenCode this is `"build"` or `"plan"`; consumed by the factory
    /// via `RemoteAgentManager::set_mode` so the first prompt lands on the
    /// chosen agent. Optional — when absent the server picks its default.
    #[serde(default)]
    pub session_mode: Option<String>,
    /// Persisted OpenCode session id (`ses_...`) for resume. Written back to
    /// `conversation.extra.sessionKey` after each send (see
    /// `aionui-conversation` `persist_session_key`); reloaded by
    /// `factory/remote.rs` and validated against the server on rebuild so a
    /// stale id is discarded rather than producing a failed first prompt.
    /// Mirrors `OpenClawBuildExtra::session_key`. OpenCode HTTP path only.
    #[serde(default, rename = "sessionKey")]
    pub session_key: Option<String>,
}

/// Aionrs-specific fields extracted from `extra` in build task options.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AionrsBuildExtra {
    #[serde(default)]
    pub system_prompt: Option<String>,
    #[serde(default)]
    pub preset_rules: Option<String>,
    #[serde(default = "default_aionrs_max_tokens")]
    pub max_tokens: u32,
    #[serde(default)]
    pub max_turns: Option<usize>,
    #[serde(default)]
    pub session_mode: Option<String>,
    #[serde(default)]
    pub team_mcp_stdio_config: Option<TeamMcpStdioConfig>,
    #[serde(default)]
    pub guide_mcp_config: Option<GuideMcpConfig>,
    #[serde(default)]
    pub backend: Option<String>,
    #[serde(default)]
    pub user_id: Option<String>,
}

fn default_aionrs_max_tokens() -> u32 {
    8192
}

/// ACP model information returned by the ACP backend.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcpModelInfo {
    pub model_id: String,
    pub model_name: Option<String>,
    pub provider: Option<String>,
}

/// A slash command item available in a conversation session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlashCommandItem {
    pub command: String,
    pub description: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_build_extra_session_key_uses_session_key_alias() {
        // Reads from the persisted `conversation.extra.sessionKey` key.
        let extra: RemoteBuildExtra =
            serde_json::from_value(serde_json::json!({ "remote_agent_id": "ra_1", "sessionKey": "ses_abc" })).unwrap();
        assert_eq!(extra.session_key.as_deref(), Some("ses_abc"));

        // Serializes back to `sessionKey`, matching `persist_session_key`'s write.
        let json = serde_json::to_value(&extra).unwrap();
        assert_eq!(json["sessionKey"], "ses_abc");
        assert!(json.get("session_key").is_none());
    }

    #[test]
    fn remote_build_extra_session_key_defaults_to_none() {
        // Absent on a brand-new conversation — must not fail deserialization.
        let extra: RemoteBuildExtra = serde_json::from_value(serde_json::json!({ "remote_agent_id": "ra_1" })).unwrap();
        assert_eq!(extra.session_key, None);
    }
}
