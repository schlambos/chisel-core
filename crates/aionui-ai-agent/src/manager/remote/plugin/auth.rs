//! Bearer-token validation for the plugin webserver.
//!
//! Maps an incoming `Authorization: Bearer …` header to a
//! `remote_agent_id` via a SQL lookup on the plaintext
//! `plugin_token` column (see migration 010).

use std::sync::Arc;

use aionui_db::IRemoteAgentRepository;

use super::registry::PluginTokenValidator;

/// Resolves plugin bearer tokens against the `remote_agents` table.
pub struct DbPluginTokenValidator {
    repo: Arc<dyn IRemoteAgentRepository>,
}

/// Build a shareable validator backed by the given repository.
pub fn db_token_validator(repo: Arc<dyn IRemoteAgentRepository>) -> Arc<dyn PluginTokenValidator> {
    Arc::new(DbPluginTokenValidator { repo })
}

#[async_trait::async_trait]
impl PluginTokenValidator for DbPluginTokenValidator {
    async fn resolve(&self, token: &str) -> Option<String> {
        self.repo
            .find_by_plugin_token(token)
            .await
            .ok()
            .flatten()
            .map(|row| row.id)
    }
}
