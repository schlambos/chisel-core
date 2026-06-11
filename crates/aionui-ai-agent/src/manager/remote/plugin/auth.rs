//! Bearer-token validation for the plugin webserver.
//!
//! Maps an incoming `Authorization: Bearer …` header to a
//! `remote_agent_id` via a SQL lookup on the plaintext
//! `plugin_token` column (see migration 010).
//!
//! ## Process-global validator
//!
//! [`set_global_validator`] / [`global_validator`] let the composition
//! root (`aionui-app`) install the DB-backed validator once at boot.
//! Code that needs to start the plugin webserver eagerly (e.g.
//! `ensure_local_fs_mcp`) reads it back without requiring a
//! constructor-injected repo — the same OnceLock pattern used by
//! `SnapshotDepsRegistry`.

use std::sync::{Arc, OnceLock};

use aionui_db::IRemoteAgentRepository;

use super::registry::PluginTokenValidator;

// ── Process-global validator ───────────────────────────────────────

/// Process-wide slot for the DB-backed token validator. Set once at
/// app boot; read by any code path that needs to start the plugin
/// webserver without a local `IRemoteAgentRepository` handle.
static GLOBAL_VALIDATOR: OnceLock<Arc<dyn PluginTokenValidator>> = OnceLock::new();

/// Install the global plugin-token validator. Idempotent — second and
/// subsequent calls are silent no-ops. Call from the composition root
/// (`AppServices::from_config`) after the DB is ready.
pub fn set_global_validator(v: Arc<dyn PluginTokenValidator>) {
    let _ = GLOBAL_VALIDATOR.set(v);
}

/// Retrieve the global validator, if one was installed.
pub fn global_validator() -> Option<Arc<dyn PluginTokenValidator>> {
    GLOBAL_VALIDATOR.get().cloned()
}

// ── DB-backed validator ────────────────────────────────────────────

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
