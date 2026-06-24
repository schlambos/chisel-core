//! Wire local `opencode serve` instances into the plugin channel.
//!
//! Local OpenCode shares the host with AionCore, so the plugin
//! dials loopback. Each spawned instance still needs:
//!
//! 1. The process-wide plugin webserver running (separate port
//!    from the main API listener).
//! 2. A `remote_agents` row whose `plugin_token` the webserver
//!    can resolve (same auth path as the remote install flow).
//! 3. An `AIONCORE_URL` base URL **without** a `/plugin` suffix —
//!    the plugin client appends `/plugin/hello` itself.

use std::sync::Arc;

use chisl_db::{CreateRemoteAgentParams, IRemoteAgentRepository, UpdateRemoteAgentParams};

use crate::manager::remote::plugin::{db_token_validator, ensure_plugin_server, plugin_listen_addr};
use crate::manager::remote::reachability;

/// Placeholder until the child prints its real listening port.
const LOOPBACK_PLACEHOLDER_URL: &str = "http://127.0.0.1:0";

/// Ensure the plugin webserver is bound and return the loopback
/// base URL the spawned plugin should dial (trailing slash).
pub async fn ensure_loopback_plugin_endpoint(repo: Arc<dyn IRemoteAgentRepository>) -> Result<String, String> {
    let plan = reachability::plan("http://127.0.0.1:1");
    let bind = plugin_listen_addr(&plan);
    let validator = db_token_validator(repo);
    let bound = ensure_plugin_server(bind, validator)
        .await
        .map_err(|e| format!("failed to start plugin webserver: {e}"))?;
    Ok(format!("http://127.0.0.1:{}/", bound.port()))
}

/// Register a local instance as a remote-agent row so the plugin
/// webserver can authenticate its bearer token.
pub async fn register_local_agent(
    repo: &Arc<dyn IRemoteAgentRepository>,
    name: &str,
    plugin_token: &str,
) -> Result<String, String> {
    let row = repo
        .create(CreateRemoteAgentParams {
            name,
            protocol: "opencode",
            url: LOOPBACK_PLACEHOLDER_URL,
            auth_type: "none",
            auth_token: None,
            allow_insecure: true,
            avatar: None,
            description: Some("Local OpenCode instance (managed by AionCore)"),
            device_id: None,
            device_public_key: None,
            device_private_key: None,
            device_token: None,
            tool_host: Some("server"),
        })
        .await
        .map_err(|e| format!("failed to register local agent: {e}"))?;

    repo.set_plugin_token(&row.id, Some(plugin_token))
        .await
        .map_err(|e| format!("failed to set plugin token: {e}"))?;

    Ok(row.id)
}

/// Update the registered agent's OpenCode URL once the child port
/// is known.
pub async fn set_agent_opencode_url(
    repo: &Arc<dyn IRemoteAgentRepository>,
    agent_id: &str,
    port: u16,
) -> Result<(), String> {
    let url = format!("http://127.0.0.1:{port}/");
    repo.update(
        agent_id,
        UpdateRemoteAgentParams {
            url: Some(&url),
            ..Default::default()
        },
    )
    .await
    .map_err(|e| format!("failed to update local agent url: {e}"))?;
    Ok(())
}

/// Remove the registry row when a local instance stops.
pub async fn unregister_local_agent(repo: &Arc<dyn IRemoteAgentRepository>, agent_id: &str) {
    if let Err(e) = repo.delete(agent_id).await {
        tracing::warn!(agent_id, error = %e, "failed to delete local agent registry row");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chisl_db::{SqliteRemoteAgentRepository, init_database_memory};
    use std::sync::{Mutex, MutexGuard};

    /// Process-wide lock for tests that mutate `AIONUI_PLUGIN_PORT`.
    /// The env var is process-global and read at call time by
    /// [`crate::manager::remote::plugin::port::resolve_plugin_port`], so
    /// two tests setting it concurrently would race; this lock serialises
    /// them. Tests outside this module also read the same var (see
    /// `plugin_listen_addr_uses_fixed_default_port` in
    /// `manager/remote/plugin/server/tests.rs`), so the lock is held
    /// until the test fully restores the previous value.
    static PLUGIN_PORT_ENV_LOCK: Mutex<()> = Mutex::new(());

    fn lock_plugin_port_env() -> MutexGuard<'static, ()> {
        PLUGIN_PORT_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Pick a random free TCP port. We bind to `127.0.0.1:0`, let the
    /// OS assign one, then drop the listener immediately. There's a
    /// tiny TOCTOU window where another process could grab the port
    /// before the test binds it, but in practice this is reliable
    /// enough for unit tests.
    fn pick_free_port() -> u16 {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("bind :0");
        let port = listener.local_addr().expect("local_addr").port();
        drop(listener);
        port
    }

    #[tokio::test]
    async fn register_and_resolve_plugin_token() {
        let db = init_database_memory().await.unwrap();
        let repo: Arc<dyn IRemoteAgentRepository> = Arc::new(SqliteRemoteAgentRepository::new(db.pool().clone()));

        let token = "local-test-token-abc";
        let agent_id = register_local_agent(&repo, "Test Local", token).await.unwrap();

        let row = repo.find_by_plugin_token(token).await.unwrap().unwrap();
        assert_eq!(row.id, agent_id);
        assert_eq!(row.plugin_token.as_deref(), Some(token));
    }

    #[tokio::test]
    async fn ensure_loopback_plugin_endpoint_returns_loopback_url() {
        // The plugin webserver's listen port is fixed at
        // `DEFAULT_PLUGIN_PORT` in production so remote Docker installs
        // have a stable dial-back target. A dev `chislcore` instance
        // running on the same host is therefore holding that port when
        // these tests execute — bind an ephemeral port via the
        // `AIONUI_PLUGIN_PORT` env override so the test can succeed
        // regardless of what's already listening. The lock guards
        // against parallel tests reading a half-set env var.
        let _env_guard = lock_plugin_port_env();
        let prev = std::env::var(crate::manager::remote::plugin::PLUGIN_PORT_ENV).ok();
        let test_port = pick_free_port();
        // SAFETY: test-only env mutation; serialised by
        // `PLUGIN_PORT_ENV_LOCK`. Restored before return.
        unsafe {
            std::env::set_var(crate::manager::remote::plugin::PLUGIN_PORT_ENV, test_port.to_string());
        }

        let db = init_database_memory().await.unwrap();
        let repo: Arc<dyn IRemoteAgentRepository> = Arc::new(SqliteRemoteAgentRepository::new(db.pool().clone()));

        let url = ensure_loopback_plugin_endpoint(repo).await.unwrap();
        assert!(url.starts_with("http://127.0.0.1:"));
        assert!(url.ends_with('/'));
        assert!(!url.contains("/plugin"));
        assert_eq!(url, format!("http://127.0.0.1:{test_port}/"));

        // Restore the env var so other tests see the same baseline.
        // SAFETY: paired with the set_var above; same lock held.
        match prev {
            Some(v) => unsafe {
                std::env::set_var(crate::manager::remote::plugin::PLUGIN_PORT_ENV, v);
            },
            None => unsafe {
                std::env::remove_var(crate::manager::remote::plugin::PLUGIN_PORT_ENV);
            },
        }
    }
}
