//! Process-global singleton managing local OpenCode instances.
//!
//! The renderer can ask AionCore to spin up zero or more
//! `opencode serve` processes on its behalf, each with the
//! Chisl plugin auto-injected. All spawned children live in
//! the same in-memory map keyed by instance id; the host's
//! graceful-shutdown path calls [`kill_all`] so a server
//! restart doesn't leak orphans.
//!
//! ## Concurrency
//!
//! - `instances: Mutex<HashMap<…>>` — the inner state is mutated
//!   in short critical sections; we never hold the lock across
//!   an `.await` that touches another awaitable outside of
//!   `tokio::fs` (port capture stays inside the mutex because
//!   the borrowed `&mut OpenCodeInstance` is `'static`-enough
//!   for our 30 s budget).
//! - `data_dir` / `remote_repo`: `tokio::sync::RwLock` because
//!   they are configured exactly once at startup but read on
//!   every start request.
//!
//! The [`global`] accessor is a `OnceLock<Arc<…>>` that mirrors
//! the same pattern used by the bg-process manager and the
//! plugin registry, so test code that needs isolation can
//! construct its own manager with [`LocalOpenCodeManager::new`].

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use aionui_api_types::{
    LocalOpenCodeInstance, LocalOpenCodeListResponse, LocalOpenCodeStatus, StartLocalOpenCodeRequest,
};
use aionui_db::IRemoteAgentRepository;
use tokio::sync::Mutex;
use tracing::info;
use uuid::Uuid;

use super::config::generate_opencode_config;
use super::instance::OpenCodeInstance;
use super::plugin_channel::{
    ensure_loopback_plugin_endpoint, register_local_agent, set_agent_opencode_url, unregister_local_agent,
};

/// Process-global singleton.
static MANAGER: OnceLock<Arc<LocalOpenCodeManager>> = OnceLock::new();

/// Get or create the global manager singleton.
///
/// Installs one on first call so any code path that needs the
/// manager (the route handlers, the graceful-shutdown hook,
/// ad-hoc tests) can grab a shared `Arc` without having to
/// thread the manager through `AppServices`.
pub fn global() -> Arc<LocalOpenCodeManager> {
    MANAGER.get_or_init(|| Arc::new(LocalOpenCodeManager::new())).clone()
}

/// Kill all running local OpenCode instances. Called during
/// graceful shutdown.
pub async fn kill_all_local_opencode() -> usize {
    global().kill_all().await
}

/// Manages multiple local OpenCode instances.
///
/// New instances are appended to the `instances` map. Stopped
/// instances stay in the map (so the renderer can still see
/// their final state) until a future cleanup pass removes
/// them; the current scope doesn't expose a delete endpoint
/// because the renderer just hides the row on Stopped.
pub struct LocalOpenCodeManager {
    instances: Mutex<HashMap<String, OpenCodeInstance>>,
    /// Base data directory for instance isolation.
    /// Each instance gets `{data_dir}/local-opencode/{id}/`.
    data_dir: tokio::sync::RwLock<Option<PathBuf>>,
    /// Remote-agent repository for plugin-token registration.
    remote_repo: tokio::sync::RwLock<Option<Arc<dyn IRemoteAgentRepository>>>,
}

impl LocalOpenCodeManager {
    /// Construct a fresh manager. Production code should use
    /// [`global`] so shutdown hooks can find the same instance.
    pub fn new() -> Self {
        Self {
            instances: Mutex::new(HashMap::new()),
            data_dir: tokio::sync::RwLock::new(None),
            remote_repo: tokio::sync::RwLock::new(None),
        }
    }

    /// Configure the base data directory for instance
    /// isolation.
    pub async fn set_data_dir(&self, dir: PathBuf) {
        *self.data_dir.write().await = Some(dir);
    }

    /// Configure the remote-agent repository used to register
    /// per-instance plugin tokens.
    pub async fn set_remote_repo(&self, repo: Arc<dyn IRemoteAgentRepository>) {
        *self.remote_repo.write().await = Some(repo);
    }

    /// Start a new local OpenCode instance.
    ///
    /// Registers a `remote_agents` row, ensures the plugin
    /// webserver is listening, injects the Chisl plugin via
    /// `OPENCODE_CONFIG_CONTENT`, then spawns `opencode serve`.
    pub async fn start(&self, req: StartLocalOpenCodeRequest) -> Result<LocalOpenCodeInstance, String> {
        let data_dir = self
            .data_dir
            .read()
            .await
            .clone()
            .ok_or_else(|| "data directory not configured".to_string())?;
        let repo = self
            .remote_repo
            .read()
            .await
            .clone()
            .ok_or_else(|| "remote agent repository not configured".to_string())?;

        let name = req.name.unwrap_or_else(|| "Local OpenCode".to_string());
        let working_dir = req
            .working_dir
            .map(PathBuf::from)
            .unwrap_or_else(|| dirs::home_dir().unwrap_or_else(|| PathBuf::from(".")));

        let plugin_token = Uuid::new_v4().to_string();
        let plugin_endpoint = ensure_loopback_plugin_endpoint(repo.clone()).await?;
        let agent_id = register_local_agent(&repo, &name, &plugin_token).await?;

        let instance_data_dir = data_dir.join("local-opencode").join(&agent_id);
        let mut instance = OpenCodeInstance::new(
            name.clone(),
            working_dir,
            instance_data_dir,
            agent_id.clone(),
            plugin_token.clone(),
        );

        // Base URL only — the plugin appends `/plugin/hello`.
        let config_content = generate_opencode_config(&plugin_endpoint, &plugin_token);

        let port = match instance.spawn(&config_content).await {
            Ok(port) => port,
            Err(e) => {
                unregister_local_agent(&repo, &agent_id).await;
                return Err(e);
            }
        };

        if let Err(e) = set_agent_opencode_url(&repo, &agent_id, port).await {
            instance.stop().await;
            unregister_local_agent(&repo, &agent_id).await;
            return Err(e);
        }

        let response = LocalOpenCodeInstance {
            id: instance.id.clone(),
            name: instance.name.clone(),
            port,
            status: instance.status,
            pid: instance.pid,
            agent_id: instance.agent_id.clone(),
            working_dir: instance.working_dir.to_string_lossy().to_string(),
            created_at: instance.created_at,
        };

        let mut instances = self.instances.lock().await;
        instances.insert(instance.id.clone(), instance);

        Ok(response)
    }

    /// Stop a running instance.
    ///
    /// The registry row is kept so a subsequent [`restart`] can
    /// reuse the same plugin token.
    pub async fn stop(&self, id: &str) -> Result<(), String> {
        let mut instances = self.instances.lock().await;
        let instance = instances
            .get_mut(id)
            .ok_or_else(|| format!("instance '{id}' not found"))?;
        instance.stop().await;
        Ok(())
    }

    /// Restart a stopped/crashed instance.
    pub async fn restart(&self, id: &str) -> Result<LocalOpenCodeInstance, String> {
        let repo = self
            .remote_repo
            .read()
            .await
            .clone()
            .ok_or_else(|| "remote agent repository not configured".to_string())?;

        let plugin_endpoint = ensure_loopback_plugin_endpoint(repo.clone()).await?;

        let mut instances = self.instances.lock().await;
        let instance = instances
            .get_mut(id)
            .ok_or_else(|| format!("instance '{id}' not found"))?;

        if !instance.can_restart() {
            return Err("restart limit exceeded (3 restarts in 5 minutes)".to_string());
        }

        instance.stop().await;

        let config_content = generate_opencode_config(&plugin_endpoint, &instance.plugin_token);
        let port = match instance.spawn(&config_content).await {
            Ok(port) => port,
            Err(e) => return Err(e),
        };
        set_agent_opencode_url(&repo, &instance.agent_id, port).await?;
        instance.record_restart();

        Ok(LocalOpenCodeInstance {
            id: instance.id.clone(),
            name: instance.name.clone(),
            port,
            status: instance.status,
            pid: instance.pid,
            agent_id: instance.agent_id.clone(),
            working_dir: instance.working_dir.to_string_lossy().to_string(),
            created_at: instance.created_at,
        })
    }

    /// List all instances.
    pub async fn list(&self) -> LocalOpenCodeListResponse {
        let mut instances = self.instances.lock().await;
        let list: Vec<LocalOpenCodeInstance> = instances
            .values_mut()
            .map(|inst| {
                let _ = inst.check_crash();
                LocalOpenCodeInstance {
                    id: inst.id.clone(),
                    name: inst.name.clone(),
                    port: inst.port.unwrap_or(0),
                    status: inst.status,
                    pid: inst.pid,
                    agent_id: inst.agent_id.clone(),
                    working_dir: inst.working_dir.to_string_lossy().to_string(),
                    created_at: inst.created_at,
                }
            })
            .collect();
        LocalOpenCodeListResponse { instances: list }
    }

    /// Kill all running instances. Returns the count of killed
    /// instances.
    pub async fn kill_all(&self) -> usize {
        let repo = self.remote_repo.read().await.clone();
        let mut instances = self.instances.lock().await;
        let mut killed = 0;
        for (_, instance) in instances.iter_mut() {
            if instance.status == LocalOpenCodeStatus::Running || instance.status == LocalOpenCodeStatus::Starting {
                let agent_id = instance.agent_id.clone();
                instance.stop().await;
                if let Some(repo) = &repo {
                    unregister_local_agent(repo, &agent_id).await;
                }
                killed += 1;
            }
        }
        if killed > 0 {
            info!(killed, "killed all local OpenCode instances on shutdown");
        }
        killed
    }
}

impl Default for LocalOpenCodeManager {
    fn default() -> Self {
        Self::new()
    }
}
