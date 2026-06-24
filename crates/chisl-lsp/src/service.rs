//! Session manager + spawn business logic.
//!
//! Per AGENTS.md, this file owns the business logic and must not import
//! axum. The routes layer ([`crate::routes`]) handles HTTP/WS adaptation.

use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use chisl_runtime::Builder;
use dashmap::DashMap;
use tracing::{info, warn};

use crate::languages::{self, LanguageConfig};
use crate::transport::LspChild;

/// How long the watcher task waits for the child to exit after a kill signal
/// before force-killing it.
const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a session can remain unattached before the TTL reaper removes it.
const UNATTACHED_TTL: Duration = Duration::from_secs(60);

/// A live LSP session. Reserved by `start_session`, materialized by
/// `attach_transport` (which spawns the language-server process). Process
/// lifetime is owned by a watcher task; the session is removed from the
/// map when the child exits.
struct Session {
    language: &'static str,
    workspace: Option<String>,
    /// `true` once `attach_transport` has spawned the child; subsequent
    /// attach attempts return `SessionAlreadyAttached`. Atomic so we don't
    /// need an async lock just for an idempotency check.
    attached: AtomicBool,
    /// Kill sender stored here so `shutdown_all` / `stop_session` can signal
    /// the watcher. `take()` is called under the std Mutex (held briefly,
    /// never across an await). `None` until `attach_transport` stores the
    /// sender.
    kill: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    /// When the session was reserved by `start_session`. Available for
    /// future diagnostics / metrics; the TTL reaper currently relies on
    /// the `attached` flag alone.
    #[expect(dead_code, reason = "reserved for future TTL-age diagnostics")]
    created: Instant,
}

#[derive(Clone, Debug)]
pub struct ServerStatus {
    pub language: &'static str,
    pub command: &'static str,
    pub installed: bool,
    pub install_hint: Option<&'static str>,
}

/// Errors returned by `LspService` operations.
#[derive(Debug, thiserror::Error)]
pub enum LspError {
    #[error("unsupported language: {0}")]
    UnsupportedLanguage(String),
    #[error("language server '{command}' is not installed: {hint}")]
    NotInstalled { command: &'static str, hint: &'static str },
    #[error("failed to spawn language server: {0}")]
    SpawnFailed(String),
    #[error("session {0} not found")]
    SessionNotFound(String),
    #[error("session {0} already attached to a transport")]
    SessionAlreadyAttached(String),
}

pub struct LspService {
    sessions: Arc<DashMap<String, Arc<Session>>>,
}

impl Default for LspService {
    fn default() -> Self {
        Self::new()
    }
}

impl LspService {
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(DashMap::new()),
        }
    }

    pub fn list_servers(&self) -> Vec<ServerStatus> {
        languages::LANGUAGES.iter().map(Self::status_for).collect()
    }

    fn status_for(cfg: &'static LanguageConfig) -> ServerStatus {
        let installed = languages::locate(cfg.command).is_some();
        ServerStatus {
            language: cfg.language,
            command: cfg.command,
            installed,
            install_hint: if installed { None } else { Some(cfg.install_hint) },
        }
    }

    /// Reserve a session id for the given language. The actual child is
    /// spawned lazily by `attach_transport` so that a renderer that calls
    /// `/start` then never opens the WebSocket does not leave a zombie.
    pub fn start_session(&self, language: &str, workspace: Option<String>) -> Result<String, LspError> {
        let cfg = languages::find(language).ok_or_else(|| LspError::UnsupportedLanguage(language.to_owned()))?;
        if languages::locate(cfg.command).is_none() {
            return Err(LspError::NotInstalled {
                command: cfg.command,
                hint: cfg.install_hint,
            });
        }
        let id = uuid::Uuid::now_v7().to_string();
        self.sessions.insert(
            id.clone(),
            Arc::new(Session {
                language: cfg.language,
                workspace,
                attached: AtomicBool::new(false),
                kill: std::sync::Mutex::new(None),
                created: Instant::now(),
            }),
        );
        info!(session_id = %id, language = cfg.language, "lsp session reserved");

        // Spawn a TTL reaper: if no WebSocket attaches within UNATTACHED_TTL,
        // the session is orphaned — remove it from the map.
        let sessions_ptr = self.sessions.clone();
        let reaper_id = id.clone();
        tokio::spawn(async move {
            tokio::time::sleep(UNATTACHED_TTL).await;
            if let Some(pair) = sessions_ptr.remove_if(&reaper_id, |_, s| !s.attached.load(Ordering::SeqCst)) {
                info!(session_id = %reaper_id, "reaped unattached LSP session");
                drop(pair);
            }
        });

        Ok(id)
    }

    /// Stop a session. Sends the kill signal if the child is running, then
    /// removes the session from the map. The watcher task handles actual
    /// child termination with a graceful shutdown timeout.
    pub fn stop_session(&self, session_id: &str) -> Result<(), LspError> {
        let (_, session) = self
            .sessions
            .remove(session_id)
            .ok_or_else(|| LspError::SessionNotFound(session_id.to_owned()))?;
        if let Some(kill_tx) = session.kill.lock().unwrap().take() {
            let _ = kill_tx.send(());
        }
        info!(session_id = %session_id, "lsp session stopped");
        Ok(())
    }

    /// Spawn the language server child and hand back the LspChild handle the
    /// transport bridge needs. Idempotent against the same `session_id`:
    /// repeated calls are rejected with `SessionAlreadyAttached`.
    pub async fn attach_transport(&self, session_id: &str) -> Result<LspChild, LspError> {
        let sess = self
            .sessions
            .get(session_id)
            .ok_or_else(|| LspError::SessionNotFound(session_id.to_owned()))?
            .clone();

        let cfg =
            languages::find(sess.language).ok_or_else(|| LspError::UnsupportedLanguage(sess.language.to_owned()))?;

        if sess.attached.swap(true, Ordering::SeqCst) {
            return Err(LspError::SessionAlreadyAttached(session_id.to_owned()));
        }

        let mut builder = Builder::new(cfg.command);
        builder
            .args(cfg.args.iter().copied())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(ws) = sess.workspace.as_deref()
            && !ws.is_empty()
        {
            builder.current_dir(ws);
        }

        let mut child = builder.spawn().map_err(|e| LspError::SpawnFailed(e.to_string()))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| LspError::SpawnFailed("missing stdin".to_owned()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| LspError::SpawnFailed("missing stdout".to_owned()))?;

        if let Some(mut stderr) = child.stderr.take() {
            let lang = cfg.language;
            tokio::spawn(async move {
                use tokio::io::AsyncReadExt;
                let mut buf = [0u8; 4096];
                loop {
                    match stderr.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            let s = String::from_utf8_lossy(&buf[..n]);
                            for line in s.lines().filter(|l| !l.is_empty()) {
                                warn!(target: "lsp.stderr", language = lang, "{}", line);
                            }
                        }
                    }
                }
            });
        }

        // Two kill channels: the Session holds one (for shutdown_all /
        // stop_session), the bridge holds one (for WS-close signalling).
        // The watcher selects on both plus child.wait().
        let (session_kill_tx, session_kill_rx) = tokio::sync::oneshot::channel::<()>();
        let (bridge_kill_tx, bridge_kill_rx) = tokio::sync::oneshot::channel::<()>();

        *sess.kill.lock().unwrap() = Some(session_kill_tx);

        // Move ownership of the Child into a watcher task. When the
        // transport bridge signals `bridge_kill_rx`, when `shutdown_all`
        // / `stop_session` signals `session_kill_rx`, or when the child
        // exits on its own, the watcher reaps the child and removes the
        // session from the map.
        let sessions_ptr = self.sessions.clone();
        let session_id_owned = session_id.to_owned();
        tokio::spawn(async move {
            let child_exited = tokio::select! {
                _ = session_kill_rx => {
                    info!(session_id = %session_id_owned, "lsp child kill requested (session shutdown)");
                    false
                }
                _ = bridge_kill_rx => {
                    info!(session_id = %session_id_owned, "lsp child kill requested (bridge exit)");
                    false
                }
                result = child.wait() => {
                    let _ = result;
                    info!(session_id = %session_id_owned, "lsp child exited");
                    true
                }
            };

            if !child_exited {
                // Give the child a chance to exit gracefully; force-kill
                // if it hasn't exited within the timeout.
                match tokio::time::timeout(GRACEFUL_SHUTDOWN_TIMEOUT, child.wait()).await {
                    Ok(Ok(_)) => {
                        info!(session_id = %session_id_owned, "lsp child exited after kill signal");
                    }
                    Ok(Err(e)) => {
                        warn!(session_id = %session_id_owned, error = %e, "lsp child wait error after kill signal");
                    }
                    Err(_) => {
                        warn!(session_id = %session_id_owned, "lsp child didn't exit within 5s, force-killing");
                        let _ = child.kill().await;
                    }
                }
            }

            drop(child);
            sessions_ptr.remove(&session_id_owned);
        });

        Ok(LspChild {
            stdin,
            stdout,
            kill: bridge_kill_tx,
        })
    }

    /// Gracefully tear down all LSP sessions. Sends kill signals to running
    /// children and clears the session map so no new transports can attach.
    /// Watcher tasks handle actual child termination asynchronously (with a
    /// 5 s timeout before force-kill).
    pub async fn shutdown_all(&self) {
        let count = self.sessions.len();
        for entry in self.sessions.iter() {
            if let Some(kill_tx) = entry.value().kill.lock().unwrap().take() {
                let _ = kill_tx.send(());
            }
        }
        self.sessions.clear();
        info!(count, "lsp shutdown_all complete");
    }
}
