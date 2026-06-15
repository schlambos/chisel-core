//! Session manager + spawn business logic.
//!
//! Per AGENTS.md, this file owns the business logic and must not import
//! axum. The routes layer ([`crate::routes`]) handles HTTP/WS adaptation.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use tracing::info;
use uuid::Uuid;

use crate::pty::{SharedTerminalSession, TerminalSession};

/// A live terminal session. Reserved by `start_session`, materialized by
/// `attach_transport` (which spawns the shell process).
struct Session {
    /// Session ID
    id: String,
    /// Initial command if any
    command: Option<String>,
    /// Working directory
    cwd: Option<String>,
    /// Terminal columns and rows (protected by mutex for interior mutability)
    size: Mutex<PtySize>,
    /// Creation timestamp (milliseconds since UNIX epoch)
    created_at: u64,
    /// `true` once `attach_transport` has spawned the PTY; subsequent
    /// attach attempts return `SessionAlreadyAttached`.
    attached: AtomicBool,
    /// The actual PTY session (None until attached, protected by mutex)
    pty: Mutex<Option<SharedTerminalSession>>,
}

/// Terminal size for session tracking
#[derive(Clone, Copy, Debug)]
struct PtySize {
    pub cols: u16,
    pub rows: u16,
}

impl Session {
    fn new(id: String, command: Option<String>, cwd: Option<String>, cols: u16, rows: u16) -> Self {
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        Self {
            id,
            command,
            cwd,
            size: Mutex::new(PtySize { cols, rows }),
            created_at,
            attached: AtomicBool::new(false),
            pty: Mutex::new(None),
        }
    }
}

/// Errors returned by `TerminalService` operations.
#[derive(Debug, thiserror::Error)]
pub enum TerminalError {
    #[error("session {0} not found")]
    SessionNotFound(String),
    #[error("session {0} already attached to a transport")]
    SessionAlreadyAttached(String),
    #[error("failed to spawn terminal: {0}")]
    SpawnFailed(String),
    #[error("failed to resize terminal: {0}")]
    ResizeFailed(String),
    #[error("failed to kill session: {0}")]
    KillFailed(String),
}

/// Terminal session information for listing.
#[derive(Clone, Debug)]
pub struct SessionInfo {
    pub session_id: String,
    pub created_at: u64,
    pub cols: u16,
    pub rows: u16,
    pub is_active: bool,
}

pub struct TerminalService {
    sessions: Arc<DashMap<String, Arc<Session>>>,
}

impl Default for TerminalService {
    fn default() -> Self {
        Self::new()
    }
}

impl TerminalService {
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(DashMap::new()),
        }
    }

    /// Create a new terminal session with the given parameters.
    /// The actual PTY is spawned lazily by `attach_transport`.
    pub fn create_session(
        &self,
        command: Option<String>,
        cwd: Option<String>,
        cols: Option<u16>,
        rows: Option<u16>,
    ) -> Result<String, TerminalError> {
        let cols = cols.unwrap_or(80);
        let rows = rows.unwrap_or(24);
        let id = Uuid::now_v7().to_string();

        self.sessions.insert(
            id.clone(),
            Arc::new(Session::new(id.clone(), command.clone(), cwd.clone(), cols, rows)),
        );

        info!(
            session_id = %id,
            command = command.as_deref().unwrap_or("none"),
            cwd = cwd.as_deref().unwrap_or("none"),
            cols, rows,
            "terminal session reserved"
        );

        Ok(id)
    }

    /// Get a list of all active terminal sessions.
    pub fn list_sessions(&self) -> Vec<SessionInfo> {
        self.sessions
            .iter()
            .map(|entry| {
                let session = entry.value();
                let size = session.size.lock().unwrap();
                SessionInfo {
                    session_id: session.id.clone(),
                    created_at: session.created_at,
                    cols: size.cols,
                    rows: size.rows,
                    is_active: session.attached.load(Ordering::SeqCst),
                }
            })
            .collect()
    }

    /// Kill a terminal session by ID.
    pub fn kill_session(&self, session_id: &str) -> Result<(), TerminalError> {
        // Clone the Arc out first, so we release the DashMap shard lock
        let session = self
            .sessions
            .get(session_id)
            .map(|entry| entry.value().clone());

        // Now remove from the map (no ref held)
        self.sessions.remove(session_id);

        // Kill the session outside of any DashMap lock
        if let Some(session) = session {
            let pty = session.pty.lock().unwrap();
            if let Some(ref pty_session) = *pty {
                pty_session.kill();
            }
            info!(session_id = %session_id, "terminal session killed");
            Ok(())
        } else {
            Err(TerminalError::SessionNotFound(session_id.to_owned()))
        }
    }

    /// Resize a terminal session.
    pub fn resize_session(&self, session_id: &str, cols: u16, rows: u16) -> Result<(), TerminalError> {
        let entry = self
            .sessions
            .get(session_id)
            .ok_or_else(|| TerminalError::SessionNotFound(session_id.to_owned()))?;

        let session = entry.value();

        // Update the session dimensions
        {
            let mut size = session.size.lock().unwrap();
            size.cols = cols;
            size.rows = rows;
        }

        // Resize the PTY if it exists
        {
            let pty = session.pty.lock().unwrap();
            if let Some(ref pty_session) = *pty {
                pty_session.resize(cols, rows)
                    .map_err(|e| TerminalError::ResizeFailed(e))?;
            }
        }

        info!(session_id = %session_id, cols, rows, "terminal session resized");
        Ok(())
    }

    /// Attach a transport to the session, spawning the PTY if not already attached.
    /// Returns the shared terminal session handle.
    pub async fn attach_transport(&self, session_id: &str) -> Result<SharedTerminalSession, TerminalError> {
        let entry = self
            .sessions
            .get(session_id)
            .ok_or_else(|| TerminalError::SessionNotFound(session_id.to_owned()))?;

        let session = entry.value();

        // Check if already attached (idempotency check)
        if session.attached.swap(true, Ordering::SeqCst) {
            // Already attached, return the existing PTY
            let pty = session.pty.lock().unwrap();
            return pty
                .clone()
                .ok_or_else(|| TerminalError::SessionNotFound(session_id.to_owned()));
        }

        // Get the current size
        let size = session.size.lock().unwrap();

        // Spawn the PTY session
        let pty_session = match TerminalSession::new(
            session.command.clone(),
            session.cwd.clone(),
            size.cols,
            size.rows,
        ) {
            Ok(s) => s,
            Err(e) => {
                // Reset attached flag so future attempts can try again
                session.attached.store(false, Ordering::SeqCst);
                return Err(TerminalError::SpawnFailed(e));
            }
        };

        let shared_pty = SharedTerminalSession::new(pty_session);

        // Store the PTY in the session
        {
            let mut pty_guard = session.pty.lock().unwrap();
            *pty_guard = Some(shared_pty.clone());
        }

        info!(session_id = %session_id, "terminal PTY spawned and attached");

        Ok(shared_pty)
    }

    /// Get a reference to a session's PTY if it exists.
    pub fn get_pty(&self, session_id: &str) -> Option<SharedTerminalSession> {
        self.sessions
            .get(session_id)
            .and_then(|entry| {
                let pty = entry.pty.lock().unwrap();
                pty.clone()
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_create_and_list() {
        let service = TerminalService::new();
        
        let id1 = service
            .create_session(Some("echo hello".to_string()), None, None, None)
            .unwrap();
        let id2 = service
            .create_session(None, Some("/tmp".to_string()), Some(120), Some(40))
            .unwrap();

        let sessions = service.list_sessions();
        assert_eq!(sessions.len(), 2);
        
        let session1 = sessions.iter().find(|s| s.session_id == id1).unwrap();
        assert_eq!(session1.cols, 80);
        assert_eq!(session1.rows, 24);
        
        let session2 = sessions.iter().find(|s| s.session_id == id2).unwrap();
        assert_eq!(session2.cols, 120);
        assert_eq!(session2.rows, 40);
    }

    #[test]
    fn test_service_kill() {
        let service = TerminalService::new();
        
        let id = service
            .create_session(None, None, None, None)
            .unwrap();

        assert!(service.kill_session(&id).is_ok());
        assert!(service.kill_session(&id).is_err()); // Should fail - already killed
    }

    #[test]
    fn test_service_kill_nonexistent() {
        let service = TerminalService::new();
        
        let result = service.kill_session("nonexistent");
        assert!(matches!(result, Err(TerminalError::SessionNotFound(_))));
    }

    #[test]
    fn test_service_resize() {
        let service = TerminalService::new();
        
        let id = service
            .create_session(None, None, Some(80), Some(24))
            .unwrap();

        assert!(service.resize_session(&id, 120, 40).is_ok());
        
        let sessions = service.list_sessions();
        let session = sessions.iter().find(|s| s.session_id == id).unwrap();
        assert_eq!(session.cols, 120);
        assert_eq!(session.rows, 40);
    }

    #[test]
    fn test_service_resize_nonexistent() {
        let service = TerminalService::new();
        
        let result = service.resize_session("nonexistent", 120, 40);
        assert!(matches!(result, Err(TerminalError::SessionNotFound(_))));
    }
}
