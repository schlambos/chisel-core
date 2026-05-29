//! M06 — propagate conversation rename / archive to the remote OpenCode
//! session.
//!
//! Registered with `ConversationService::with_update_hook` (wired in
//! `aionui-app`). After any conversation `update()`, this hook re-reads the
//! row; if it is an OpenCode-bound remote conversation with a known server
//! `sessionKey`, it mirrors the current title and archive flag to the server
//! via `PATCH /session/{id}` (see [`RemoteAgentService::update_session`]).
//!
//! The HTTP call is spawned (fire-and-forget) so a slow or unreachable server
//! never blocks the user's rename — the local row was already updated before
//! this hook ran. Failures are logged, not propagated (per the
//! `OnConversationUpdate` contract).

use std::sync::Arc;

use aionui_common::OnConversationUpdate;
use aionui_db::IConversationRepository;
use async_trait::async_trait;
use tracing::{debug, warn};

use crate::services::remote::{RemoteAgentService, RemoteSessionPatch};

/// Reads the post-update conversation row and mirrors title/archive to the
/// bound OpenCode session.
pub struct RemoteSessionSyncHook {
    service: Arc<RemoteAgentService>,
    conversation_repo: Arc<dyn IConversationRepository>,
}

impl RemoteSessionSyncHook {
    pub fn new(service: Arc<RemoteAgentService>, conversation_repo: Arc<dyn IConversationRepository>) -> Self {
        Self {
            service,
            conversation_repo,
        }
    }
}

#[async_trait]
impl OnConversationUpdate for RemoteSessionSyncHook {
    async fn on_conversation_updated(&self, conversation_id: &str) {
        // Re-read the row to inspect type + extra + current name.
        let row = match self.conversation_repo.get(conversation_id).await {
            Ok(Some(row)) => row,
            Ok(None) => return,
            Err(e) => {
                warn!(conversation_id, error = %e, "M06: failed to load conversation for remote-session sync");
                return;
            }
        };

        if row.r#type != "remote" {
            return;
        }

        // Extract the remote binding from `extra`. Both keys are required to
        // address the server session.
        let extra: serde_json::Value = serde_json::from_str(&row.extra).unwrap_or_else(|_| serde_json::json!({}));
        let remote_agent_id = match extra.get("remote_agent_id").and_then(|v| v.as_str()) {
            Some(id) => id.to_string(),
            None => return,
        };
        let session_id = match extra
            .get("sessionKey")
            .or_else(|| extra.get("session_key"))
            .and_then(|v| v.as_str())
        {
            // No server session yet (not created/synced) — nothing to mirror.
            Some(s) if !s.is_empty() => s.to_string(),
            _ => return,
        };

        let archived = extra.get("archived").and_then(|v| v.as_bool()).unwrap_or(false);
        let patch = RemoteSessionPatch {
            title: Some(row.name.clone()),
            archived: Some(archived),
        };

        // Fire-and-forget so the rename response isn't blocked on the server.
        let service = Arc::clone(&self.service);
        let conv_id = conversation_id.to_string();
        tokio::spawn(async move {
            match service.update_session(&remote_agent_id, &session_id, patch).await {
                Ok(()) => {
                    debug!(
                        conversation_id = %conv_id,
                        session_id = %session_id,
                        archived,
                        "M06: synced rename/archive to remote OpenCode session"
                    );
                }
                Err(e) => {
                    warn!(
                        conversation_id = %conv_id,
                        session_id = %session_id,
                        error = %e,
                        "M06: failed to sync rename/archive to remote OpenCode session"
                    );
                }
            }
        });
    }
}
