//! Background sync that mirrors OpenCode remote-agent sessions into Chisl's
//! conversation list. Phase 4a of the cross-device handoff design: every
//! session that exists on a connected OpenCode server shows up as a row in
//! the sidebar, grouped under the workspace OpenCode reports for it. The
//! user "attaches" by clicking the row — the existing
//! `factory/remote.rs::build` path then resumes the persisted `sessionKey`.
//!
//! Lifecycle is intentionally simple: a single tokio task runs a 60s poll
//! loop and re-reads the remote-agent list on every tick. Adding or
//! deleting a remote agent therefore takes effect within one tick without
//! any explicit start/stop wiring.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use aionui_ai_agent::RemoteAgentService;
use aionui_api_types::{RemoteSessionInfo, WebSocketMessage};
use aionui_common::{ConversationSource, ConversationStatus, generate_short_id};
use aionui_db::{IConversationRepository, IRemoteAgentRepository, models::ConversationRow};
use aionui_realtime::EventBroadcaster;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

/// User id used for sync-discovered conversations in local mode. The
/// remote-agents table is currently not scoped per-user, so a single
/// system-default user owns all sync rows. Multi-user deployments can
/// extend this to iterate over users with at least one configured remote.
pub const SYNC_USER_ID: &str = "system_default_user";

/// Default interval between sync ticks. Slow enough to be invisible in
/// resource use, fast enough that a session created on another device
/// shows up within ~a minute.
pub const DEFAULT_SYNC_INTERVAL_SECS: u64 = 60;

#[derive(Clone)]
pub struct RemoteSessionSyncService {
    remote_agent_repo: Arc<dyn IRemoteAgentRepository>,
    remote_agent_service: Arc<RemoteAgentService>,
    conversation_repo: Arc<dyn IConversationRepository>,
    broadcaster: Arc<dyn EventBroadcaster>,
    user_id: String,
    interval: Duration,
}

impl RemoteSessionSyncService {
    pub fn new(
        remote_agent_repo: Arc<dyn IRemoteAgentRepository>,
        remote_agent_service: Arc<RemoteAgentService>,
        conversation_repo: Arc<dyn IConversationRepository>,
        broadcaster: Arc<dyn EventBroadcaster>,
    ) -> Self {
        Self {
            remote_agent_repo,
            remote_agent_service,
            conversation_repo,
            broadcaster,
            user_id: SYNC_USER_ID.to_string(),
            interval: Duration::from_secs(DEFAULT_SYNC_INTERVAL_SECS),
        }
    }

    /// Spawn the polling loop as a detached tokio task. First sync runs
    /// immediately so the sidebar populates without waiting a full
    /// interval after app start.
    pub fn spawn(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            info!(
                interval_secs = self.interval.as_secs(),
                "remote-session sync loop started"
            );
            loop {
                if let Err(e) = self.sync_once().await {
                    warn!(error = %e, "remote-session sync tick failed");
                }
                tokio::time::sleep(self.interval).await;
            }
        })
    }

    /// One reconciliation pass over every configured OpenCode remote
    /// agent. Returns the number of conversations newly created in this
    /// tick — useful for the manual-refresh endpoint and tests.
    pub async fn sync_once(&self) -> Result<usize, String> {
        let agents = self
            .remote_agent_repo
            .list()
            .await
            .map_err(|e| format!("list remote agents: {e}"))?;
        let opencode_agents: Vec<_> = agents.into_iter().filter(|a| a.protocol == "opencode").collect();
        if opencode_agents.is_empty() {
            return Ok(0);
        }

        let existing_keys = self.existing_session_keys().await?;
        let mut created = 0;
        for agent in &opencode_agents {
            let sessions = match self.remote_agent_service.list_sessions(&agent.id).await {
                Ok(s) => s,
                Err(e) => {
                    // Don't abort the loop on a single agent's failure —
                    // a flaky server should not stop us from syncing
                    // others. The error surfaces at the next tick.
                    warn!(remote_agent_id = %agent.id, error = %e, "list_sessions failed; skipping agent");
                    continue;
                }
            };

            for session in sessions {
                if existing_keys.contains(&(agent.id.clone(), session.id.clone())) {
                    continue;
                }
                match self.insert_conversation(&agent.id, &session).await {
                    Ok(conv_id) => {
                        created += 1;
                        debug!(
                            remote_agent_id = %agent.id,
                            session_id = %session.id,
                            conversation_id = %conv_id,
                            "mirrored remote session into Chisl conversation"
                        );
                        self.broadcast_created(&conv_id);
                    }
                    Err(e) => warn!(
                        remote_agent_id = %agent.id,
                        session_id = %session.id,
                        error = %e,
                        "failed to mirror remote session"
                    ),
                }
            }
        }
        Ok(created)
    }

    /// Collect `(remote_agent_id, sessionKey)` pairs from every existing
    /// remote conversation so we can skip already-mirrored sessions in
    /// the current tick. Paginates through `list_paginated`; the
    /// remote-conversation set should be small in practice.
    async fn existing_session_keys(&self) -> Result<HashSet<(String, String)>, String> {
        let mut keys: HashSet<(String, String)> = HashSet::new();
        let mut cursor: Option<String> = None;
        loop {
            let filters = aionui_db::ConversationFilters {
                cursor: cursor.clone(),
                limit: 200,
                source: None,
                cron_job_id: None,
                pinned: None,
            };
            let result = self
                .conversation_repo
                .list_paginated(&self.user_id, &filters)
                .await
                .map_err(|e| format!("list conversations: {e}"))?;
            let last_id = result.items.last().map(|r| r.id.clone());
            let has_more = result.has_more;
            for row in result.items {
                if row.r#type != "remote" {
                    continue;
                }
                let Ok(extra) = serde_json::from_str::<serde_json::Value>(&row.extra) else {
                    continue;
                };
                let agent_id = extra.get("remote_agent_id").and_then(|v| v.as_str()).unwrap_or("");
                let session_key = extra.get("sessionKey").and_then(|v| v.as_str()).unwrap_or("");
                if !agent_id.is_empty() && !session_key.is_empty() {
                    keys.insert((agent_id.to_string(), session_key.to_string()));
                }
            }
            if !has_more {
                break;
            }
            cursor = last_id;
        }
        Ok(keys)
    }

    /// Insert a single sync-discovered remote conversation. Bypasses
    /// `ConversationService::create` because that path strips
    /// `extra.sessionKey` (guard for user-initiated creates that must
    /// never inherit a prior session). Sync rows explicitly need the
    /// session id from the start, so we build the row directly.
    async fn insert_conversation(&self, remote_agent_id: &str, session: &RemoteSessionInfo) -> Result<String, String> {
        let id = generate_short_id();
        let created_at = session
            .created_at
            .or(session.updated_at)
            .unwrap_or_else(aionui_common::now_ms);
        let updated_at = session.updated_at.unwrap_or(created_at);
        let title = session.title.clone().unwrap_or_else(|| "Untitled session".to_string());
        let remote_workspace = session.directory.clone().unwrap_or_default();

        let extra = serde_json::json!({
            "workspace": "",
            "remote_workspace": remote_workspace,
            "remote_agent_id": remote_agent_id,
            "sessionKey": session.id,
            "skills": [],
        });

        let row = ConversationRow {
            id: id.clone(),
            user_id: self.user_id.clone(),
            name: title,
            r#type: "remote".to_string(),
            extra: extra.to_string(),
            model: None,
            status: Some(status_str(ConversationStatus::Pending)),
            source: Some(source_str(ConversationSource::Aionui)),
            channel_chat_id: None,
            pinned: false,
            pinned_at: None,
            created_at,
            updated_at,
        };

        self.conversation_repo
            .create(&row)
            .await
            .map_err(|e| format!("insert conversation: {e}"))?;
        Ok(id)
    }

    /// Mirror the WS event `ConversationService` emits after its own
    /// `create()` so the renderer's sidebar refreshes the same way for
    /// sync-created rows as for user-created ones.
    fn broadcast_created(&self, conversation_id: &str) {
        let payload = serde_json::json!({
            "conversation_id": conversation_id,
            "action": "created",
            "source": "aionui",
        });
        let event = WebSocketMessage::new("conversation.listChanged", payload);
        self.broadcaster.broadcast(event);
    }
}

/// `serde_json::to_string` on a `ConversationStatus` produces a quoted
/// literal (e.g. `"\"pending\""`). The DB stores the unquoted form so we
/// strip the quotes here. Mirrors `enum_to_db` in
/// `aionui-conversation::convert`, kept private to avoid widening that
/// module's surface for one call site.
fn status_str(s: ConversationStatus) -> String {
    serde_json::to_string(&s)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string()
}

fn source_str(s: ConversationSource) -> String {
    serde_json::to_string(&s)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string()
}
