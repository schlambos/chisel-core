//! Agent-session operations on ConversationService.
//!
//! These forward to the active AgentInstance (via `self.task(id)`) for
//! mode/model/usage/slash-commands/side-question/openclaw-runtime queries,
//! plus workspace browsing that needs the conversations.extra.workspace
//! field.
//!
//! Kept in a separate file from service.rs to avoid pushing that file
//! over 2000 lines.

use std::path::Component;

use aionui_ai_agent::AgentInstance;
use aionui_ai_agent::protocol::events::{AgentStreamEvent, OpencodeSessionCompactedData};
use aionui_api_types::{
    AgentModeResponse, GetModelInfoResponse, RemoteSkillInfo, SetModeRequest, SetModelRequest, SideQuestionRequest,
    SideQuestionResponse, SlashCommandItem, WebSocketMessage, WorkspaceBrowseQuery, WorkspaceEntry,
};
use aionui_common::AppError;
use aionui_db::SortOrder;
use serde_json::{Value, json};
use tokio::time::{Duration, timeout};
use tracing::{debug, warn};

use crate::service::ConversationService;

const MAX_DIR_DEPTH: usize = 10;

impl ConversationService {
    // ── Mode ────────────────────────────────────────────────────────

    pub async fn get_mode(&self, conversation_id: &str) -> Result<AgentModeResponse, AppError> {
        // Use `get_or_build_agent` (not lookup-only `task`) so the mode query
        // matches `get_model`'s behavior: it warms up / waits for the agent to
        // attach instead of 404-ing during the pre-warmup race on conversation
        // open. For remote OpenCode this is what lets the server-discovered
        // agent catalog (`available_modes`) reach the selector — a bare `task`
        // lookup returns 404 before the SSE connect finishes, and the renderer
        // never retries, so it silently falls back to the static build/plan
        // list. The agent is already built on the same mount by `get_model`, so
        // this adds no new spawn behavior.
        self.get_or_build_agent(conversation_id).await?.get_mode().await
    }

    pub async fn set_mode(&self, conversation_id: &str, req: SetModeRequest) -> Result<(), AppError> {
        if req.mode.trim().is_empty() {
            return Err(AppError::BadRequest("mode must not be empty".into()));
        }
        self.task(conversation_id)?.set_mode(&req.mode).await
    }

    // ── M07: remote message/part edit & delete ─────────────────────

    /// Resolve the OpenCode `messageID` persisted on a local message row's
    /// `content._opencode.message_id`. Returns `BadRequest` when the row is not
    /// addressable (e.g. a non-remote message or one created before M07).
    async fn resolve_opencode_message_id(&self, conversation_id: &str, row_id: &str) -> Result<String, AppError> {
        let row = self
            .conversation_repo()
            .get_message_by_id(conversation_id, row_id)
            .await?
            .ok_or_else(|| AppError::NotFound(format!("Message '{row_id}' not found")))?;
        let content: serde_json::Value = serde_json::from_str(&row.content).unwrap_or_default();
        content
            .get("_opencode")
            .and_then(|o| o.get("message_id"))
            .and_then(|v| v.as_str())
            .map(String::from)
            .ok_or_else(|| AppError::BadRequest("This message is not editable/deletable on the remote server".into()))
    }

    /// M07: delete a message both on the remote OpenCode session and locally,
    /// then broadcast a removal event so connected clients drop it.
    pub async fn delete_remote_message(&self, conversation_id: &str, row_id: &str) -> Result<(), AppError> {
        let opencode_message_id = self.resolve_opencode_message_id(conversation_id, row_id).await?;
        self.task(conversation_id)?
            .delete_remote_message(&opencode_message_id)
            .await?;
        // Best-effort local cleanup — the server delete already succeeded.
        if let Err(e) = self.conversation_repo().delete_message(row_id).await {
            tracing::warn!(conversation_id, row_id, error = %e, "M07: server delete ok but local row delete failed");
        }
        self.broadcaster().broadcast(aionui_api_types::WebSocketMessage::new(
            "message.removed",
            serde_json::json!({ "conversation_id": conversation_id, "msg_id": row_id }),
        ));
        Ok(())
    }

    /// M07: edit a text message both on the remote server and locally. The
    /// renderer passes the new text; the OpenCode part id is taken from the
    /// row's `content._opencode.part_id` when present, otherwise the message's
    /// own id (user text messages are a single text part).
    pub async fn edit_remote_message(
        &self,
        conversation_id: &str,
        row_id: &str,
        new_text: &str,
    ) -> Result<(), AppError> {
        if new_text.trim().is_empty() {
            return Err(AppError::BadRequest("Edited text must not be empty".into()));
        }
        let opencode_message_id = self.resolve_opencode_message_id(conversation_id, row_id).await?;
        let row = self
            .conversation_repo()
            .get_message_by_id(conversation_id, row_id)
            .await?
            .ok_or_else(|| AppError::NotFound(format!("Message '{row_id}' not found")))?;
        let content: serde_json::Value = serde_json::from_str(&row.content).unwrap_or_default();
        let part_id = content
            .get("_opencode")
            .and_then(|o| o.get("part_id"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| AppError::BadRequest("This message has no editable text part".into()))?
            .to_string();
        self.task(conversation_id)?
            .edit_remote_message_part(&opencode_message_id, &part_id, new_text)
            .await?;
        Ok(())
    }

    // ── M01–M05: session lifecycle (fork / revert / share / summarize / diff) ──

    /// M01: fork the remote session, optionally from a local message row id
    /// (resolved to its OpenCode messageID). Returns the new OpenCode session id.
    pub async fn fork_remote_session(
        &self,
        conversation_id: &str,
        from_row_id: Option<&str>,
    ) -> Result<String, AppError> {
        let message_id = match from_row_id {
            Some(row_id) => Some(self.resolve_opencode_message_id(conversation_id, row_id).await?),
            None => None,
        };
        self.task(conversation_id)?
            .fork_remote_session(message_id.as_deref())
            .await
    }

    /// M02: revert the remote session to a local message row id.
    ///
    /// Persists `extra.is_reverted` / `extra.revert_message_id` (local row id,
    /// same key the renderer uses in `computeRevertedRegion`) and broadcasts
    /// `conversation.listChanged(updated)` so the open chat refetches `extra`
    /// without a separate renderer PATCH.
    pub async fn revert_remote_session(&self, conversation_id: &str, row_id: &str) -> Result<(), AppError> {
        let message_id = self.resolve_opencode_message_id(conversation_id, row_id).await?;
        self.task(conversation_id)?
            .revert_remote_session(&message_id, None)
            .await?;
        self.update_extra(
            conversation_id,
            serde_json::json!({
                "is_reverted": true,
                "revert_message_id": row_id,
            }),
        )
        .await?;
        self.broadcast_conversation_updated(conversation_id).await
    }

    /// M02: restore all reverted messages on the remote session.
    pub async fn unrevert_remote_session(&self, conversation_id: &str) -> Result<(), AppError> {
        self.task(conversation_id)?.unrevert_remote_session().await?;
        self.update_extra(
            conversation_id,
            serde_json::json!({
                "is_reverted": false,
                "revert_message_id": serde_json::Value::Null,
            }),
        )
        .await?;
        self.broadcast_conversation_updated(conversation_id).await
    }

    /// M04: summarize/compact the remote session.
    ///
    /// After the HTTP call returns, waits briefly for `session.compacted` on the
    /// agent event bus (idle chats have no `StreamRelay`), then persists
    /// `extra.compaction_*` and broadcasts `conversation.listChanged(updated)` plus
    /// `message.stream` (`opencode_session_compacted`) like M02 revert.
    pub async fn summarize_remote_session(&self, conversation_id: &str) -> Result<(), AppError> {
        let agent = self.get_or_build_agent(conversation_id).await?;
        let mut rx = agent.subscribe();
        agent.summarize_remote_session().await?;
        self.finalize_remote_session_compaction(conversation_id, &agent, &mut rx)
            .await
    }

    /// M03: share the remote session. Returns the share URL.
    pub async fn share_remote_session(&self, conversation_id: &str) -> Result<String, AppError> {
        self.task(conversation_id)?.share_remote_session().await
    }

    /// M03: unshare the remote session.
    pub async fn unshare_remote_session(&self, conversation_id: &str) -> Result<(), AppError> {
        self.task(conversation_id)?.unshare_remote_session().await
    }

    /// M05: fetch the remote session file diff (optionally for a message row id).
    pub async fn remote_session_diff(
        &self,
        conversation_id: &str,
        from_row_id: Option<&str>,
    ) -> Result<serde_json::Value, AppError> {
        let message_id = match from_row_id {
            Some(row_id) => Some(self.resolve_opencode_message_id(conversation_id, row_id).await?),
            None => None,
        };
        self.task(conversation_id)?
            .remote_session_diff(message_id.as_deref())
            .await
    }

    // ── Global config (M19) ─────────────────────────────────────────

    /// M19: read the remote OpenCode server's global configuration tree.
    pub async fn get_global_config(&self, conversation_id: &str) -> Result<serde_json::Value, AppError> {
        self.task(conversation_id)?.get_global_config().await
    }

    /// M19: shallow-merge a partial config into the remote OpenCode server's
    /// global configuration. Returns the new effective config.
    pub async fn patch_global_config(
        &self,
        conversation_id: &str,
        partial: serde_json::Value,
    ) -> Result<serde_json::Value, AppError> {
        self.task(conversation_id)?.patch_global_config(partial).await
    }

    /// M19 (Option A): read the remote OpenCode server's effective config tree.
    pub async fn get_effective_config(&self, conversation_id: &str) -> Result<serde_json::Value, AppError> {
        self.task(conversation_id)?.get_effective_config().await
    }

    /// M15: read the remote OpenCode server's LSP server statuses
    /// (`GET /lsp`) — `[{id, name, root, status:"connected"|"error"}]`.
    pub async fn get_lsp_status(&self, conversation_id: &str) -> Result<serde_json::Value, AppError> {
        self.task(conversation_id)?.get_lsp_status().await
    }

    /// M16: read the remote OpenCode server's VCS info (`GET /vcs`).
    pub async fn get_vcs_info(&self, conversation_id: &str) -> Result<serde_json::Value, AppError> {
        self.task(conversation_id)?.get_vcs_info().await
    }

    /// M16: read the remote OpenCode server's working-tree status
    /// (`GET /vcs/status`).
    pub async fn get_vcs_status(&self, conversation_id: &str) -> Result<serde_json::Value, AppError> {
        self.task(conversation_id)?.get_vcs_status().await
    }

    /// M16: read the structured working-tree diff (`GET /vcs/diff?mode=…`).
    /// `mode` is `"git"` (working tree vs HEAD) or `"branch"` (current vs default).
    pub async fn get_vcs_diff(&self, conversation_id: &str, mode: &str) -> Result<serde_json::Value, AppError> {
        self.task(conversation_id)?.get_vcs_diff(mode).await
    }

    // ── V2 session API (M22) ────────────────────────────────────────

    /// M22: compact the remote session using V2 (with V1 fallback).
    ///
    /// Same post-compact fan-out as [`summarize_remote_session`].
    pub async fn compact_remote_session(&self, conversation_id: &str) -> Result<(), AppError> {
        let agent = self.get_or_build_agent(conversation_id).await?;
        let mut rx = agent.subscribe();
        agent.compact_remote_session().await?;
        self.finalize_remote_session_compaction(conversation_id, &agent, &mut rx)
            .await
    }

    /// M22: get the session's active context window.
    pub async fn get_session_context(&self, conversation_id: &str) -> Result<serde_json::Value, AppError> {
        self.task(conversation_id)?.get_session_context().await
    }

    /// M22: get V2 session messages with cursor-based pagination.
    pub async fn get_v2_messages(
        &self,
        conversation_id: &str,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<serde_json::Value, AppError> {
        self.task(conversation_id)?.get_v2_messages(limit, cursor).await
    }

    /// M22: fetch V2 model list.
    pub async fn fetch_v2_model_list(&self, conversation_id: &str) -> Result<serde_json::Value, AppError> {
        self.task(conversation_id)?.fetch_v2_model_list().await
    }

    /// M22: fetch V2 provider list.
    pub async fn fetch_v2_provider_list(&self, conversation_id: &str) -> Result<serde_json::Value, AppError> {
        self.task(conversation_id)?.fetch_v2_provider_list().await
    }

    /// M13: list files on the remote OpenCode workspace.
    pub async fn remote_list_files(&self, conversation_id: &str, path: &str) -> Result<serde_json::Value, AppError> {
        self.task(conversation_id)?.remote_list_files(path).await
    }

    /// M13: read a file from the remote OpenCode workspace.
    pub async fn remote_read_file(&self, conversation_id: &str, path: &str) -> Result<serde_json::Value, AppError> {
        self.task(conversation_id)?.remote_read_file(path).await
    }

    /// M13: find files on the remote OpenCode workspace.
    pub async fn remote_find_files(
        &self,
        conversation_id: &str,
        query: &str,
        limit: Option<u32>,
    ) -> Result<serde_json::Value, AppError> {
        self.task(conversation_id)?.remote_find_files(query, limit).await
    }

    /// M13: text search on the remote OpenCode workspace.
    pub async fn remote_find_text(
        &self,
        conversation_id: &str,
        pattern: &str,
        limit: Option<u32>,
    ) -> Result<serde_json::Value, AppError> {
        self.task(conversation_id)?.remote_find_text(pattern, limit).await
    }

    /// M13: symbol search on the remote OpenCode workspace.
    pub async fn remote_find_symbols(&self, conversation_id: &str, query: &str) -> Result<serde_json::Value, AppError> {
        self.task(conversation_id)?.remote_find_symbols(query).await
    }

    // ── Model ───────────────────────────────────────────────────────

    pub async fn get_model(&self, conversation_id: &str) -> Result<GetModelInfoResponse, AppError> {
        self.get_or_build_agent(conversation_id).await?.get_model().await
    }

    pub async fn set_model(&self, conversation_id: &str, req: SetModelRequest) -> Result<(), AppError> {
        if req.model_id.trim().is_empty() {
            return Err(AppError::BadRequest("model_id must not be empty".into()));
        }
        self.get_or_build_agent(conversation_id)
            .await?
            .set_model(&req.model_id)
            .await
    }

    // ── Usage / Slash commands ──────────────────────────────────────

    pub async fn get_usage(&self, conversation_id: &str) -> Result<Option<serde_json::Value>, AppError> {
        self.get_or_build_agent(conversation_id).await?.get_usage().await
    }

    pub async fn get_slash_commands(&self, conversation_id: &str) -> Result<Vec<SlashCommandItem>, AppError> {
        self.get_or_build_agent(conversation_id)
            .await?
            .get_slash_commands()
            .await
    }

    /// M10: server-side skill catalog (OpenCode `GET /skill`). Returns an
    /// empty list for non-OpenCode backends so the picker renders nothing.
    pub async fn get_skills(&self, conversation_id: &str) -> Result<Vec<RemoteSkillInfo>, AppError> {
        self.get_or_build_agent(conversation_id).await?.get_skills().await
    }

    // ── Side question ───────────────────────────────────────────────

    pub async fn handle_side_question(
        &self,
        conversation_id: &str,
        req: SideQuestionRequest,
    ) -> Result<SideQuestionResponse, AppError> {
        // `AgentInstance::handle_side_question` already validates that the
        // question is non-empty; no need to duplicate the check here.
        self.task(conversation_id)?.handle_side_question(req).await
    }

    // ── OpenClaw runtime diagnostics ────────────────────────────────

    pub async fn get_openclaw_runtime(&self, conversation_id: &str) -> Result<serde_json::Value, AppError> {
        self.task(conversation_id)?.get_openclaw_runtime().await
    }

    // ── Workspace browsing ──────────────────────────────────────────

    /// Resolve the absolute workspace path stored on the conversation's
    /// `extra.workspace` field. Used by the per-conversation VCS
    /// endpoints (Task 18.1) to bridge between the conversation id in
    /// the URL and the on-disk directory we should query.
    pub async fn get_workspace_path(&self, conversation_id: &str) -> Result<String, AppError> {
        let row = self
            .conversation_repo()
            .get(conversation_id)
            .await
            .map_err(|e| AppError::Internal(format!("Failed to load conversation: {e}")))?
            .ok_or_else(|| AppError::NotFound(format!("Conversation '{conversation_id}' not found")))?;

        let extra: serde_json::Value =
            serde_json::from_str(&row.extra).map_err(|e| AppError::Internal(format!("Invalid extra JSON: {e}")))?;
        let workspace = extra
            .get("workspace")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_owned();
        if workspace.is_empty() {
            return Err(AppError::BadRequest("Conversation has no workspace assigned".into()));
        }
        Ok(workspace)
    }

    /// Enumerate entries under `query.path` inside the conversation's
    /// workspace root. Enforces workspace isolation (no traversal outside
    /// the root, with an allowance for symlinked sub-directories) and a
    /// depth cap of [`MAX_DIR_DEPTH`].
    pub async fn browse_workspace(
        &self,
        conversation_id: &str,
        query: WorkspaceBrowseQuery,
    ) -> Result<Vec<WorkspaceEntry>, AppError> {
        if query.path.trim().is_empty() {
            return Err(AppError::BadRequest("path must not be empty".into()));
        }

        if let Ok(agent) = self.get_or_build_agent(conversation_id).await {
            if let Some(entries) = agent
                .browse_remote_workspace(&query.path, query.search.as_deref())
                .await?
            {
                return Ok(entries);
            }
        }

        let row = self
            .conversation_repo()
            .get(conversation_id)
            .await
            .map_err(|e| AppError::Internal(format!("Failed to load conversation: {e}")))?
            .ok_or_else(|| AppError::NotFound(format!("Conversation '{conversation_id}' not found")))?;

        let extra: serde_json::Value =
            serde_json::from_str(&row.extra).map_err(|e| AppError::Internal(format!("Invalid extra JSON: {e}")))?;
        let workspace = extra
            .get("workspace")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_owned();
        if workspace.is_empty() {
            return Err(AppError::BadRequest("Conversation has no workspace assigned".into()));
        }

        let relative_path = query.path.trim_start_matches('/');
        let relative_path_obj = std::path::Path::new(relative_path);
        if relative_path_obj
            .components()
            .any(|component| matches!(component, Component::ParentDir))
        {
            return Err(AppError::BadRequest(
                "Path traversal outside workspace is not allowed".into(),
            ));
        }

        // Resolve the browsed path relative to the workspace root
        let base = std::path::Path::new(&workspace);
        let browse_path = if relative_path.is_empty() {
            base.to_path_buf()
        } else {
            base.join(relative_path_obj)
        };

        // Security: reject direct traversal outside the workspace root, but allow
        // symlinked directories mounted inside the workspace (e.g. native skill
        // dirs that point at the builtin skills corpus under data-dir).
        let canonical_base = base
            .canonicalize()
            .map_err(|e| AppError::Internal(format!("Failed to resolve workspace path: {e}")))?;
        let canonical_browse = browse_path
            .canonicalize()
            .map_err(|_| AppError::NotFound("Directory not found".into()))?;
        if !browse_path.starts_with(base) && !canonical_browse.starts_with(&canonical_base) {
            return Err(AppError::BadRequest(
                "Path traversal outside workspace is not allowed".into(),
            ));
        }

        // Check depth limit
        let depth = relative_path_obj.components().count();
        if depth > MAX_DIR_DEPTH {
            return Err(AppError::BadRequest(format!(
                "Directory depth exceeds maximum of {MAX_DIR_DEPTH}"
            )));
        }

        let mut entries = Vec::new();
        let mut dir_reader = tokio::fs::read_dir(&canonical_browse)
            .await
            .map_err(|e| AppError::Internal(format!("Failed to read directory: {e}")))?;

        while let Ok(Some(entry)) = dir_reader.next_entry().await {
            let name = entry.file_name().to_string_lossy().into_owned();

            // Apply search filter if provided
            if let Some(ref search) = query.search
                && !search.is_empty()
                && !name.to_lowercase().contains(&search.to_lowercase())
            {
                continue;
            }

            let entry_path = entry.path();
            let metadata = tokio::fs::metadata(&entry_path)
                .await
                .map_err(|e| AppError::Internal(format!("Failed to read entry metadata: {e}")))?;

            let entry_type = if metadata.is_dir() { "directory" } else { "file" };

            entries.push(WorkspaceEntry {
                name,
                entry_type: entry_type.into(),
            });
        }

        // Sort: directories first, then alphabetically
        entries.sort_by(|a, b| {
            let type_cmp = a.entry_type.cmp(&b.entry_type);
            if type_cmp == std::cmp::Ordering::Equal {
                a.name.to_lowercase().cmp(&b.name.to_lowercase())
            } else {
                type_cmp
            }
        });

        Ok(entries)
    }

    /// After manual summarize/compact, collect `OpencodeSessionCompacted` from the
    /// agent bus (no turn-scoped relay) and mirror revert's extra + WS fan-out.
    async fn finalize_remote_session_compaction(
        &self,
        conversation_id: &str,
        agent: &AgentInstance,
        rx: &mut tokio::sync::broadcast::Receiver<AgentStreamEvent>,
    ) -> Result<(), AppError> {
        let mut compacted = match timeout(Duration::from_secs(15), Self::recv_session_compacted(rx)).await {
            Ok(Some(data)) => data,
            Ok(None) => {
                debug!(
                    conversation_id = %conversation_id,
                    "compact finished without OpencodeSessionCompacted event"
                );
                OpencodeSessionCompactedData {
                    summary: String::new(),
                    tokens_reclaimed: 0,
                    original_start_message_id: String::new(),
                    original_end_message_id: String::new(),
                }
            }
            Err(_) => {
                debug!(
                    conversation_id = %conversation_id,
                    "timed out waiting for OpencodeSessionCompacted after compact"
                );
                OpencodeSessionCompactedData {
                    summary: String::new(),
                    tokens_reclaimed: 0,
                    original_start_message_id: String::new(),
                    original_end_message_id: String::new(),
                }
            }
        };

        // The live OpenCode server (1.15.x) emits a bare `session.compacted`
        // = `{sessionID}` with no summary, range, or token metrics. When that
        // happens we pull the truth from the server's own transcript: the
        // compaction is a user message carrying a `compaction` part, and the
        // structured summary lives in the assistant message whose `parentID`
        // is that user message. This is what OpenCode's own UI renders.
        let transcript = self.fetch_compaction_from_server(agent).await;
        if compacted.summary.is_empty() || compacted.original_end_message_id.is_empty() {
            if let Some(transcript) = transcript.as_ref() {
                if compacted.summary.is_empty() {
                    compacted.summary = transcript.summary_markdown.clone();
                }
                if compacted.original_end_message_id.is_empty() {
                    compacted.original_end_message_id = transcript.compaction_message_id.clone();
                }
            }
        }

        if compacted.summary.is_empty() {
            warn!(
                conversation_id = %conversation_id,
                "OpenCode compaction completed but no transcript summary message was found"
            );
        }

        // OpenCode anchors the divider at the compaction boundary — the
        // compaction message itself, with the retained tail (new context)
        // below it. `compaction_end_message_id` is that boundary; the
        // renderer draws the firm divider after it and renders the summary
        // markdown below. Map the OpenCode id to a local row when possible so
        // the renderer can match it against the persisted transcript.
        let marker_row = if compacted.original_end_message_id.is_empty() {
            None
        } else {
            self.resolve_local_row_for_opencode_message(conversation_id, &compacted.original_end_message_id)
                .await?
        };
        let fallback_visible_boundary = match (marker_row.as_ref(), transcript.as_ref()) {
            (Some(_), _) => None,
            (None, Some(t)) => {
                let mut row = None;
                for opencode_id in t.previous_visible_message_ids.iter().rev() {
                    if let Some(resolved) = self
                        .resolve_local_row_for_opencode_message(conversation_id, opencode_id)
                        .await?
                    {
                        row = Some(resolved);
                        break;
                    }
                }
                row
            }
            (None, None) => None,
        };
        let end_anchor = if compacted.original_end_message_id.is_empty() {
            serde_json::Value::Null
        } else if let Some(row) = marker_row.as_ref().or(fallback_visible_boundary.as_ref()) {
            serde_json::Value::String(row.clone())
        } else {
            serde_json::Value::String(compacted.original_end_message_id.clone())
        };

        let marker_value = if compacted.original_end_message_id.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::Value::String(compacted.original_end_message_id.clone())
        };
        let summary_message_value = transcript
            .as_ref()
            .and_then(|t| t.summary_message_id.as_ref())
            .map(|id| serde_json::Value::String(id.clone()))
            .unwrap_or(serde_json::Value::Null);

        // Keep `compaction_start_message_id` populated for backward
        // compatibility with older renderers, but it is no longer the primary
        // anchor. Prefer the resolved start range; fall back to the boundary.
        let start_row_id = if compacted.original_start_message_id.is_empty() {
            None
        } else {
            self.resolve_local_row_for_opencode_message(conversation_id, &compacted.original_start_message_id)
                .await?
        };
        let start_anchor = match start_row_id {
            Some(row) => serde_json::Value::String(row),
            None if !compacted.original_start_message_id.is_empty() => {
                serde_json::Value::String(compacted.original_start_message_id.clone())
            }
            None => end_anchor.clone(),
        };

        let summary_value = if compacted.summary.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::Value::String(compacted.summary.clone())
        };

        self.update_extra(
            conversation_id,
            json!({
                "compaction_start_message_id": start_anchor,
                "compaction_end_message_id": end_anchor,
                "compaction_marker_message_id": marker_value,
                "compaction_summary_message_id": summary_message_value,
                "compaction_tokens_reclaimed": compacted.tokens_reclaimed,
                "compaction_summary": summary_value,
            }),
        )
        .await?;
        self.broadcast_conversation_updated(conversation_id).await?;
        self.broadcast_opencode_session_compacted(conversation_id, &compacted);
        Ok(())
    }

    /// Pull the compaction summary + boundary from the server transcript when
    /// the `session.compacted` event was bare. Returns
    /// `(summary_markdown, boundary_opencode_message_id)`.
    ///
    /// OpenCode's data model: the compaction is a **user** message carrying a
    /// part of `type:"compaction"`; the structured summary is the **assistant**
    /// message whose `info.summary` is set and whose `info.parentID` equals the
    /// compaction user message id. The summary text is the joined `text` parts.
    /// The boundary anchor is the compaction user message id (the divider sits
    /// after it, with the retained tail below). Best-effort: any failure
    /// returns `None` and the caller falls back to event data.
    async fn fetch_compaction_from_server(&self, agent: &AgentInstance) -> Option<CompactionTranscript> {
        let raw = agent.get_v2_messages(None, None).await.ok()?;
        extract_compaction_transcript(&raw)
    }

    async fn recv_session_compacted(
        rx: &mut tokio::sync::broadcast::Receiver<AgentStreamEvent>,
    ) -> Option<OpencodeSessionCompactedData> {
        loop {
            match rx.recv().await {
                Ok(AgentStreamEvent::OpencodeSessionCompacted(data)) => return Some(data),
                Ok(_) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
            }
        }
    }

    /// Map an OpenCode `messageID` from `session.compacted` to a local message row id.
    async fn resolve_local_row_for_opencode_message(
        &self,
        conversation_id: &str,
        opencode_message_id: &str,
    ) -> Result<Option<String>, AppError> {
        let mut page = 1u32;
        const PAGE_SIZE: u32 = 200;
        loop {
            let batch = self
                .conversation_repo()
                .get_messages(conversation_id, page, PAGE_SIZE, SortOrder::Asc)
                .await?;
            if batch.items.is_empty() {
                return Ok(None);
            }
            for row in &batch.items {
                let content: serde_json::Value = serde_json::from_str(&row.content).unwrap_or_default();
                if content
                    .get("_opencode")
                    .and_then(|o| o.get("message_id"))
                    .and_then(|v| v.as_str())
                    == Some(opencode_message_id)
                {
                    return Ok(Some(row.id.clone()));
                }
            }
            if batch.items.len() < PAGE_SIZE as usize {
                return Ok(None);
            }
            page += 1;
        }
    }

    fn broadcast_opencode_session_compacted(&self, conversation_id: &str, data: &OpencodeSessionCompactedData) {
        let event = AgentStreamEvent::OpencodeSessionCompacted(data.clone());
        let mut event_data = match serde_json::to_value(&event) {
            Ok(v) => v,
            Err(_) => return,
        };
        aionui_common::normalize_keys_to_snake_case(&mut event_data);
        let payload = json!({
            "conversation_id": conversation_id,
            "msg_id": ConversationService::mint_msg_id(),
            "type": event_data.get("type").cloned().unwrap_or(json!("opencode_session_compacted")),
            "data": event_data.get("data").cloned().unwrap_or(json!({})),
            "hidden": false,
        });
        self.broadcaster()
            .broadcast(WebSocketMessage::new("message.stream", payload));
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CompactionTranscript {
    compaction_message_id: String,
    summary_message_id: Option<String>,
    summary_markdown: String,
    previous_visible_message_ids: Vec<String>,
}

fn extract_compaction_transcript(raw: &Value) -> Option<CompactionTranscript> {
    let items = raw.get("items").and_then(|v| v.as_array()).or_else(|| raw.as_array())?;

    let mut compaction_index = None;
    for (index, msg) in items.iter().enumerate() {
        if role(msg) == Some("user") && has_part_type(msg, "compaction") {
            compaction_index = Some(index);
        }
    }
    let compaction_index = compaction_index?;
    let compaction_message_id = message_id(&items[compaction_index])?.to_string();

    let previous_visible_message_ids = items[..compaction_index]
        .iter()
        .filter(|msg| has_visible_transcript_part(msg))
        .filter_map(|msg| message_id(msg).map(str::to_string))
        .collect::<Vec<_>>();

    let mut fallback_summary = None;
    let mut parented_summary = None;
    for msg in &items[compaction_index + 1..] {
        if role(msg) != Some("assistant") || !has_summary_metadata(msg) {
            continue;
        }
        let summary_markdown = text_parts(msg).concat();
        if summary_markdown.is_empty() {
            continue;
        }
        let candidate = CompactionTranscript {
            compaction_message_id: compaction_message_id.clone(),
            summary_message_id: message_id(msg).map(str::to_string),
            summary_markdown,
            previous_visible_message_ids: previous_visible_message_ids.clone(),
        };
        if parent_id(msg) == Some(compaction_message_id.as_str()) {
            parented_summary = Some(candidate);
            break;
        }
        if fallback_summary.is_none() {
            fallback_summary = Some(candidate);
        }
    }

    parented_summary.or(fallback_summary)
}

fn message_info(msg: &Value) -> &Value {
    msg.get("info").unwrap_or(msg)
}

fn message_id(msg: &Value) -> Option<&str> {
    let info = message_info(msg);
    info.get("id")
        .or_else(|| info.get("messageID"))
        .or_else(|| info.get("messageId"))
        .or_else(|| info.get("message_id"))
        .and_then(|v| v.as_str())
}

fn role(msg: &Value) -> Option<&str> {
    message_info(msg).get("role").and_then(|v| v.as_str())
}

fn parent_id(msg: &Value) -> Option<&str> {
    let info = message_info(msg);
    info.get("parentID")
        .or_else(|| info.get("parentId"))
        .or_else(|| info.get("parent_id"))
        .and_then(|v| v.as_str())
}

fn parts(msg: &Value) -> Option<&Vec<Value>> {
    msg.get("parts").and_then(|v| v.as_array())
}

fn has_part_type(msg: &Value, part_type: &str) -> bool {
    parts(msg)
        .map(|parts| {
            parts
                .iter()
                .any(|part| part.get("type").and_then(|v| v.as_str()) == Some(part_type))
        })
        .unwrap_or(false)
}

fn has_visible_transcript_part(msg: &Value) -> bool {
    parts(msg)
        .map(|parts| {
            parts.iter().any(|part| {
                matches!(
                    part.get("type").and_then(|v| v.as_str()),
                    Some("text" | "reasoning" | "tool" | "retry")
                )
            })
        })
        .unwrap_or(false)
}

fn has_summary_metadata(msg: &Value) -> bool {
    let info = message_info(msg);
    info.get("summary")
        .or_else(|| info.get("isSummary"))
        .or_else(|| info.get("is_summary"))
        .map(|v| !v.is_null() && v.as_bool() != Some(false))
        .unwrap_or(false)
}

fn text_parts(msg: &Value) -> Vec<&str> {
    parts(msg)
        .into_iter()
        .flat_map(|parts| parts.iter())
        .filter(|part| part.get("type").and_then(|v| v.as_str()) == Some("text"))
        .filter_map(|part| {
            part.get("text")
                .or_else(|| part.get("content"))
                .and_then(|v| v.as_str())
        })
        .filter(|text| !text.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extracts_compaction_transcript_from_array_response() {
        let raw = json!([
            {
                "info": { "id": "msg_old", "role": "user" },
                "parts": [{ "type": "text", "text": "old" }]
            },
            {
                "info": { "id": "msg_compact", "role": "user" },
                "parts": [{ "type": "compaction" }]
            },
            {
                "info": {
                    "id": "msg_summary",
                    "role": "assistant",
                    "parentID": "msg_compact",
                    "summary": true
                },
                "parts": [
                    { "type": "text", "text": "# Goal\nKeep exact markdown." },
                    { "type": "text", "text": "\n# Next Steps\nContinue." }
                ]
            },
            {
                "info": { "id": "msg_tail", "role": "user" },
                "parts": [{ "type": "text", "text": "tail" }]
            }
        ]);

        let transcript = extract_compaction_transcript(&raw).expect("transcript");

        assert_eq!(transcript.compaction_message_id, "msg_compact");
        assert_eq!(transcript.summary_message_id.as_deref(), Some("msg_summary"));
        assert_eq!(
            transcript.summary_markdown,
            "# Goal\nKeep exact markdown.\n# Next Steps\nContinue."
        );
        assert_eq!(transcript.previous_visible_message_ids, vec!["msg_old"]);
    }

    #[test]
    fn extracts_compaction_transcript_from_items_response_and_parent_variants() {
        let raw = json!({
            "items": [
                {
                    "info": { "id": "msg_old", "role": "assistant" },
                    "parts": [{ "type": "reasoning", "text": "thinking" }]
                },
                {
                    "info": { "id": "msg_compact", "role": "user" },
                    "parts": [{ "type": "compaction" }]
                },
                {
                    "info": {
                        "id": "msg_summary",
                        "role": "assistant",
                        "parent_id": "msg_compact",
                        "is_summary": true
                    },
                    "parts": [{ "type": "text", "content": "## Files\n- a.ts" }]
                }
            ],
            "cursor": null
        });

        let transcript = extract_compaction_transcript(&raw).expect("transcript");

        assert_eq!(transcript.compaction_message_id, "msg_compact");
        assert_eq!(transcript.summary_message_id.as_deref(), Some("msg_summary"));
        assert_eq!(transcript.summary_markdown, "## Files\n- a.ts");
        assert_eq!(transcript.previous_visible_message_ids, vec!["msg_old"]);
    }

    #[test]
    fn returns_none_when_summary_missing() {
        let raw = json!({
            "items": [
                {
                    "info": { "id": "msg_compact", "role": "user" },
                    "parts": [{ "type": "compaction" }]
                }
            ]
        });

        assert!(extract_compaction_transcript(&raw).is_none());
    }
}
