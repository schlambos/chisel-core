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

use aionui_api_types::{
    AgentModeResponse, GetModelInfoResponse, RemoteSkillInfo, SetModeRequest, SetModelRequest, SideQuestionRequest,
    SideQuestionResponse, SlashCommandItem, WorkspaceBrowseQuery, WorkspaceEntry,
};
use aionui_common::AppError;

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
    pub async fn revert_remote_session(&self, conversation_id: &str, row_id: &str) -> Result<(), AppError> {
        let message_id = self.resolve_opencode_message_id(conversation_id, row_id).await?;
        self.task(conversation_id)?
            .revert_remote_session(&message_id, None)
            .await
    }

    /// M02: restore all reverted messages on the remote session.
    pub async fn unrevert_remote_session(&self, conversation_id: &str) -> Result<(), AppError> {
        self.task(conversation_id)?.unrevert_remote_session().await
    }

    /// M04: summarize/compact the remote session.
    pub async fn summarize_remote_session(&self, conversation_id: &str) -> Result<(), AppError> {
        self.task(conversation_id)?.summarize_remote_session().await
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
    pub async fn compact_remote_session(
        &self,
        conversation_id: &str,
        instructions: Option<&str>,
    ) -> Result<(), AppError> {
        self.task(conversation_id)?.compact_remote_session(instructions).await
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
        self.task(conversation_id)?
            .remote_find_files(query, limit)
            .await
    }

    /// M13: text search on the remote OpenCode workspace.
    pub async fn remote_find_text(
        &self,
        conversation_id: &str,
        pattern: &str,
        limit: Option<u32>,
    ) -> Result<serde_json::Value, AppError> {
        self.task(conversation_id)?
            .remote_find_text(pattern, limit)
            .await
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
}
