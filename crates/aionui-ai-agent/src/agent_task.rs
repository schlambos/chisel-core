//! Minimal public contract for a running agent task.
//!
//! `IAgentTask` captures **only** the operations that every agent type
//! implements identically and that the generic task_manager / idle_scanner /
//! message-flow code actually needs. Anything that is type-specific
//! (session modes, session keys, model switching, config options, pending
//! confirmation lists, approval memory, ACP usage, OpenClaw diagnostics,
//! etc.) lives as **inherent** methods on each concrete `XxxAgentManager`
//! and is reached through the `AgentInstance` enum — forcing every callsite
//! to say out loud which agent type it is addressing.
//!
//! Replaces the old bloated `IAgentManager` trait + `as_any()` downcast
//! pattern (deleted in PR #8c).
use std::sync::Arc;

use aionui_common::{AgentKillReason, AgentType, AppError, ConversationStatus, TimestampMs};
use tokio::sync::broadcast;

use crate::manager::acp::AcpAgentManager;
use crate::manager::aionrs::AionrsAgentManager;
use crate::manager::nanobot::NanobotAgentManager;
use crate::manager::openclaw::OpenClawAgentManager;
use crate::manager::remote::RemoteAgentManager;
use crate::protocol::events::AgentStreamEvent;
use crate::types::SendMessageData;

use aionui_api_types::{
    GetModelInfoResponse, ModelInfoEntry, ModelInfoPayload, RemoteSkillInfo, SideQuestionRequest, SideQuestionResponse,
    SlashCommandItem, WorkspaceEntry,
};

#[cfg(any(test, feature = "test-support"))]
use aionui_common::Confirmation;

/// Ten-method public surface every agent type implements identically.
///
/// Object-safe by construction (no generic methods, no `Self` by value).
/// Used by generic lifecycle code (task_manager, idle_scanner, stream
/// fan-out) that genuinely does not care which agent type it is dealing
/// with. For type-specific operations, match on [`AgentInstance`] and
/// call the concrete manager's inherent methods.
#[async_trait::async_trait]
pub trait IAgentTask: Send + Sync {
    /// The type of agent this task controls.
    fn agent_type(&self) -> AgentType;

    /// Conversation ID this task is bound to.
    fn conversation_id(&self) -> &str;

    /// Working directory for this agent session.
    fn workspace(&self) -> &str;

    /// Current conversation status. `None` if the agent has not
    /// transitioned into a known status yet.
    fn status(&self) -> Option<ConversationStatus>;

    /// Timestamp (ms) of the last activity (message send, event received).
    fn last_activity_at(&self) -> TimestampMs;

    /// Subscribe to the agent's stream event channel.
    fn subscribe(&self) -> broadcast::Receiver<AgentStreamEvent>;

    /// Send a user message to the agent. Returns once the agent has
    /// accepted the turn; actual streaming proceeds on the broadcast
    /// channel returned by [`Self::subscribe`].
    async fn send_message(&self, data: SendMessageData) -> Result<(), AppError>;

    /// Stop the current streaming response without killing the agent.
    async fn cancel(&self) -> Result<(), AppError>;

    /// Terminate the agent process.
    ///
    /// - `reason: Some(IdleTimeout)` — idle cleanup
    /// - `reason: None` — explicit user/system kill
    fn kill(&self, reason: Option<AgentKillReason>) -> Result<(), AppError>;
}

/// Extended trait used exclusively by the `AgentInstance::Mock` variant so
/// tests can inject richer fake behaviour (pending confirmations, approval
/// memory, fake session keys, etc.) without polluting the production
/// `IAgentTask` contract with trait-level defaults that would be lies for
/// at least one concrete manager.
///
/// Every method has a sensible identity-style default so simple mocks only
/// need to implement the ten `IAgentTask` methods and pick up nothing for
/// free.
#[cfg(any(test, feature = "test-support"))]
#[async_trait::async_trait]
pub trait IMockAgent: IAgentTask {
    fn get_confirmations(&self) -> Vec<Confirmation> {
        Vec::new()
    }
    fn check_approval(&self, _action: &str, _command_type: Option<&str>) -> bool {
        false
    }
    fn confirm(
        &self,
        _msg_id: &str,
        _call_id: &str,
        _data: serde_json::Value,
        _always_allow: bool,
    ) -> Result<(), AppError> {
        Ok(())
    }
    fn get_session_key(&self) -> Option<String> {
        None
    }
    async fn mode(&self) -> Result<aionui_api_types::AgentModeResponse, AppError> {
        Ok(aionui_api_types::AgentModeResponse {
            mode: "default".into(),
            initialized: false,
            available_modes: None,
        })
    }
    async fn set_mode(&self, _mode: &str) -> Result<(), AppError> {
        Err(AppError::BadRequest(
            "Mode switching is not supported for this mock".into(),
        ))
    }
    async fn get_model(&self) -> Result<GetModelInfoResponse, AppError> {
        Ok(GetModelInfoResponse { model_info: None })
    }
    async fn set_model(&self, _model_id: &str) -> Result<(), AppError> {
        Err(AppError::BadRequest(
            "Model switching is not supported for this mock".into(),
        ))
    }
    async fn get_usage(&self) -> Result<Option<serde_json::Value>, AppError> {
        Ok(None)
    }
    async fn get_slash_commands(&self) -> Result<Vec<SlashCommandItem>, AppError> {
        Ok(Vec::new())
    }
    async fn get_skills(&self) -> Result<Vec<RemoteSkillInfo>, AppError> {
        Ok(Vec::new())
    }
    async fn handle_side_question(&self, _req: SideQuestionRequest) -> Result<SideQuestionResponse, AppError> {
        Ok(SideQuestionResponse {
            status: "unsupported".into(),
            answer: None,
        })
    }
    async fn get_openclaw_runtime(&self) -> Result<serde_json::Value, AppError> {
        Ok(serde_json::Value::Null)
    }
}

/// Concrete, closed-set dispatcher for the five agent variants.
///
/// Every generic path holds an `AgentInstance` (not `Arc<dyn IAgentTask>`):
/// this gives us the `IAgentTask` ten-method surface via [`Self::as_task`]
/// **and** lets type-specific routes recover the concrete manager with a
/// single `match` — no `as_any` / `downcast_ref` anywhere. Adding a new
/// agent type means adding a new variant here; every `match` in the
/// codebase then fails to compile until it explicitly handles the new
/// type, which is the compile-time pressure we want.
#[derive(Clone)]
pub enum AgentInstance {
    Acp(Arc<AcpAgentManager>),
    Aionrs(Arc<AionrsAgentManager>),
    OpenClaw(Arc<OpenClawAgentManager>),
    Nanobot(Arc<NanobotAgentManager>),
    Remote(Arc<RemoteAgentManager>),
    /// Test-only trait-object escape hatch used by downstream crates
    /// (conversation/cron/team/app tests) to inject fake agents without
    /// spinning up a real CLI or WebSocket connection. Gated behind
    /// `#[cfg(any(test, feature = "test-support"))]`: production builds
    /// never see this variant, so every `match` in release code can
    /// rely on the five-variant closed set. The trait object is
    /// [`IMockAgent`] (extends `IAgentTask`) so mocks can also override
    /// the enum-level helpers — `get_confirmations`, `check_approval`,
    /// `confirm`, `get_session_key`, `get_mode`, `set_mode`.
    #[cfg(any(test, feature = "test-support"))]
    Mock(Arc<dyn IMockAgent>),
}

impl AgentInstance {
    /// Common `IAgentTask` view, regardless of variant.
    pub fn as_task(&self) -> &dyn IAgentTask {
        match self {
            Self::Acp(m) => m.as_ref(),
            Self::Aionrs(m) => m.as_ref(),
            Self::OpenClaw(m) => m.as_ref(),
            Self::Nanobot(m) => m.as_ref(),
            Self::Remote(m) => m.as_ref(),
            #[cfg(any(test, feature = "test-support"))]
            Self::Mock(m) => m.as_ref(),
        }
    }

    // ── Convenience forwarders ───────────────────────────────────────
    //
    // These stay in the final API (not a migration crutch): they turn
    // `instance.agent_type()` into a direct vtable-free call on the
    // concrete `Arc<XxxManager>`, and they keep callsites terse.

    /// The type of agent this instance controls.
    pub fn agent_type(&self) -> AgentType {
        self.as_task().agent_type()
    }

    /// Conversation ID this task is bound to.
    pub fn conversation_id(&self) -> &str {
        self.as_task().conversation_id()
    }

    /// Working directory for this agent session.
    pub fn workspace(&self) -> &str {
        self.as_task().workspace()
    }

    /// Current conversation status.
    pub fn status(&self) -> Option<ConversationStatus> {
        self.as_task().status()
    }

    /// Timestamp (ms) of the last activity.
    pub fn last_activity_at(&self) -> TimestampMs {
        self.as_task().last_activity_at()
    }

    /// Subscribe to the stream event channel.
    pub fn subscribe(&self) -> broadcast::Receiver<AgentStreamEvent> {
        self.as_task().subscribe()
    }

    /// Send a user message to the agent.
    pub async fn send_message(&self, data: SendMessageData) -> Result<(), AppError> {
        self.as_task().send_message(data).await
    }

    /// Cancel the current streaming response without killing the agent.
    pub async fn cancel(&self) -> Result<(), AppError> {
        self.as_task().cancel().await
    }

    /// Terminate the agent process.
    pub fn kill(&self, reason: Option<AgentKillReason>) -> Result<(), AppError> {
        self.as_task().kill(reason)
    }

    /// Terminate the agent process and return a future that resolves when the
    /// underlying OS process has exited.
    pub fn kill_and_wait(
        &self,
        reason: Option<AgentKillReason>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        match self {
            Self::Acp(m) => m.kill_and_wait(reason),
            Self::OpenClaw(m) => m.kill_and_wait(reason),
            Self::Nanobot(m) => m.kill_and_wait(reason),
            Self::Aionrs(m) => m.kill_and_wait(reason),
            Self::Remote(m) => m.kill_and_wait(reason),
            #[cfg(any(test, feature = "test-support"))]
            Self::Mock(_) => Box::pin(std::future::ready(())),
        }
    }

    // ── Cross-variant semi-specific helpers ──────────────────────────
    //
    // These fan out to inherent methods on concrete managers. Variants
    // that don't support the operation return a sensible zero-value
    // rather than an error: "no pending confirmations" and "no session
    // key" are honest statements about those variants.

    /// Pending confirmation items for this task.
    ///
    /// ACP currently tracks permission prompts inline through the
    /// permission router (not surfaced here), so returns empty.
    /// Aionrs / OpenClaw / Remote maintain inline confirmation lists.
    /// Nanobot has no concept of confirmations.
    pub fn get_confirmations(&self) -> Vec<aionui_common::Confirmation> {
        match self {
            Self::Acp(_) => Vec::new(),
            Self::Aionrs(m) => m.get_confirmations(),
            Self::OpenClaw(m) => m.get_confirmations(),
            Self::Nanobot(_) => Vec::new(),
            Self::Remote(m) => m.get_confirmations(),
            #[cfg(any(test, feature = "test-support"))]
            Self::Mock(m) => m.get_confirmations(),
        }
    }

    /// Submit a confirmation response for a pending tool call.
    pub fn confirm(
        &self,
        msg_id: &str,
        call_id: &str,
        data: serde_json::Value,
        always_allow: bool,
    ) -> Result<(), AppError> {
        match self {
            Self::Acp(m) => m.confirm(msg_id, call_id, data, always_allow),
            Self::Aionrs(m) => m.confirm(msg_id, call_id, data, always_allow),
            Self::OpenClaw(m) => m.confirm(msg_id, call_id, data, always_allow),
            Self::Nanobot(m) => m.confirm(msg_id, call_id, data, always_allow),
            Self::Remote(m) => m.confirm(msg_id, call_id, data, always_allow),
            #[cfg(any(test, feature = "test-support"))]
            Self::Mock(m) => m.confirm(msg_id, call_id, data, always_allow),
        }
    }

    /// Check whether an action is auto-approved in this session.
    pub fn check_approval(&self, action: &str, command_type: Option<&str>) -> bool {
        match self {
            Self::Acp(_) => false,
            Self::Aionrs(m) => m.check_approval(action, command_type),
            Self::OpenClaw(m) => m.check_approval(action, command_type),
            Self::Nanobot(_) => false,
            Self::Remote(m) => m.check_approval(action, command_type),
            #[cfg(any(test, feature = "test-support"))]
            Self::Mock(m) => m.check_approval(action, command_type),
        }
    }

    /// Session key for agent types that expose one (OpenClaw `sessionKey`,
    /// Remote OpenCode `ses_...`). Persisted to `conversation.extra.sessionKey`.
    pub fn get_session_key(&self) -> Option<String> {
        match self {
            Self::OpenClaw(m) => m.get_session_key(),
            Self::Remote(m) => m.get_session_key(),
            Self::Acp(_) | Self::Aionrs(_) | Self::Nanobot(_) => None,
            #[cfg(any(test, feature = "test-support"))]
            Self::Mock(m) => m.get_session_key(),
        }
    }

    /// Get the current session mode. ACP, Aionrs, and the Remote OpenCode
    /// protocol model a mode. Other variants report `mode = "default"`,
    /// `initialized = false` so cron / UI can skip mode reconciliation.
    pub async fn get_mode(&self) -> Result<aionui_api_types::AgentModeResponse, AppError> {
        match self {
            Self::Acp(m) => m.mode().await,
            Self::Aionrs(m) => m.mode().await,
            Self::Remote(m) => m.mode().await,
            Self::OpenClaw(_) | Self::Nanobot(_) => Ok(aionui_api_types::AgentModeResponse {
                mode: "default".into(),
                initialized: false,
                available_modes: None,
            }),
            #[cfg(any(test, feature = "test-support"))]
            Self::Mock(m) => m.mode().await,
        }
    }

    /// Set the session mode. Supported by ACP, Aionrs, and Remote (OpenCode
    /// only). Other variants return `BadRequest` so the caller can surface
    /// an actionable error rather than silently no-op.
    pub async fn set_mode(&self, mode: &str) -> Result<(), AppError> {
        match self {
            Self::Acp(m) => m.set_mode(mode).await,
            Self::Aionrs(m) => m.set_mode(mode).await,
            Self::Remote(m) => m.set_mode(mode).await,
            Self::OpenClaw(_) | Self::Nanobot(_) => Err(AppError::BadRequest(
                "Mode switching is not supported for this agent type".into(),
            )),
            #[cfg(any(test, feature = "test-support"))]
            Self::Mock(m) => m.set_mode(mode).await,
        }
    }

    /// M07: delete a message on the remote OpenCode session. Only the Remote
    /// (OpenCode) variant supports this; others return `BadRequest`.
    pub async fn delete_remote_message(&self, message_id: &str) -> Result<(), AppError> {
        match self {
            Self::Remote(m) => m.opencode_delete_message(message_id).await,
            _ => Err(AppError::BadRequest(
                "Message delete is only supported for OpenCode remote conversations".into(),
            )),
        }
    }

    /// M07: delete a single message part on the remote OpenCode session.
    pub async fn delete_remote_message_part(&self, message_id: &str, part_id: &str) -> Result<(), AppError> {
        match self {
            Self::Remote(m) => m.opencode_delete_message_part(message_id, part_id).await,
            _ => Err(AppError::BadRequest(
                "Part delete is only supported for OpenCode remote conversations".into(),
            )),
        }
    }

    /// M07: edit a text part on the remote OpenCode session.
    pub async fn edit_remote_message_part(
        &self,
        message_id: &str,
        part_id: &str,
        new_text: &str,
    ) -> Result<(), AppError> {
        match self {
            Self::Remote(m) => m.opencode_edit_message_part(message_id, part_id, new_text).await,
            _ => Err(AppError::BadRequest(
                "Part edit is only supported for OpenCode remote conversations".into(),
            )),
        }
    }

    /// M01: fork the remote OpenCode session (optionally from a message id).
    /// Returns the new server-side session id.
    pub async fn fork_remote_session(&self, message_id: Option<&str>) -> Result<String, AppError> {
        match self {
            Self::Remote(m) => m.opencode_fork(message_id).await,
            _ => Err(AppError::BadRequest(
                "Fork is only supported for OpenCode remote conversations".into(),
            )),
        }
    }

    /// M02: revert the remote OpenCode session to a message/part.
    pub async fn revert_remote_session(&self, message_id: &str, part_id: Option<&str>) -> Result<(), AppError> {
        match self {
            Self::Remote(m) => m.opencode_revert(message_id, part_id).await,
            _ => Err(AppError::BadRequest(
                "Revert is only supported for OpenCode remote conversations".into(),
            )),
        }
    }

    /// M02: restore all reverted messages on the remote OpenCode session.
    pub async fn unrevert_remote_session(&self) -> Result<(), AppError> {
        match self {
            Self::Remote(m) => m.opencode_unrevert().await,
            _ => Err(AppError::BadRequest(
                "Unrevert is only supported for OpenCode remote conversations".into(),
            )),
        }
    }

    /// M04: summarize/compact the remote OpenCode session.
    pub async fn summarize_remote_session(&self) -> Result<(), AppError> {
        match self {
            Self::Remote(m) => m.opencode_summarize().await,
            _ => Err(AppError::BadRequest(
                "Summarize is only supported for OpenCode remote conversations".into(),
            )),
        }
    }

    /// M03: share the remote OpenCode session. Returns the share URL.
    pub async fn share_remote_session(&self) -> Result<String, AppError> {
        match self {
            Self::Remote(m) => m.opencode_share().await,
            _ => Err(AppError::BadRequest(
                "Share is only supported for OpenCode remote conversations".into(),
            )),
        }
    }

    /// M03: unshare the remote OpenCode session.
    pub async fn unshare_remote_session(&self) -> Result<(), AppError> {
        match self {
            Self::Remote(m) => m.opencode_unshare().await,
            _ => Err(AppError::BadRequest(
                "Unshare is only supported for OpenCode remote conversations".into(),
            )),
        }
    }

    /// M05: fetch the remote OpenCode session file diff (optionally per message).
    pub async fn remote_session_diff(&self, message_id: Option<&str>) -> Result<serde_json::Value, AppError> {
        match self {
            Self::Remote(m) => m.opencode_session_diff(message_id).await,
            _ => Err(AppError::BadRequest(
                "Diff is only supported for OpenCode remote conversations".into(),
            )),
        }
    }

    /// M19: read the OpenCode server's global configuration (`GET /global/config`).
    pub async fn get_global_config(&self) -> Result<serde_json::Value, AppError> {
        match self {
            Self::Remote(m) => m.opencode_get_global_config().await,
            _ => Err(AppError::BadRequest(
                "Global config is only supported for OpenCode remote conversations".into(),
            )),
        }
    }

    /// M19: shallow-merge a partial config into the OpenCode server's global
    /// configuration (`PATCH /global/config`). Returns the new effective config.
    pub async fn patch_global_config(&self, partial: serde_json::Value) -> Result<serde_json::Value, AppError> {
        match self {
            Self::Remote(m) => m.opencode_patch_global_config(partial).await,
            _ => Err(AppError::BadRequest(
                "Global config is only supported for OpenCode remote conversations".into(),
            )),
        }
    }

    /// M19 (Option A): read the OpenCode server's **effective** config
    /// (`GET /config`) — the merged view the engine runs, used to detect edits
    /// shadowed by a higher-precedence layer.
    pub async fn get_effective_config(&self) -> Result<serde_json::Value, AppError> {
        match self {
            Self::Remote(m) => m.opencode_get_effective_config().await,
            _ => Err(AppError::BadRequest(
                "Global config is only supported for OpenCode remote conversations".into(),
            )),
        }
    }

    /// M15 — read the remote OpenCode server's LSP status array (`GET /lsp`).
    pub async fn get_lsp_status(&self) -> Result<serde_json::Value, AppError> {
        match self {
            Self::Remote(m) => m.opencode_get_lsp_status().await,
            _ => Err(AppError::BadRequest(
                "LSP status is only supported for OpenCode remote conversations".into(),
            )),
        }
    }

    /// M16 — read the remote OpenCode server's VCS info (`GET /vcs`).
    pub async fn get_vcs_info(&self) -> Result<serde_json::Value, AppError> {
        match self {
            Self::Remote(m) => m.opencode_get_vcs_info().await,
            _ => Err(AppError::BadRequest(
                "VCS is only supported for OpenCode remote conversations".into(),
            )),
        }
    }

    /// M16 — read the remote OpenCode server's working-tree status
    /// (`GET /vcs/status`).
    pub async fn get_vcs_status(&self) -> Result<serde_json::Value, AppError> {
        match self {
            Self::Remote(m) => m.opencode_get_vcs_status().await,
            _ => Err(AppError::BadRequest(
                "VCS is only supported for OpenCode remote conversations".into(),
            )),
        }
    }

    /// M16 — read the structured working-tree diff (`GET /vcs/diff?mode=…`).
    pub async fn get_vcs_diff(&self, mode: &str) -> Result<serde_json::Value, AppError> {
        match self {
            Self::Remote(m) => m.opencode_get_vcs_diff(mode).await,
            _ => Err(AppError::BadRequest(
                "VCS is only supported for OpenCode remote conversations".into(),
            )),
        }
    }

    /// M22: compact the remote OpenCode session using V2 (with V1 fallback).
    pub async fn compact_remote_session(&self, instructions: Option<&str>) -> Result<(), AppError> {
        match self {
            Self::Remote(m) => m.opencode_compact(instructions).await,
            _ => Err(AppError::BadRequest(
                "Compact is only supported for OpenCode remote conversations".into(),
            )),
        }
    }

    /// M22: get the session's active context window.
    pub async fn get_session_context(&self) -> Result<serde_json::Value, AppError> {
        match self {
            Self::Remote(m) => m.opencode_get_context().await,
            _ => Err(AppError::BadRequest(
                "Context is only available for OpenCode remote conversations".into(),
            )),
        }
    }

    /// M22: get V2 session messages with cursor-based pagination.
    pub async fn get_v2_messages(
        &self,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<serde_json::Value, AppError> {
        match self {
            Self::Remote(m) => m.opencode_v2_messages(limit, cursor).await,
            _ => Err(AppError::BadRequest(
                "V2 messages is only available for OpenCode remote conversations".into(),
            )),
        }
    }

    /// M22: fetch V2 model list.
    pub async fn fetch_v2_model_list(&self) -> Result<serde_json::Value, AppError> {
        match self {
            Self::Remote(m) => m.fetch_v2_model_list().await,
            _ => Err(AppError::BadRequest(
                "V2 models is only available for OpenCode remote conversations".into(),
            )),
        }
    }

    /// M22: fetch V2 provider list.
    pub async fn fetch_v2_provider_list(&self) -> Result<serde_json::Value, AppError> {
        match self {
            Self::Remote(m) => m.fetch_v2_provider_list().await,
            _ => Err(AppError::BadRequest(
                "V2 providers is only available for OpenCode remote conversations".into(),
            )),
        }
    }

    /// M13: remote OpenCode file tree (`GET /file`).
    pub async fn remote_list_files(&self, path: &str) -> Result<serde_json::Value, AppError> {
        match self {
            Self::Remote(m) => m.opencode_list_files(path).await,
            _ => Err(AppError::BadRequest(
                "Remote file listing requires an OpenCode remote conversation".into(),
            )),
        }
    }

    /// M13: read a file from the remote OpenCode workspace.
    pub async fn remote_read_file(&self, path: &str) -> Result<serde_json::Value, AppError> {
        match self {
            Self::Remote(m) => m.opencode_read_file(path).await,
            _ => Err(AppError::BadRequest(
                "Remote file read requires an OpenCode remote conversation".into(),
            )),
        }
    }

    /// M13: find files on the remote OpenCode workspace.
    pub async fn remote_find_files(&self, query: &str, limit: Option<u32>) -> Result<serde_json::Value, AppError> {
        match self {
            Self::Remote(m) => m.opencode_find_files(query, limit).await,
            _ => Err(AppError::BadRequest(
                "Remote file search requires an OpenCode remote conversation".into(),
            )),
        }
    }

    /// M13: text search on the remote OpenCode workspace.
    pub async fn remote_find_text(&self, pattern: &str, limit: Option<u32>) -> Result<serde_json::Value, AppError> {
        match self {
            Self::Remote(m) => m.opencode_find_text(pattern, limit).await,
            _ => Err(AppError::BadRequest(
                "Remote text search requires an OpenCode remote conversation".into(),
            )),
        }
    }

    /// M13: symbol search on the remote OpenCode workspace.
    pub async fn remote_find_symbols(&self, query: &str) -> Result<serde_json::Value, AppError> {
        match self {
            Self::Remote(m) => m.opencode_find_symbols(query).await,
            _ => Err(AppError::BadRequest(
                "Remote symbol search requires an OpenCode remote conversation".into(),
            )),
        }
    }

    /// M13: browse the remote OpenCode file tree when `tool_host: "server"`.
    /// Returns `None` when the conversation should use local filesystem browsing.
    pub async fn browse_remote_workspace(
        &self,
        path: &str,
        search: Option<&str>,
    ) -> Result<Option<Vec<WorkspaceEntry>>, AppError> {
        match self {
            Self::Remote(m) if m.uses_server_tool_host() => {
                let raw = m.opencode_list_files(path).await?;
                Ok(Some(map_opencode_file_nodes(raw, search)))
            }
            _ => Ok(None),
        }
    }

    /// Get the current session model info. Only ACP exposes a model
    /// catalog; other variants report `model_info = None` so the UI can
    /// hide the model picker without an error.
    pub async fn get_model(&self) -> Result<GetModelInfoResponse, AppError> {
        match self {
            Self::Acp(m) => {
                let sdk_model = m.model().await;
                let sdk_info = sdk_model.map(map_sdk_model_to_payload);
                let cc_switch_info = if m.is_claude_backend() {
                    crate::cc_switch::read_claude_model_info()
                } else {
                    None
                };
                let model_info = merge_model_info(sdk_info, cc_switch_info);
                Ok(GetModelInfoResponse { model_info })
            }
            Self::Remote(m) => m.get_model().await,
            Self::Aionrs(_) | Self::OpenClaw(_) | Self::Nanobot(_) => Ok(GetModelInfoResponse { model_info: None }),
            #[cfg(any(test, feature = "test-support"))]
            Self::Mock(m) => m.get_model().await,
        }
    }

    /// Switch the active model. Supported for ACP and Remote (OpenCode protocol).
    /// Returns `BadRequest` for unsupported agent types.
    pub async fn set_model(&self, model_id: &str) -> Result<(), AppError> {
        if model_id.trim().is_empty() {
            return Err(AppError::BadRequest("model_id must not be empty".into()));
        }
        match self {
            Self::Acp(m) => m.set_model(model_id).await,
            Self::Remote(m) => m.set_model(model_id).await,
            Self::Aionrs(_) | Self::OpenClaw(_) | Self::Nanobot(_) => Err(AppError::BadRequest(
                "Model switching is not supported for this agent type".into(),
            )),
            #[cfg(any(test, feature = "test-support"))]
            Self::Mock(m) => m.set_model(model_id).await,
        }
    }

    /// Returns the cached session usage as a snake_case JSON object. The
    /// structure mirrors the ACP SDK `UsageUpdate` schema
    /// (`used` / `size` / `cost` / `_meta`), normalised via
    /// [`aionui_common::normalize_keys_to_snake_case`] so keys land as
    /// `used` / `size` / `cost` to match the AionUI wire convention —
    /// `_meta` passes through verbatim.
    ///
    /// Non-ACP agents return `None`.
    pub async fn get_usage(&self) -> Result<Option<serde_json::Value>, AppError> {
        match self {
            Self::Acp(m) => {
                let Some(usage) = m.usage().await else { return Ok(None) };
                let mut value = serde_json::to_value(usage)
                    .map_err(|e| AppError::Internal(format!("Failed to serialize usage: {e}")))?;
                aionui_common::normalize_keys_to_snake_case(&mut value);
                Ok(Some(value))
            }
            Self::Aionrs(_) | Self::OpenClaw(_) | Self::Nanobot(_) | Self::Remote(_) => Ok(None),
            #[cfg(any(test, feature = "test-support"))]
            Self::Mock(m) => m.get_usage().await,
        }
    }

    /// Slash commands available in the current session. Only ACP exposes
    /// a slash-command catalog; other variants report an empty list
    /// (the UI renders "no commands").
    pub async fn get_slash_commands(&self) -> Result<Vec<SlashCommandItem>, AppError> {
        match self {
            Self::Acp(m) => m.load_slash_commands().await,
            Self::Aionrs(m) => m.get_slash_commands().await,
            // Native OpenCode advertises commands via GET /command; the
            // Remote manager fetches them lazily and returns an empty list
            // for non-opencode protocols (openclaw / nanobot).
            Self::Remote(m) => m.get_slash_commands_impl().await,
            Self::OpenClaw(_) | Self::Nanobot(_) => Ok(Vec::new()),
            #[cfg(any(test, feature = "test-support"))]
            Self::Mock(m) => m.get_slash_commands().await,
        }
    }

    /// M10: server-side skill catalog (OpenCode `GET /skill`). Only the Remote
    /// OpenCode variant exposes skills; other variants report an empty list so
    /// the picker renders "no skills" rather than erroring.
    pub async fn get_skills(&self) -> Result<Vec<RemoteSkillInfo>, AppError> {
        match self {
            Self::Remote(m) => m.get_skills_impl().await,
            Self::Acp(_) | Self::Aionrs(_) | Self::OpenClaw(_) | Self::Nanobot(_) => Ok(Vec::new()),
            #[cfg(any(test, feature = "test-support"))]
            Self::Mock(m) => m.get_skills().await,
        }
    }

    /// Dispatch a side-question to the agent. **Placeholder** — matches
    /// the current `AgentService::handle_side_question` behaviour: ACP
    /// agents whose behavior_policy enables side-questions return a stub
    /// "ok" response, everyone else returns `unsupported`.
    pub async fn handle_side_question(&self, req: SideQuestionRequest) -> Result<SideQuestionResponse, AppError> {
        if req.question.trim().is_empty() {
            return Err(AppError::BadRequest("question must not be empty".into()));
        }
        match self {
            Self::Acp(m) => {
                if !m.supports_side_question() {
                    return Ok(SideQuestionResponse {
                        status: "unsupported".into(),
                        answer: None,
                    });
                }
                Ok(SideQuestionResponse {
                    status: "ok".into(),
                    answer: Some("Side question support will be fully wired in app integration phase.".into()),
                })
            }
            Self::Aionrs(_) | Self::OpenClaw(_) | Self::Nanobot(_) | Self::Remote(_) => Ok(SideQuestionResponse {
                status: "unsupported".into(),
                answer: None,
            }),
            #[cfg(any(test, feature = "test-support"))]
            Self::Mock(m) => m.handle_side_question(req).await,
        }
    }

    /// OpenClaw-specific runtime diagnostics. Only OpenClaw reports
    /// diagnostics; other variants report `Value::Null` so diagnostic
    /// UIs degrade gracefully.
    pub async fn get_openclaw_runtime(&self) -> Result<serde_json::Value, AppError> {
        match self {
            Self::OpenClaw(m) => Ok(m.get_diagnostics().await),
            Self::Acp(_) | Self::Aionrs(_) | Self::Nanobot(_) | Self::Remote(_) => Ok(serde_json::Value::Null),
            #[cfg(any(test, feature = "test-support"))]
            Self::Mock(m) => m.get_openclaw_runtime().await,
        }
    }
}

/// Map the raw ACP SDK model state into the public API payload.
///
/// Kept private to this module: the only caller is
/// [`AgentInstance::get_model`]. Mirrors the helper formerly living in
/// `services/agent.rs`; do not duplicate — if the shape of
/// `ModelInfoPayload` changes, update it here.
fn map_sdk_model_to_payload(m: agent_client_protocol::schema::SessionModelState) -> ModelInfoPayload {
    let available: Vec<ModelInfoEntry> = m
        .available_models
        .iter()
        .map(|am| ModelInfoEntry {
            id: am.model_id.to_string(),
            label: am.name.clone(),
        })
        .collect();
    let current_id = m.current_model_id.to_string();
    let current_label = available
        .iter()
        .find(|e| e.id == current_id)
        .map(|e| e.label.clone())
        .unwrap_or_else(|| current_id.clone());
    ModelInfoPayload {
        current_model_id: Some(current_id),
        current_model_label: Some(current_label),
        available_models: available,
    }
}

fn merge_model_info(
    sdk_info: Option<ModelInfoPayload>,
    cc_switch_info: Option<ModelInfoPayload>,
) -> Option<ModelInfoPayload> {
    sdk_info.or(cc_switch_info)
}

fn map_opencode_file_nodes(value: serde_json::Value, search: Option<&str>) -> Vec<WorkspaceEntry> {
    let nodes = value.as_array().cloned().unwrap_or_default();
    let mut entries = Vec::new();
    for node in nodes {
        let name = node.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if name.is_empty() {
            continue;
        }
        if let Some(s) = search {
            if !s.is_empty() && !name.to_lowercase().contains(&s.to_lowercase()) {
                continue;
            }
        }
        let entry_type = node.get("type").and_then(|v| v.as_str()).unwrap_or("file").to_string();
        entries.push(WorkspaceEntry { name, entry_type });
    }
    entries.sort_by(|a, b| {
        let type_cmp = a.entry_type.cmp(&b.entry_type);
        if type_cmp == std::cmp::Ordering::Equal {
            a.name.to_lowercase().cmp(&b.name.to_lowercase())
        } else {
            type_cmp
        }
    });
    entries
}

#[cfg(test)]
mod cc_switch_model_merge_tests {
    use super::*;

    #[test]
    fn merge_prefers_sdk_model_over_cc_switch() {
        let sdk_payload = ModelInfoPayload {
            current_model_id: Some("default".into()),
            current_model_label: Some("Claude Sonnet 4.6".into()),
            available_models: vec![ModelInfoEntry {
                id: "default".into(),
                label: "Claude Sonnet 4.6".into(),
            }],
        };
        let cc_switch_payload = ModelInfoPayload {
            current_model_id: Some("default".into()),
            current_model_label: Some("DeepSeek V4".into()),
            available_models: vec![ModelInfoEntry {
                id: "default".into(),
                label: "DeepSeek V4".into(),
            }],
        };

        let result = merge_model_info(Some(sdk_payload), Some(cc_switch_payload));
        assert_eq!(
            result.unwrap().current_model_label.as_deref(),
            Some("Claude Sonnet 4.6")
        );
    }

    #[test]
    fn merge_falls_back_to_cc_switch_when_sdk_none() {
        let cc_switch_payload = ModelInfoPayload {
            current_model_id: Some("default".into()),
            current_model_label: Some("DeepSeek V4".into()),
            available_models: vec![ModelInfoEntry {
                id: "default".into(),
                label: "DeepSeek V4".into(),
            }],
        };

        let result = merge_model_info(None, Some(cc_switch_payload));
        assert_eq!(result.unwrap().current_model_label.as_deref(), Some("DeepSeek V4"));
    }

    #[test]
    fn merge_returns_none_when_both_none() {
        let result = merge_model_info(None, None);
        assert!(result.is_none());
    }
}
