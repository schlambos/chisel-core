use std::sync::Arc;

use aionui_api_types::RemoteBuildExtra;
use aionui_common::AppError;
use tracing::warn;

use crate::agent_task::AgentInstance;
use crate::factory::AgentFactoryDeps;
use crate::factory::context::FactoryContext;
use crate::manager::remote::{RemoteAgentConfig, RemoteAgentManager};
use crate::types::BuildTaskOptions;

pub(super) async fn build(
    deps: Arc<AgentFactoryDeps>,
    options: BuildTaskOptions,
    ctx: FactoryContext,
) -> Result<AgentInstance, AppError> {
    let extra: RemoteBuildExtra = serde_json::from_value(options.extra)
        .map_err(|e| AppError::BadRequest(format!("Invalid Remote build options: {e}")))?;
    let row = deps
        .remote_agent_repo
        .find_by_id(&extra.remote_agent_id)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to load remote agent config: {e}")))?
        .ok_or_else(|| AppError::NotFound(format!("Remote agent '{}' not found", extra.remote_agent_id)))?;
    let auth_token = row
        .auth_token
        .as_deref()
        .filter(|t| !t.is_empty())
        .and_then(|encrypted| {
            aionui_common::decrypt_string(encrypted, &deps.encryption_key)
                .map_err(|e| {
                    warn!(error = %e, "Failed to decrypt remote agent auth_token");
                })
                .ok()
        });
    let config = RemoteAgentConfig {
        remote_agent_id: row.id.clone(),
        protocol: row.protocol.clone(),
        url: row.url.clone(),
        auth_type: row.auth_type.clone(),
        auth_token,
        allow_insecure: row.allow_insecure,
    };
    // Reload the persisted OpenCode session id (written back to
    // `conversation.extra.sessionKey` after each send) so a rebuild —
    // app restart, conversation reopen, manager re-instantiation —
    // resumes the same server-side session instead of discarding its
    // history/token continuity. `connect()` validates it below; a stale
    // id is discarded so the next send starts fresh. Mirrors
    // `factory/openclaw.rs`.
    let resume_session_id = extra.session_key.clone();
    let agent = RemoteAgentManager::new(ctx.conversation_id, ctx.workspace, config, resume_session_id).await?;
    let arc = Arc::new(agent);
    arc.connect().await?;

    // Forward the user's pre-session model pick (e.g. from the Guid page's
    // pre-fetched model dropdown) into the manager's `desired_model` so the
    // very first message lands on the right model.  Without this, the
    // selection is silently dropped — the row JSON carries it but the
    // factory never reads it, causing the first send to fail and the
    // in-session model selector to render empty until the user picks again.
    if let Some(model_id) = extra.current_model_id.as_deref().filter(|s| !s.is_empty())
        && let Err(e) = arc.set_model(model_id).await
    {
        warn!(error = %e, model_id, "Failed to seed initial model on remote agent");
    }

    // Same pattern as `current_model_id`: forward the user's pre-session
    // mode pick (e.g. OpenCode's `build` / `plan` from the Guid page) so
    // the first prompt lands on the chosen agent without an extra
    // round-trip. Non-opencode protocols reject the call inside
    // `set_mode`; we log and continue rather than failing the build.
    if let Some(mode) = extra.session_mode.as_deref().filter(|s| !s.is_empty())
        && let Err(e) = arc.set_mode(mode).await
    {
        warn!(error = %e, mode, "Failed to seed initial mode on remote agent");
    }

    Ok(AgentInstance::Remote(arc))
}
