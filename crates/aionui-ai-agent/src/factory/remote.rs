use std::path::Path;
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
    let workspace = resolve_local_workspace(&ctx.workspace, &deps.work_dir, &ctx.conversation_id);
    let agent = RemoteAgentManager::new_with_history(
        ctx.conversation_id,
        workspace,
        config,
        resume_session_id,
        deps.conversation_repo.clone(),
    )
    .await?;
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

fn resolve_local_workspace(workspace: &str, fallback_work_dir: &Path, conversation_id: &str) -> String {
    if Path::new(workspace).is_dir() || workspace.trim().is_empty() {
        return workspace.to_owned();
    }

    if fallback_work_dir.is_dir() {
        warn!(
            conversation_id,
            stored_workspace = %workspace,
            fallback_workspace = %fallback_work_dir.display(),
            "remote conversation workspace is not local; using configured work dir for local fs"
        );
        return fallback_work_dir.to_string_lossy().into_owned();
    }

    workspace.to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_local_workspace_keeps_existing_local_directory() {
        let workspace = tempfile::TempDir::new().unwrap();
        let fallback = tempfile::TempDir::new().unwrap();

        let resolved = resolve_local_workspace(&workspace.path().to_string_lossy(), fallback.path(), "conv");

        assert_eq!(resolved, workspace.path().to_string_lossy());
    }

    #[test]
    fn resolve_local_workspace_uses_work_dir_for_non_local_remote_directory() {
        let fallback = tempfile::TempDir::new().unwrap();

        let resolved = resolve_local_workspace("/app", fallback.path(), "conv");

        assert_eq!(resolved, fallback.path().to_string_lossy());
    }

    #[test]
    fn resolve_local_workspace_keeps_original_when_no_local_fallback_exists() {
        let root = tempfile::TempDir::new().unwrap();
        let stored = root.path().join("remote-only");
        let fallback = root.path().join("missing-work-dir");
        let stored = stored.to_string_lossy().into_owned();

        let resolved = resolve_local_workspace(&stored, &fallback, "conv");

        assert_eq!(resolved, stored);
    }
}
