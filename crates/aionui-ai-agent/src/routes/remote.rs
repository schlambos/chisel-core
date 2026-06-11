//! Remote agent management API routes.
//!
//! Endpoints:
//!
//! - `GET  /api/remote-agents`                    — list remote agents
//! - `POST /api/remote-agents`                    — create new remote agent
//! - `GET  /api/remote-agents/{id}`                 — get remote agent details
//! - `PUT  /api/remote-agents/{id}`                 — update remote agent
//! - `DELETE /api/remote-agents/{id}`                 — delete remote agent
//! - `POST /api/remote-agents/test-connection`          — test connection to remote agent (without saving it)
//! - `POST /api/remote-agents/{id}/handshake`          — perform handshake with the remote agent to verify connectivity and retrieve agent info
//! - `GET  /api/remote-agents/{id}/models`             — fetch available models from an OpenCode remote agent's /provider endpoint
//! - `GET  /api/remote-agents/{id}/agents`             — fetch the selectable agent catalog from an OpenCode remote agent's /agent endpoint
//! - `GET  /api/remote-agents/{id}/skills`             — fetch the selectable skill catalog from an OpenCode remote agent's /skill endpoint
//! - `GET  /api/remote-agents/{id}/sessions`           — list active sessions on an OpenCode remote agent (for cross-device attach)
//! - `POST /api/conversations/{id}/backfill-remote-history` — Phase 4b: lazy-load historical messages from OpenCode into the local conversation

use aionui_api_types::WebSocketMessage;
use axum::Router;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Extension, Json, Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};

use aionui_api_types::{
    AgentModeOption, ApiResponse, CreateRemoteAgentRequest, HandshakeResponse, ModelInfoPayload,
    RemoteAgentHealthResponse, RemoteAgentListItem, RemoteAgentPluginInfoResponse, RemoteAgentResponse,
    RemoteSessionInfo, RemoteSkillInfo, TestRemoteAgentConnectionRequest, UpdateRemoteAgentRequest,
};
use aionui_auth::CurrentUser;
use aionui_common::AppError;

use super::state::RemoteAgentRouterState;

/// Build the remote agent router.
///
/// All routes require authentication (applied by the caller).
pub fn remote_agent_routes(state: RemoteAgentRouterState) -> Router {
    Router::new()
        .route("/api/remote-agents", get(list).post(create))
        .route("/api/remote-agents/test-connection", post(test_connection))
        .route("/api/remote-agents/{id}", get(get_one).put(update).delete(delete_one))
        .route("/api/remote-agents/{id}/health", get(ping_health))
        .route("/api/remote-agents/{id}/handshake", post(handshake))
        .route("/api/remote-agents/{id}/models", get(fetch_models))
        .route("/api/remote-agents/{id}/agents", get(fetch_agents))
        .route("/api/remote-agents/{id}/skills", get(fetch_skills))
        .route("/api/remote-agents/{id}/sessions", get(list_sessions))
        .route(
            "/api/conversations/{id}/backfill-remote-history",
            post(backfill_remote_history),
        )
        .route("/api/remote-agents/{id}/providers", get(fetch_provider_catalog))
        .route("/api/remote-agents/{id}/plugin", get(plugin_info))
        .route("/api/remote-agents/{id}/plugin/rotate-token", post(plugin_rotate_token))
        .route("/api/remote-agents/{id}/plugin/voice-mode", post(plugin_voice_mode))
        .route("/api/remote-agents/{id}/bg-processes", get(list_bg_processes))
        .route("/api/remote-agents/{id}/bg-processes/{pid}/stop", post(stop_bg_process))
        .route(
            "/api/remote-agents/{id}/bg-processes/{pid}/output",
            get(read_bg_process_output),
        )
        .route(
            "/api/remote-agents/{id}/providers/auth",
            get(fetch_provider_auth_methods),
        )
        .route(
            "/api/remote-agents/{id}/providers/{providerId}/auth",
            post(set_provider_auth),
        )
        .route(
            "/api/remote-agents/{id}/providers/{providerId}/auth",
            axum::routing::delete(delete_provider_auth),
        )
        .route(
            "/api/remote-agents/{id}/providers/{providerId}/oauth/start",
            post(start_provider_oauth),
        )
        .route(
            "/api/remote-agents/{id}/providers/{providerId}/oauth/complete",
            post(complete_provider_oauth),
        )
        .with_state(state)
}

async fn list(
    State(state): State<RemoteAgentRouterState>,
    Extension(_user): Extension<CurrentUser>,
) -> Result<Json<ApiResponse<Vec<RemoteAgentListItem>>>, AppError> {
    let items = state.service.list().await?;
    Ok(Json(ApiResponse::ok(items)))
}

async fn get_one(
    State(state): State<RemoteAgentRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<RemoteAgentResponse>>, AppError> {
    let agent = state.service.get(&id).await?;
    Ok(Json(ApiResponse::ok(agent)))
}

async fn create(
    State(state): State<RemoteAgentRouterState>,
    Extension(_user): Extension<CurrentUser>,
    body: Result<Json<CreateRemoteAgentRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ApiResponse<RemoteAgentResponse>>), AppError> {
    let Json(req) = body.map_err(|e| AppError::BadRequest(e.to_string()))?;
    let agent = state.service.create(req).await?;
    Ok((StatusCode::CREATED, Json(ApiResponse::ok(agent))))
}

async fn update(
    State(state): State<RemoteAgentRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
    body: Result<Json<UpdateRemoteAgentRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<RemoteAgentResponse>>, AppError> {
    let Json(req) = body.map_err(|e| AppError::BadRequest(e.to_string()))?;
    let agent = state.service.update(&id, req).await?;
    Ok(Json(ApiResponse::ok(agent)))
}

async fn delete_one(
    State(state): State<RemoteAgentRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    state.service.delete(&id).await?;
    Ok(Json(ApiResponse::success()))
}

async fn test_connection(
    State(state): State<RemoteAgentRouterState>,
    Extension(_user): Extension<CurrentUser>,
    body: Result<Json<TestRemoteAgentConnectionRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    let Json(req) = body.map_err(|e| AppError::BadRequest(e.to_string()))?;
    state.service.test_connection(req).await?;
    Ok(Json(ApiResponse::success()))
}

async fn handshake(
    State(state): State<RemoteAgentRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<HandshakeResponse>>, AppError> {
    let resp = state.service.handshake(&id).await?;
    Ok(Json(ApiResponse::ok(resp)))
}

async fn ping_health(
    State(state): State<RemoteAgentRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<RemoteAgentHealthResponse>>, AppError> {
    let resp = state.service.ping_health(&id).await?;
    Ok(Json(ApiResponse::ok(resp)))
}

async fn fetch_models(
    State(state): State<RemoteAgentRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<ModelInfoPayload>>, AppError> {
    let payload = state.service.fetch_models(&id).await?;
    Ok(Json(ApiResponse::ok(payload)))
}

async fn fetch_agents(
    State(state): State<RemoteAgentRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<Vec<AgentModeOption>>>, AppError> {
    let agents = state.service.fetch_agents(&id).await?;
    Ok(Json(ApiResponse::ok(agents)))
}

/// M10: fetch server-side OpenCode skill catalog for the remote agent row.
/// Used by the Guid (New Chat) page, which has no conversation yet and so
/// cannot route through the per-conversation `/skills` endpoint.
async fn fetch_skills(
    State(state): State<RemoteAgentRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<Vec<RemoteSkillInfo>>>, AppError> {
    let skills = state.service.fetch_skills(&id).await?;
    Ok(Json(ApiResponse::ok(skills)))
}

async fn list_sessions(
    State(state): State<RemoteAgentRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<Vec<RemoteSessionInfo>>>, AppError> {
    let sessions = state.service.list_sessions(&id).await?;
    Ok(Json(ApiResponse::ok(sessions)))
}

/// Phase 4b: lazy-load the OpenCode message transcript into the local
/// conversation the first time the user opens it.
///
/// Idempotent — once `extra.history_loaded` is true we short-circuit. The
/// renderer calls this on conversation open when it detects the flag
/// is false; subsequent calls are cheap no-ops. Emits
/// `conversation.listChanged(updated)` after the run so clients refresh
/// the message list view.
async fn backfill_remote_history(
    State(state): State<RemoteAgentRouterState>,
    Extension(user): Extension<CurrentUser>,
    Path(conversation_id): Path<String>,
) -> Result<Json<ApiResponse<BackfillResult>>, AppError> {
    let row = state
        .conversation_repo
        .get(&conversation_id)
        .await
        .map_err(|e| AppError::Internal(format!("conversation lookup: {e}")))?
        .filter(|r| r.user_id == user.id)
        .ok_or_else(|| AppError::NotFound(format!("Conversation '{conversation_id}' not found")))?;

    if row.r#type != "remote" {
        return Err(AppError::BadRequest(
            "Backfill only applies to type=remote conversations".into(),
        ));
    }

    let mut extra: serde_json::Value = serde_json::from_str(&row.extra)
        .map_err(|e| AppError::Internal(format!("conversation extra was not JSON: {e}")))?;
    if extra.get("history_loaded").and_then(|v| v.as_bool()).unwrap_or(false) {
        return Ok(Json(ApiResponse::ok(BackfillResult {
            inserted: 0,
            already_loaded: true,
        })));
    }

    // Distinguish sync-discovered rows (no local transcript) from
    // user-created rows that already streamed through Chisl. The user
    // path's messages are already in the DB via `stream_relay`; if we
    // re-fetched them from OpenCode they'd land as duplicates with
    // different ids (OpenCode part ids vs. Chisl-minted ids). Skip
    // and flip the flag so the renderer never asks again.
    let existing = state
        .conversation_repo
        .get_messages(&conversation_id, 0, 1, aionui_db::SortOrder::Asc)
        .await
        .map_err(|e| AppError::Internal(format!("count messages: {e}")))?;
    if !existing.items.is_empty() {
        return mark_loaded_and_respond(&state, &conversation_id, &mut extra, 0).await;
    }

    let agent_id = extra
        .get("remote_agent_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AppError::BadRequest("conversation.extra.remote_agent_id missing".into()))?
        .to_string();
    let session_id = extra
        .get("sessionKey")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AppError::BadRequest("conversation.extra.sessionKey missing".into()))?
        .to_string();

    let rows = state
        .service
        .fetch_session_messages(&agent_id, &conversation_id, &session_id)
        .await?;
    let inserted = rows.len();

    for msg_row in &rows {
        if let Err(e) = state.conversation_repo.insert_message(msg_row).await {
            // A duplicate id from a re-run is the most likely cause; we
            // continue rather than aborting halfway, so a partial
            // backfill still makes progress on the next attempt.
            tracing::warn!(
                conversation_id = %conversation_id,
                message_id = %msg_row.id,
                error = %e,
                "failed to insert backfilled message row"
            );
        }
    }

    mark_loaded_and_respond(&state, &conversation_id, &mut extra, inserted).await
}

#[derive(serde::Deserialize)]
struct ProviderAuthRequest {
    #[serde(default)]
    api_key: Option<String>,
    /// Full OpenCode `Auth` union body (`api` | `oauth` | `wellknown`, §8).
    #[serde(default)]
    auth: Option<serde_json::Value>,
    /// WellKnown shorthand fields.
    #[serde(default)]
    wellknown_key: Option<String>,
    #[serde(default)]
    wellknown_token: Option<String>,
}

#[derive(serde::Deserialize, Default)]
struct ProviderOAuthStartRequest {
    /// Index into `GET /provider/auth` methods array (Context7 SDK).
    #[serde(default)]
    method: Option<u32>,
    #[serde(default)]
    inputs: Option<std::collections::HashMap<String, String>>,
}

#[derive(serde::Deserialize)]
struct ProviderOAuthCompleteRequest {
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    method: Option<u32>,
}

async fn fetch_provider_catalog(
    State(state): State<RemoteAgentRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, AppError> {
    Ok(Json(ApiResponse::ok(state.service.fetch_provider_catalog(&id).await?)))
}

async fn fetch_provider_auth_methods(
    State(state): State<RemoteAgentRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, AppError> {
    Ok(Json(ApiResponse::ok(
        state.service.fetch_provider_auth_methods(&id).await?,
    )))
}

async fn set_provider_auth(
    State(state): State<RemoteAgentRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path((id, provider_id)): Path<(String, String)>,
    body: Result<Json<ProviderAuthRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    let Json(req) = body.map_err(|e| AppError::BadRequest(e.to_string()))?;
    if let Some(auth) = req.auth {
        state.service.set_provider_auth_payload(&id, &provider_id, auth).await?;
    } else if let (Some(key), Some(token)) = (req.wellknown_key, req.wellknown_token) {
        state
            .service
            .set_provider_wellknown(&id, &provider_id, &key, &token)
            .await?;
    } else if let Some(api_key) = req.api_key.filter(|k| !k.trim().is_empty()) {
        state
            .service
            .set_provider_credentials(&id, &provider_id, &api_key)
            .await?;
    } else {
        return Err(AppError::BadRequest(
            "Provide api_key, wellknown_key+wellknown_token, or auth payload".into(),
        ));
    }
    Ok(Json(ApiResponse::success()))
}

async fn delete_provider_auth(
    State(state): State<RemoteAgentRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path((id, provider_id)): Path<(String, String)>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    state.service.delete_provider_credentials(&id, &provider_id).await?;
    Ok(Json(ApiResponse::success()))
}

async fn start_provider_oauth(
    State(state): State<RemoteAgentRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path((id, provider_id)): Path<(String, String)>,
    body: Result<Json<ProviderOAuthStartRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<serde_json::Value>>, AppError> {
    let req = body.map(|Json(b)| b).unwrap_or_default();
    Ok(Json(ApiResponse::ok(
        state
            .service
            .start_provider_oauth(&id, &provider_id, req.method.unwrap_or(0), req.inputs)
            .await?,
    )))
}

async fn complete_provider_oauth(
    State(state): State<RemoteAgentRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path((id, provider_id)): Path<(String, String)>,
    body: Result<Json<ProviderOAuthCompleteRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    let Json(req) = body.map_err(|e| AppError::BadRequest(e.to_string()))?;
    state
        .service
        .complete_provider_oauth(&id, &provider_id, req.method.unwrap_or(0), req.code.as_deref())
        .await?;
    Ok(Json(ApiResponse::success()))
}

/// Flip `extra.history_loaded` to true, persist, broadcast a
/// `conversation.listChanged(updated)` event, and return the
/// response. Shared by the success path and the
/// "user-created-row-already-streamed" early return so the flag and
/// broadcast behave identically in both cases.
async fn mark_loaded_and_respond(
    state: &RemoteAgentRouterState,
    conversation_id: &str,
    extra: &mut serde_json::Value,
    inserted: usize,
) -> Result<Json<ApiResponse<BackfillResult>>, AppError> {
    if let Some(obj) = extra.as_object_mut() {
        obj.insert("history_loaded".to_string(), serde_json::Value::Bool(true));
    }
    let updates = aionui_db::ConversationRowUpdate {
        name: None,
        pinned: None,
        pinned_at: None,
        model: None,
        extra: Some(serde_json::to_string(extra).map_err(|e| AppError::Internal(format!("re-serialize extra: {e}")))?),
        status: None,
        updated_at: Some(aionui_common::now_ms()),
    };
    state
        .conversation_repo
        .update(conversation_id, &updates)
        .await
        .map_err(|e| AppError::Internal(format!("update conversation extra: {e}")))?;

    state.broadcaster.broadcast(WebSocketMessage::new(
        "conversation.listChanged",
        serde_json::json!({
            "conversation_id": conversation_id,
            "action": "updated",
            "source": "aionui",
        }),
    ));

    Ok(Json(ApiResponse::ok(BackfillResult {
        inserted,
        already_loaded: inserted == 0,
    })))
}

#[derive(serde::Serialize)]
struct BackfillResult {
    inserted: usize,
    already_loaded: bool,
}

// ── OpenCode bridge plugin ─────────────────────────────────────

/// `GET /api/remote-agents/{id}/plugin` — return the install info the
/// renderer needs to wire the OpenCode plugin (endpoint URL, token,
/// copy-paste snippets, live status). Mints a token on first call;
/// subsequent calls return the same token until [`plugin_rotate_token`]
/// is invoked.
async fn plugin_info(
    State(state): State<RemoteAgentRouterState>,
    Path(id): Path<String>,
    Extension(_user): Extension<CurrentUser>,
) -> Result<Json<ApiResponse<RemoteAgentPluginInfoResponse>>, AppError> {
    let info = state.service.plugin_install_info(&id).await?;
    Ok(Json(ApiResponse::ok(info)))
}

/// `POST /api/remote-agents/{id}/plugin/rotate-token` — issue a fresh
/// plugin token, replacing the existing one. Returns the same shape
/// as `plugin_info` so the renderer can update its UI in one round
/// trip. Any in-flight plugin connection with the old token sees a
/// 401 on its next request and must re-read the env to recover.
async fn plugin_rotate_token(
    State(state): State<RemoteAgentRouterState>,
    Path(id): Path<String>,
    Extension(_user): Extension<CurrentUser>,
) -> Result<Json<ApiResponse<RemoteAgentPluginInfoResponse>>, AppError> {
    let info = state.service.rotate_plugin_token(&id).await?;
    Ok(Json(ApiResponse::ok(info)))
}

/// `POST /api/remote-agents/{id}/plugin/voice-mode` — toggle
/// voice-mode for the plugin. The renderer invokes this when the
/// user enables/disables voice input in the chat UI. The service
/// layer pushes the toggle to the plugin channel (live + sticky
/// for replay on reconnect) and fires a UI WS notification so
/// other renderer surfaces can mirror the state without a REST
/// poll. Body: `{ "enabled": bool, "session_id"?: string }`.
async fn plugin_voice_mode(
    State(state): State<RemoteAgentRouterState>,
    Path(id): Path<String>,
    Extension(_user): Extension<CurrentUser>,
    body: Result<Json<VoiceModeRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    let Json(req) = body.map_err(|e| AppError::BadRequest(e.to_string()))?;
    state.service.set_voice_mode(&id, req.enabled, req.session_id).await?;
    Ok(Json(ApiResponse::success()))
}

/// Body shape for `POST /api/remote-agents/{id}/plugin/voice-mode`.
/// `session_id` is `None` for the unsessioned (global) toggle.
#[derive(serde::Deserialize)]
struct VoiceModeRequest {
    enabled: bool,
    #[serde(default)]
    session_id: Option<String>,
}

// ── Background processes (UI REST) ──────────────────────────────

/// `GET /api/remote-agents/{id}/bg-processes` — list every
/// background process the plugin webserver currently knows about
/// for this agent row (running + recent terminal). Cheap — reads
/// the in-memory [`crate::manager::remote::plugin::bg::BgProcessManager`]
/// under a short mutex and clones the snapshots. The renderer
/// can poll this on its existing 5 s cadence without a meaningful
/// host-side cost.
async fn list_bg_processes(
    State(state): State<RemoteAgentRouterState>,
    Path(id): Path<String>,
    Extension(_user): Extension<CurrentUser>,
) -> Result<Json<ApiResponse<aionui_api_types::BgProcessListResponse>>, AppError> {
    // First make sure the agent row exists — the list endpoint
    // is auth-gated but we should still 404 for an unknown id
    // rather than returning an empty list, so the UI can
    // distinguish "agent exists, no processes yet" from "wrong
    // id".
    state.service.get(&id).await?;
    let processes = crate::services::bg_process::list_bg_processes(&id);
    Ok(Json(ApiResponse::ok(aionui_api_types::BgProcessListResponse {
        processes,
    })))
}

/// `POST /api/remote-agents/{id}/bg-processes/{pid}/stop` — stop a
/// running background process. Idempotent for already-terminal
/// processes: the response is the post-stop snapshot either way.
/// Translates the plugin `BgError` to the standard `AppError`
/// shape so the renderer can show the same error UI it uses for
/// every other REST call.
async fn stop_bg_process(
    State(state): State<RemoteAgentRouterState>,
    Path((id, pid)): Path<(String, String)>,
    Extension(_user): Extension<CurrentUser>,
) -> Result<Json<ApiResponse<aionui_api_types::BgProcessUiInfo>>, AppError> {
    state.service.get(&id).await?;
    let process = crate::services::bg_process::stop_bg_process(&id, &pid)
        .await
        .map_err(bg_error_to_app_error)?;
    Ok(Json(ApiResponse::ok(process)))
}

/// `GET /api/remote-agents/{id}/bg-processes/{pid}/output?offset=N`
/// — read the ring-buffer slice from `offset` to the current
/// write head. `offset` defaults to 0 when the caller is asking
/// for the full buffer; pass a previous `next_offset` to resume.
/// The renderer should treat `output_bytes` on the process
/// snapshot as the source of truth for "how much has been
/// emitted total" — `output` is the *retained* slice and may be
/// shorter when `truncated == true`.
async fn read_bg_process_output(
    State(state): State<RemoteAgentRouterState>,
    Path((id, pid)): Path<(String, String)>,
    Extension(_user): Extension<CurrentUser>,
    axum::extract::Query(query): axum::extract::Query<BgReadOutputQuery>,
) -> Result<Json<ApiResponse<aionui_api_types::BgProcessOutputResponse>>, AppError> {
    state.service.get(&id).await?;
    let offset = query.offset.unwrap_or(0);
    let (output, next_offset, process) =
        crate::services::bg_process::read_bg_process(&id, &pid, offset).map_err(bg_error_to_app_error)?;
    Ok(Json(ApiResponse::ok(aionui_api_types::BgProcessOutputResponse {
        output,
        next_offset,
        process,
    })))
}

/// Query string for `GET .../bg-processes/{pid}/output`. `offset`
/// is the absolute byte offset the caller has already consumed;
/// defaults to 0 (read the whole ring from the start).
#[derive(serde::Deserialize, Default)]
struct BgReadOutputQuery {
    #[serde(default)]
    offset: Option<u64>,
}

/// Map a plugin `BgError` to the standard `AppError` shape. The
/// `code()` and message we surface in the response body match
/// the plugin webserver's wire shape so a renderer that already
/// knows `bg.denied` / `bg.no_approver` / etc. can render the
/// same error UI for both surfaces.
fn bg_error_to_app_error(e: crate::manager::remote::plugin::BgError) -> AppError {
    use crate::manager::remote::plugin::BgError as Bg;
    match e {
        Bg::NotFound(id) => AppError::NotFound(format!("background process '{id}' not found")),
        Bg::LimitExceeded(n) => AppError::BadRequest(format!(
            "agent already has {n} running background processes (limit: {n})"
        )),
        Bg::Denied => AppError::BadRequest("shell approver rejected the command".into()),
        Bg::NoApprover => AppError::BadRequest("no shell approver registered for this workspace".into()),
        Bg::ApprovalTimedOut => AppError::BadRequest("approval timed out".into()),
        Bg::Spawn(msg) => AppError::Internal(format!("failed to spawn shell: {msg}")),
        Bg::Invalid(msg) => AppError::BadRequest(msg),
    }
}
