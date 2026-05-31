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
    RemoteAgentHealthResponse, RemoteAgentListItem, RemoteAgentResponse, RemoteSessionInfo, RemoteSkillInfo,
    TestRemoteAgentConnectionRequest, UpdateRemoteAgentRequest,
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
