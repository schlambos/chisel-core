use crate::state::ConversationRouterState;
use aionui_api_types::{
    AgentModeResponse, ApiResponse, GetModelInfoResponse, RemoteSkillInfo, SetModeRequest, SetModelRequest,
    SideQuestionRequest, SideQuestionResponse, SlashCommandItem, WorkspaceBrowseQuery, WorkspaceEntry,
};
use aionui_auth::CurrentUser;
use aionui_common::AppError;
use axum::Router;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Extension, Json, Path, Query, State};
use axum::routing::{delete, get, post};

/// Build the conversation-ops router (no auth layer applied — the caller is
/// responsible for wrapping this with the auth middleware).
pub fn conversation_ops_routes(state: ConversationRouterState) -> Router {
    Router::new()
        .route("/api/conversations/{id}/side-question", post(side_question))
        .route("/api/conversations/{id}/slash-commands", get(get_slash_commands))
        .route("/api/conversations/{id}/skills", get(get_skills))
        .route("/api/conversations/{id}/usage", get(get_usage))
        .route("/api/conversations/{id}/mode", get(get_mode).put(set_mode))
        .route("/api/conversations/{id}/model", get(get_model).put(set_model))
        .route("/api/conversations/{id}/openclaw/runtime", get(get_openclaw_runtime))
        .route("/api/conversations/{id}/workspace", get(browse_workspace))
        .route(
            "/api/conversations/{id}/opencode-message/{messageId}",
            delete(delete_opencode_message).put(edit_opencode_message),
        )
        .route("/api/conversations/{id}/opencode/fork", post(fork_session))
        .route("/api/conversations/{id}/opencode/revert", post(revert_session))
        .route("/api/conversations/{id}/opencode/unrevert", post(unrevert_session))
        .route("/api/conversations/{id}/opencode/summarize", post(summarize_session))
        .route(
            "/api/conversations/{id}/opencode/share",
            post(share_session).delete(unshare_session),
        )
        .route("/api/conversations/{id}/opencode/diff", get(session_diff))
        .route(
            "/api/conversations/{id}/opencode/config",
            get(get_global_config).patch(patch_global_config),
        )
        .route(
            "/api/conversations/{id}/opencode/config/effective",
            get(get_effective_config),
        )
        .route("/api/conversations/{id}/opencode/lsp", get(get_lsp_status))
        .route("/api/conversations/{id}/opencode/vcs", get(get_vcs_info))
        .route("/api/conversations/{id}/opencode/vcs/status", get(get_vcs_status))
        .route("/api/conversations/{id}/opencode/vcs/diff", get(get_vcs_diff))
        .route("/api/conversations/{id}/opencode/compact", post(compact_session))
        .route("/api/conversations/{id}/opencode/context", get(get_session_context))
        .route("/api/conversations/{id}/opencode/v2-messages", get(get_v2_messages))
        .route("/api/conversations/{id}/opencode/v2-models", get(get_v2_model_list))
        .route(
            "/api/conversations/{id}/opencode/v2-providers",
            get(get_v2_provider_list),
        )
        .with_state(state)
}

#[derive(serde::Deserialize)]
struct ForkRequest {
    /// Local message row id to fork from. `None` forks from the latest message.
    #[serde(default)]
    message_id: Option<String>,
}

#[derive(serde::Serialize)]
struct ForkResponse {
    session_id: String,
}

async fn fork_session(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
    body: Result<Json<ForkRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<ForkResponse>>, AppError> {
    let from = body.ok().and_then(|Json(b)| b.message_id);
    let session_id = state.service.fork_remote_session(&id, from.as_deref()).await?;
    Ok(Json(ApiResponse::ok(ForkResponse { session_id })))
}

#[derive(serde::Deserialize)]
struct RevertRequest {
    /// Local message row id to revert to.
    message_id: String,
}

async fn revert_session(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
    body: Result<Json<RevertRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    let Json(req) = body.map_err(|e| AppError::BadRequest(e.to_string()))?;
    state.service.revert_remote_session(&id, &req.message_id).await?;
    Ok(Json(ApiResponse::success()))
}

async fn unrevert_session(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    state.service.unrevert_remote_session(&id).await?;
    Ok(Json(ApiResponse::success()))
}

async fn summarize_session(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    state.service.summarize_remote_session(&id).await?;
    Ok(Json(ApiResponse::success()))
}

#[derive(serde::Serialize)]
struct ShareResponse {
    url: String,
}

async fn share_session(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<ShareResponse>>, AppError> {
    let url = state.service.share_remote_session(&id).await?;
    Ok(Json(ApiResponse::ok(ShareResponse { url })))
}

async fn unshare_session(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    state.service.unshare_remote_session(&id).await?;
    Ok(Json(ApiResponse::success()))
}

#[derive(serde::Deserialize)]
struct DiffQuery {
    /// Local message row id to scope the diff to. `None` = whole session.
    #[serde(default)]
    message_id: Option<String>,
}

async fn session_diff(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
    Query(query): Query<DiffQuery>,
) -> Result<Json<ApiResponse<serde_json::Value>>, AppError> {
    let diff = state
        .service
        .remote_session_diff(&id, query.message_id.as_deref())
        .await?;
    Ok(Json(ApiResponse::ok(diff)))
}

/// M19: read the remote OpenCode server's global configuration tree.
async fn get_global_config(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, AppError> {
    Ok(Json(ApiResponse::ok(state.service.get_global_config(&id).await?)))
}

/// M19: shallow-merge a partial config into the remote OpenCode server's
/// global configuration. Body is the partial config object; the response is
/// the new effective config returned by the server.
async fn patch_global_config(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
    body: Result<Json<serde_json::Value>, JsonRejection>,
) -> Result<Json<ApiResponse<serde_json::Value>>, AppError> {
    let Json(partial) = body.map_err(|e| AppError::BadRequest(e.to_string()))?;
    Ok(Json(ApiResponse::ok(
        state.service.patch_global_config(&id, partial).await?,
    )))
}

/// M19 (Option A): read the remote OpenCode server's effective (merged) config,
/// used by the renderer to flag edits shadowed by a higher-precedence layer.
async fn get_effective_config(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, AppError> {
    Ok(Json(ApiResponse::ok(state.service.get_effective_config(&id).await?)))
}

/// M15: read the remote OpenCode server's LSP server statuses
/// (`GET /lsp`). The renderer derives the header badge from this list.
async fn get_lsp_status(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, AppError> {
    Ok(Json(ApiResponse::ok(state.service.get_lsp_status(&id).await?)))
}

/// M16: read the remote OpenCode server's VCS info (`GET /vcs`).
async fn get_vcs_info(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, AppError> {
    Ok(Json(ApiResponse::ok(state.service.get_vcs_info(&id).await?)))
}

/// M16: read the remote OpenCode server's working-tree status (`GET /vcs/status`).
async fn get_vcs_status(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, AppError> {
    Ok(Json(ApiResponse::ok(state.service.get_vcs_status(&id).await?)))
}

#[derive(serde::Deserialize)]
struct VcsDiffQuery {
    /// `"git"` (default) or `"branch"`. Anything else falls back to `"git"`.
    #[serde(default)]
    mode: Option<String>,
}

/// M16: read the structured working-tree diff (`GET /vcs/diff?mode=…`).
async fn get_vcs_diff(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
    Query(query): Query<VcsDiffQuery>,
) -> Result<Json<ApiResponse<serde_json::Value>>, AppError> {
    let mode = query.mode.as_deref().unwrap_or("git");
    Ok(Json(ApiResponse::ok(state.service.get_vcs_diff(&id, mode).await?)))
}

/// M07: delete a remote OpenCode message (and its local row). `messageId` is
/// the local message row id; the service resolves the OpenCode id from it.
async fn delete_opencode_message(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path((id, message_id)): Path<(String, String)>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    state.service.delete_remote_message(&id, &message_id).await?;
    Ok(Json(ApiResponse::success()))
}

/// M07: edit the text of a remote OpenCode message. Body: `{ "text": "…" }`.
async fn edit_opencode_message(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path((id, message_id)): Path<(String, String)>,
    body: Result<Json<EditMessageRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    let Json(req) = body.map_err(|e| AppError::BadRequest(e.to_string()))?;
    state.service.edit_remote_message(&id, &message_id, &req.text).await?;
    Ok(Json(ApiResponse::success()))
}

#[derive(serde::Deserialize)]
struct EditMessageRequest {
    text: String,
}

// ── Route handlers ─────────────────────────────────────────────────

async fn get_mode(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<AgentModeResponse>>, AppError> {
    Ok(Json(ApiResponse::ok(state.service.get_mode(&id).await?)))
}

async fn set_mode(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
    body: Result<Json<SetModeRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    let Json(req) = body.map_err(|e| AppError::BadRequest(e.to_string()))?;
    state.service.set_mode(&id, req).await?;
    Ok(Json(ApiResponse::success()))
}

async fn get_model(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<GetModelInfoResponse>>, AppError> {
    Ok(Json(ApiResponse::ok(state.service.get_model(&id).await?)))
}

async fn set_model(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
    body: Result<Json<SetModelRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    let Json(req) = body.map_err(|e| AppError::BadRequest(e.to_string()))?;
    state.service.set_model(&id, req).await?;
    Ok(Json(ApiResponse::success()))
}

async fn get_usage(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<Option<serde_json::Value>>>, AppError> {
    Ok(Json(ApiResponse::ok(state.service.get_usage(&id).await?)))
}

async fn side_question(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
    Json(req): Json<SideQuestionRequest>,
) -> Result<Json<ApiResponse<SideQuestionResponse>>, AppError> {
    Ok(Json(ApiResponse::ok(
        state.service.handle_side_question(&id, req).await?,
    )))
}

async fn get_slash_commands(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<Vec<SlashCommandItem>>>, AppError> {
    Ok(Json(ApiResponse::ok(state.service.get_slash_commands(&id).await?)))
}

/// M10: server-side skill catalog (OpenCode `GET /skill`).
async fn get_skills(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<Vec<RemoteSkillInfo>>>, AppError> {
    Ok(Json(ApiResponse::ok(state.service.get_skills(&id).await?)))
}

async fn get_openclaw_runtime(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, AppError> {
    Ok(Json(ApiResponse::ok(state.service.get_openclaw_runtime(&id).await?)))
}

async fn browse_workspace(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
    Query(query): Query<WorkspaceBrowseQuery>,
) -> Result<Json<ApiResponse<Vec<WorkspaceEntry>>>, AppError> {
    Ok(Json(ApiResponse::ok(state.service.browse_workspace(&id, query).await?)))
}

#[derive(serde::Deserialize)]
struct CompactRequest {
    #[serde(default)]
    instructions: Option<String>,
}

async fn compact_session(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
    body: Option<Json<CompactRequest>>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    let instructions = body.and_then(|Json(b)| b.instructions);
    state
        .service
        .compact_remote_session(&id, instructions.as_deref())
        .await?;
    Ok(Json(ApiResponse::success()))
}

async fn get_session_context(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, AppError> {
    Ok(Json(ApiResponse::ok(state.service.get_session_context(&id).await?)))
}

#[derive(serde::Deserialize)]
struct V2MessagesQuery {
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    cursor: Option<String>,
}

async fn get_v2_messages(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
    Query(query): Query<V2MessagesQuery>,
) -> Result<Json<ApiResponse<serde_json::Value>>, AppError> {
    Ok(Json(ApiResponse::ok(
        state
            .service
            .get_v2_messages(&id, query.limit, query.cursor.as_deref())
            .await?,
    )))
}

async fn get_v2_model_list(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, AppError> {
    Ok(Json(ApiResponse::ok(state.service.fetch_v2_model_list(&id).await?)))
}

async fn get_v2_provider_list(
    State(state): State<ConversationRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, AppError> {
    Ok(Json(ApiResponse::ok(state.service.fetch_v2_provider_list(&id).await?)))
}
