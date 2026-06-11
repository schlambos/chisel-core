//! Local OpenCode management API routes (Phase 4).
//!
//! Endpoints:
//!
//! - `POST /api/local-opencode/start`    — spawn a new `opencode serve` with
//!   auto-injected plugin config
//! - `POST /api/local-opencode/{id}/stop`    — stop a running instance
//! - `POST /api/local-opencode/{id}/restart` — restart a stopped/crashed
//!   instance (bounded by the manager's restart policy)
//! - `GET  /api/local-opencode`             — list all instances with
//!   live status
//!
//! All routes require authentication; the auth middleware is
//! applied at the call site in `aionui-app`'s router, mirroring
//! the other module routers.

use axum::Router;
use axum::extract::{Extension, Json, Path, State};
use axum::routing::{get, post};

use aionui_api_types::{ApiResponse, LocalOpenCodeInstance, LocalOpenCodeListResponse, StartLocalOpenCodeRequest};
use aionui_auth::CurrentUser;
use aionui_common::AppError;

use super::state::LocalOpenCodeRouterState;

/// Build the local OpenCode router.
///
/// All routes require authentication (applied by the caller).
pub fn local_opencode_routes(state: LocalOpenCodeRouterState) -> Router {
    Router::new()
        .route("/api/local-opencode", get(list))
        .route("/api/local-opencode/start", post(start))
        .route("/api/local-opencode/{id}/stop", post(stop))
        .route("/api/local-opencode/{id}/restart", post(restart))
        .with_state(state)
}

async fn list(
    State(state): State<LocalOpenCodeRouterState>,
    Extension(_user): Extension<CurrentUser>,
) -> Result<Json<ApiResponse<LocalOpenCodeListResponse>>, AppError> {
    let result = state.manager.list().await;
    Ok(Json(ApiResponse::ok(result)))
}

async fn start(
    State(state): State<LocalOpenCodeRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Json(req): Json<StartLocalOpenCodeRequest>,
) -> Result<Json<ApiResponse<LocalOpenCodeInstance>>, AppError> {
    let instance = state
        .manager
        .start(req)
        .await
        .map_err(|e| AppError::Internal(format!("failed to start local opencode: {e}")))?;
    Ok(Json(ApiResponse::ok(instance)))
}

async fn stop(
    State(state): State<LocalOpenCodeRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    state.manager.stop(&id).await.map_err(AppError::NotFound)?;
    Ok(Json(ApiResponse::success()))
}

async fn restart(
    State(state): State<LocalOpenCodeRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<LocalOpenCodeInstance>>, AppError> {
    let instance = state
        .manager
        .restart(&id)
        .await
        .map_err(|e| AppError::Internal(format!("failed to restart local opencode: {e}")))?;
    Ok(Json(ApiResponse::ok(instance)))
}
