//! Axum handlers for the terminal module.
//!
//! Three routers:
//! * `terminal_routes`        — HTTP control plane (`/api/terminal/sessions`)
//! * `terminal_ws_routes`     — WebSocket transport (`/api/terminal/ws/:session_id`)
//!
//! The split mirrors the existing `chisl-lsp` pattern so the
//! application router can apply the same CSRF / auth treatment: the HTTP
//! routes go through `auth_middleware`, the WS upgrade is exempt.

use axum::Json;
use axum::Router;
use axum::extract::Path;
use axum::extract::State;
use axum::extract::WebSocketUpgrade;
use axum::extract::rejection::JsonRejection;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use serde_json::json;
use tracing::{debug, warn};

use chisl_api_types::ApiResponse;
use chisl_api_types::terminal::{
    TerminalCreateSessionRequest, TerminalCreateSessionResponse, TerminalKillSessionRequest,
    TerminalListSessionsResponse, TerminalResizeSessionRequest, TerminalSessionInfo,
};
use chisl_auth::extract_token_from_ws_headers;

use crate::service::TerminalError;
use crate::state::TerminalRouterState;
use crate::transport;

/// HTTP control-plane routes. Caller is responsible for layering on the
/// app-wide auth middleware.
pub fn terminal_routes(state: TerminalRouterState) -> Router {
    Router::new()
        .route("/api/terminal/sessions", post(create_session))
        .route("/api/terminal/sessions", get(list_sessions))
        .route("/api/terminal/sessions/kill", post(kill_session))
        .route("/api/terminal/sessions/resize", post(resize_session))
        .with_state(state)
}

/// WebSocket transport routes. Caller mounts these *outside* the CSRF
/// middleware layer (same treatment as `/ws`).
pub fn terminal_ws_routes(state: TerminalRouterState) -> Router {
    Router::new()
        .route("/api/terminal/ws/{session_id}", get(ws_upgrade))
        .with_state(state)
}

async fn create_session(
    State(state): State<TerminalRouterState>,
    body: Result<Json<TerminalCreateSessionRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<TerminalCreateSessionResponse>>, (StatusCode, Json<serde_json::Value>)> {
    let Json(req) = body.map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "success": false, "error": e.to_string(), "code": "BAD_REQUEST" })),
        )
    })?;

    let session_id = state
        .service
        .create_session(req.command, req.cwd, req.cols, req.rows)
        .map_err(map_error)?;

    Ok(Json(ApiResponse::ok(TerminalCreateSessionResponse { session_id })))
}

async fn list_sessions(State(state): State<TerminalRouterState>) -> Json<ApiResponse<TerminalListSessionsResponse>> {
    let sessions: Vec<TerminalSessionInfo> = state
        .service
        .list_sessions()
        .into_iter()
        .map(|s| TerminalSessionInfo {
            session_id: s.session_id,
            created_at: s.created_at.to_string(),
            cols: s.cols,
            rows: s.rows,
            is_active: s.is_active,
        })
        .collect();

    Json(ApiResponse::ok(TerminalListSessionsResponse { sessions }))
}

async fn kill_session(
    State(state): State<TerminalRouterState>,
    body: Result<Json<TerminalKillSessionRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<()>>, (StatusCode, Json<serde_json::Value>)> {
    let Json(req) = body.map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "success": false, "error": e.to_string(), "code": "BAD_REQUEST" })),
        )
    })?;

    state.service.kill_session(&req.session_id).map_err(map_error)?;

    Ok(Json(ApiResponse::success()))
}

async fn resize_session(
    State(state): State<TerminalRouterState>,
    body: Result<Json<TerminalResizeSessionRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<()>>, (StatusCode, Json<serde_json::Value>)> {
    let Json(req) = body.map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "success": false, "error": e.to_string(), "code": "BAD_REQUEST" })),
        )
    })?;

    state
        .service
        .resize_session(&req.session_id, req.cols, req.rows)
        .map_err(map_error)?;

    Ok(Json(ApiResponse::success()))
}

async fn ws_upgrade(
    ws: WebSocketUpgrade,
    State(state): State<TerminalRouterState>,
    Path(session_id): Path<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    if !state.local {
        let token = match extract_token_from_ws_headers(&headers) {
            Some(t) => t,
            None => {
                return Err((
                    StatusCode::UNAUTHORIZED,
                    Json(json!({
                        "success": false,
                        "error": "Authentication required for terminal WebSocket connection",
                        "code": "UNAUTHORIZED"
                    })),
                ));
            }
        };

        if state.jwt_service.verify(&token).is_err() {
            return Err((
                StatusCode::FORBIDDEN,
                Json(json!({
                    "success": false,
                    "error": "Invalid or expired authentication token",
                    "code": "FORBIDDEN"
                })),
            ));
        }
    }

    debug!(session_id = %session_id, "terminal ws upgrade: authenticated");

    let svc = state.service.clone();

    Ok(ws.on_upgrade(move |socket| async move {
        match svc.attach_transport(&session_id).await {
            Ok(pty) => match transport::ws_upgrade_handler(socket, pty).await {
                Ok(_) => {}
                Err(e) => {
                    warn!(session_id = %session_id, error = %e, "terminal ws upgrade handler failed");
                }
            },
            Err(e) => {
                warn!(session_id = %session_id, error = %e, "terminal attach_transport failed");
            }
        }
    }))
}

fn map_error(e: TerminalError) -> (StatusCode, Json<serde_json::Value>) {
    let (status, code) = match &e {
        TerminalError::SessionNotFound(_) => (StatusCode::NOT_FOUND, "TERMINAL_SESSION_NOT_FOUND"),
        TerminalError::SessionAlreadyAttached(_) => (StatusCode::CONFLICT, "TERMINAL_SESSION_ATTACHED"),
        TerminalError::SpawnFailed(_) => (StatusCode::INTERNAL_SERVER_ERROR, "TERMINAL_SPAWN_FAILED"),
        TerminalError::ResizeFailed(_) => (StatusCode::INTERNAL_SERVER_ERROR, "TERMINAL_RESIZE_FAILED"),
        TerminalError::KillFailed(_) => (StatusCode::INTERNAL_SERVER_ERROR, "TERMINAL_KILL_FAILED"),
    };

    (
        status,
        Json(json!({ "success": false, "error": e.to_string(), "code": code })),
    )
}
