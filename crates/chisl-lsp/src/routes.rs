//! Axum handlers for the LSP module.
//!
//! Three routers:
//! * `lsp_routes`        — HTTP control plane (`/api/lsp/servers`, `/api/lsp/sessions`)
//! * `lsp_ws_routes`     — WebSocket transport (`/api/lsp/ws/:session_id`)
//!
//! The split mirrors the existing `chisl-realtime` `/ws` pattern so the
//! application router can apply the same CSRF / auth treatment: the HTTP
//! routes go through `auth_middleware`, the WS upgrade is exempt.

use axum::Json;
use axum::Router;
use axum::extract::Path;
use axum::extract::State;
use axum::extract::WebSocketUpgrade;
use axum::extract::rejection::JsonRejection;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use serde_json::json;
use tracing::warn;

use chisl_api_types::ApiResponse;
use chisl_api_types::lsp::{
    LspServerInfoResponse, LspStartSessionRequest, LspStartSessionResponse, LspStopSessionRequest,
};

use crate::service::LspError;
use crate::state::LspRouterState;
use crate::transport;

/// HTTP control-plane routes. Caller is responsible for layering on the
/// app-wide auth middleware.
pub fn lsp_routes(state: LspRouterState) -> Router {
    Router::new()
        .route("/api/lsp/servers", get(list_servers))
        .route("/api/lsp/sessions", post(start_session))
        .route("/api/lsp/sessions/stop", post(stop_session))
        .with_state(state)
}

/// WebSocket transport routes. Caller mounts these *outside* the CSRF
/// middleware layer (same treatment as `/ws`).
pub fn lsp_ws_routes(state: LspRouterState) -> Router {
    Router::new()
        .route("/api/lsp/ws/{session_id}", get(ws_upgrade))
        .with_state(state)
}

async fn list_servers(State(state): State<LspRouterState>) -> Json<ApiResponse<Vec<LspServerInfoResponse>>> {
    let items: Vec<LspServerInfoResponse> = state
        .service
        .list_servers()
        .into_iter()
        .map(|s| LspServerInfoResponse {
            language: s.language.to_owned(),
            installed: s.installed,
            command: s.command.to_owned(),
            install_hint: s.install_hint.map(str::to_owned),
        })
        .collect();
    Json(ApiResponse::ok(items))
}

async fn start_session(
    State(state): State<LspRouterState>,
    body: Result<Json<LspStartSessionRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<LspStartSessionResponse>>, (StatusCode, Json<serde_json::Value>)> {
    let Json(req) = body.map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "success": false, "error": e.to_string(), "code": "BAD_REQUEST" })),
        )
    })?;
    let id = state
        .service
        .start_session(&req.language, req.workspace.clone())
        .map_err(map_error)?;
    Ok(Json(ApiResponse::ok(LspStartSessionResponse {
        session_id: id,
        language: req.language,
    })))
}

async fn stop_session(
    State(state): State<LspRouterState>,
    body: Result<Json<LspStopSessionRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<()>>, (StatusCode, Json<serde_json::Value>)> {
    let Json(req) = body.map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "success": false, "error": e.to_string(), "code": "BAD_REQUEST" })),
        )
    })?;
    state.service.stop_session(&req.session_id).map_err(map_error)?;
    Ok(Json(ApiResponse::success()))
}

async fn ws_upgrade(
    ws: WebSocketUpgrade,
    State(state): State<LspRouterState>,
    Path(session_id): Path<String>,
) -> impl IntoResponse {
    let svc = state.service.clone();
    ws.on_upgrade(move |socket| async move {
        match svc.attach_transport(&session_id).await {
            Ok(child) => {
                transport::bridge(socket, child).await;
            }
            Err(e) => {
                warn!(session_id = %session_id, error = %e, "lsp attach_transport failed");
                // Best-effort close; the socket has already been upgraded.
            }
        }
    })
}

fn map_error(e: LspError) -> (StatusCode, Json<serde_json::Value>) {
    let (status, code) = match &e {
        LspError::UnsupportedLanguage(_) => (StatusCode::BAD_REQUEST, "LSP_UNSUPPORTED_LANGUAGE"),
        LspError::NotInstalled { .. } => (StatusCode::FAILED_DEPENDENCY, "LSP_NOT_INSTALLED"),
        LspError::SpawnFailed(_) => (StatusCode::INTERNAL_SERVER_ERROR, "LSP_SPAWN_FAILED"),
        LspError::SessionNotFound(_) => (StatusCode::NOT_FOUND, "LSP_SESSION_NOT_FOUND"),
        LspError::SessionAlreadyAttached(_) => (StatusCode::CONFLICT, "LSP_SESSION_ATTACHED"),
    };
    (
        status,
        Json(json!({ "success": false, "error": e.to_string(), "code": code })),
    )
}
