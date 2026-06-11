//! Sidecar reverse proxy.
//!
//! Phase 3 WS3: exposes a management API to register a LOCAL service
//! (e.g. OpenVSCode Server, ttyd) listening on `127.0.0.1:<port>` and
//! then reverse-proxies requests for that service through the AionCore
//! HTTP server so the Electron renderer can embed it in a webview
//! under a single authenticated origin.
//!
//! Security posture (intentional, do not relax without a security review):
//!
//! - The target host is hardcoded to `127.0.0.1` — never configurable
//!   from the request path. The proxy is not a generic forward proxy.
//! - The set of reachable ports is the registration set. Unregistered
//!   ports cannot be reached through the proxy surface; `POST
//!   /api/sidecars` is the allowlist.
//! - Proxy auth is a per-sidecar opaque token (UUID v4) issued once at
//!   registration. The first navigation carries it as `?sct=<token>`,
//!   the server validates, issues an HttpOnly `sidecar_{id}=<token>`
//!   cookie scoped to `/sidecar/{id}`, and strips the query string
//!   before forwarding. Subsequent requests (subresources, XHR, WS
//!   upgrades) are validated via the cookie. Wrong / missing creds
//!   return 403 with a small JSON body; unknown ids return 404.
//! - Token comparison uses a constant-time wrapper. UUID v4 has 122
//!   bits of entropy, so timing leakage is not exploitable in
//!   practice, but the wrapper mirrors the plugin-token posture and
//!   keeps the door closed if the token format ever changes.
//! - Hop-by-hop headers are stripped on the way in and out; the proxy
//!   never logs tokens or cookies; connect timeout is 3s but no
//!   response-body timeout (long-lived streams OK).
//!
//! Layout:
//!
//! - `SidecarRegistry` is the in-memory `id → SidecarEntry` map (with
//!   `name+port` lookup for idempotent re-registration). Owned by the
//!   app router state.
//! - `sidecar_routes` builds the auth-gated management router
//!   (`/api/sidecars`) — merged inside the auth layer.
//! - `sidecar_proxy_routes` builds the unauth proxy router
//!   (`/sidecar/{id}/...`) — merged OUTSIDE the auth layer because
//!   the embedded webview can't send Authorization headers. Auth is
//!   the per-sidecar token / cookie described above.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::rejection::JsonRejection;
use axum::extract::ws::{Message as AxumMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{Extension, Json, Path, State};
use axum::http::header::COOKIE;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{any, delete, get};
use futures_util::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use reqwest::header::{HOST, HeaderMap as ReqHeaderMap, HeaderName as ReqHeaderName, HeaderValue as ReqHeaderValue};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tracing::{debug, warn};
use uuid::Uuid;

use aionui_api_types::{ApiResponse, ErrorResponse};
use aionui_auth::CurrentUser;
use aionui_common::AppError;

// =========================================================================
// Router state
// =========================================================================

#[derive(Clone)]
pub struct SidecarState {
    pub registry: SidecarRegistry,
    pub http_client: reqwest::Client,
}

impl SidecarState {
    pub fn new() -> Self {
        let http_client = reqwest::Client::builder()
            .connect_timeout(SIDECAR_CONNECT_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("sidecar reqwest client build");
        Self {
            registry: SidecarRegistry::new(),
            http_client,
        }
    }
}

impl Default for SidecarState {
    fn default() -> Self {
        Self::new()
    }
}

// =========================================================================
// Public DTOs
// =========================================================================

#[derive(Debug, Clone)]
pub struct SidecarEntry {
    pub id: String,
    pub name: String,
    pub port: u16,
    pub token: String,
    pub created_at_ms: i64,
}

#[derive(Debug, Serialize)]
pub struct SidecarRegistration {
    pub id: String,
    pub name: String,
    pub port: u16,
    pub url: String,
    pub token: String,
}

#[derive(Debug, Serialize)]
pub struct SidecarListItem {
    pub id: String,
    pub name: String,
    pub port: u16,
    pub url: String,
}

#[derive(Debug, Serialize)]
pub struct SidecarListResponse {
    pub sidecars: Vec<SidecarListItem>,
}

#[derive(Debug, Deserialize)]
pub struct CreateSidecarRequest {
    pub name: String,
    pub port: u16,
}

// =========================================================================
// Registry
// =========================================================================

pub const MAX_SIDECARS: usize = 16;
pub const SIDECAR_TARGET_HOST: &str = "127.0.0.1";
const SIDECAR_MIN_PORT: u16 = 1024;
const SIDECAR_MAX_PORT: u16 = 65535;
const SIDECAR_NAME_MAX: usize = 64;
const SIDECAR_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
];

#[derive(Clone)]
pub struct SidecarRegistry {
    inner: Arc<Mutex<RegistryInner>>,
}

struct RegistryInner {
    by_id: HashMap<String, SidecarEntry>,
    by_name_port: HashMap<(String, u16), String>,
}

impl SidecarRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(RegistryInner {
                by_id: HashMap::new(),
                by_name_port: HashMap::new(),
            })),
        }
    }

    pub async fn register_or_get(&self, name: &str, port: u16, now_ms: i64) -> Result<RegisterOutcome, AppError> {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            return Err(AppError::BadRequest("name is required".into()));
        }
        if trimmed.chars().count() > SIDECAR_NAME_MAX {
            return Err(AppError::BadRequest(format!(
                "name exceeds {SIDECAR_NAME_MAX} characters"
            )));
        }
        if !(SIDECAR_MIN_PORT..=SIDECAR_MAX_PORT).contains(&port) {
            return Err(AppError::BadRequest(format!(
                "port must be in {SIDECAR_MIN_PORT}..={SIDECAR_MAX_PORT}"
            )));
        }

        let mut inner = self.inner.lock().await;
        let key = (trimmed.to_lowercase(), port);
        if let Some(existing_id) = inner.by_name_port.get(&key)
            && let Some(existing) = inner.by_id.get(existing_id).cloned()
        {
            return Ok(RegisterOutcome::Existing(existing));
        }

        if inner.by_id.len() >= MAX_SIDECARS {
            tracing::warn!(count = inner.by_id.len(), max = MAX_SIDECARS, "sidecar limit reached");
            return Err(AppError::BadRequest(format!(
                "sidecar limit reached ({MAX_SIDECARS}); delete an existing sidecar first"
            )));
        }

        let entry = SidecarEntry {
            id: Uuid::new_v4().to_string(),
            name: trimmed.to_owned(),
            port,
            token: Uuid::new_v4().to_string(),
            created_at_ms: now_ms,
        };
        inner.by_id.insert(entry.id.clone(), entry.clone());
        inner.by_name_port.insert(key, entry.id.clone());
        Ok(RegisterOutcome::New(entry))
    }

    pub async fn list(&self) -> Vec<SidecarEntry> {
        let inner = self.inner.lock().await;
        let mut out: Vec<SidecarEntry> = inner.by_id.values().cloned().collect();
        out.sort_by_key(|e| e.created_at_ms);
        out
    }

    pub async fn delete(&self, id: &str) -> bool {
        let mut inner = self.inner.lock().await;
        if let Some(entry) = inner.by_id.remove(id) {
            let key = (entry.name.to_lowercase(), entry.port);
            inner.by_name_port.remove(&key);
            true
        } else {
            false
        }
    }

    pub async fn get(&self, id: &str) -> Option<SidecarEntry> {
        self.inner.lock().await.by_id.get(id).cloned()
    }
}

impl Default for SidecarRegistry {
    fn default() -> Self {
        Self::new()
    }
}

pub enum RegisterOutcome {
    New(SidecarEntry),
    Existing(SidecarEntry),
}

// Manual Debug so the test assertions can use `unwrap_err`.
impl std::fmt::Debug for RegisterOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::New(e) | Self::Existing(e) => f
                .debug_struct("RegisterOutcome")
                .field("id", &e.id)
                .field("name", &e.name)
                .field("port", &e.port)
                .finish_non_exhaustive(),
        }
    }
}

// =========================================================================
// Cookie helpers
// =========================================================================

fn cookie_name(id: &str) -> String {
    format!("sidecar_{id}")
}

fn build_set_cookie(id: &str, token: &str) -> String {
    format!(
        "{name}={token}; Path=/sidecar/{id}; HttpOnly; SameSite=Lax",
        name = cookie_name(id)
    )
}

fn extract_cookie_value(headers: &HeaderMap, id: &str) -> Option<String> {
    let raw = headers.get(COOKIE)?.to_str().ok()?;
    let target = format!("{}=", cookie_name(id));
    for part in raw.split(';') {
        let trimmed = part.trim();
        if let Some(rest) = trimmed.strip_prefix(&target) {
            return Some(rest.to_owned());
        }
    }
    None
}

fn filter_cookie_header(value: &str) -> String {
    let mut kept: Vec<&str> = Vec::new();
    for part in value.split(';') {
        let trimmed = part.trim_start();
        let name = trimmed.split('=').next().unwrap_or("");
        if !name.starts_with("sidecar_") {
            kept.push(part.trim());
        }
    }
    kept.join("; ")
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

// =========================================================================
// Auth outcome for the proxy
// =========================================================================

enum ProxyAuth {
    Ok { issue_cookie: bool },
    Denied,
}

fn authorize_proxy(entry: &SidecarEntry, headers: &HeaderMap, query: Option<&str>) -> ProxyAuth {
    if let Some(cookie_token) = extract_cookie_value(headers, &entry.id)
        && ct_eq(cookie_token.as_bytes(), entry.token.as_bytes())
    {
        return ProxyAuth::Ok { issue_cookie: false };
    }
    if let Some(qs) = query
        && let Some(provided) = parse_query_param(qs, "sct")
        && ct_eq(provided.as_bytes(), entry.token.as_bytes())
    {
        return ProxyAuth::Ok { issue_cookie: true };
    }
    ProxyAuth::Denied
}

fn parse_query_param(query: &str, name: &str) -> Option<String> {
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = match pair.split_once('=') {
            Some((k, v)) => (k, v),
            None => (pair, ""),
        };
        if k == name {
            return Some(url_decode(v));
        }
    }
    None
}

fn url_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hi = hex_digit(bytes[i + 1]);
                let lo = hex_digit(bytes[i + 2]);
                match (hi, lo) {
                    (Some(h), Some(l)) => {
                        out.push((h << 4) | l);
                        i += 3;
                    }
                    _ => {
                        out.push(bytes[i]);
                        i += 1;
                    }
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).unwrap_or_else(|_| input.to_owned())
}

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn strip_sct(query: &str) -> String {
    let mut kept: Vec<&str> = Vec::new();
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let name = pair.split('=').next().unwrap_or("");
        if name == "sct" {
            continue;
        }
        kept.push(pair);
    }
    kept.join("&")
}

fn is_websocket_upgrade(headers: &HeaderMap) -> bool {
    let has_upgrade = headers
        .get("upgrade")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"));
    let has_connection = headers
        .get("connection")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.split(',').any(|c| c.trim().eq_ignore_ascii_case("Upgrade")));
    has_upgrade && has_connection
}

// =========================================================================
// Router builders
// =========================================================================

pub fn sidecar_routes(state: SidecarState) -> Router {
    Router::new()
        .route("/api/sidecars", get(list_sidecars).post(create_sidecar))
        .route("/api/sidecars/{id}", delete(delete_sidecar))
        .with_state(state)
}

pub fn sidecar_proxy_routes(state: SidecarState) -> Router {
    // Single combined handler for each path. `WebSocketUpgrade` is
    // an optional extractor: it succeeds (Some) only when the
    // request has the right upgrade headers, and the handler uses
    // it to start the WS pump. For non-WS requests the extractor
    // resolves to `None` and the handler proceeds with the HTTP
    // forwarding path. This keeps the WS and HTTP code paths in
    // the same route so the auth layer and state extraction are
    // shared, and the `Body` extractor is reused for both.
    Router::new()
        .route("/sidecar/{id}/", any(proxy_root))
        .route("/sidecar/{id}", any(proxy_root))
        .route("/sidecar/{id}/{*path}", any(proxy_handler))
        .route_layer(axum::middleware::from_fn_with_state(state.clone(), sidecar_auth_layer))
        .with_state(state)
}

use axum::extract::ConnectInfo;

async fn sidecar_auth_layer(
    State(state): State<SidecarState>,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let is_loopback = match addr.ip() {
        std::net::IpAddr::V4(v4) => v4.is_loopback(),
        std::net::IpAddr::V6(v6) => v6.is_loopback(),
    };
    if !is_loopback {
        tracing::warn!(
            "SideCar proxy rejected: client connected from non-loopback address {}",
            addr
        );
        return forbidden("sidecar proxy only allows loopback connections");
    }

    let (mut parts, body) = req.into_parts();

    let id = match sidecar_id_from_uri(parts.uri.path()) {
        Some(id) => id,
        None => return forbidden("missing sidecar id"),
    };
    let query = parts.uri.query().unwrap_or("").to_owned();
    let entry = match state.registry.get(&id).await {
        Some(e) => e,
        None => {
            debug!(sidecar_id = %id, "proxy request for unknown sidecar id");
            return not_found_response();
        }
    };
    let auth = authorize_proxy(&entry, &parts.headers, Some(&query));
    match auth {
        ProxyAuth::Denied => {
            warn!(sidecar_id = %id, "proxy request denied (bad token/cookie)");
            forbidden("invalid or missing sidecar token")
        }
        ProxyAuth::Ok { issue_cookie } => {
            // Always insert the IssueCookie extension so the
            // `Extension<Option<IssueCookie>>` extractor in the
            // handlers can use it without panicking. Use `None`
            // when no cookie needs to be issued on the response.
            let issue = if issue_cookie {
                Some(IssueCookie {
                    id: id.clone(),
                    token: entry.token.clone(),
                })
            } else {
                None
            };
            parts.extensions.insert(issue);
            parts.extensions.insert(ProxyEntry(entry));
            let req = Request::from_parts(parts, body);
            next.run(req).await
        }
    }
}

fn sidecar_id_from_uri(path: &str) -> Option<String> {
    let stripped = path.strip_prefix("/sidecar/")?;
    let (id, _) = stripped.split_once('/').unwrap_or((stripped, ""));
    if id.is_empty() { None } else { Some(id.to_owned()) }
}

#[derive(Clone)]
struct IssueCookie {
    id: String,
    token: String,
}

#[derive(Clone)]
struct ProxyEntry(pub SidecarEntry);

/// `WebSocketUpgrade` rejects with a 400 when the request isn't a
/// proper upgrade. We need it to be optional (because the same
/// route serves both HTTP and WebSocket traffic), so we wrap it.
/// `OptionalWebSocketUpgrade::from_request_parts` always succeeds —
/// the inner `WebSocketUpgrade` is `Some` only when the upgrade
/// headers are well-formed.
struct OptionalWebSocketUpgrade(Option<WebSocketUpgrade>);

impl<S> axum::extract::FromRequestParts<S> for OptionalWebSocketUpgrade
where
    S: Clone + Send + Sync + 'static,
{
    type Rejection = std::convert::Infallible;
    async fn from_request_parts(parts: &mut axum::http::request::Parts, _state: &S) -> Result<Self, Self::Rejection> {
        match WebSocketUpgrade::from_request_parts(parts, &()).await {
            Ok(ws) => Ok(OptionalWebSocketUpgrade(Some(ws))),
            Err(_) => Ok(OptionalWebSocketUpgrade(None)),
        }
    }
}

fn forbidden(msg: &str) -> Response {
    let body = ErrorResponse::new(msg, "FORBIDDEN");
    (StatusCode::FORBIDDEN, Json(body)).into_response()
}

fn not_found_response() -> Response {
    let body = ErrorResponse::new("sidecar not found", "NOT_FOUND");
    (StatusCode::NOT_FOUND, Json(body)).into_response()
}

// =========================================================================
// Management handlers
// =========================================================================

async fn list_sidecars(
    State(state): State<SidecarState>,
    Extension(_user): Extension<CurrentUser>,
) -> Result<Json<ApiResponse<SidecarListResponse>>, AppError> {
    let entries = state.registry.list().await;
    let items: Vec<SidecarListItem> = entries
        .into_iter()
        .map(|e| {
            let url = public_url(&e.id);
            SidecarListItem {
                id: e.id,
                name: e.name,
                port: e.port,
                url,
            }
        })
        .collect();
    Ok(Json(ApiResponse::ok(SidecarListResponse { sidecars: items })))
}

async fn create_sidecar(
    State(state): State<SidecarState>,
    Extension(_user): Extension<CurrentUser>,
    body: Result<Json<CreateSidecarRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<SidecarRegistration>>, AppError> {
    let Json(req) = body.map_err(|e| AppError::BadRequest(e.to_string()))?;
    let now_ms = aionui_common::now_ms();
    let outcome = state.registry.register_or_get(&req.name, req.port, now_ms).await?;
    let entry = match outcome {
        RegisterOutcome::New(e) | RegisterOutcome::Existing(e) => e,
    };
    let url = public_url(&entry.id);
    Ok(Json(ApiResponse::ok(SidecarRegistration {
        id: entry.id,
        name: entry.name,
        port: entry.port,
        url,
        token: entry.token,
    })))
}

async fn delete_sidecar(
    State(state): State<SidecarState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    let removed = state.registry.delete(&id).await;
    if !removed {
        return Err(AppError::NotFound(format!("sidecar '{id}' not found")));
    }
    Ok(Json(ApiResponse::success()))
}

fn public_url(id: &str) -> String {
    format!("/sidecar/{id}/")
}

// =========================================================================
// Combined proxy handler (HTTP + WebSocket on the same route)
// =========================================================================

/// Root path. Inspects the request and either:
/// - starts a WebSocket pump (when the upgrade headers are present
///   and `OptionalWebSocketUpgrade` resolved to Some), or
/// - forwards as HTTP.
async fn proxy_root(
    State(state): State<SidecarState>,
    Extension(entry): Extension<ProxyEntry>,
    Extension(issue): Extension<Option<IssueCookie>>,
    method: Method,
    headers: HeaderMap,
    uri: axum::http::Uri,
    ws: OptionalWebSocketUpgrade,
    body: Body,
) -> Response {
    let entry = entry.0;
    let issue_cookie = issue;
    let raw_query = uri.query().unwrap_or("").to_owned();
    let cleaned_query = strip_sct(&raw_query);
    let cleaned_query_opt = if cleaned_query.is_empty() {
        None
    } else {
        Some(cleaned_query)
    };

    if method == Method::GET
        && is_websocket_upgrade(&headers)
        && let Some(ws) = ws.0
    {
        let path = String::new();
        let mut response = ws.on_upgrade(move |client| async move {
            pump_ws_bidirectional(entry, path, cleaned_query_opt, client).await;
        });
        if let Some(c) = issue_cookie
            && let Ok(v) = HeaderValue::from_str(&build_set_cookie(&c.id, &c.token))
        {
            response.headers_mut().insert("set-cookie", v);
        }
        return response;
    }

    let issue_cookie_flag = issue_cookie.is_some();
    http_proxy(
        &state,
        entry,
        issue_cookie_flag,
        method,
        headers,
        body,
        "",
        cleaned_query_opt.as_deref(),
    )
    .await
}

/// Sub-path handler, same dual dispatch as `proxy_root`.
async fn proxy_handler(
    State(state): State<SidecarState>,
    Extension(entry): Extension<ProxyEntry>,
    Extension(issue): Extension<Option<IssueCookie>>,
    method: Method,
    headers: HeaderMap,
    uri: axum::http::Uri,
    ws: OptionalWebSocketUpgrade,
    Path((_id, path)): Path<(String, String)>,
    body: Body,
) -> Response {
    let entry = entry.0;
    let issue_cookie = issue;
    let raw_query = uri.query().unwrap_or("").to_owned();
    let cleaned_query = strip_sct(&raw_query);
    let cleaned_query_opt = if cleaned_query.is_empty() {
        None
    } else {
        Some(cleaned_query)
    };

    if method == Method::GET
        && is_websocket_upgrade(&headers)
        && let Some(ws) = ws.0
    {
        let mut response = ws.on_upgrade(move |client| async move {
            pump_ws_bidirectional(entry, path, cleaned_query_opt, client).await;
        });
        if let Some(c) = issue_cookie
            && let Ok(v) = HeaderValue::from_str(&build_set_cookie(&c.id, &c.token))
        {
            response.headers_mut().insert("set-cookie", v);
        }
        return response;
    }

    let issue_cookie_flag = issue_cookie.is_some();
    http_proxy(
        &state,
        entry,
        issue_cookie_flag,
        method,
        headers,
        body,
        &path,
        cleaned_query_opt.as_deref(),
    )
    .await
}

// -------------------------------------------------------------------------
// HTTP proxy implementation
// -------------------------------------------------------------------------

async fn http_proxy(
    state: &SidecarState,
    entry: SidecarEntry,
    issue_cookie: bool,
    method: Method,
    headers: HeaderMap,
    body: Body,
    path: &str,
    query: Option<&str>,
) -> Response {
    let target_url = build_target_url(entry.port, path, query);

    let mut req_headers = ReqHeaderMap::new();
    if let Ok(host_value) = ReqHeaderValue::from_str(&format!("{SIDECAR_TARGET_HOST}:{}", entry.port)) {
        req_headers.insert(HOST, host_value);
    }

    for (name, value) in headers.iter() {
        let lower = name.as_str().to_ascii_lowercase();
        if HOP_BY_HOP.contains(&lower.as_str()) {
            continue;
        }
        if lower == "cookie" {
            if let Ok(raw) = value.to_str() {
                let filtered = filter_cookie_header(raw);
                if !filtered.is_empty()
                    && let Ok(rv) = ReqHeaderValue::from_str(&filtered)
                {
                    req_headers.insert(ReqHeaderName::from_static("cookie"), rv);
                }
            }
            continue;
        }
        if let (Ok(n), Ok(v)) = (ReqHeaderName::from_bytes(name.as_str().as_bytes()), value.to_str()) {
            if let Ok(rv) = ReqHeaderValue::from_str(v) {
                req_headers.insert(n, rv);
            }
        }
    }

    let proto = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("http");
    if let Ok(v) = ReqHeaderValue::from_str(proto) {
        req_headers.insert("x-forwarded-proto", v);
    }
    let host_header = headers.get("host").and_then(|v| v.to_str().ok()).unwrap_or("localhost");
    if let Ok(v) = ReqHeaderValue::from_str(host_header) {
        req_headers.insert("x-forwarded-host", v);
    }

    let client = &state.http_client;
    let method_for_req = method.clone();
    let mut request = client
        .request(method_for_req, &target_url)
        .headers(req_headers)
        .timeout(Duration::from_secs(30 * 60));

    if !matches!(method, Method::GET | Method::HEAD) {
        let body_bytes = match body.collect().await {
            Ok(c) => c.to_bytes().to_vec(),
            Err(e) => {
                warn!(sidecar_id = %entry.id, error = %e, "request body read failed");
                let body = ErrorResponse::new("sidecar request body unreadable", "BAD_REQUEST");
                return (StatusCode::BAD_REQUEST, Json(body)).into_response();
            }
        };
        request = request.body(body_bytes);
    }
    let upstream = match request.send().await {
        Ok(r) => r,
        Err(e) => {
            warn!(sidecar_id = %entry.id, error = %e, "upstream request failed");
            let body = ErrorResponse::new("sidecar upstream unreachable", "BAD_GATEWAY");
            return (StatusCode::BAD_GATEWAY, Json(body)).into_response();
        }
    };

    let status = StatusCode::from_u16(upstream.status().as_u16()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut response = Response::builder().status(status);
    for (name, value) in upstream.headers().iter() {
        let lower = name.as_str().to_ascii_lowercase();
        if HOP_BY_HOP.contains(&lower.as_str()) {
            continue;
        }
        if let (Ok(n), Ok(v)) = (HeaderName::from_bytes(name.as_str().as_bytes()), value.to_str()) {
            response = response.header(n, v);
        }
    }
    if issue_cookie && let Ok(v) = HeaderValue::from_str(&build_set_cookie(&entry.id, &entry.token)) {
        response = response.header("set-cookie", v);
    }
    let body_bytes = match upstream.bytes().await {
        Ok(b) => b.to_vec(),
        Err(e) => {
            warn!(sidecar_id = %entry.id, error = %e, "upstream body read failed");
            let body = ErrorResponse::new("sidecar upstream body unreadable", "BAD_GATEWAY");
            return (StatusCode::BAD_GATEWAY, Json(body)).into_response();
        }
    };
    let body = Body::from(body_bytes);
    response
        .body(body)
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

fn build_target_url(port: u16, path: &str, query: Option<&str>) -> String {
    let normalized = if path.is_empty() || path.starts_with('/') {
        path.to_owned()
    } else {
        format!("/{path}")
    };
    let qs = query
        .filter(|q| !q.is_empty())
        .map(|q| format!("?{q}"))
        .unwrap_or_default();
    format!("http://{SIDECAR_TARGET_HOST}:{port}{normalized}{qs}")
}

fn build_ws_url(port: u16, path: &str, query: Option<&str>) -> String {
    let normalized = if path.is_empty() || path.starts_with('/') {
        path.to_owned()
    } else {
        format!("/{path}")
    };
    let qs = query
        .filter(|q| !q.is_empty())
        .map(|q| format!("?{q}"))
        .unwrap_or_default();
    format!("ws://{SIDECAR_TARGET_HOST}:{port}{normalized}{qs}")
}

// -------------------------------------------------------------------------
// WebSocket pump
// -------------------------------------------------------------------------

pub async fn pump_ws_bidirectional(entry: SidecarEntry, path: String, query: Option<String>, client: WebSocket) {
    let ws_url = build_ws_url(entry.port, &path, query.as_deref());
    let connect = tokio::time::timeout(SIDECAR_CONNECT_TIMEOUT, tokio_tungstenite::connect_async(ws_url)).await;
    let (upstream, _resp) = match connect {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => {
            warn!(sidecar_id = %entry.id, error = %e, "websocket connect failed");
            return;
        }
        Err(_) => {
            warn!(sidecar_id = %entry.id, "websocket connect timeout");
            return;
        }
    };

    let (mut upstream_tx, mut upstream_rx) = upstream.split();
    let (mut client_tx, mut client_rx) = client.split();

    let upstream_tx_handle = tokio::spawn(async move {
        while let Some(msg) = client_rx.next().await {
            let msg = match msg {
                Ok(m) => m,
                Err(_) => break,
            };
            if let Some(frame) = axum_to_tungstenite(msg)
                && upstream_tx.send(frame).await.is_err()
            {
                break;
            }
        }
    });

    let client_tx_handle = tokio::spawn(async move {
        while let Some(msg) = upstream_rx.next().await {
            let msg = match msg {
                Ok(m) => m,
                Err(_) => break,
            };
            if let Some(frame) = tungstenite_to_axum(msg)
                && client_tx.send(frame).await.is_err()
            {
                break;
            }
        }
    });

    tokio::select! {
        _ = upstream_tx_handle => {}
        _ = client_tx_handle => {}
    }
}

fn axum_to_tungstenite(msg: AxumMessage) -> Option<WsMessage> {
    match msg {
        AxumMessage::Text(t) => Some(WsMessage::Text(t.to_string().into())),
        AxumMessage::Binary(b) => Some(WsMessage::Binary(b)),
        AxumMessage::Ping(p) => Some(WsMessage::Ping(p)),
        AxumMessage::Pong(p) => Some(WsMessage::Pong(p)),
        AxumMessage::Close(_) => Some(WsMessage::Close(None)),
    }
}

fn tungstenite_to_axum(msg: WsMessage) -> Option<AxumMessage> {
    match msg {
        WsMessage::Text(t) => Some(AxumMessage::Text(t.to_string().into())),
        WsMessage::Binary(b) => Some(AxumMessage::Binary(b)),
        WsMessage::Ping(p) => Some(AxumMessage::Ping(p)),
        WsMessage::Pong(p) => Some(AxumMessage::Pong(p)),
        WsMessage::Close(_) => Some(AxumMessage::Close(None)),
        _ => None,
    }
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests;
