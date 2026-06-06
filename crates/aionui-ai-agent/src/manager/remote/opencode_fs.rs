//! OpenCode file / find / path endpoints (M13).
//!
//! Used when `tool_host: "server"` so Chisl can browse and search the remote
//! OpenCode server's working tree instead of the local filesystem.

use std::time::Duration;

use aionui_common::AppError;
use reqwest::header::AUTHORIZATION;
use serde_json::{Value, json};

use super::agent::{RemoteAgentConfig, build_auth_header, normalize_base_url};
use super::opencode_context::append_v1_directory;

fn authed_get(
    http_client: &reqwest::Client,
    url: &str,
    auth_header: Option<&str>,
    timeout_secs: u64,
) -> reqwest::RequestBuilder {
    let mut req = http_client.get(url).timeout(Duration::from_secs(timeout_secs));
    if let Some(h) = auth_header {
        req = req.header(AUTHORIZATION, h);
    }
    req
}

fn authed_post_json(
    http_client: &reqwest::Client,
    url: &str,
    auth_header: Option<&str>,
    body: Value,
    timeout_secs: u64,
) -> reqwest::RequestBuilder {
    let mut req = http_client.post(url).json(&body).timeout(Duration::from_secs(timeout_secs));
    if let Some(h) = auth_header {
        req = req.header(AUTHORIZATION, h);
    }
    req
}

async fn parse_json(resp: reqwest::Response, label: &str) -> Result<Value, AppError> {
    if !resp.status().is_success() {
        let status = resp.status();
        let body_text = resp.text().await.unwrap_or_default();
        return Err(AppError::BadGateway(format!(
            "OpenCode {label} returned {status}: {body_text}"
        )));
    }
    resp.json()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode {label} response was not JSON: {e}")))
}

fn append_query_param(url: &mut String, key: &str, value: &str) {
    let enc = super::opencode_context::encode_query_value(value);
    let sep = if url.contains('?') { '&' } else { '?' };
    url.push_str(&format!("{sep}{key}={enc}"));
}

/// `GET /path` — workspace metadata for the resolved directory.
pub async fn fetch_path(
    http_client: &reqwest::Client,
    cfg: &RemoteAgentConfig,
    workspace: &str,
) -> Result<Value, AppError> {
    let base_url = normalize_base_url(&cfg.url);
    let auth = build_auth_header(&cfg.auth_type, cfg.auth_token.as_deref());
    let url = append_v1_directory(&format!("{base_url}/path"), cfg, workspace);
    let resp = authed_get(http_client, &url, auth.as_deref(), 15)
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode GET /path failed: {e}")))?;
    parse_json(resp, "GET /path").await
}

/// `GET /file?path=` — list files under a relative path.
pub async fn list_files(
    http_client: &reqwest::Client,
    cfg: &RemoteAgentConfig,
    workspace: &str,
    path: &str,
) -> Result<Value, AppError> {
    let base_url = normalize_base_url(&cfg.url);
    let auth = build_auth_header(&cfg.auth_type, cfg.auth_token.as_deref());
    let mut url = append_v1_directory(&format!("{base_url}/file"), cfg, workspace);
    let sep = if url.contains('?') { '&' } else { '?' };
    url.push_str(&format!("{sep}path={}", super::opencode_context::encode_query_value(path)));
    let resp = authed_get(http_client, &url, auth.as_deref(), 30)
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode GET /file failed: {e}")))?;
    parse_json(resp, "GET /file").await
}

/// `POST /file/content` — read file contents (optional partial range).
pub async fn read_file_content(
    http_client: &reqwest::Client,
    cfg: &RemoteAgentConfig,
    workspace: &str,
    path: &str,
    start: Option<u64>,
    end: Option<u64>,
) -> Result<Value, AppError> {
    let base_url = normalize_base_url(&cfg.url);
    let auth = build_auth_header(&cfg.auth_type, cfg.auth_token.as_deref());
    let url = append_v1_directory(&format!("{base_url}/file/content"), cfg, workspace);
    let mut body = json!({ "path": path });
    if let Some(s) = start {
        body["start"] = json!(s);
    }
    if let Some(e) = end {
        body["end"] = json!(e);
    }
    let resp = authed_post_json(http_client, &url, auth.as_deref(), body, 30)
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode POST /file/content failed: {e}")))?;
    parse_json(resp, "POST /file/content").await
}

/// `GET /find/file?query=` — find files/directories by name.
pub async fn find_files(
    http_client: &reqwest::Client,
    cfg: &RemoteAgentConfig,
    workspace: &str,
    query: &str,
    limit: Option<u32>,
) -> Result<Value, AppError> {
    let base_url = normalize_base_url(&cfg.url);
    let auth = build_auth_header(&cfg.auth_type, cfg.auth_token.as_deref());
    let mut url = append_v1_directory(&format!("{base_url}/find/file"), cfg, workspace);
    append_query_param(&mut url, "query", query);
    if let Some(l) = limit {
        url.push_str(&format!("&limit={l}"));
    }
    let resp = authed_get(http_client, &url, auth.as_deref(), 30)
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode GET /find/file failed: {e}")))?;
    parse_json(resp, "GET /find/file").await
}

/// `GET /find?pattern=` — grep-style text search.
pub async fn find_text(
    http_client: &reqwest::Client,
    cfg: &RemoteAgentConfig,
    workspace: &str,
    pattern: &str,
    limit: Option<u32>,
) -> Result<Value, AppError> {
    let base_url = normalize_base_url(&cfg.url);
    let auth = build_auth_header(&cfg.auth_type, cfg.auth_token.as_deref());
    let mut url = append_v1_directory(&format!("{base_url}/find"), cfg, workspace);
    append_query_param(&mut url, "pattern", pattern);
    if let Some(l) = limit {
        url.push_str(&format!("&limit={l}"));
    }
    let resp = authed_get(http_client, &url, auth.as_deref(), 60)
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode GET /find failed: {e}")))?;
    parse_json(resp, "GET /find").await
}

/// `GET /find/symbol?query=` — LSP symbol search.
pub async fn find_symbols(
    http_client: &reqwest::Client,
    cfg: &RemoteAgentConfig,
    workspace: &str,
    query: &str,
) -> Result<Value, AppError> {
    let base_url = normalize_base_url(&cfg.url);
    let auth = build_auth_header(&cfg.auth_type, cfg.auth_token.as_deref());
    let mut url = append_v1_directory(&format!("{base_url}/find/symbol"), cfg, workspace);
    append_query_param(&mut url, "query", query);
    let resp = authed_get(http_client, &url, auth.as_deref(), 30)
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode GET /find/symbol failed: {e}")))?;
    parse_json(resp, "GET /find/symbol").await
}

/// `GET /formatter` — list formatters for the directory.
pub async fn list_formatters(
    http_client: &reqwest::Client,
    cfg: &RemoteAgentConfig,
    workspace: &str,
) -> Result<Value, AppError> {
    let base_url = normalize_base_url(&cfg.url);
    let auth = build_auth_header(&cfg.auth_type, cfg.auth_token.as_deref());
    let url = append_v1_directory(&format!("{base_url}/formatter"), cfg, workspace);
    let resp = authed_get(http_client, &url, auth.as_deref(), 15)
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode GET /formatter failed: {e}")))?;
    parse_json(resp, "GET /formatter").await
}
