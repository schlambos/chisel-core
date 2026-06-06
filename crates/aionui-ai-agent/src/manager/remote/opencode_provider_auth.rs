//! OpenCode provider authentication helpers (M12).
//!
//! Proxies credential management to the remote OpenCode server so users can
//! configure LLM providers without SSH access.

use std::time::Duration;

use aionui_common::AppError;
use reqwest::header::AUTHORIZATION;
use serde_json::{Value, json};

use super::agent::{RemoteAgentConfig, build_auth_header, normalize_base_url};

async fn parse_json(resp: reqwest::Response, label: &str) -> Result<Value, AppError> {
    if !resp.status().is_success() {
        let status = resp.status();
        let body_text = resp.text().await.unwrap_or_default();
        return Err(AppError::BadGateway(format!(
            "OpenCode {label} returned {status}: {body_text}"
        )));
    }
    let text = resp.text().await.unwrap_or_default();
    if text.trim().is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_str(&text)
        .map_err(|e| AppError::BadGateway(format!("OpenCode {label} response was not JSON: {e}")))
}

fn auth(cfg: &RemoteAgentConfig) -> Option<String> {
    build_auth_header(&cfg.auth_type, cfg.auth_token.as_deref())
}

/// `PUT /auth/{providerID}` — set provider credentials (API key shape).
pub async fn set_provider_auth(
    http_client: &reqwest::Client,
    cfg: &RemoteAgentConfig,
    provider_id: &str,
    payload: Value,
) -> Result<(), AppError> {
    let base_url = normalize_base_url(&cfg.url);
    let url = format!("{base_url}/auth/{provider_id}");
    let mut req = http_client
        .put(&url)
        .json(&payload)
        .timeout(Duration::from_secs(15));
    if let Some(ref h) = auth(cfg) {
        req = req.header(AUTHORIZATION, h);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode PUT /auth failed: {e}")))?;
    parse_json(resp, "PUT /auth").await.map(|_| ())
}

/// `DELETE /auth/{providerID}` — clear provider credentials.
pub async fn delete_provider_auth(
    http_client: &reqwest::Client,
    cfg: &RemoteAgentConfig,
    provider_id: &str,
) -> Result<(), AppError> {
    let base_url = normalize_base_url(&cfg.url);
    let url = format!("{base_url}/auth/{provider_id}");
    let mut req = http_client.delete(&url).timeout(Duration::from_secs(15));
    if let Some(ref h) = auth(cfg) {
        req = req.header(AUTHORIZATION, h);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode DELETE /auth failed: {e}")))?;
    if resp.status().is_success() || resp.status().as_u16() == 204 {
        Ok(())
    } else {
        let status = resp.status();
        let body_text = resp.text().await.unwrap_or_default();
        Err(AppError::BadGateway(format!(
            "OpenCode DELETE /auth returned {status}: {body_text}"
        )))
    }
}

/// `GET /provider/{providerID}/oauth/authorize` — start OAuth; returns authorize URL JSON.
pub async fn start_provider_oauth(
    http_client: &reqwest::Client,
    cfg: &RemoteAgentConfig,
    provider_id: &str,
) -> Result<Value, AppError> {
    let base_url = normalize_base_url(&cfg.url);
    let url = format!("{base_url}/provider/{provider_id}/oauth/authorize");
    let mut req = http_client.get(&url).timeout(Duration::from_secs(15));
    if let Some(ref h) = auth(cfg) {
        req = req.header(AUTHORIZATION, h);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode OAuth authorize failed: {e}")))?;
    parse_json(resp, "GET /provider/oauth/authorize").await
}

/// `GET /provider/{providerID}/oauth/callback?code=` — complete OAuth.
pub async fn complete_provider_oauth(
    http_client: &reqwest::Client,
    cfg: &RemoteAgentConfig,
    provider_id: &str,
    code: &str,
) -> Result<(), AppError> {
    let base_url = normalize_base_url(&cfg.url);
    let enc = super::opencode_context::encode_query_value(code);
    let url = format!("{base_url}/provider/{provider_id}/oauth/callback?code={enc}");
    let mut req = http_client.get(&url).timeout(Duration::from_secs(30));
    if let Some(ref h) = auth(cfg) {
        req = req.header(AUTHORIZATION, h);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode OAuth callback failed: {e}")))?;
    parse_json(resp, "GET /provider/oauth/callback").await.map(|_| ())
}

/// `GET /provider` — list providers with auth state (enriched catalog).
pub async fn list_providers(
    http_client: &reqwest::Client,
    cfg: &RemoteAgentConfig,
) -> Result<Value, AppError> {
    let base_url = normalize_base_url(&cfg.url);
    let url = format!("{base_url}/provider");
    let mut req = http_client.get(&url).timeout(Duration::from_secs(15));
    if let Some(ref h) = auth(cfg) {
        req = req.header(AUTHORIZATION, h);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode GET /provider failed: {e}")))?;
    parse_json(resp, "GET /provider").await
}

/// Convenience: set API key credentials.
pub async fn set_api_key(
    http_client: &reqwest::Client,
    cfg: &RemoteAgentConfig,
    provider_id: &str,
    api_key: &str,
) -> Result<(), AppError> {
    set_provider_auth(http_client, cfg, provider_id, json!({ "type": "api", "key": api_key })).await
}
