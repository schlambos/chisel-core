//! OpenCode provider authentication helpers (M12).
//!
//! Proxies credential management to the remote OpenCode server so users can
//! configure LLM providers without SSH access.
//!
//! API surface (OpenCode server §8 / Context7 SDK `@opencode-ai/sdk`):
//! - `GET /provider` — catalog `{ all, default, connected }` (§9)
//! - `GET /provider/auth` — auth methods per provider id (§8)
//! - `PUT /auth/{providerID}` — set `Auth` union: api | oauth | wellknown (§8)
//! - `DELETE /auth/{providerID}` — clear credentials (§8)
//! - `POST /provider/{providerID}/oauth/authorize` — `{ method, inputs? }` (§8, §27.8)
//! - `POST /provider/{providerID}/oauth/callback` — `{ method, code? }` (§8)

use std::collections::HashMap;
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

fn provider_url(base_url: &str, path: &str) -> String {
    format!("{base_url}{path}")
}

/// `PUT /auth/{providerID}` — set provider credentials (`Auth` union body).
pub async fn set_provider_auth(
    http_client: &reqwest::Client,
    cfg: &RemoteAgentConfig,
    provider_id: &str,
    payload: Value,
) -> Result<(), AppError> {
    let url = provider_url(&normalize_base_url(&cfg.url), &format!("/auth/{provider_id}"));
    let mut req = http_client.put(&url).json(&payload).timeout(Duration::from_secs(15));
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
    let url = provider_url(&normalize_base_url(&cfg.url), &format!("/auth/{provider_id}"));
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

/// `POST /provider/{providerID}/oauth/authorize` — begin OAuth (§8, recipe §27.8).
///
/// `method` is the index into the provider's auth-methods array from
/// `GET /provider/auth`. Falls back to legacy `GET` on older servers.
pub async fn start_provider_oauth(
    http_client: &reqwest::Client,
    cfg: &RemoteAgentConfig,
    provider_id: &str,
    method_index: u32,
    inputs: Option<&HashMap<String, String>>,
) -> Result<Value, AppError> {
    let base_url = normalize_base_url(&cfg.url);
    let url = provider_url(&base_url, &format!("/provider/{provider_id}/oauth/authorize"));
    let mut body = json!({ "method": method_index });
    if let Some(inp) = inputs.filter(|m| !m.is_empty()) {
        body["inputs"] = serde_json::to_value(inp)
            .map_err(|e| AppError::Internal(format!("OAuth inputs serialization failed: {e}")))?;
    }

    let mut req = http_client.post(&url).json(&body).timeout(Duration::from_secs(15));
    if let Some(ref h) = auth(cfg) {
        req = req.header(AUTHORIZATION, h);
    }
    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => return Err(AppError::BadGateway(format!("OpenCode OAuth authorize failed: {e}"))),
    };

    if resp.status().as_u16() == 405 || resp.status().as_u16() == 404 {
        // Legacy servers (pre-POST oauth) — best-effort GET fallback.
        let legacy_url = provider_url(&base_url, &format!("/provider/{provider_id}/oauth/authorize"));
        let mut legacy = http_client.get(&legacy_url).timeout(Duration::from_secs(15));
        if let Some(ref h) = auth(cfg) {
            legacy = legacy.header(AUTHORIZATION, h);
        }
        let legacy_resp = legacy
            .send()
            .await
            .map_err(|e| AppError::BadGateway(format!("OpenCode OAuth authorize (legacy GET) failed: {e}")))?;
        return parse_json(legacy_resp, "GET /provider/oauth/authorize").await;
    }

    parse_json(resp, "POST /provider/oauth/authorize").await
}

/// `POST /provider/{providerID}/oauth/callback` — complete OAuth flow.
pub async fn complete_provider_oauth(
    http_client: &reqwest::Client,
    cfg: &RemoteAgentConfig,
    provider_id: &str,
    method_index: u32,
    code: Option<&str>,
) -> Result<(), AppError> {
    let base_url = normalize_base_url(&cfg.url);
    let url = provider_url(&base_url, &format!("/provider/{provider_id}/oauth/callback"));
    let mut body = json!({ "method": method_index });
    if let Some(c) = code.filter(|s| !s.is_empty()) {
        body["code"] = json!(c);
    }

    let mut req = http_client.post(&url).json(&body).timeout(Duration::from_secs(30));
    if let Some(ref h) = auth(cfg) {
        req = req.header(AUTHORIZATION, h);
    }
    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => return Err(AppError::BadGateway(format!("OpenCode OAuth callback failed: {e}"))),
    };

    if resp.status().as_u16() == 405 || resp.status().as_u16() == 404 {
        let code = code.ok_or_else(|| AppError::BadRequest("OAuth authorization code is required".into()))?;
        let enc = super::opencode_context::encode_query_value(code);
        let legacy_url = provider_url(
            &base_url,
            &format!("/provider/{provider_id}/oauth/callback?code={enc}"),
        );
        let mut legacy = http_client.get(&legacy_url).timeout(Duration::from_secs(30));
        if let Some(ref h) = auth(cfg) {
            legacy = legacy.header(AUTHORIZATION, h);
        }
        let legacy_resp = legacy
            .send()
            .await
            .map_err(|e| AppError::BadGateway(format!("OpenCode OAuth callback (legacy GET) failed: {e}")))?;
        return parse_json(legacy_resp, "GET /provider/oauth/callback").await.map(|_| ());
    }

    parse_json(resp, "POST /provider/oauth/callback").await.map(|_| ())
}

/// `GET /provider` — full catalog with models (§9).
pub async fn list_providers(
    http_client: &reqwest::Client,
    cfg: &RemoteAgentConfig,
) -> Result<Value, AppError> {
    let url = provider_url(&normalize_base_url(&cfg.url), "/provider");
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

/// `GET /provider/auth` — supported auth methods keyed by provider id (§8).
pub async fn list_provider_auth_methods(
    http_client: &reqwest::Client,
    cfg: &RemoteAgentConfig,
) -> Result<Value, AppError> {
    let url = provider_url(&normalize_base_url(&cfg.url), "/provider/auth");
    let mut req = http_client.get(&url).timeout(Duration::from_secs(15));
    if let Some(ref h) = auth(cfg) {
        req = req.header(AUTHORIZATION, h);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode GET /provider/auth failed: {e}")))?;
    parse_json(resp, "GET /provider/auth").await
}

/// Convenience: `{ type: "api", key }` credentials (§8 `ApiAuth`).
pub async fn set_api_key(
    http_client: &reqwest::Client,
    cfg: &RemoteAgentConfig,
    provider_id: &str,
    api_key: &str,
) -> Result<(), AppError> {
    set_provider_auth(
        http_client,
        cfg,
        provider_id,
        json!({ "type": "api", "key": api_key }),
    )
    .await
}

/// Convenience: `{ type: "wellknown", key, token }` credentials (§8 `WellKnownAuth`).
pub async fn set_wellknown_auth(
    http_client: &reqwest::Client,
    cfg: &RemoteAgentConfig,
    provider_id: &str,
    key: &str,
    token: &str,
) -> Result<(), AppError> {
    set_provider_auth(
        http_client,
        cfg,
        provider_id,
        json!({ "type": "wellknown", "key": key, "token": token }),
    )
    .await
}
