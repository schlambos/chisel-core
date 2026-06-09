//! V2 session API transport + helpers (M22).
//!
//! The OpenCode server runs two parallel session APIs:
//! - V1 (legacy): `/session/{id}/prompt_async`, `/session/{id}/summarize`, etc.
//! - V2 (newer):  `/api/session/{id}/prompt`, `/api/session/{id}/compact`, etc.
//!
//! This module provides V2-aware transport helpers and the V2-specific
//! endpoint implementations. V1 endpoints remain in `agent.rs` for now.

use std::time::Duration;

use aionui_common::AppError;
use reqwest::header::AUTHORIZATION;
#[allow(unused_imports)] // `json!` is used only by the inline tests below
use serde_json::{Value, json};

use super::agent::RemoteAgentConfig;
use super::opencode_context::append_v2_location;
use super::opencode_payloads::{OpencodeV2CompactRequest, OpencodeV2PromptRequest, OpencodeV2PromptText};

/// Optional server-tools location scoping for V2 routes.
pub type V2Location<'a> = Option<(&'a RemoteAgentConfig, &'a str)>;

fn scoped_v2_url(base: &str, cfg: V2Location<'_>) -> String {
    match cfg {
        Some((c, ws)) => append_v2_location(base, c, ws),
        None => base.to_string(),
    }
}

/// Shared transport for V2 session-scoped requests.
/// Mirrors `opencode_session_request` but targets `/api/session/{sessionID}...`.
#[allow(clippy::too_many_arguments)]
pub async fn v2_session_request(
    http_client: &reqwest::Client,
    url: &str,
    auth_header: Option<&str>,
    method: reqwest::Method,
    body: Option<Value>,
    timeout_secs: u64,
) -> Result<Value, AppError> {
    let mut req = http_client
        .request(method, url)
        .timeout(Duration::from_secs(timeout_secs));
    if let Some(ref b) = body {
        req = req.json(b);
    }
    if let Some(h) = auth_header {
        req = req.header(AUTHORIZATION, h);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode V2 session request failed: {e}")))?;
    if !resp.status().is_success() {
        let status = resp.status();
        if status.as_u16() == 204 {
            return Ok(Value::Null);
        }
        let body_text = resp.text().await.unwrap_or_default();
        return Err(AppError::BadGateway(format!(
            "OpenCode V2 session request returned {status}: {body_text}"
        )));
    }
    let text = resp.text().await.unwrap_or_default();
    if text.trim().is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_str(&text)
        .map_err(|e| AppError::BadGateway(format!("OpenCode V2 session response was not JSON: {e}")))
}

fn v2_session_url(base_url: &str, session_id: &str, subpath: &str, location: V2Location<'_>) -> String {
    scoped_v2_url(&format!("{base_url}/api/session/{session_id}{subpath}"), location)
}

/// V2 compact the session (`POST /api/session/{sessionID}/compact`).
/// Replaces V1 `POST /session/{id}/summarize`. The V2 endpoint does not
/// require `providerID`/`modelID` in the body — the server uses the
/// session's current model.
pub async fn v2_compact(
    http_client: &reqwest::Client,
    base_url: &str,
    auth_header: Option<&str>,
    session_id: &str,
    instructions: Option<&str>,
    location: V2Location<'_>,
) -> Result<(), AppError> {
    let body = OpencodeV2CompactRequest { instructions: instructions.map(String::from) };
    let url = v2_session_url(base_url, session_id, "/compact", location);
    v2_session_request(
        http_client,
        &url,
        auth_header,
        reqwest::Method::POST,
        Some(serde_json::to_value(&body).unwrap()),
        120,
    )
    .await
    .map(|_| ())
}

/// V2 get session context (`GET /api/session/{sessionID}/context`).
/// Returns the active context messages (all messages after the last compaction).
pub async fn v2_get_context(
    http_client: &reqwest::Client,
    base_url: &str,
    auth_header: Option<&str>,
    session_id: &str,
    location: V2Location<'_>,
) -> Result<Value, AppError> {
    let url = v2_session_url(base_url, session_id, "/context", location);
    v2_session_request(
        http_client,
        &url,
        auth_header,
        reqwest::Method::GET,
        None,
        30,
    )
    .await
}

/// V2 prompt (`POST /api/session/{sessionID}/prompt`).
/// Returns the `SessionMessage` response (user message as processed).
///
/// D1 fix: OpenCode V2 `Prompt` schema is `{ text, files?, agents?, references? }`
/// (see `core/src/session-prompt.ts` `Prompt` class). The V2 server does NOT
/// accept per-prompt `model`, `agent`, or `skills` fields — they are stripped
/// server-side, silently breaking per-prompt model/agent overrides and skill
/// injection on the V2 path. Callers that need per-prompt model/agent overrides
/// must route through the V1 prompt path; callers that need skill injection
/// must use V1 or accept the loss. See `opencode_send_v2` and the dispatch
/// policy in `agent.rs` `send_message` for the narrow V1 fallback rules.
///
/// Only `text` is sent in the `prompt` object. The `delivery` field is the
/// V2 server's own queueing mode (`"immediate"` runs the agent loop
/// synchronously; `"deferred"` queues it).
pub async fn v2_prompt(
    http_client: &reqwest::Client,
    base_url: &str,
    auth_header: Option<&str>,
    session_id: &str,
    prompt_text: &str,
    delivery: Option<&str>,
    location: V2Location<'_>,
) -> Result<Value, AppError> {
    let body = OpencodeV2PromptRequest {
        prompt: OpencodeV2PromptText {
            text: prompt_text.to_string(),
            files: Vec::new(),
            agent: None,
            reference: None,
        },
        delivery: delivery.map(String::from),
    };

    let url = v2_session_url(base_url, session_id, "/prompt", location);
    v2_session_request(
        http_client,
        &url,
        auth_header,
        reqwest::Method::POST,
        Some(serde_json::to_value(&body).unwrap()),
        120,
    )
    .await
}

/// V2 session list (`GET /api/session`). Returns the raw JSON response
/// with cursor-based pagination (`{ items, cursor: { previous, next } }`).
pub async fn v2_list_sessions(
    http_client: &reqwest::Client,
    base_url: &str,
    auth_header: Option<&str>,
    limit: Option<u32>,
    cursor: Option<&str>,
    location: V2Location<'_>,
) -> Result<Value, AppError> {
    let mut url = format!("{base_url}/api/session");
    let mut sep = '?';
    if let Some(l) = limit {
        url.push_str(&format!("{sep}limit={l}"));
        sep = '&';
    }
    if let Some(c) = cursor {
        url.push_str(&format!("{sep}cursor={c}"));
    }
    let url = scoped_v2_url(&url, location);
    let mut req = http_client.get(&url).timeout(Duration::from_secs(15));
    if let Some(h) = auth_header {
        req = req.header(AUTHORIZATION, h);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode V2 session list failed: {e}")))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body_text = resp.text().await.unwrap_or_default();
        return Err(AppError::BadGateway(format!(
            "OpenCode V2 session list returned {status}: {body_text}"
        )));
    }
    resp.json()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode V2 session list response was not JSON: {e}")))
}

/// V2 session messages (`GET /api/session/{sessionID}/message`).
/// Returns the raw JSON response with cursor-based pagination.
pub async fn v2_get_messages(
    http_client: &reqwest::Client,
    base_url: &str,
    auth_header: Option<&str>,
    session_id: &str,
    limit: Option<u32>,
    cursor: Option<&str>,
    location: V2Location<'_>,
) -> Result<Value, AppError> {
    let mut subpath = "/message".to_string();
    let mut sep = '?';
    if let Some(l) = limit {
        subpath.push_str(&format!("{sep}limit={l}"));
        sep = '&';
    }
    if let Some(c) = cursor {
        subpath.push_str(&format!("{sep}cursor={c}"));
    }
    let url = v2_session_url(base_url, session_id, &subpath, location);
    v2_session_request(
        http_client,
        &url,
        auth_header,
        reqwest::Method::GET,
        None,
        30,
    )
    .await
}

/// V2 wait for session idle (`POST /api/session/{sessionID}/wait`).
/// Blocks until the session's agent loop becomes idle (204) or times out.
pub async fn v2_wait(
    http_client: &reqwest::Client,
    base_url: &str,
    auth_header: Option<&str>,
    session_id: &str,
    location: V2Location<'_>,
) -> Result<(), AppError> {
    let url = v2_session_url(base_url, session_id, "/wait", location);
    v2_session_request(
        http_client,
        &url,
        auth_header,
        reqwest::Method::POST,
        None,
        300,
    )
    .await
    .map(|_| ())
}

/// V2 model list (`GET /api/model`). Returns the raw JSON array of `ModelV2Info`.
pub async fn fetch_v2_models(
    http_client: &reqwest::Client,
    base_url: &str,
    auth_header: Option<&str>,
    location: V2Location<'_>,
) -> Result<Value, AppError> {
    let url = scoped_v2_url(&format!("{base_url}/api/model"), location);
    let mut req = http_client.get(&url).timeout(Duration::from_secs(10));
    if let Some(h) = auth_header {
        req = req.header(AUTHORIZATION, h);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode V2 model list failed: {e}")))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body_text = resp.text().await.unwrap_or_default();
        return Err(AppError::BadGateway(format!(
            "OpenCode V2 model list returned {status}: {body_text}"
        )));
    }
    resp.json()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode V2 model list response was not JSON: {e}")))
}

/// V2 provider list (`GET /api/provider`). Returns the raw JSON array of `ProviderV2Info`.
pub async fn fetch_v2_providers(
    http_client: &reqwest::Client,
    base_url: &str,
    auth_header: Option<&str>,
    location: V2Location<'_>,
) -> Result<Value, AppError> {
    let url = scoped_v2_url(&format!("{base_url}/api/provider"), location);
    let mut req = http_client.get(&url).timeout(Duration::from_secs(10));
    if let Some(h) = auth_header {
        req = req.header(AUTHORIZATION, h);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode V2 provider list failed: {e}")))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body_text = resp.text().await.unwrap_or_default();
        return Err(AppError::BadGateway(format!(
            "OpenCode V2 provider list returned {status}: {body_text}"
        )));
    }
    resp.json()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode V2 provider list response was not JSON: {e}")))
}

/// Parse V2 model data into the existing `ModelInfoEntry` format for backward
/// compatibility. V2 `/api/model` returns a flat array of `ModelV2Info` objects,
/// each with `id`, `providerID`, `name`, `enabled`, `status`, `limit.context`.
pub fn parse_v2_models_as_entries(models: &[Value]) -> Vec<aionui_api_types::ModelInfoEntry> {
    let mut entries = Vec::new();
    for model in models {
        let enabled = model.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false);
        if !enabled {
            continue;
        }
        let provider_id = match model.get("providerID").and_then(|v| v.as_str()) {
            Some(id) => id,
            None => continue,
        };
        let model_id = match model.get("id").and_then(|v| v.as_str()) {
            Some(id) => id,
            None => continue,
        };
        let name = model.get("name").and_then(|v| v.as_str()).unwrap_or(model_id);
        entries.push(aionui_api_types::ModelInfoEntry {
            id: format!("{provider_id}::{model_id}"),
            label: format!("[{provider_id}] {name}"),
        });
    }
    entries
}

/// Extract `model_id -> context_window` from V2 model data.
/// V2 models carry `limit.context` directly, so no separate `/config/providers`
/// call is needed.
pub fn parse_v2_context_limits(models: &[Value]) -> std::collections::HashMap<String, u64> {
    let mut out = std::collections::HashMap::new();
    for model in models {
        let model_id = match model.get("id").and_then(|v| v.as_str()) {
            Some(id) => id,
            None => continue,
        };
        if let Some(ctx) = model
            .get("limit")
            .and_then(|l| l.get("context"))
            .and_then(serde_json::Value::as_u64)
            && ctx > 0
        {
            out.insert(model_id.to_string(), ctx);
        }
    }
    out
}

/// V2 model metadata — enriched model info for the renderer.
#[derive(Debug, Clone, serde::Serialize)]
pub struct V2ModelMetadata {
    pub id: String,
    pub provider_id: String,
    pub name: String,
    pub status: String,
    pub enabled: bool,
    pub context_limit: u64,
    pub output_limit: u64,
    pub family: Option<String>,
    pub supports_tools: bool,
}

/// Parse V2 model data into enriched metadata.
pub fn parse_v2_model_metadata(models: &[Value]) -> Vec<V2ModelMetadata> {
    models
        .iter()
        .filter_map(|model| {
            let enabled = model.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false);
            let id = model.get("id")?.as_str()?.to_string();
            let provider_id = model.get("providerID")?.as_str()?.to_string();
            let name = model.get("name")?.as_str()?.to_string();
            let status = model
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("active")
                .to_string();
            let context_limit = model
                .get("limit")
                .and_then(|l| l.get("context"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let output_limit = model
                .get("limit")
                .and_then(|l| l.get("output"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let family = model.get("family").and_then(|v| v.as_str()).map(String::from);
            let supports_tools = model
                .get("capabilities")
                .and_then(|c| c.get("tools"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if enabled {
                Some(V2ModelMetadata {
                    id,
                    provider_id,
                    name,
                    status,
                    enabled,
                    context_limit,
                    output_limit,
                    family,
                    supports_tools,
                })
            } else {
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_v2_models_as_entries() {
        let models = json!([
            {
                "id": "claude-sonnet-4-5",
                "apiID": "claude-sonnet-4-5",
                "providerID": "anthropic",
                "name": "Claude Sonnet 4.5",
                "enabled": true,
                "status": "active",
                "limit": { "context": 200000, "output": 64000 }
            },
            {
                "id": "gpt-5",
                "apiID": "gpt-5",
                "providerID": "openai",
                "name": "GPT-5",
                "enabled": false,
                "status": "deprecated",
                "limit": { "context": 400000, "output": 128000 }
            }
        ]);
        let arr = models.as_array().unwrap();
        let entries = parse_v2_models_as_entries(arr);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "anthropic::claude-sonnet-4-5");
        assert_eq!(entries[0].label, "[anthropic] Claude Sonnet 4.5");
    }

    #[test]
    fn parses_v2_context_limits() {
        let models = json!([
            {
                "id": "claude-sonnet-4-5",
                "providerID": "anthropic",
                "enabled": true,
                "limit": { "context": 200000, "output": 64000 }
            },
            {
                "id": "gpt-5",
                "providerID": "openai",
                "enabled": true,
                "limit": { "context": 400000 }
            }
        ]);
        let arr = models.as_array().unwrap();
        let limits = parse_v2_context_limits(arr);
        assert_eq!(limits.get("claude-sonnet-4-5"), Some(&200000));
        assert_eq!(limits.get("gpt-5"), Some(&400000));
    }

    #[test]
    fn parses_v2_model_metadata() {
        let models = json!([
            {
                "id": "claude-sonnet-4-5",
                "apiID": "claude-sonnet-4-5",
                "providerID": "anthropic",
                "name": "Claude Sonnet 4.5",
                "enabled": true,
                "status": "active",
                "family": "claude",
                "limit": { "context": 200000, "output": 64000 },
                "capabilities": { "tools": true, "input": ["text"], "output": ["text"] }
            }
        ]);
        let arr = models.as_array().unwrap();
        let meta = parse_v2_model_metadata(arr);
        assert_eq!(meta.len(), 1);
        assert_eq!(meta[0].family.as_deref(), Some("claude"));
        assert!(meta[0].supports_tools);
        assert_eq!(meta[0].context_limit, 200000);
    }
}
