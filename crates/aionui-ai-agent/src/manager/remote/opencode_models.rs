//! OpenCode model context-window discovery + context-usage math.
//!
//! OpenCode advertises per-model limits at `GET /config/providers`
//! (`providers[].models[id].limit.context`). It does NOT push context-usage
//! events, but every assistant `message.updated` carries `info.tokens`
//! (`{input, output, reasoning, cache:{read, write}}`). We combine the two to
//! synthesize the `acp_context_usage` event the renderer already consumes
//! (shape: `{ used, size }`, mirroring the ACP `UsageUpdate`).

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use aionui_api_types::ModelInfoEntry;
use reqwest::header::AUTHORIZATION;
use serde_json::Value;
use tracing::warn;

/// Fetch a `model_id -> context_window` map from `GET /config/providers`.
/// Best-effort: any failure yields an empty map so the meter falls back to
/// the renderer's default limit rather than blocking the conversation.
pub async fn fetch_context_limits(
    http_client: &reqwest::Client,
    base_url: &str,
    auth_header: Option<&str>,
) -> HashMap<String, u64> {
    let url = format!("{base_url}/config/providers");
    let mut req = http_client.get(&url).timeout(Duration::from_secs(10));
    if let Some(h) = auth_header {
        req = req.header(AUTHORIZATION, h);
    }

    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "OpenCode GET /config/providers failed");
            return HashMap::new();
        }
    };
    if !resp.status().is_success() {
        warn!(status = %resp.status(), "OpenCode GET /config/providers returned non-success");
        return HashMap::new();
    }

    let body: Value = match resp.json().await {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, "OpenCode GET /config/providers response was not JSON");
            return HashMap::new();
        }
    };

    parse_context_limits(&body)
}

/// Parse `GET /provider` into selectable model entries.
///
/// Only surfaces models from authenticated providers. When the server returns
/// a non-empty `connected` list, only those providers are included. When
/// `connected` is empty (older OpenCode builds that don't report it), all
/// providers are included as a fallback.
pub fn parse_provider_model_entries(body: &Value) -> Vec<ModelInfoEntry> {
    let connected: HashSet<&str> = body
        .get("connected")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();

    let mut entries = Vec::new();
    let Some(all) = body.get("all").and_then(|v| v.as_array()) else {
        return entries;
    };

    for provider in all {
        let provider_id = match provider.get("id").and_then(|v| v.as_str()) {
            Some(id) if connected.is_empty() || connected.contains(id) => id,
            _ => continue,
        };
        let Some(models) = provider.get("models").and_then(|v| v.as_object()) else {
            continue;
        };
        for (model_id, model) in models {
            let name = model.get("name").and_then(|v| v.as_str()).unwrap_or(model_id);
            entries.push(ModelInfoEntry {
                id: format!("{provider_id}::{model_id}"),
                label: format!("[{provider_id}] {name}"),
            });
        }
    }
    entries
}

/// Extract the `model_id -> limit.context` map from a `/config/providers` body.
/// Models without a positive `limit.context` are skipped (the renderer then
/// falls back to its default context limit).
pub fn parse_context_limits(body: &Value) -> HashMap<String, u64> {
    let mut out = HashMap::new();
    let providers = match body.get("providers").and_then(|v| v.as_array()) {
        Some(p) => p,
        None => return out,
    };
    for provider in providers {
        let models = match provider.get("models").and_then(|v| v.as_object()) {
            Some(m) => m,
            None => continue,
        };
        for (model_id, model) in models {
            if let Some(ctx) = model
                .get("limit")
                .and_then(|l| l.get("context"))
                .and_then(serde_json::Value::as_u64)
                && ctx > 0
            {
                out.insert(model_id.clone(), ctx);
            }
        }
    }
    out
}

/// Tokens currently occupying the context window, derived from an assistant
/// message's `info.tokens`. Sums the prompt (`input` + cached read/write) and
/// the generation (`output` + `reasoning`); missing fields count as zero.
/// `cache.read`/`cache.write` are reported by providers separately from
/// `input` (they are not a subset of it), so summing does not double-count.
pub fn context_tokens_used(tokens: &Value) -> u64 {
    let at = |path: &[&str]| -> u64 {
        let mut cur = tokens;
        for key in path {
            match cur.get(key) {
                Some(v) => cur = v,
                None => return 0,
            }
        }
        cur.as_u64().unwrap_or(0)
    };
    at(&["input"]) + at(&["output"]) + at(&["reasoning"]) + at(&["cache", "read"]) + at(&["cache", "write"])
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_model_context_limits() {
        let body = json!({
            "providers": [
                {
                    "id": "anthropic",
                    "models": {
                        "claude-sonnet-4-5": { "limit": { "context": 200000, "output": 64000 } },
                        "claude-haiku": { "limit": { "context": 100000, "output": 8192 } }
                    }
                },
                {
                    "id": "openai",
                    "models": {
                        "gpt-5": { "limit": { "context": 400000, "output": 128000 } }
                    }
                }
            ],
            "default": {}
        });
        let limits = parse_context_limits(&body);
        assert_eq!(limits.get("claude-sonnet-4-5"), Some(&200000));
        assert_eq!(limits.get("claude-haiku"), Some(&100000));
        assert_eq!(limits.get("gpt-5"), Some(&400000));
    }

    #[test]
    fn skips_models_without_positive_context() {
        let body = json!({
            "providers": [
                { "id": "p", "models": {
                    "no-limit": { "limit": { "output": 1000 } },
                    "zero": { "limit": { "context": 0 } },
                    "ok": { "limit": { "context": 8000 } }
                } }
            ],
            "default": {}
        });
        let limits = parse_context_limits(&body);
        assert!(!limits.contains_key("no-limit"));
        assert!(!limits.contains_key("zero"));
        assert_eq!(limits.get("ok"), Some(&8000));
    }

    #[test]
    fn parse_handles_malformed_body() {
        assert!(parse_context_limits(&json!({})).is_empty());
        assert!(parse_context_limits(&json!({ "providers": "nope" })).is_empty());
        assert!(parse_context_limits(&json!([])).is_empty());
    }

    #[test]
    fn sums_prompt_and_generation_tokens() {
        let tokens = json!({
            "input": 1200,
            "output": 350,
            "reasoning": 80,
            "cache": { "read": 5000, "write": 100 }
        });
        // 1200 + 350 + 80 + 5000 + 100
        assert_eq!(context_tokens_used(&tokens), 6730);
    }

    #[test]
    fn missing_token_fields_count_as_zero() {
        assert_eq!(context_tokens_used(&json!({ "input": 42 })), 42);
        assert_eq!(context_tokens_used(&json!({})), 0);
        assert_eq!(context_tokens_used(&json!({ "cache": { "read": 9 } })), 9);
    }

    #[test]
    fn parse_provider_model_entries_connected_only() {
        // When connected is non-empty, only authenticated providers are shown.
        let body = json!({
            "connected": ["opencode"],
            "all": [
                {
                    "id": "opencode",
                    "models": { "zen-fast": { "name": "Zen Fast" } }
                },
                {
                    "id": "anthropic",
                    "models": { "claude-sonnet-4-5": { "name": "Claude Sonnet" } }
                }
            ]
        });
        let entries = parse_provider_model_entries(&body);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "opencode::zen-fast");
        assert_eq!(entries[0].label, "[opencode] Zen Fast");
    }

    #[test]
    fn parse_provider_model_entries_fallback_when_connected_empty() {
        // Older OpenCode builds that don't report connected — show all providers.
        let body = json!({
            "connected": [],
            "all": [
                {
                    "id": "anthropic",
                    "models": { "claude-sonnet-4-5": { "name": "Claude Sonnet" } }
                }
            ]
        });
        let entries = parse_provider_model_entries(&body);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "anthropic::claude-sonnet-4-5");
    }
}
