//! Sync collaboration primitives (M20).
//!
//! The OpenCode server exposes a sync subsystem:
//! - `POST /sync/start` — start workspace sync loops
//! - `POST /sync/history` — get event log since last known sequence
//! - `POST /sync/replay` — validate and replay events
//! - `POST /sync/steal` — take over a session from another client
//!
//! Phase 1 uses `/sync/history` as a reconnection backstop: after SSE
//! reconnect, call it with the last known aggregate sequences to retrieve
//! any events missed during the gap.

use std::time::Duration;

use chisl_common::AppError;
use reqwest::header::AUTHORIZATION;
use serde_json::Value;

use super::opencode_payloads::{OpencodeSyncHistoryRequest, OpencodeSyncStealRequest};

/// A single sync event from `/sync/history`.
#[derive(Debug, Clone)]
pub struct SyncEvent {
    pub id: String,
    pub aggregate_id: String,
    pub seq: u64,
    pub event_type: String,
    pub data: Value,
}

/// Fetch sync events since the given aggregate sequences.
/// `since` maps `aggregate_id -> last_known_seq`; aggregates not listed
/// get their full history. Returns events with `seq > last_known_seq`.
pub async fn fetch_sync_history(
    http_client: &reqwest::Client,
    base_url: &str,
    auth_header: Option<&str>,
    since: &std::collections::HashMap<String, u64>,
) -> Result<Vec<SyncEvent>, AppError> {
    let url = format!("{base_url}/sync/history");
    let body = OpencodeSyncHistoryRequest(since.clone());

    let mut req = http_client.post(&url).json(&body).timeout(Duration::from_secs(15));
    if let Some(h) = auth_header {
        req = req.header(AUTHORIZATION, h);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode sync/history request failed: {e}")))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body_text = resp.text().await.unwrap_or_default();
        return Err(AppError::BadGateway(format!(
            "OpenCode sync/history returned {status}: {body_text}"
        )));
    }

    let events_val: Value = resp
        .json()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode sync/history response was not JSON: {e}")))?;

    parse_sync_events(&events_val)
}

fn parse_sync_events(val: &Value) -> Result<Vec<SyncEvent>, AppError> {
    let arr = val
        .as_array()
        .ok_or_else(|| AppError::BadGateway("OpenCode sync/history response was not an array".into()))?;
    let mut events = Vec::new();
    for item in arr {
        let id = item.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let aggregate_id = item
            .get("aggregate_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let seq = item.get("seq").and_then(|v| v.as_u64()).unwrap_or(0);
        let event_type = item.get("type").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let data = item.get("data").cloned().unwrap_or(Value::Null);
        events.push(SyncEvent {
            id,
            aggregate_id,
            seq,
            event_type,
            data,
        });
    }
    Ok(events)
}

/// Start sync loops for workspaces with active sessions.
/// Returns `true` if sync was started.
pub async fn start_sync(
    http_client: &reqwest::Client,
    base_url: &str,
    auth_header: Option<&str>,
) -> Result<bool, AppError> {
    let url = format!("{base_url}/sync/start");
    let mut req = http_client.post(&url).timeout(Duration::from_secs(10));
    if let Some(h) = auth_header {
        req = req.header(AUTHORIZATION, h);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode sync/start request failed: {e}")))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body_text = resp.text().await.unwrap_or_default();
        return Err(AppError::BadGateway(format!(
            "OpenCode sync/start returned {status}: {body_text}"
        )));
    }
    resp.json()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode sync/start response was not JSON: {e}")))
}

/// Steal a session into the current workspace.
/// Returns the session ID of the stolen session.
pub async fn steal_session(
    http_client: &reqwest::Client,
    base_url: &str,
    auth_header: Option<&str>,
    session_id: &str,
) -> Result<String, AppError> {
    let url = format!("{base_url}/sync/steal");
    let body = OpencodeSyncStealRequest {
        session_id: session_id.to_string(),
    };
    let mut req = http_client.post(&url).json(&body).timeout(Duration::from_secs(15));
    if let Some(h) = auth_header {
        req = req.header(AUTHORIZATION, h);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode sync/steal request failed: {e}")))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body_text = resp.text().await.unwrap_or_default();
        return Err(AppError::BadGateway(format!(
            "OpenCode sync/steal returned {status}: {body_text}"
        )));
    }
    let val: Value = resp
        .json()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode sync/steal response was not JSON: {e}")))?;
    val.get("sessionID")
        .and_then(|v| v.as_str())
        .map(String::from)
        .ok_or_else(|| AppError::BadGateway(format!("OpenCode sync/steal response missing sessionID: {val}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_sync_events() {
        let val = json!([
            {
                "id": "evt-1",
                "aggregate_id": "ses-abc",
                "seq": 5,
                "type": "session.updated",
                "data": { "title": "Hello" }
            },
            {
                "id": "evt-2",
                "aggregate_id": "ses-abc",
                "seq": 6,
                "type": "message.created",
                "data": { "text": "world" }
            }
        ]);
        let events = parse_sync_events(&val).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].id, "evt-1");
        assert_eq!(events[0].seq, 5);
        assert_eq!(events[0].event_type, "session.updated");
        assert_eq!(events[1].aggregate_id, "ses-abc");
    }

    #[test]
    fn parse_handles_empty_array() {
        let events = parse_sync_events(&json!([])).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn parse_rejects_non_array() {
        assert!(parse_sync_events(&json!({})).is_err());
        assert!(parse_sync_events(&json!("nope")).is_err());
    }
}
