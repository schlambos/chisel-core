//! OpenCode-side MCP registration for the client's `LocalFsMcpServer`.
//!
//! Owns the wire calls to OpenCode's `mcp.add` / `mcp.disconnect` HTTP
//! endpoints. Separated from `agent.rs` so the OpenCode HTTP plumbing
//! stays in one file and `agent.rs` is not pushed past its line budget.

use std::time::Duration;

use reqwest::header::AUTHORIZATION;
use serde_json::json;
use tracing::{info, warn};
use uuid::Uuid;

use super::local_fs_mcp::LocalFsMcpServer;
use super::reachability::{Resolution, resolve};

/// Name used to register the client MCP with OpenCode. Per-conversation
/// so multiple sessions on one server don't collide.
pub fn mcp_name_for(conversation_id: &str) -> String {
    format!("aionui-local-fs-{conversation_id}")
}

/// Env var the user can set to override the auto-resolved LAN URL —
/// useful for containerized setups, multi-homed hosts, or when the user
/// has a pre-existing tunnel they prefer to use.
pub const PUBLIC_URL_ENV: &str = "AIONUI_LOCAL_FS_MCP_PUBLIC_URL";

/// Start the client MCP server bound to the LAN-routable interface and
/// register it with the remote OpenCode. On success the returned
/// `LocalFsMcpServer` must be kept alive for the duration of the
/// OpenCode session and `disconnect_from_opencode` should be called on
/// teardown.
pub async fn start_and_register(
    http_client: &reqwest::Client,
    base_url: &str,
    auth_header: Option<&str>,
    conversation_id: &str,
    workspace_root: &str,
) -> Result<LocalFsMcpServer, String> {
    let resolution = resolve(base_url);
    let bind = resolution.bind_addr();

    let token = Uuid::new_v4().to_string();
    let server = LocalFsMcpServer::start(workspace_root.into(), bind, token.clone())
        .await
        .map_err(|e| format!("failed to start local fs MCP server: {e}"))?;

    // For env-overrides the user's URL is taken verbatim. For LAN/loopback
    // we splice in the OS-assigned port now that the server is bound.
    let reachable = match &resolution {
        Resolution::Override { .. } => resolution.clone().into_reachable(0),
        _ => resolution.clone().into_reachable(server.bind_addr().port()),
    };

    let name = mcp_name_for(conversation_id);
    let payload = json!({
        "name": name,
        "config": {
            "type": "remote",
            "url": reachable.public_url,
            "enabled": true,
            "oauth": false,
            "headers": {
                "Authorization": format!("Bearer {token}"),
            },
            "timeout": 30000,
        },
    });

    let mut req = http_client
        .post(format!("{base_url}/mcp"))
        .json(&payload)
        .timeout(Duration::from_secs(30));
    if let Some(h) = auth_header {
        req = req.header(AUTHORIZATION, h);
    }

    let resp = req.send().await.map_err(|e| {
        // Server is dropped here — port frees.
        format!("OpenCode mcp.add request failed: {e}")
    })?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("OpenCode mcp.add returned {status}: {body}"));
    }

    info!(
        conversation_id = %conversation_id,
        mcp_name = %name,
        public_url = %reachable.public_url,
        provider = reachable.provider,
        "registered local fs MCP with OpenCode"
    );

    Ok(server)
}

/// Best-effort: tell OpenCode to drop the MCP registration. Failures are
/// logged but never propagated — teardown should be robust to a remote
/// that's already gone.
pub async fn disconnect_from_opencode(
    http_client: &reqwest::Client,
    base_url: &str,
    auth_header: Option<&str>,
    conversation_id: &str,
) {
    let name = mcp_name_for(conversation_id);
    let mut req = http_client
        .post(format!("{base_url}/mcp/{name}/disconnect"))
        .timeout(Duration::from_secs(10));
    if let Some(h) = auth_header {
        req = req.header(AUTHORIZATION, h);
    }
    match req.send().await {
        Ok(resp) if resp.status().is_success() => {
            info!(conversation_id = %conversation_id, mcp_name = %name, "disconnected MCP from OpenCode");
        }
        Ok(resp) => {
            warn!(
                conversation_id = %conversation_id,
                mcp_name = %name,
                status = %resp.status(),
                "OpenCode mcp.disconnect returned non-success"
            );
        }
        Err(e) => {
            warn!(
                conversation_id = %conversation_id,
                mcp_name = %name,
                error = %e,
                "OpenCode mcp.disconnect request failed"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mcp_name_includes_conversation_id() {
        assert_eq!(mcp_name_for("abc"), "aionui-local-fs-abc");
    }
}
