//! OpenCode-side MCP registration for the client's `LocalFsMcpServer`.
//!
//! Owns the wire calls to OpenCode's `mcp.add` / `mcp.connect` /
//! `mcp.disconnect` HTTP endpoints. Separated from `agent.rs` so the
//! OpenCode HTTP plumbing stays in one file and `agent.rs` is not pushed
//! past its line budget.
//!
//! Reachability is *measured, not guessed*: the client MCP server binds
//! all interfaces, then for each candidate advertised IP (see
//! `reachability`) we register it, force OpenCode to dial back, and watch
//! our own server for the inbound hit. The first candidate OpenCode can
//! actually reach wins. This is robust to multi-homed hosts, VPNs, and
//! asymmetric routing without any hard-coded address.

use std::sync::Arc;
use std::time::Duration;

use reqwest::header::AUTHORIZATION;
use serde_json::json;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::local_fs_mcp::{ContactProbe, LocalFsMcpServer, ShellApprover};
use super::reachability::{Plan, current_route_ip, plan};

/// Name used to register the client MCP with OpenCode. Per-conversation
/// so multiple sessions on one server don't collide.
pub fn mcp_name_for(conversation_id: &str) -> String {
    format!("aionui-local-fs-{conversation_id}")
}

/// Env var the user can set to override the auto-resolved LAN URL —
/// useful for containerized setups, multi-homed hosts, or when the user
/// has a pre-existing tunnel they prefer to use.
pub const PUBLIC_URL_ENV: &str = "AIONUI_LOCAL_FS_MCP_PUBLIC_URL";

/// How long to wait for OpenCode to dial back on a single candidate before
/// moving on. A LAN/mesh dial-back is sub-second; this only bites when a
/// candidate IP is genuinely unreachable. Overridable via
/// `AIONUI_LOCAL_FS_MCP_VERIFY_MS` (used by tests to keep the loop fast).
const DEFAULT_VERIFY_MS: u64 = 3000;

/// Env var to override the per-candidate verification timeout, in
/// milliseconds.
const VERIFY_MS_ENV: &str = "AIONUI_LOCAL_FS_MCP_VERIFY_MS";

fn verify_timeout() -> Duration {
    std::env::var(VERIFY_MS_ENV)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_millis(DEFAULT_VERIFY_MS))
}

/// How often the reachability guardian checks for a network change that
/// would invalidate the advertised URL.
const GUARDIAN_INTERVAL: Duration = Duration::from_secs(15);

/// Borrowed bundle identifying which already-running MCP server to
/// register and how to reach OpenCode. Shared by initial setup and the
/// guardian's re-registration.
struct RegisterCtx<'a> {
    http_client: &'a reqwest::Client,
    base_url: &'a str,
    auth_header: Option<&'a str>,
    conversation_id: &'a str,
    /// OS-assigned port the server is bound to (stable across the session).
    port: u16,
    /// Bearer token the server expects.
    token: &'a str,
    /// Reachability signal for verifying OpenCode dials back.
    probe: &'a ContactProbe,
}

/// Result of registering the MCP across the candidate list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RegistrationOutcome {
    /// A candidate was registered and OpenCode dialed back — proven good.
    Verified,
    /// No candidate verified, but a best-guess URL is registered (OpenCode
    /// may still reach it; verification can yield false negatives).
    Unverified,
    /// Not even the best-guess registration was accepted by OpenCode.
    Failed,
}

/// Start the client MCP server bound to all interfaces and register it
/// with the remote OpenCode, selecting an advertised address OpenCode can
/// actually reach. On success the returned `LocalFsMcpServer` must be kept
/// alive for the duration of the OpenCode session and
/// `disconnect_from_opencode` should be called on teardown. A background
/// [`spawn_reachability_guardian`] should be started to re-register if the
/// network later changes.
pub async fn start_and_register(
    http_client: &reqwest::Client,
    base_url: &str,
    auth_header: Option<&str>,
    conversation_id: &str,
    workspace_root: &str,
    approver: Option<Arc<dyn ShellApprover>>,
) -> Result<LocalFsMcpServer, String> {
    let plan = plan(base_url);
    let bind = plan.bind_addr();

    let token = Uuid::new_v4().to_string();
    let server = LocalFsMcpServer::start(workspace_root.into(), bind, token.clone(), approver)
        .await
        .map_err(|e| format!("failed to start local fs MCP server: {e}"))?;
    let probe = server.contact_probe();

    let ctx = RegisterCtx {
        http_client,
        base_url,
        auth_header,
        conversation_id,
        port: server.bind_addr().port(),
        token: &token,
        probe: &probe,
    };
    match register_candidates(&ctx, plan).await {
        // Server is up and at least a best-guess URL is registered — keep it.
        RegistrationOutcome::Verified | RegistrationOutcome::Unverified => Ok(server),
        // OpenCode rejected every registration; drop the server (its port
        // frees on Drop) so the caller treats fs tools as unavailable.
        RegistrationOutcome::Failed => {
            Err("OpenCode rejected local fs MCP registration for every candidate".to_string())
        }
    }
}

/// Register the MCP across the plan's candidates against an already-running
/// server (identified by `port`/`token`/`probe`). Tries each candidate in
/// order, keeping the first OpenCode actually dials back to; otherwise
/// registers the best guess. Reusable by both initial setup and the
/// guardian's re-registration after a network change.
async fn register_candidates(ctx: &RegisterCtx<'_>, plan: Plan) -> RegistrationOutcome {
    let name = mcp_name_for(ctx.conversation_id);
    let candidates = plan.reachables(ctx.port);
    if candidates.is_empty() {
        warn!(conversation_id = %ctx.conversation_id, "no reachability candidates for local fs MCP");
        return RegistrationOutcome::Failed;
    }
    let candidate_count = candidates.len();

    for (idx, cand) in candidates.iter().enumerate() {
        ctx.probe.reset();

        if let Err(e) = register_mcp(
            ctx.http_client,
            ctx.base_url,
            ctx.auth_header,
            &name,
            &cand.public_url,
            ctx.token,
        )
        .await
        {
            warn!(
                candidate = %cand.public_url,
                provider = cand.provider,
                attempt = idx + 1,
                candidate_count,
                error = %e,
                "local fs MCP registration failed for candidate; trying next"
            );
            continue;
        }

        if verify_dial_back(ctx.http_client, ctx.base_url, ctx.auth_header, &name, ctx.probe).await {
            info!(
                conversation_id = %ctx.conversation_id,
                mcp_name = %name,
                public_url = %cand.public_url,
                provider = cand.provider,
                attempt = idx + 1,
                candidate_count,
                "verified local fs MCP reachable from OpenCode"
            );
            return RegistrationOutcome::Verified;
        }

        warn!(
            conversation_id = %ctx.conversation_id,
            public_url = %cand.public_url,
            provider = cand.provider,
            attempt = idx + 1,
            candidate_count,
            "OpenCode did not dial back on this candidate; trying next"
        );
        // Clean the failed registration before re-registering the name.
        disconnect_from_opencode(ctx.http_client, ctx.base_url, ctx.auth_header, ctx.conversation_id).await;
    }

    // Nothing verified. Register the best guess (first candidate) anyway so
    // the agent still functions if verification was a false negative — but
    // make the degraded state loud and actionable.
    let fallback = &candidates[0];
    ctx.probe.reset();
    if let Err(e) = register_mcp(
        ctx.http_client,
        ctx.base_url,
        ctx.auth_header,
        &name,
        &fallback.public_url,
        ctx.token,
    )
    .await
    {
        warn!(
            conversation_id = %ctx.conversation_id,
            mcp_name = %name,
            error = %e,
            "could not register any local fs MCP candidate with OpenCode"
        );
        return RegistrationOutcome::Failed;
    }
    warn!(
        conversation_id = %ctx.conversation_id,
        mcp_name = %name,
        public_url = %fallback.public_url,
        provider = fallback.provider,
        candidate_count,
        "could not verify any reachability candidate; registered best guess UNVERIFIED — \
         remote file tools may fail. If this persists, set {PUBLIC_URL_ENV} to a URL OpenCode can reach."
    );
    RegistrationOutcome::Unverified
}

/// Spawn a background task that re-selects and re-registers the MCP
/// reachability whenever the local route to OpenCode changes (VPN toggle,
/// DHCP renewal, Wi-Fi handoff) — the advertised URL would otherwise go
/// stale with no recovery until the conversation is reopened. The server
/// itself keeps running on the same port (it binds all interfaces); only
/// the advertised address handed to OpenCode is refreshed.
///
/// The returned handle should be `abort`ed on teardown.
pub fn spawn_reachability_guardian(
    http_client: reqwest::Client,
    base_url: String,
    auth_header: Option<String>,
    conversation_id: String,
    port: u16,
    token: String,
    probe: ContactProbe,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_ip = current_route_ip(&base_url);
        loop {
            tokio::time::sleep(GUARDIAN_INTERVAL).await;
            let now_ip = current_route_ip(&base_url);
            if now_ip == last_ip {
                continue;
            }
            info!(
                conversation_id = %conversation_id,
                old_ip = ?last_ip,
                new_ip = ?now_ip,
                "network change detected; re-registering local fs MCP reachability"
            );
            let ctx = RegisterCtx {
                http_client: &http_client,
                base_url: &base_url,
                auth_header: auth_header.as_deref(),
                conversation_id: &conversation_id,
                port,
                token: &token,
                probe: &probe,
            };
            let outcome = register_candidates(&ctx, plan(&base_url)).await;
            // Advance the watermark regardless of outcome so we don't
            // re-register every tick; the next genuine change re-fires.
            last_ip = now_ip;
            if outcome == RegistrationOutcome::Failed {
                warn!(
                    conversation_id = %conversation_id,
                    "local fs MCP re-registration after network change failed; will retry on next change"
                );
            }
        }
    })
}

/// Force OpenCode to dial the just-registered MCP now (rather than lazily
/// on first tool use) and watch our own server for the inbound request.
/// Returns true once contacted within the verification window.
async fn verify_dial_back(
    http_client: &reqwest::Client,
    base_url: &str,
    auth_header: Option<&str>,
    name: &str,
    probe: &ContactProbe,
) -> bool {
    let timeout = verify_timeout();
    let connect = force_connect(http_client, base_url, auth_header, name, timeout);
    let wait = probe.wait_for_first_contact(timeout);
    tokio::pin!(connect, wait);
    // Return as soon as the server is contacted, without blocking on the
    // connect response (which may sit open while OpenCode dials a dead IP).
    tokio::select! {
        biased;
        contacted = &mut wait => contacted,
        _ = &mut connect => wait.await,
    }
}

/// POST OpenCode's `mcp.add` to register a remote MCP at `url`.
async fn register_mcp(
    http_client: &reqwest::Client,
    base_url: &str,
    auth_header: Option<&str>,
    name: &str,
    url: &str,
    token: &str,
) -> Result<(), String> {
    let payload = json!({
        "name": name,
        "config": {
            // OpenCode's "remote" transport (HTTP/SSE). Kept as-is because
            // it is known-good against the deployed server version; a
            // future switch to streamable "http" should be gated on a
            // capability check.
            "type": "remote",
            "url": url,
            "enabled": true,
            "oauth": false,
            "headers": {
                "Authorization": format!("Bearer {token}"),
            },
            // Generous so a `run_shell` call isn't abandoned while it waits
            // for the user to approve the command (the approval blocks the
            // MCP request). The fast filesystem tools return well within this
            // either way, so the larger ceiling costs them nothing.
            "timeout": 300000,
        },
    });

    let mut req = http_client
        .post(format!("{base_url}/mcp"))
        .json(&payload)
        .timeout(Duration::from_secs(30));
    if let Some(h) = auth_header {
        req = req.header(AUTHORIZATION, h);
    }

    let resp = req
        .send()
        .await
        .map_err(|e| format!("OpenCode mcp.add request failed: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("OpenCode mcp.add returned {status}: {body}"));
    }
    Ok(())
}

/// Best-effort: tell OpenCode to (re)connect the MCP now. Failures are
/// swallowed — `verify_dial_back` relies on the server's own contact
/// signal, not this call's response, and older servers may lack the
/// endpoint entirely.
async fn force_connect(
    http_client: &reqwest::Client,
    base_url: &str,
    auth_header: Option<&str>,
    name: &str,
    timeout: Duration,
) {
    let mut req = http_client
        .post(format!("{base_url}/mcp/{name}/connect"))
        .timeout(timeout);
    if let Some(h) = auth_header {
        req = req.header(AUTHORIZATION, h);
    }
    match req.send().await {
        Ok(resp) => debug!(mcp_name = %name, status = %resp.status(), "requested OpenCode MCP connect"),
        Err(e) => debug!(mcp_name = %name, error = %e, "OpenCode MCP connect request failed (non-fatal)"),
    }
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
