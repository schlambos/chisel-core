//! OpenCode-side MCP registration for the client's `LocalFsMcpServer`.
//!
//! Owns the wire calls to OpenCode's `mcp.add` / `mcp.connect` /
//! `mcp.disconnect` HTTP endpoints. Separated from `agent.rs` so the
//! OpenCode HTTP plumbing stays in one file and `agent.rs` is not pushed
//! past its line budget.
//!
//! ## Single-named-slot design
//!
//! OpenCode's MCP registry is **instance-global, not session-scoped**:
//! `MCP.tools()` returns every connected client's tools to every session's
//! LLM (`opencode/packages/opencode/src/mcp/index.ts:663-696`). When we
//! used to register per-conversation names like `aionui-local-fs-{id}`,
//! multiple conversations accumulated on the OpenCode instance and the
//! LLM saw N indistinguishable variants of every tool — picking the
//! wrong workspace at random. The user-visible symptom was intermittent
//! "stat failed: No such file or directory" mid-conversation.
//!
//! We collapse to a single stable slot named [`MCP_NAME`]. Each AionUI
//! conversation still runs its own [`LocalFsMcpServer`] on its own port
//! (workspace-scoped, bearer-protected). The OpenCode-side `aionui-local-fs`
//! slot is **owned by exactly one conversation at a time** and re-acquired
//! on prompt boundaries. The [`SlotOwner`] map plus a per-base-url
//! [`tokio::sync::Mutex`] keep ownership and the on-server registration in
//! lock-step.
//!
//! Trade-off: two conversations on the same OpenCode instance cannot send
//! prompts truly concurrently — the later prompt's slot acquisition tears
//! down the earlier one's MCP connection, surfacing as a clean error. This
//! matches realistic interactive usage (one turn at a time) and is
//! considerably less harmful than the silent wrong-workspace bug it
//! replaces.
//!
//! Reachability is *measured, not guessed*: the client MCP server binds
//! all interfaces, then for each candidate advertised IP (see
//! `reachability`) we register it, force OpenCode to dial back, and watch
//! our own server for the inbound hit. The first candidate OpenCode can
//! actually reach wins. This is robust to multi-homed hosts, VPNs, and
//! asymmetric routing without any hard-coded address.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::Duration;

use reqwest::header::AUTHORIZATION;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::local_fs_mcp::{ContactProbe, LocalFsMcpServer, ShellApprover, SnapshotHook};
use super::reachability::{Plan, current_route_ip, plan};

/// Stable MCP name registered with OpenCode for the client-side fs bridge.
/// One slot per OpenCode instance — see module docs for the why.
pub const MCP_NAME: &str = "aionui-local-fs";

/// OpenCode MCP tool-call timeout registered in [`register_mcp`].
pub const MCP_TOOL_TIMEOUT_MS: u64 = 300_000;
/// Shell approver must resolve before OpenCode's MCP client gives up.
pub const SHELL_APPROVAL_WAIT_MS: u64 = MCP_TOOL_TIMEOUT_MS - 60_000;

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

// ── Slot-ownership coordination ────────────────────────────────────────

/// Process-wide map: OpenCode `base_url` → conversation+port that currently
/// owns the [`MCP_NAME`] slot on that instance. The slot is a single
/// registration on OpenCode; multiple AionUI conversations against the
/// same instance must take turns owning it. The map lets a conversation
/// skip a re-registration HTTP call when nothing has changed since the
/// previous turn.
///
/// Pairs with [`REGISTER_MUTEXES`]: any read-modify-write of "is this
/// conversation the current owner" must hold the per-base-url async mutex
/// so we never see the map disagreeing with what OpenCode actually has
/// registered.
static SLOT_OWNERS: OnceLock<StdMutex<HashMap<String, SlotOwner>>> = OnceLock::new();

/// Per-base-url mutex serializing acquire/release of the [`MCP_NAME`] slot.
/// Without this, two conversations registering concurrently could both
/// succeed against OpenCode (last-write-wins on the server side) while
/// [`SLOT_OWNERS`] records whichever called [`claim_slot`] last — a
/// mismatch that would make a later "I already own it" fast-path silently
/// route tool calls to the wrong workspace.
static REGISTER_MUTEXES: OnceLock<StdMutex<HashMap<String, Arc<AsyncMutex<()>>>>> = OnceLock::new();

/// Process-wide map: `base_url` → which conversation currently has a turn
/// in flight against that OpenCode instance. While set, other conversations
/// asking to claim the [`MCP_NAME`] slot block on the [`TurnSignal::notify`]
/// until the active turn finishes. Without this gate, a second
/// conversation's claim re-points `aionui-local-fs` mid-turn, which makes
/// the first conversation's in-flight tool calls land on the *second*
/// conversation's MCP server — surfacing as both a "Not connected" error
/// for the first turn AND an approval prompt misattributed to the second
/// conversation's UI (the second server's `ShellApprover` is bound to its
/// own conversation_id).
static TURN_SIGNALS: OnceLock<StdMutex<HashMap<String, Arc<TurnSignal>>>> = OnceLock::new();

/// Process-wide set: which OpenCode `base_url`s have had their stale
/// `aionui-local-fs*` registrations swept already. Prevents repeating the
/// `GET /mcp` + per-entry disconnect work on every conversation. An chislcore
/// upgrade leaves legacy `aionui-local-fs-{conv_id}` and prior-process
/// `aionui-local-fs` entries on the LAN OpenCode; without this sweep the
/// model in a fresh session sees those stale tool names in its catalog and
/// may dial dead URLs, getting "Not connected" errors and going off-script.
static SWEPT_BASE_URLS: OnceLock<StdMutex<HashSet<String>>> = OnceLock::new();

/// Watermark for "no turn has gone past this point" — used to time out
/// blocked waiters if the active conversation crashes without emitting
/// a Finish event, so a future turn doesn't deadlock the slot forever.
const TURN_WAIT_TIMEOUT: Duration = Duration::from_secs(600);

#[derive(Clone, Debug, PartialEq, Eq)]
struct SlotOwner {
    conversation_id: String,
    port: u16,
}

/// Identifies the conversation currently driving an in-flight turn on a
/// given OpenCode `base_url`. The paired [`Notify`] wakes waiters when the
/// active turn finishes (`Finish` event observed, `kill()` called, or
/// timeout).
struct TurnSignal {
    /// Some(conversation_id) while a turn is in flight; None when free.
    active: AsyncMutex<Option<String>>,
    /// Woken on every `release_turn` so blocked waiters re-check `active`.
    notify: Notify,
}

fn slot_owners_map() -> &'static StdMutex<HashMap<String, SlotOwner>> {
    SLOT_OWNERS.get_or_init(|| StdMutex::new(HashMap::new()))
}

fn register_mutex_map() -> &'static StdMutex<HashMap<String, Arc<AsyncMutex<()>>>> {
    REGISTER_MUTEXES.get_or_init(|| StdMutex::new(HashMap::new()))
}

fn turn_signal_map() -> &'static StdMutex<HashMap<String, Arc<TurnSignal>>> {
    TURN_SIGNALS.get_or_init(|| StdMutex::new(HashMap::new()))
}

fn swept_set() -> &'static StdMutex<HashSet<String>> {
    SWEPT_BASE_URLS.get_or_init(|| StdMutex::new(HashSet::new()))
}

fn turn_signal_for(base_url: &str) -> Arc<TurnSignal> {
    let mut map = turn_signal_map().lock().expect("turn-signal map poisoned");
    map.entry(base_url.to_string())
        .or_insert_with(|| {
            Arc::new(TurnSignal {
                active: AsyncMutex::new(None),
                notify: Notify::new(),
            })
        })
        .clone()
}

/// Per-base-url tokio mutex. Cached across calls so all concurrent
/// claim/disconnect operations against the same OpenCode serialize on the
/// same handle.
fn register_mutex_for(base_url: &str) -> Arc<AsyncMutex<()>> {
    let mut map = register_mutex_map().lock().expect("register-mutex map poisoned");
    map.entry(base_url.to_string())
        .or_insert_with(|| Arc::new(AsyncMutex::new(())))
        .clone()
}

/// True iff `(conversation_id, port)` is the current owner of the slot on
/// `base_url`. Caller-visible fast-path: if this is true, the OpenCode
/// `mcp.add` HTTP call can be skipped for this turn.
pub fn owns_slot(base_url: &str, conversation_id: &str, port: u16) -> bool {
    let map = match slot_owners_map().lock() {
        Ok(m) => m,
        Err(_) => return false,
    };
    map.get(base_url)
        .is_some_and(|s| s.conversation_id == conversation_id && s.port == port)
}

fn claim_slot(base_url: &str, conversation_id: &str, port: u16) {
    if let Ok(mut map) = slot_owners_map().lock() {
        map.insert(
            base_url.to_string(),
            SlotOwner {
                conversation_id: conversation_id.to_string(),
                port,
            },
        );
    }
}

/// Release the slot iff `conversation_id` is the current owner. Returns
/// true on release. If another conversation has taken over, the slot is
/// left untouched — disconnecting it would break their session.
fn release_slot_if_owner(base_url: &str, conversation_id: &str) -> bool {
    let mut map = match slot_owners_map().lock() {
        Ok(m) => m,
        Err(_) => return false,
    };
    let Some(owner) = map.get(base_url) else {
        return false;
    };
    if owner.conversation_id != conversation_id {
        return false;
    }
    map.remove(base_url);
    true
}

// ── Turn serialization ──────────────────────────────────────────────────

/// Block until no other conversation has a turn in flight against
/// `base_url`, then mark this conversation as the active turn.
///
/// Idempotent for the same `(base_url, conversation_id)` — re-entrancy
/// (e.g. a follow-up `ensure_local_fs_mcp` mid-turn) does not deadlock.
///
/// A turn that hasn't released within [`TURN_WAIT_TIMEOUT`] is assumed
/// crashed; the next waiter takes over rather than deadlocking the slot
/// forever. Surfaces a `warn!` so a stuck turn is visible.
pub async fn acquire_turn(base_url: &str, conversation_id: &str) {
    let signal = turn_signal_for(base_url);
    let waited_for = loop {
        // Lock-take + decide pattern. Doing this under `active.lock()`
        // means a release that races our recheck still wakes us via the
        // `Notify` arm below; the lock prevents two waiters from both
        // observing `None` and both claiming the slot.
        let mut active = signal.active.lock().await;
        match active.as_deref() {
            None => {
                *active = Some(conversation_id.to_string());
                return;
            }
            Some(id) if id == conversation_id => {
                // Re-entrant call — same conversation, already active.
                return;
            }
            Some(other) => {
                let other = other.to_string();
                // Arm the notification BEFORE dropping the lock so a
                // release that fires between drop and `.await` is still
                // delivered to us.
                let notified = signal.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                drop(active);

                info!(
                    base_url,
                    waiting_conversation = conversation_id,
                    active_conversation = %other,
                    "waiting for in-flight turn to finish before claiming MCP slot"
                );

                match tokio::time::timeout(TURN_WAIT_TIMEOUT, &mut notified).await {
                    Ok(()) => continue,
                    Err(_) => break other,
                }
            }
        }
    };

    // Timed out — force-take the slot so this conversation can proceed.
    // The stuck conversation will discover it has lost the turn the next
    // time it tries to release.
    warn!(
        base_url,
        waiting_conversation = conversation_id,
        stuck_conversation = %waited_for,
        timeout_secs = TURN_WAIT_TIMEOUT.as_secs(),
        "in-flight turn did not release; taking the slot anyway"
    );
    let mut active = signal.active.lock().await;
    *active = Some(conversation_id.to_string());
}

/// Mark `conversation_id`'s turn as finished on `base_url` iff it is the
/// current active turn. No-op if another conversation has taken over (e.g.
/// after a [`acquire_turn`] timeout). Wakes any blocked waiters.
pub async fn release_turn(base_url: &str, conversation_id: &str) {
    let signal = turn_signal_for(base_url);
    let mut active = signal.active.lock().await;
    let is_owner = active.as_deref() == Some(conversation_id);
    if !is_owner {
        return;
    }
    *active = None;
    drop(active);
    signal.notify.notify_one();
}

// ── Stale-registration sweep ────────────────────────────────────────────

/// Disconnect every `aionui-local-fs*` MCP currently registered on the
/// given OpenCode instance. Runs once per `base_url` per AionCore process
/// (idempotent thereafter).
///
/// **Why this exists:** an chislcore restart or upgrade leaves the
/// previous process's MCP registrations live on the LAN OpenCode. The
/// model in a fresh session sees those stale tool names — both
/// legacy per-conversation `aionui-local-fs-{conv_id}_*` and any prior
/// `aionui-local-fs_*` pointing at a dead AionCore port — and may try to
/// use them, returning "Not connected" errors and confusing the agent
/// loop.
pub async fn sweep_stale_registrations(http_client: &reqwest::Client, base_url: &str, auth_header: Option<&str>) {
    {
        let mut swept = match swept_set().lock() {
            Ok(s) => s,
            Err(_) => return,
        };
        if !swept.insert(base_url.to_string()) {
            return; // already swept this base_url this process
        }
    }

    let url = format!("{base_url}/mcp");
    let mut req = http_client.get(&url).timeout(Duration::from_secs(10));
    if let Some(h) = auth_header {
        req = req.header(AUTHORIZATION, h);
    }
    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => {
            warn!(base_url, error = %e, "stale-MCP sweep skipped — could not query OpenCode /mcp");
            return;
        }
    };
    if !resp.status().is_success() {
        let status = resp.status();
        warn!(base_url, %status, "stale-MCP sweep skipped — non-success from /mcp");
        return;
    }
    let body: serde_json::Value = match resp.json().await {
        Ok(v) => v,
        Err(e) => {
            warn!(base_url, error = %e, "stale-MCP sweep skipped — could not parse /mcp body");
            return;
        }
    };

    let stale_names = stale_mcp_names_from_status(&body);
    if stale_names.is_empty() {
        debug!(base_url, "no stale aionui-local-fs registrations to sweep");
        return;
    }

    info!(
        base_url,
        count = stale_names.len(),
        names = ?stale_names,
        "sweeping stale aionui-local-fs registrations from OpenCode"
    );

    for name in stale_names {
        let mut req = http_client
            .post(format!("{base_url}/mcp/{name}/disconnect"))
            .timeout(Duration::from_secs(5));
        if let Some(h) = auth_header {
            req = req.header(AUTHORIZATION, h);
        }
        match req.send().await {
            Ok(resp) if resp.status().is_success() => {
                debug!(base_url, %name, "swept stale MCP registration");
            }
            Ok(resp) => {
                warn!(base_url, %name, status = %resp.status(), "stale MCP disconnect returned non-success");
            }
            Err(e) => {
                warn!(base_url, %name, error = %e, "stale MCP disconnect request failed");
            }
        }
    }
}

/// Extract every key from OpenCode's `mcp.status` payload whose name starts
/// with `aionui-local-fs`. Tolerant of unexpected shapes — returns empty if
/// the body doesn't look like an object.
fn stale_mcp_names_from_status(body: &serde_json::Value) -> Vec<String> {
    let Some(obj) = body.as_object() else {
        return Vec::new();
    };
    obj.keys()
        .filter(|k| k.starts_with("aionui-local-fs"))
        .cloned()
        .collect()
}

// ── Public API ─────────────────────────────────────────────────────────

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

impl RegistrationOutcome {
    fn is_success(self) -> bool {
        matches!(self, Self::Verified | Self::Unverified)
    }
}

/// Start the client MCP server bound to all interfaces and register it
/// with the remote OpenCode, selecting an advertised address OpenCode can
/// actually reach. On success the returned `LocalFsMcpServer` must be kept
/// alive for the duration of the OpenCode session and
/// `disconnect_from_opencode` should be called on teardown. A background
/// [`spawn_reachability_guardian`] should be started to re-register if the
/// network later changes.
#[allow(clippy::too_many_arguments)]
pub async fn start_and_register(
    http_client: &reqwest::Client,
    base_url: &str,
    auth_header: Option<&str>,
    conversation_id: &str,
    workspace_root: &str,
    approver: Option<Arc<dyn ShellApprover>>,
    elicitation: Option<Arc<dyn super::local_fs_mcp::ElicitationHandler>>,
    snapshot_hook: Option<SnapshotHook>,
) -> Result<LocalFsMcpServer, String> {
    // First-touch-per-process: clear any aionui-local-fs* leftovers from a
    // prior chislcore run before registering ours. Idempotent across
    // conversations on the same OpenCode.
    sweep_stale_registrations(http_client, base_url, auth_header).await;

    let plan = plan(base_url);
    let bind = plan.bind_addr();

    let token = Uuid::new_v4().to_string();
    let server = LocalFsMcpServer::start(
        workspace_root.into(),
        bind,
        token.clone(),
        approver,
        elicitation,
        snapshot_hook,
    )
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
    if acquire_slot_inner(&ctx, plan).await.is_success() {
        Ok(server)
    } else {
        // OpenCode rejected every registration; drop the server (its port
        // frees on Drop) so the caller treats fs tools as unavailable.
        Err("OpenCode rejected local fs MCP registration for every candidate".to_string())
    }
}

/// Re-register the [`MCP_NAME`] slot to point at an already-running
/// `LocalFsMcpServer` and take ownership for `conversation_id`. Used when
/// a conversation's local server is still alive but another conversation
/// has since claimed the OpenCode slot — typical when the user switches
/// tabs between turns and the previous tab's prompt re-pointed the slot.
///
/// Idempotent: no-op when this conversation+port already owns the slot.
pub async fn ensure_slot_owned(
    http_client: &reqwest::Client,
    base_url: &str,
    auth_header: Option<&str>,
    conversation_id: &str,
    port: u16,
    token: &str,
    probe: &ContactProbe,
) -> Result<(), String> {
    if owns_slot(base_url, conversation_id, port) {
        return Ok(());
    }
    let ctx = RegisterCtx {
        http_client,
        base_url,
        auth_header,
        conversation_id,
        port,
        token,
        probe,
    };
    if acquire_slot_inner(&ctx, plan(base_url)).await.is_success() {
        Ok(())
    } else {
        Err("OpenCode rejected local fs MCP re-registration".to_string())
    }
}

async fn acquire_slot_inner(ctx: &RegisterCtx<'_>, plan: Plan) -> RegistrationOutcome {
    // Serialize register-and-claim against any other conversation racing
    // for the same OpenCode instance. Without this lock, two conversations
    // could both succeed on the server side while [`SLOT_OWNERS`] only
    // records whichever called [`claim_slot`] last.
    let mutex = register_mutex_for(ctx.base_url);
    let _guard = mutex.lock().await;

    // Re-check ownership inside the lock — another task may have just
    // finished a claim for us, in which case we have nothing to do.
    if owns_slot(ctx.base_url, ctx.conversation_id, ctx.port) {
        return RegistrationOutcome::Verified;
    }

    let outcome = register_candidates(ctx, plan).await;
    if outcome.is_success() {
        claim_slot(ctx.base_url, ctx.conversation_id, ctx.port);
    }
    outcome
}

/// Register the MCP across the plan's candidates against an already-running
/// server (identified by `port`/`token`/`probe`). Tries each candidate in
/// order, keeping the first OpenCode actually dials back to; otherwise
/// registers the best guess. Reusable by both initial setup and the
/// guardian's re-registration after a network change.
async fn register_candidates(ctx: &RegisterCtx<'_>, plan: Plan) -> RegistrationOutcome {
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
            MCP_NAME,
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

        if verify_dial_back(ctx.http_client, ctx.base_url, ctx.auth_header, MCP_NAME, ctx.probe).await {
            info!(
                conversation_id = %ctx.conversation_id,
                mcp_name = MCP_NAME,
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
        force_disconnect_unlocked(ctx.http_client, ctx.base_url, ctx.auth_header).await;
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
        MCP_NAME,
        &fallback.public_url,
        ctx.token,
    )
    .await
    {
        warn!(
            conversation_id = %ctx.conversation_id,
            mcp_name = MCP_NAME,
            error = %e,
            "could not register any local fs MCP candidate with OpenCode"
        );
        return RegistrationOutcome::Failed;
    }
    warn!(
        conversation_id = %ctx.conversation_id,
        mcp_name = MCP_NAME,
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
            // Only re-register if we still own the slot — another
            // conversation may have taken over since we last registered,
            // in which case clobbering them on a network change would
            // break their in-flight turn.
            let still_owner = owns_slot(&base_url, &conversation_id, port);
            info!(
                conversation_id = %conversation_id,
                old_ip = ?last_ip,
                new_ip = ?now_ip,
                still_owner,
                "network change detected; re-evaluating local fs MCP reachability"
            );
            last_ip = now_ip;
            if !still_owner {
                continue;
            }
            let ctx = RegisterCtx {
                http_client: &http_client,
                base_url: &base_url,
                auth_header: auth_header.as_deref(),
                conversation_id: &conversation_id,
                port,
                token: &token,
                probe: &probe,
            };
            let outcome = acquire_slot_inner(&ctx, plan(&base_url)).await;
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
    // Typed body — `OpencodeMcpAddRequest` pins the wire shape (transport
    // "remote", bearer header, generous timeout for approval-blocked shells).
    let payload = super::opencode_payloads::OpencodeMcpAddRequest {
        name: name.to_string(),
        config: super::opencode_payloads::OpencodeMcpRemoteConfig::remote(url, token, MCP_TOOL_TIMEOUT_MS),
    };

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

/// Tell OpenCode to drop the [`MCP_NAME`] registration **iff** the calling
/// conversation currently owns the slot. Other conversations' ownership is
/// preserved — disconnecting a slot another conversation just claimed
/// would break their session. Failures from OpenCode are logged but never
/// propagated; teardown must be robust to a remote that's already gone.
pub async fn disconnect_from_opencode(
    http_client: &reqwest::Client,
    base_url: &str,
    auth_header: Option<&str>,
    conversation_id: &str,
) {
    // Serialize against any concurrent acquire — we must not race a
    // disconnect against another conversation's claim.
    let mutex = register_mutex_for(base_url);
    let _guard = mutex.lock().await;

    if !release_slot_if_owner(base_url, conversation_id) {
        debug!(
            conversation_id,
            mcp_name = MCP_NAME,
            "skipping OpenCode mcp.disconnect — slot not owned by this conversation"
        );
        return;
    }

    force_disconnect_unlocked(http_client, base_url, auth_header).await;
}

/// Issue the OpenCode disconnect HTTP call directly, without touching the
/// slot-owner map or holding the per-base-url mutex. Used both by
/// [`disconnect_from_opencode`] (which has already validated ownership and
/// taken the lock) and by candidate cleanup in [`register_candidates`]
/// (where the lock is held one level up).
async fn force_disconnect_unlocked(http_client: &reqwest::Client, base_url: &str, auth_header: Option<&str>) {
    let mut req = http_client
        .post(format!("{base_url}/mcp/{MCP_NAME}/disconnect"))
        .timeout(Duration::from_secs(10));
    if let Some(h) = auth_header {
        req = req.header(AUTHORIZATION, h);
    }
    match req.send().await {
        Ok(resp) if resp.status().is_success() => {
            info!(mcp_name = MCP_NAME, "disconnected MCP from OpenCode");
        }
        Ok(resp) => {
            warn!(
                mcp_name = MCP_NAME,
                status = %resp.status(),
                "OpenCode mcp.disconnect returned non-success"
            );
        }
        Err(e) => {
            warn!(
                mcp_name = MCP_NAME,
                error = %e,
                "OpenCode mcp.disconnect request failed"
            );
        }
    }
}

/// Test-only visibility into slot ownership (integration tests are separate binaries).
#[doc(hidden)]
pub fn owns_slot_for_test(base_url: &str, conversation_id: &str, port: u16) -> bool {
    owns_slot(base_url, conversation_id, port)
}

#[cfg(test)]
mod tests {
    use super::*;

    // The `SLOT_OWNERS`, `TURN_SIGNALS`, etc. maps are process-wide
    // statics. Tests share that state, so each test below must use a
    // base_url unique to itself — never a shared one, and never call any
    // helper that clears the whole map (that would race with other
    // tests running in parallel). The test names embed the test name
    // into the URL to make uniqueness mechanical.

    #[test]
    fn mcp_name_is_stable_constant() {
        assert_eq!(MCP_NAME, "aionui-local-fs");
    }

    #[test]
    fn claim_and_owns_round_trip() {
        let url = "http://test-claim-owns:4096";
        assert!(!owns_slot(url, "conv-a", 5000));
        claim_slot(url, "conv-a", 5000);
        assert!(owns_slot(url, "conv-a", 5000));
        // A different conversation does not own it.
        assert!(!owns_slot(url, "conv-b", 5000));
        // Wrong port for the same conversation is also not ownership —
        // the port disambiguates conversation lifecycle (restart, etc.).
        assert!(!owns_slot(url, "conv-a", 5001));
    }

    #[test]
    fn claim_overwrites_previous_owner() {
        let url = "http://test-overwrite:4096";
        claim_slot(url, "conv-a", 5000);
        claim_slot(url, "conv-b", 5001);
        assert!(!owns_slot(url, "conv-a", 5000));
        assert!(owns_slot(url, "conv-b", 5001));
    }

    #[test]
    fn release_only_when_caller_is_owner() {
        let url = "http://test-release:4096";
        claim_slot(url, "conv-a", 5000);

        // Wrong conversation cannot release.
        assert!(!release_slot_if_owner(url, "conv-b"));
        assert!(owns_slot(url, "conv-a", 5000));

        // Owner can release.
        assert!(release_slot_if_owner(url, "conv-a"));
        assert!(!owns_slot(url, "conv-a", 5000));

        // Release on an empty slot returns false (idempotent teardown).
        assert!(!release_slot_if_owner(url, "conv-a"));
    }

    #[test]
    fn ownership_is_scoped_per_base_url() {
        let url_a = "http://test-base-a:4096";
        let url_b = "http://test-base-b:4096";
        claim_slot(url_a, "conv-1", 5000);
        claim_slot(url_b, "conv-2", 5000);
        assert!(owns_slot(url_a, "conv-1", 5000));
        assert!(owns_slot(url_b, "conv-2", 5000));
        // Cross-instance: conv-1 does not own the slot on url_b.
        assert!(!owns_slot(url_b, "conv-1", 5000));
    }

    #[tokio::test]
    async fn register_mutex_is_cached_per_base_url() {
        let url = "http://test-mutex-cache:4096";
        let m1 = register_mutex_for(url);
        let m2 = register_mutex_for(url);
        // Same logical mutex (Arc points to same allocation).
        assert!(Arc::ptr_eq(&m1, &m2));

        let m3 = register_mutex_for("http://different:4096");
        assert!(!Arc::ptr_eq(&m1, &m3));
    }

    #[test]
    fn stale_mcp_names_picks_aionui_local_fs_keys() {
        let body = serde_json::json!({
            "aionui-local-fs": {"status": "connected"},
            "aionui-local-fs-99d13a6b": {"status": "connected"},
            "aionui-local-fs-old": {"status": "failed", "error": "..."},
            "other-mcp": {"status": "connected"},
            "github": {"status": "connected"},
        });
        let mut names = stale_mcp_names_from_status(&body);
        names.sort();
        assert_eq!(
            names,
            vec![
                "aionui-local-fs".to_string(),
                "aionui-local-fs-99d13a6b".to_string(),
                "aionui-local-fs-old".to_string(),
            ]
        );
    }

    #[test]
    fn stale_mcp_names_tolerates_non_object_body() {
        assert!(stale_mcp_names_from_status(&serde_json::json!(null)).is_empty());
        assert!(stale_mcp_names_from_status(&serde_json::json!([])).is_empty());
        assert!(stale_mcp_names_from_status(&serde_json::json!("string")).is_empty());
    }

    #[tokio::test]
    async fn acquire_turn_then_release_lets_other_through() {
        // Use a unique base_url so this test doesn't share state with the
        // process-wide TURN_SIGNALS map.
        let base = "http://test-acquire-release:4096";

        // First conversation acquires the slot.
        acquire_turn(base, "conv-a").await;

        // Second conversation's acquire must block until release.
        let task = tokio::spawn(async move {
            acquire_turn(base, "conv-b").await;
        });

        // Give the spawned task a chance to start waiting. With no timeout
        // helper this is a small heuristic sleep — if it flakes, raise it.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!task.is_finished(), "conv-b should still be waiting");

        // Releasing the active turn must wake conv-b.
        release_turn(base, "conv-a").await;
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("conv-b should have acquired within 1s")
            .expect("conv-b task panicked");

        // Cleanup so subsequent tests aren't blocked by lingering ownership.
        release_turn(base, "conv-b").await;
    }

    #[tokio::test]
    async fn acquire_turn_is_reentrant_for_same_conversation() {
        let base = "http://test-reentrant:4096";
        acquire_turn(base, "conv-a").await;
        // A second acquire from the same conversation must return promptly.
        tokio::time::timeout(Duration::from_millis(100), acquire_turn(base, "conv-a"))
            .await
            .expect("re-entrant acquire should be immediate");
        release_turn(base, "conv-a").await;
    }

    #[tokio::test]
    async fn release_by_non_owner_is_noop() {
        let base = "http://test-noop-release:4096";
        acquire_turn(base, "conv-a").await;
        // Releasing under a different conversation must not free the slot.
        release_turn(base, "conv-b").await;
        let signal = turn_signal_for(base);
        let active = signal.active.lock().await;
        assert_eq!(active.as_deref(), Some("conv-a"));
        drop(active);
        release_turn(base, "conv-a").await;
    }
}
