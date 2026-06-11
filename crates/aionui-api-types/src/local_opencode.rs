//! Types for the local `opencode serve` process manager (Phase 4).
//!
//! These mirror the shape the AionUi renderer consumes when it
//! shows the "Local OpenCode" panel: start / stop / restart / list
//! endpoints plus a status enum that is small enough to embed in
//! WebSocket events.

use serde::{Deserialize, Serialize};

/// Request body for `POST /api/local-opencode/start`.
///
/// All fields are optional; the manager fills in defaults (a
/// friendly display name and the user's home directory) so the
/// renderer can post an empty body for the "quick start" button.
#[derive(Debug, Deserialize)]
pub struct StartLocalOpenCodeRequest {
    /// Optional display name (defaults to "Local OpenCode").
    #[serde(default)]
    pub name: Option<String>,
    /// Working directory for the OpenCode instance.
    /// Defaults to the user's home directory.
    #[serde(default)]
    pub working_dir: Option<String>,
}

/// Live status of a local OpenCode instance.
///
/// The renderer watches this enum to colour-code the instance
/// row (Starting → spinning, Running → green dot, Stopped → grey,
/// Crashed → red). Kept intentionally small so the wire payload
/// on the planned WebSocket `localOpenCode.statusChanged` event
/// stays a single byte + 1 char of text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalOpenCodeStatus {
    Starting,
    Running,
    Stopped,
    Crashed,
}

/// Response describing a local OpenCode instance.
///
/// Returned by the start, restart, and list endpoints. `port` is
/// `0` while the instance is still Starting (the renderer treats
/// `0` as "not yet known").
#[derive(Debug, Serialize)]
pub struct LocalOpenCodeInstance {
    /// Stable id (UUID v4) generated at start time.
    pub id: String,
    /// User-friendly display name.
    pub name: String,
    /// Port the spawned `opencode serve` is listening on.
    /// `0` when the port has not been captured yet (Starting).
    pub port: u16,
    /// Current lifecycle status.
    pub status: LocalOpenCodeStatus,
    /// OS process id, `None` when the child is no longer running.
    pub pid: Option<u32>,
    /// The remote-agent id this instance was registered as.
    ///
    /// The renderer can re-use this when it wants the OpenCode
    /// plugin to dial back to AionCore (the value is also the
    /// `AIONCORE_TOKEN`'s owner record).
    pub agent_id: String,
    /// Working directory the OpenCode process was spawned in.
    pub working_dir: String,
    /// Unix-epoch millisecond timestamp the instance was created.
    pub created_at: u64,
}

/// Response for `GET /api/local-opencode`.
///
/// Wraps the instance list so the renderer can use the standard
/// `ApiResponse::data` envelope without special-casing the empty
/// case.
#[derive(Debug, Serialize)]
pub struct LocalOpenCodeListResponse {
    pub instances: Vec<LocalOpenCodeInstance>,
}
