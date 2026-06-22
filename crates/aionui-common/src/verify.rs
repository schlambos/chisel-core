//! Configuration type for the verify-hook feature (T1 Unit 5).
//!
//! When the agent applies an edit-type tool, the verify hook reads
//! `.chisl/verify.json` from the workspace root and optionally runs the
//! configured shell command, emitting a `VerifyResult` stream event so
//! the frontend can display a pass/fail toast.

use serde::{Deserialize, Serialize};

/// Default wall-clock timeout for the verification command.
const DEFAULT_TIMEOUT_SECS: u64 = 120;

/// Default: verification runs automatically after every edit completion.
const DEFAULT_AUTO_RUN: bool = true;

fn default_verify_timeout_secs() -> u64 {
    DEFAULT_TIMEOUT_SECS
}

fn default_verify_auto_run() -> bool {
    DEFAULT_AUTO_RUN
}

/// Project-defined verification command, read from
/// `{workspace_root}/.chisl/verify.json`.
///
/// A missing file is normal (no verification). A present-but-malformed
/// file warrants a `warn!` log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyConfig {
    /// Shell command to run (e.g. `"cargo build"` or `"npm test"`).
    pub command: String,
    /// Wall-clock timeout in seconds. Defaults to 120.
    #[serde(default = "default_verify_timeout_secs")]
    pub timeout_secs: u64,
    /// Whether to run automatically after each edit completion. Defaults to true.
    #[serde(default = "default_verify_auto_run")]
    pub auto_run: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_minimal_applies_defaults() {
        let json = serde_json::json!({ "command": "cargo build" });
        let cfg: VerifyConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.command, "cargo build");
        assert_eq!(cfg.timeout_secs, 120);
        assert!(cfg.auto_run);
    }

    #[test]
    fn deserialize_explicit_overrides_defaults() {
        let json = serde_json::json!({
            "command": "npm test",
            "timeout_secs": 30,
            "auto_run": false
        });
        let cfg: VerifyConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.command, "npm test");
        assert_eq!(cfg.timeout_secs, 30);
        assert!(!cfg.auto_run);
    }
}
