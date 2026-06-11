//! Local OpenCode process management.
//!
//! Spawns, monitors, and stops local `opencode serve` instances
//! with auto-injected plugin configuration via
//! `OPENCODE_CONFIG_CONTENT`.
//!
//! Public surface:
//!
//! - [`manager::LocalOpenCodeManager`] — the core manager; use
//!   [`manager::global`] to get the process-wide singleton.
//! - [`manager::kill_all_local_opencode`] — graceful-shutdown
//!   hook.
//! - [`config::generate_opencode_config`] — pure helper that
//!   builds the `OPENCODE_CONFIG_CONTENT` JSON.
//! - [`instance::OpenCodeInstance`] — single-instance lifecycle
//!   (useful for tests and for callers that want to manage
//!   their own process instead of going through the singleton).

pub mod config;
pub mod instance;
pub mod manager;
mod plugin_channel;

pub use manager::{LocalOpenCodeManager, global, kill_all_local_opencode};
