//! Plugin listen port — fixed default for remote Docker installs, with
//! an env-var override so tests (and operators) can pick a different one.

use chisl_common::constants::DEFAULT_PLUGIN_PORT;

/// Env var to override the plugin webserver listen port. Operators set
/// this when the default `64921` is already in use (e.g. a dev
/// `chislcore` instance holding the port); tests use it to bind on an
/// ephemeral port so they don't fight whatever else is listening on
/// `DEFAULT_PLUGIN_PORT` on the host.
pub const PLUGIN_PORT_ENV: &str = "AIONUI_PLUGIN_PORT";

/// TCP port the plugin webserver binds. Reads [`PLUGIN_PORT_ENV`] first
/// and falls back to [`DEFAULT_PLUGIN_PORT`] (`64921`) when unset or
/// unparseable. Read at call time so a test that mutates the env var
/// sees the new value on the next resolve.
pub fn resolve_plugin_port() -> u16 {
    std::env::var(PLUGIN_PORT_ENV)
        .ok()
        .and_then(|s| s.trim().parse::<u16>().ok())
        .filter(|p| *p != 0)
        .unwrap_or(DEFAULT_PLUGIN_PORT)
}
