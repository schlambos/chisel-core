//! Plugin listen port — hardcoded for remote Docker installs.

use aionui_common::constants::DEFAULT_PLUGIN_PORT;

/// TCP port the plugin webserver binds (`0.0.0.0:64921`).
pub fn resolve_plugin_port() -> u16 {
    DEFAULT_PLUGIN_PORT
}
