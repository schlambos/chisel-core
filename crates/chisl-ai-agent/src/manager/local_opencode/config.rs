//! `OPENCODE_CONFIG_CONTENT` generation for auto-injecting the
//! Chisl plugin into locally spawned `opencode serve` instances.
//!
//! `opencode serve` honours an `OPENCODE_CONFIG_CONTENT`
//! environment variable whose value is a JSON document. When set,
//! the served child uses this document as its in-memory config
//! and skips reading the on-disk `opencode.json` / `~/.config/
//! opencode/...` file. That gives us a clean way to inject the
//! `@chisl/chisl-opencode-plugin` entry plus the dial-back
//! environment variables (`AIONCORE_URL`, `AIONCORE_TOKEN`).
//! `AIONCORE_URL` is the plugin webserver **base** URL (no
//! `/plugin` suffix); the plugin client appends paths itself.
//! without having to mutate the user's home directory.
//!
//! See the upstream OpenCode docs for `OPENCODE_CONFIG_CONTENT`
//! for the full schema; we only use the two fields we need.

use serde_json::json;

/// Generate the `OPENCODE_CONFIG_CONTENT` JSON value.
///
/// This is passed as an environment variable to `opencode serve`
/// and overrides the on-disk config to inject the Chisl plugin
/// with the correct AionCore dial-back URL and per-instance
/// plugin token.
///
/// The output is a JSON string suitable for the
/// `OPENCODE_CONFIG_CONTENT` env var. Serialization is infallible
/// for this statically-known shape; any panic is a programmer
/// error worth tripping on rather than swallowing.
pub fn generate_opencode_config(plugin_endpoint_url: &str, plugin_token: &str) -> String {
    let config = json!({
        "plugin": ["@chisl/chisl-opencode-plugin"],
        "env": {
            "AIONCORE_URL": plugin_endpoint_url,
            "AIONCORE_TOKEN": plugin_token,
        }
    });
    serde_json::to_string(&config).expect("config serialization is infallible")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn generates_valid_json_with_plugin_and_env() {
        let config = generate_opencode_config("http://127.0.0.1:3456", "test-token-abc");
        let parsed: Value = serde_json::from_str(&config).unwrap();
        assert_eq!(
            parsed["plugin"].as_array().unwrap()[0].as_str().unwrap(),
            "@chisl/chisl-opencode-plugin"
        );
        assert_eq!(parsed["env"]["AIONCORE_URL"].as_str().unwrap(), "http://127.0.0.1:3456");
        assert_eq!(parsed["env"]["AIONCORE_TOKEN"].as_str().unwrap(), "test-token-abc");
    }

    #[test]
    fn round_trips_with_https_endpoint() {
        let config = generate_opencode_config("https://example.com:4111/", "tok");
        let parsed: Value = serde_json::from_str(&config).unwrap();
        assert_eq!(
            parsed["env"]["AIONCORE_URL"].as_str().unwrap(),
            "https://example.com:4111/"
        );
    }
}
