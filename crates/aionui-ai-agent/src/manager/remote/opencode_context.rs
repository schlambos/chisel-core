//! OpenCode request-context query parameters (`directory`, `workspace`).
//!
//! V1 routes accept `?directory=` / `?workspace=`; V2 routes accept
//! `?location[directory]=` / `?location[workspace]=`. In server-tools mode
//! Chisl passes the conversation workspace so the remote OpenCode instance
//! scopes file/VCS/config operations to the correct project tree.

use super::agent::{RemoteAgentConfig, is_server_tool_host};

/// Percent-encode a path for use in a query string value.
pub fn encode_query_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Workspace directory to advertise to OpenCode when in server-tools mode.
pub fn server_directory(cfg: &RemoteAgentConfig, workspace: &str) -> Option<String> {
    if !is_server_tool_host(cfg) {
        return None;
    }
    let ws = workspace.trim();
    if ws.is_empty() {
        return None;
    }
    Some(ws.to_string())
}

/// Append V1 `directory` query param when server-tools mode is active.
pub fn append_v1_directory(url: &str, cfg: &RemoteAgentConfig, workspace: &str) -> String {
    append_v1_directory_value(url, server_directory(cfg, workspace).as_deref())
}

/// Append a pre-computed V1 `directory` query param. No-op when `directory`
/// is `None` or empty. Callers that resolve the directory once up-front
/// (e.g. before moving into a detached `tokio::spawn` where `&self` is
/// unavailable) use this instead of re-deriving it from config — see the
/// permission/question reply paths in `agent.rs`, which MUST be scoped to
/// the same per-directory OpenCode app instance as the session that raised
/// them or the reply 404s with `PermissionNotFoundError`.
pub fn append_v1_directory_value(url: &str, directory: Option<&str>) -> String {
    match directory {
        Some(dir) if !dir.trim().is_empty() => {
            let sep = if url.contains('?') { '&' } else { '?' };
            format!("{url}{sep}directory={}", encode_query_value(dir))
        }
        _ => url.to_string(),
    }
}

/// Append V2 `location[directory]` query param when server-tools mode is active.
pub fn append_v2_location(url: &str, cfg: &RemoteAgentConfig, workspace: &str) -> String {
    let Some(dir) = server_directory(cfg, workspace) else {
        return url.to_string();
    };
    let sep = if url.contains('?') { '&' } else { '?' };
    format!("{url}{sep}location[directory]={}", encode_query_value(&dir))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_spaces_and_special_chars() {
        assert_eq!(encode_query_value("/home/user/my project"), "/home/user/my%20project");
    }

    #[test]
    fn appends_directory_in_server_mode() {
        let cfg = RemoteAgentConfig {
            remote_agent_id: "ra".into(),
            protocol: "opencode".into(),
            url: "http://127.0.0.1:4096".into(),
            auth_type: "none".into(),
            auth_token: None,
            allow_insecure: false,
            tool_host: "server".into(),
        };
        let url = append_v1_directory("http://h/session", &cfg, "/repo/app");
        assert!(url.contains("directory=/repo/app") || url.contains("directory=%2Frepo%2Fapp"));
    }

    #[test]
    fn skips_directory_in_local_mode() {
        let cfg = RemoteAgentConfig {
            remote_agent_id: "ra".into(),
            protocol: "opencode".into(),
            url: "http://127.0.0.1:4096".into(),
            auth_type: "none".into(),
            auth_token: None,
            allow_insecure: false,
            tool_host: "local".into(),
        };
        assert_eq!(
            append_v1_directory("http://h/session", &cfg, "/repo/app"),
            "http://h/session"
        );
    }
}
