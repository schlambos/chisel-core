//! OpenCode slash-command discovery and template expansion.
//!
//! OpenCode advertises commands at `GET /command`; each entry carries a
//! `template` body (with `$1`/`$2`/`$ARGUMENTS` placeholders). OpenCode's
//! server does NOT intercept `/`-prefixed prompts — clients are expected
//! to expand the template before sending. This module owns both halves
//! (fetch + expand) so `agent.rs` doesn't grow further.

use std::sync::OnceLock;
use std::time::Duration;

use regex::Regex;
use reqwest::header::AUTHORIZATION;
use serde::Deserialize;
use serde_json::Value;
use tracing::{debug, warn};

use chisl_api_types::SlashCommandItem;

/// One entry from OpenCode's `GET /command` response. We keep the full
/// shape (not just name/description) because `template`, `agent`, and
/// `model` are needed at send time to expand `/name args` into the real
/// prompt OpenCode will forward to the LLM.
#[derive(Debug, Clone, Deserialize)]
pub struct OpenCodeCommand {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub template: Option<String>,
    #[serde(default)]
    pub agent: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub subtask: Option<bool>,
}

impl OpenCodeCommand {
    pub fn to_slash_item(&self) -> SlashCommandItem {
        SlashCommandItem {
            command: format!("/{}", self.name),
            description: self.description.clone().unwrap_or_default(),
        }
    }
}

/// Fetch the command catalog. Best-effort: any failure returns an empty
/// list so the menu stays empty rather than blocking the conversation.
pub async fn fetch(http_client: &reqwest::Client, base_url: &str, auth_header: Option<&str>) -> Vec<OpenCodeCommand> {
    let url = format!("{base_url}/command");
    let mut req = http_client.get(&url).timeout(Duration::from_secs(10));
    if let Some(h) = auth_header {
        req = req.header(AUTHORIZATION, h);
    }

    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "OpenCode GET /command failed");
            return Vec::new();
        }
    };
    if !resp.status().is_success() {
        warn!(status = %resp.status(), "OpenCode GET /command returned non-success");
        return Vec::new();
    }

    let body: Value = match resp.json().await {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, "OpenCode GET /command response was not JSON");
            return Vec::new();
        }
    };

    let arr = match body.as_array() {
        Some(a) => a,
        None => {
            warn!("OpenCode GET /command response was not an array");
            return Vec::new();
        }
    };

    arr.iter()
        .filter_map(|v| match serde_json::from_value::<OpenCodeCommand>(v.clone()) {
            Ok(c) if !c.name.is_empty() => Some(c),
            Ok(_) => None,
            Err(e) => {
                debug!(error = %e, "Skipping unparseable OpenCode command entry");
                None
            }
        })
        .collect()
}

/// Detect a slash invocation. Returns `(name, args)` where `args` is the
/// rest of the line (trimmed). `name` matches `[A-Za-z0-9_-]+` so it
/// can't accidentally swallow path-like leading tokens.
pub fn parse_invocation(content: &str) -> Option<(&str, &str)> {
    let rest = content.strip_prefix('/')?;
    let name_end = rest
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '-'))
        .unwrap_or(rest.len());
    if name_end == 0 {
        return None;
    }
    let name = &rest[..name_end];
    let args = rest[name_end..].trim();
    Some((name, args))
}

/// Substitute `$ARGUMENTS` and `$1`..`$9` against a positional argv
/// built from `args`. Tokens are whitespace-split (no shell quoting —
/// matches the OpenCode TUI's behavior of treating everything after
/// the command as a single argument string). Adjacent digits aren't
/// matched (`$99` stays literal); other `$<word>` placeholders pass
/// through so server-side expansion (if ever added) can still see them.
pub fn expand_template(template: &str, args: &str) -> String {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        // (?:ARGUMENTS) — full token; or a single 1..9 digit NOT followed by
        // another digit (negative lookahead is unavailable in Rust regex,
        // so we capture the digit and the trailing char, then re-emit
        // the trailing char in the replacement closure).
        Regex::new(r"\$(ARGUMENTS|[1-9])(\D|$)").expect("static regex")
    });

    let positional: Vec<&str> = args.split_whitespace().collect();
    re.replace_all(template, |caps: &regex::Captures<'_>| {
        let token = &caps[1];
        let trailing = caps.get(2).map(|m| m.as_str()).unwrap_or("");
        let replacement: String = if token == "ARGUMENTS" {
            args.to_string()
        } else {
            let idx = token.parse::<usize>().unwrap_or(0);
            positional.get(idx - 1).copied().unwrap_or("").to_string()
        };
        format!("{replacement}{trailing}")
    })
    .into_owned()
}

/// Run a slash command via `POST /session/{sessionID}/command`. The server
/// expands the command template; clients should prefer this over local
/// template expansion when the command is in the catalog.
pub async fn execute_server_command(
    http_client: &reqwest::Client,
    url: &str,
    auth_header: Option<&str>,
    command_line: &str,
    agent: Option<&str>,
    model: Option<&str>,
) -> Result<(), chisl_common::AppError> {
    use chisl_common::AppError;
    use reqwest::header::AUTHORIZATION;

    let body = super::opencode_payloads::OpencodeCommandRequest {
        command: command_line.to_string(),
        agent: agent.map(String::from),
        model: model.map(String::from),
    };
    let mut req = http_client.post(url).json(&body).timeout(Duration::from_secs(120));
    if let Some(h) = auth_header {
        req = req.header(AUTHORIZATION, h);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode POST /command failed: {e}")))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body_text = resp.text().await.unwrap_or_default();
        return Err(AppError::BadGateway(format!(
            "OpenCode POST /command returned {status}: {body_text}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_simple_invocation() {
        assert_eq!(parse_invocation("/init"), Some(("init", "")));
        assert_eq!(parse_invocation("/review src/main.rs"), Some(("review", "src/main.rs")));
        assert_eq!(parse_invocation("/foo  a  b "), Some(("foo", "a  b")));
    }

    #[test]
    fn rejects_non_slash_or_path_like() {
        assert_eq!(parse_invocation("hello"), None);
        assert_eq!(parse_invocation("/"), None);
        // Leading slash followed by non-name char is not a command.
        assert_eq!(parse_invocation("/ foo"), None);
        assert_eq!(parse_invocation("/path/to/file"), Some(("path", "/to/file")));
    }

    #[test]
    fn expand_substitutes_arguments_token() {
        assert_eq!(
            expand_template("Review $ARGUMENTS please", "src/foo.rs"),
            "Review src/foo.rs please"
        );
    }

    #[test]
    fn expand_substitutes_positional() {
        assert_eq!(
            expand_template("from $1 to $2", "src/a.rs src/b.rs"),
            "from src/a.rs to src/b.rs"
        );
    }

    #[test]
    fn expand_missing_positional_becomes_empty() {
        assert_eq!(expand_template("only $1 and $2", "alpha"), "only alpha and ");
    }

    #[test]
    fn expand_preserves_unknown_placeholders() {
        // $99 is out of our 1..=9 range; leave it alone so server-side
        // expansion (if ever added) can still see it.
        assert_eq!(expand_template("ten=$99", ""), "ten=$99");
    }

    #[test]
    fn deserialize_minimal_command() {
        let json = serde_json::json!({"name": "init"});
        let cmd: OpenCodeCommand = serde_json::from_value(json).unwrap();
        assert_eq!(cmd.name, "init");
        assert!(cmd.description.is_none());
        assert!(cmd.template.is_none());
    }

    #[test]
    fn deserialize_full_command() {
        let json = serde_json::json!({
            "name": "review",
            "description": "Review code",
            "template": "Please review $ARGUMENTS",
            "agent": "build",
            "model": "claude-sonnet-4",
            "source": "command",
            "subtask": true,
        });
        let cmd: OpenCodeCommand = serde_json::from_value(json).unwrap();
        assert_eq!(cmd.name, "review");
        assert_eq!(cmd.description.as_deref(), Some("Review code"));
        assert_eq!(cmd.template.as_deref(), Some("Please review $ARGUMENTS"));
        assert_eq!(cmd.agent.as_deref(), Some("build"));
        assert_eq!(cmd.subtask, Some(true));
    }

    #[test]
    fn to_slash_item_prepends_slash() {
        let cmd = OpenCodeCommand {
            name: "review".into(),
            description: Some("Review code".into()),
            template: None,
            agent: None,
            model: None,
            source: None,
            subtask: None,
        };
        let item = cmd.to_slash_item();
        assert_eq!(item.command, "/review");
        assert_eq!(item.description, "Review code");
    }
}
