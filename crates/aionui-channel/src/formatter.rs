use std::sync::LazyLock;

use regex::Regex;

use crate::types::PluginType;

/// Convert text to the target IM platform format.
///
/// - Telegram: escape HTML, then convert markdown → HTML tags
/// - Fallback: escape HTML special chars
pub fn format_text_for_platform(text: &str, platform: PluginType) -> String {
    match platform {
        PluginType::Telegram => markdown_to_telegram_html(text),
        _ => escape_html(text),
    }
}

// ── Telegram ─────────────────────────────────────────────────────

static RE_CODE_BLOCK: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"```(?:\w*)\n?([\s\S]*?)```").unwrap());
static RE_INLINE_CODE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"`([^`]+)`").unwrap());
static RE_BOLD_STAR: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\*\*(.+?)\*\*").unwrap());
static RE_BOLD_UNDER: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"__(.+?)__").unwrap());
static RE_ITALIC_STAR: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\*(.+?)\*").unwrap());
static RE_ITALIC_UNDER: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"_(.+?)_").unwrap());
static RE_LINK: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\[([^\]]+)\]\(([^)]+)\)").unwrap());

fn markdown_to_telegram_html(text: &str) -> String {
    let s = escape_html(text);
    let s = RE_CODE_BLOCK.replace_all(&s, "<pre><code>$1</code></pre>");
    let s = RE_INLINE_CODE.replace_all(&s, "<code>$1</code>");
    let s = RE_BOLD_STAR.replace_all(&s, "<b>$1</b>");
    let s = RE_BOLD_UNDER.replace_all(&s, "<b>$1</b>");
    let s = RE_ITALIC_STAR.replace_all(&s, "<i>$1</i>");
    let s = RE_ITALIC_UNDER.replace_all(&s, "<i>$1</i>");
    let s = RE_LINK.replace_all(&s, r#"<a href="$2">$1</a>"#);
    s.into_owned()
}

// ── Helpers ──────────────────────────────────────────────────────

fn escape_html(text: &str) -> String {
    text.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}
