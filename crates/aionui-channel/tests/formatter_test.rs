use aionui_channel::formatter::format_text_for_platform;
use aionui_channel::types::PluginType;

// ── Telegram: escape HTML, then convert markdown to HTML tags ────

#[test]
fn telegram_bold_and_code() {
    let input = "**bold** and `code`";
    let result = format_text_for_platform(input, PluginType::Telegram);
    assert!(result.contains("<b>bold</b>"), "got: {result}");
    assert!(result.contains("<code>code</code>"), "got: {result}");
}

#[test]
fn telegram_escapes_raw_html() {
    let input = "<script>alert(1)</script>";
    let result = format_text_for_platform(input, PluginType::Telegram);
    assert!(!result.contains("<script>"), "got: {result}");
    assert!(result.contains("&lt;script&gt;"), "got: {result}");
}

#[test]
fn telegram_code_block() {
    let input = "```rust\nfn main() {}\n```";
    let result = format_text_for_platform(input, PluginType::Telegram);
    assert!(result.contains("<pre><code>"), "got: {result}");
    assert!(result.contains("fn main()"), "got: {result}");
}

#[test]
fn telegram_link() {
    let input = "[click](https://example.com)";
    let result = format_text_for_platform(input, PluginType::Telegram);
    assert!(
        result.contains(r#"<a href="https://example.com">click</a>"#),
        "got: {result}"
    );
}

// ── Fallback: escape HTML ────────────────────────────────────────

#[test]
fn fallback_escapes_html() {
    let input = "<b>bold</b>";
    let result = format_text_for_platform(input, PluginType::Slack);
    assert!(result.contains("&lt;b&gt;"), "got: {result}");
}
