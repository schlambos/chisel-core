//! Per-language server configuration. The "vertical slice" implementation
//! covers TypeScript end-to-end via `typescript-language-server`; the other
//! five entries are wired into the table so `/api/lsp/sessions` returns a
//! consistent answer regardless of whether the binary is actually installed.

use std::path::PathBuf;

/// Static configuration for a single supported language.
#[derive(Debug, Clone)]
pub struct LanguageConfig {
    /// Frontend language id (matches Monaco's language id and our
    /// `editorLanguage.ts` inference).
    pub language: &'static str,
    /// Program name to invoke. Resolved through `which` at session-start time
    /// so we can report `installed: false` cleanly without spawning.
    pub command: &'static str,
    /// Arguments passed verbatim after `command`.
    pub args: &'static [&'static str],
    /// Human-readable install hint shown when the binary is missing.
    pub install_hint: &'static str,
}

/// The full set of supported languages.
pub const LANGUAGES: &[LanguageConfig] = &[
    LanguageConfig {
        language: "typescript",
        command: "typescript-language-server",
        args: &["--stdio"],
        install_hint: "Install via `npm i -g typescript-language-server typescript`",
    },
    LanguageConfig {
        language: "javascript",
        command: "typescript-language-server",
        args: &["--stdio"],
        install_hint: "Install via `npm i -g typescript-language-server typescript`",
    },
    LanguageConfig {
        language: "python",
        command: "pyright-langserver",
        args: &["--stdio"],
        install_hint: "Install via `npm i -g pyright`",
    },
    LanguageConfig {
        language: "rust",
        command: "rust-analyzer",
        args: &[],
        install_hint: "Install via `rustup component add rust-analyzer`",
    },
    LanguageConfig {
        language: "go",
        command: "gopls",
        args: &[],
        install_hint: "Install via `go install golang.org/x/tools/gopls@latest`",
    },
    LanguageConfig {
        language: "powershell",
        command: "pwsh",
        args: &[],
        install_hint: "Install PowerShell + PowerShell Editor Services",
    },
];

pub fn find(language: &str) -> Option<&'static LanguageConfig> {
    LANGUAGES.iter().find(|l| l.language == language)
}

/// Returns the absolute path to the binary if it is on PATH, otherwise None.
pub fn locate(command: &str) -> Option<PathBuf> {
    which::which(command).ok()
}
