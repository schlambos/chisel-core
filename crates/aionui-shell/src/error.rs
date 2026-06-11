use aionui_common::AppError;

#[derive(Debug, thiserror::Error)]
pub enum ShellError {
    #[error("file not found: {0}")]
    FileNotFound(String),

    #[error("directory not found: {0}")]
    DirectoryNotFound(String),

    #[error("invalid URL: {0}")]
    InvalidUrl(String),

    #[error("tool not installed: {0}")]
    ToolNotInstalled(String),

    #[error("command failed: {0}")]
    CommandFailed(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<ShellError> for AppError {
    fn from(err: ShellError) -> Self {
        match err {
            ShellError::FileNotFound(path) => AppError::BadRequest(format!("file not found: {path}")),
            ShellError::DirectoryNotFound(path) => AppError::BadRequest(format!("directory not found: {path}")),
            ShellError::InvalidUrl(msg) => AppError::BadRequest(format!("invalid URL: {msg}")),
            ShellError::ToolNotInstalled(tool) => AppError::BadRequest(format!("tool not installed: {tool}")),
            ShellError::CommandFailed(msg) => AppError::Internal(format!("command failed: {msg}")),
            ShellError::Io(e) => AppError::Internal(format!("IO error: {e}")),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SttError {
    #[error("STT is not enabled")]
    Disabled,

    #[error("OpenAI STT is not configured: missing API key")]
    OpenaiNotConfigured,

    #[error("Deepgram STT is not configured: missing API key")]
    DeepgramNotConfigured,

    #[error("STT request failed: {0}")]
    RequestFailed(String),

    #[error("STT unknown error: {0}")]
    Unknown(String),
}

impl SttError {
    pub fn error_code(&self) -> &'static str {
        match self {
            Self::Disabled => "STT_DISABLED",
            Self::OpenaiNotConfigured => "STT_OPENAI_NOT_CONFIGURED",
            Self::DeepgramNotConfigured => "STT_DEEPGRAM_NOT_CONFIGURED",
            Self::RequestFailed(_) => "STT_REQUEST_FAILED",
            Self::Unknown(_) => "STT_UNKNOWN",
        }
    }

    pub fn status_code(&self) -> u16 {
        match self {
            Self::Disabled | Self::OpenaiNotConfigured | Self::DeepgramNotConfigured => 400,
            Self::RequestFailed(_) => 502,
            Self::Unknown(_) => 500,
        }
    }
}

impl From<SttError> for AppError {
    fn from(err: SttError) -> Self {
        match &err {
            SttError::Disabled | SttError::OpenaiNotConfigured | SttError::DeepgramNotConfigured => {
                AppError::BadRequest(err.to_string())
            }
            SttError::RequestFailed(_) => AppError::BadGateway(err.to_string()),
            SttError::Unknown(_) => AppError::Internal(err.to_string()),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TtsError {
    #[error("TTS is not enabled")]
    Disabled,

    #[error("OpenAI TTS is not configured: missing API key")]
    OpenaiNotConfigured,

    #[error("TTS provider '{0}' is not supported server-side")]
    ProviderNotSupported(String),

    #[error("TTS text must not be empty")]
    TextEmpty,

    #[error("TTS text exceeds maximum length of {0} characters")]
    TextTooLong(usize),

    #[error("TTS request failed: {0}")]
    RequestFailed(String),

    #[error("TTS unknown error: {0}")]
    Unknown(String),
}

impl TtsError {
    pub fn error_code(&self) -> &'static str {
        match self {
            Self::Disabled => "TTS_DISABLED",
            Self::OpenaiNotConfigured => "TTS_OPENAI_NOT_CONFIGURED",
            Self::ProviderNotSupported(_) => "TTS_PROVIDER_NOT_SUPPORTED",
            Self::TextEmpty => "TTS_TEXT_EMPTY",
            Self::TextTooLong(_) => "TTS_TEXT_TOO_LONG",
            Self::RequestFailed(_) => "TTS_REQUEST_FAILED",
            Self::Unknown(_) => "TTS_UNKNOWN",
        }
    }

    pub fn status_code(&self) -> u16 {
        match self {
            Self::Disabled
            | Self::OpenaiNotConfigured
            | Self::ProviderNotSupported(_)
            | Self::TextEmpty
            | Self::TextTooLong(_) => 400,
            Self::RequestFailed(_) => 502,
            Self::Unknown(_) => 500,
        }
    }
}

impl From<TtsError> for AppError {
    fn from(err: TtsError) -> Self {
        match &err {
            TtsError::Disabled
            | TtsError::OpenaiNotConfigured
            | TtsError::ProviderNotSupported(_)
            | TtsError::TextEmpty
            | TtsError::TextTooLong(_) => AppError::BadRequest(err.to_string()),
            TtsError::RequestFailed(_) => AppError::BadGateway(err.to_string()),
            TtsError::Unknown(_) => AppError::Internal(err.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_not_found_maps_to_bad_request() {
        let err: AppError = ShellError::FileNotFound("/tmp/missing.txt".into()).into();
        assert!(matches!(err, AppError::BadRequest(msg) if msg.contains("/tmp/missing.txt")));
    }

    #[test]
    fn directory_not_found_maps_to_bad_request() {
        let err: AppError = ShellError::DirectoryNotFound("/tmp/nodir".into()).into();
        assert!(matches!(err, AppError::BadRequest(msg) if msg.contains("/tmp/nodir")));
    }

    #[test]
    fn invalid_url_maps_to_bad_request() {
        let err: AppError = ShellError::InvalidUrl("not a url".into()).into();
        assert!(matches!(err, AppError::BadRequest(msg) if msg.contains("not a url")));
    }

    #[test]
    fn tool_not_installed_maps_to_bad_request() {
        let err: AppError = ShellError::ToolNotInstalled("vscode".into()).into();
        assert!(matches!(err, AppError::BadRequest(msg) if msg.contains("vscode")));
    }

    #[test]
    fn command_failed_maps_to_internal() {
        let err: AppError = ShellError::CommandFailed("exit code 1".into()).into();
        assert!(matches!(err, AppError::Internal(msg) if msg.contains("exit code 1")));
    }

    #[test]
    fn io_error_maps_to_internal() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "permission denied");
        let err: AppError = ShellError::Io(io_err).into();
        assert!(matches!(err, AppError::Internal(msg) if msg.contains("permission denied")));
    }

    #[test]
    fn shell_error_display_messages() {
        assert_eq!(
            ShellError::FileNotFound("/a.txt".into()).to_string(),
            "file not found: /a.txt"
        );
        assert_eq!(
            ShellError::DirectoryNotFound("/dir".into()).to_string(),
            "directory not found: /dir"
        );
        assert_eq!(ShellError::InvalidUrl("bad".into()).to_string(), "invalid URL: bad");
        assert_eq!(
            ShellError::ToolNotInstalled("code".into()).to_string(),
            "tool not installed: code"
        );
        assert_eq!(
            ShellError::CommandFailed("oops".into()).to_string(),
            "command failed: oops"
        );
    }

    #[test]
    fn stt_disabled_maps_to_bad_request() {
        let err: AppError = SttError::Disabled.into();
        assert!(matches!(err, AppError::BadRequest(msg) if msg.contains("not enabled")));
    }

    #[test]
    fn stt_openai_not_configured_maps_to_bad_request() {
        let err: AppError = SttError::OpenaiNotConfigured.into();
        assert!(matches!(err, AppError::BadRequest(msg) if msg.contains("OpenAI")));
    }

    #[test]
    fn stt_deepgram_not_configured_maps_to_bad_request() {
        let err: AppError = SttError::DeepgramNotConfigured.into();
        assert!(matches!(err, AppError::BadRequest(msg) if msg.contains("Deepgram")));
    }

    #[test]
    fn stt_request_failed_maps_to_bad_gateway() {
        let err: AppError = SttError::RequestFailed("HTTP 401".into()).into();
        assert!(matches!(err, AppError::BadGateway(msg) if msg.contains("HTTP 401")));
    }

    #[test]
    fn stt_unknown_maps_to_internal() {
        let err: AppError = SttError::Unknown("unexpected".into()).into();
        assert!(matches!(err, AppError::Internal(msg) if msg.contains("unexpected")));
    }

    #[test]
    fn stt_error_codes() {
        assert_eq!(SttError::Disabled.error_code(), "STT_DISABLED");
        assert_eq!(SttError::OpenaiNotConfigured.error_code(), "STT_OPENAI_NOT_CONFIGURED");
        assert_eq!(
            SttError::DeepgramNotConfigured.error_code(),
            "STT_DEEPGRAM_NOT_CONFIGURED"
        );
        assert_eq!(SttError::RequestFailed("x".into()).error_code(), "STT_REQUEST_FAILED");
        assert_eq!(SttError::Unknown("x".into()).error_code(), "STT_UNKNOWN");
    }

    #[test]
    fn stt_status_codes() {
        assert_eq!(SttError::Disabled.status_code(), 400);
        assert_eq!(SttError::OpenaiNotConfigured.status_code(), 400);
        assert_eq!(SttError::DeepgramNotConfigured.status_code(), 400);
        assert_eq!(SttError::RequestFailed("x".into()).status_code(), 502);
        assert_eq!(SttError::Unknown("x".into()).status_code(), 500);
    }

    #[test]
    fn stt_error_display_messages() {
        assert_eq!(SttError::Disabled.to_string(), "STT is not enabled");
        assert_eq!(
            SttError::OpenaiNotConfigured.to_string(),
            "OpenAI STT is not configured: missing API key"
        );
        assert_eq!(
            SttError::DeepgramNotConfigured.to_string(),
            "Deepgram STT is not configured: missing API key"
        );
        assert_eq!(
            SttError::RequestFailed("timeout".into()).to_string(),
            "STT request failed: timeout"
        );
        assert_eq!(SttError::Unknown("oops".into()).to_string(), "STT unknown error: oops");
    }

    #[test]
    fn tts_disabled_maps_to_bad_request() {
        let err: AppError = TtsError::Disabled.into();
        assert!(matches!(err, AppError::BadRequest(msg) if msg.contains("not enabled")));
    }

    #[test]
    fn tts_openai_not_configured_maps_to_bad_request() {
        let err: AppError = TtsError::OpenaiNotConfigured.into();
        assert!(matches!(err, AppError::BadRequest(msg) if msg.contains("OpenAI")));
    }

    #[test]
    fn tts_provider_not_supported_maps_to_bad_request() {
        let err: AppError = TtsError::ProviderNotSupported("system".into()).into();
        assert!(matches!(err, AppError::BadRequest(msg) if msg.contains("system")));
    }

    #[test]
    fn tts_text_empty_maps_to_bad_request() {
        let err: AppError = TtsError::TextEmpty.into();
        assert!(matches!(err, AppError::BadRequest(msg) if msg.contains("empty")));
    }

    #[test]
    fn tts_text_too_long_maps_to_bad_request() {
        let err: AppError = TtsError::TextTooLong(8192).into();
        assert!(matches!(err, AppError::BadRequest(msg) if msg.contains("8192")));
    }

    #[test]
    fn tts_request_failed_maps_to_bad_gateway() {
        let err: AppError = TtsError::RequestFailed("HTTP 500".into()).into();
        assert!(matches!(err, AppError::BadGateway(msg) if msg.contains("HTTP 500")));
    }

    #[test]
    fn tts_unknown_maps_to_internal() {
        let err: AppError = TtsError::Unknown("oops".into()).into();
        assert!(matches!(err, AppError::Internal(msg) if msg.contains("oops")));
    }

    #[test]
    fn tts_error_codes() {
        assert_eq!(TtsError::Disabled.error_code(), "TTS_DISABLED");
        assert_eq!(TtsError::OpenaiNotConfigured.error_code(), "TTS_OPENAI_NOT_CONFIGURED");
        assert_eq!(
            TtsError::ProviderNotSupported("system".into()).error_code(),
            "TTS_PROVIDER_NOT_SUPPORTED"
        );
        assert_eq!(TtsError::TextEmpty.error_code(), "TTS_TEXT_EMPTY");
        assert_eq!(TtsError::TextTooLong(8192).error_code(), "TTS_TEXT_TOO_LONG");
        assert_eq!(TtsError::RequestFailed("x".into()).error_code(), "TTS_REQUEST_FAILED");
        assert_eq!(TtsError::Unknown("x".into()).error_code(), "TTS_UNKNOWN");
    }

    #[test]
    fn tts_status_codes() {
        assert_eq!(TtsError::Disabled.status_code(), 400);
        assert_eq!(TtsError::OpenaiNotConfigured.status_code(), 400);
        assert_eq!(TtsError::ProviderNotSupported("x".into()).status_code(), 400);
        assert_eq!(TtsError::TextEmpty.status_code(), 400);
        assert_eq!(TtsError::TextTooLong(1).status_code(), 400);
        assert_eq!(TtsError::RequestFailed("x".into()).status_code(), 502);
        assert_eq!(TtsError::Unknown("x".into()).status_code(), 500);
    }

    #[test]
    fn tts_error_display_messages() {
        assert_eq!(TtsError::Disabled.to_string(), "TTS is not enabled");
        assert_eq!(
            TtsError::OpenaiNotConfigured.to_string(),
            "OpenAI TTS is not configured: missing API key"
        );
        assert_eq!(
            TtsError::ProviderNotSupported("system".into()).to_string(),
            "TTS provider 'system' is not supported server-side"
        );
        assert_eq!(TtsError::TextEmpty.to_string(), "TTS text must not be empty");
        assert_eq!(
            TtsError::TextTooLong(8192).to_string(),
            "TTS text exceeds maximum length of 8192 characters"
        );
        assert_eq!(
            TtsError::RequestFailed("timeout".into()).to_string(),
            "TTS request failed: timeout"
        );
        assert_eq!(TtsError::Unknown("oops".into()).to_string(), "TTS unknown error: oops");
    }
}
