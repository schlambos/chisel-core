use chisl_api_types::{OpenAITextToSpeechConfig, TtsResult};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use reqwest::Client;

use crate::error::TtsError;

const DEFAULT_BASE_URL: &str = "https://api.openai.com";
pub const TTS_MAX_TEXT_CHARS: usize = 8192;
/// MIME type for the default `mp3` payload produced by OpenAI's `/v1/audio/speech`.
pub const TTS_DEFAULT_MIME: &str = "audio/mpeg";
const DEFAULT_MODEL: &str = "tts-1";
const DEFAULT_VOICE: &str = "alloy";

pub async fn synthesize(client: &Client, config: &OpenAITextToSpeechConfig, text: &str) -> Result<TtsResult, TtsError> {
    if config.api_key.is_empty() {
        return Err(TtsError::OpenaiNotConfigured);
    }

    if text.is_empty() {
        return Err(TtsError::TextEmpty);
    }

    if text.chars().count() > TTS_MAX_TEXT_CHARS {
        return Err(TtsError::TextTooLong(TTS_MAX_TEXT_CHARS));
    }

    let model = config
        .model
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_MODEL);
    let voice = config
        .voice
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_VOICE);

    let base_url = config
        .base_url
        .as_deref()
        .unwrap_or(DEFAULT_BASE_URL)
        .trim_end_matches('/');
    let url = format!("{base_url}/v1/audio/speech");

    let body = serde_json::json!({
        "model": model,
        "voice": voice,
        "input": text,
    });

    let response = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", config.api_key))
        .json(&body)
        .send()
        .await
        .map_err(|e| TtsError::RequestFailed(format!("OpenAI request error: {e}")))?;

    let status = response.status();
    if !status.is_success() {
        // Do NOT include the response body in the error — it can contain echoes
        // of the API key or other sensitive upstream diagnostics. Surface only
        // the status code.
        return Err(TtsError::RequestFailed(format!("OpenAI API returned {status}")));
    }

    let bytes = response
        .bytes()
        .await
        .map_err(|e| TtsError::RequestFailed(format!("failed to read OpenAI response: {e}")))?;

    let audio = BASE64.encode(&bytes);

    Ok(TtsResult {
        audio,
        mime: TTS_DEFAULT_MIME.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_base_url_value() {
        assert_eq!(DEFAULT_BASE_URL, "https://api.openai.com");
    }

    #[test]
    fn default_model_and_voice_values() {
        assert_eq!(DEFAULT_MODEL, "tts-1");
        assert_eq!(DEFAULT_VOICE, "alloy");
    }

    #[test]
    fn tts_max_text_chars_constant() {
        assert_eq!(TTS_MAX_TEXT_CHARS, 8192);
    }

    #[test]
    fn tts_default_mime_constant() {
        assert_eq!(TTS_DEFAULT_MIME, "audio/mpeg");
    }

    #[tokio::test]
    async fn empty_api_key_returns_not_configured() {
        let config = OpenAITextToSpeechConfig {
            api_key: String::new(),
            base_url: None,
            model: None,
            voice: None,
        };
        let result = synthesize(&Client::new(), &config, "hello").await;
        assert!(matches!(result, Err(TtsError::OpenaiNotConfigured)));
    }

    #[tokio::test]
    async fn empty_text_returns_text_empty() {
        let config = OpenAITextToSpeechConfig {
            api_key: "sk-test".into(),
            base_url: None,
            model: None,
            voice: None,
        };
        let result = synthesize(&Client::new(), &config, "").await;
        assert!(matches!(result, Err(TtsError::TextEmpty)));
    }

    #[tokio::test]
    async fn text_over_limit_returns_too_long() {
        let config = OpenAITextToSpeechConfig {
            api_key: "sk-test".into(),
            base_url: None,
            model: None,
            voice: None,
        };
        let over_limit = "a".repeat(TTS_MAX_TEXT_CHARS + 1);
        let result = synthesize(&Client::new(), &config, &over_limit).await;
        assert!(matches!(result, Err(TtsError::TextTooLong(_))));
    }

    #[tokio::test]
    async fn text_at_limit_passes_validation() {
        // Text exactly at the limit should NOT trigger TextTooLong. The
        // network call itself will fail (no upstream) but the validation
        // gate has been cleared.
        let config = OpenAITextToSpeechConfig {
            api_key: "sk-test".into(),
            base_url: Some("http://127.0.0.1:1".into()),
            model: None,
            voice: None,
        };
        let at_limit = "a".repeat(TTS_MAX_TEXT_CHARS);
        let result = synthesize(&Client::new(), &config, &at_limit).await;
        assert!(matches!(result, Err(TtsError::RequestFailed(_))));
    }
}
