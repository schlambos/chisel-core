use aionui_api_types::{TextToSpeechConfig, TextToSpeechProvider, TtsResult};
use reqwest::Client;

use crate::error::TtsError;
use crate::tts_openai;

pub struct TtsService {
    client: Client,
}

impl TtsService {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub async fn synthesize(&self, text: &str, config: &TextToSpeechConfig) -> Result<TtsResult, TtsError> {
        if !config.enabled {
            return Err(TtsError::Disabled);
        }

        match config.provider {
            TextToSpeechProvider::Openai => {
                let openai_config = config.openai.as_ref().ok_or(TtsError::OpenaiNotConfigured)?;
                tts_openai::synthesize(&self.client, openai_config, text).await
            }
            TextToSpeechProvider::System => Err(TtsError::ProviderNotSupported("system".to_owned())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aionui_api_types::OpenAITextToSpeechConfig;

    fn make_disabled_config() -> TextToSpeechConfig {
        TextToSpeechConfig {
            enabled: false,
            provider: TextToSpeechProvider::Openai,
            openai: None,
            system: None,
        }
    }

    fn make_openai_config(api_key: &str) -> TextToSpeechConfig {
        TextToSpeechConfig {
            enabled: true,
            provider: TextToSpeechProvider::Openai,
            openai: Some(OpenAITextToSpeechConfig {
                api_key: api_key.to_owned(),
                base_url: None,
                model: Some("tts-1".into()),
                voice: Some("alloy".into()),
            }),
            system: None,
        }
    }

    fn make_system_config() -> TextToSpeechConfig {
        TextToSpeechConfig {
            enabled: true,
            provider: TextToSpeechProvider::System,
            openai: None,
            system: Some(serde_json::json!({ "voice": "default" })),
        }
    }

    #[tokio::test]
    async fn disabled_config_returns_disabled_error() {
        let svc = TtsService::new(Client::new());
        let result = svc.synthesize("hello", &make_disabled_config()).await;
        assert!(matches!(result, Err(TtsError::Disabled)));
    }

    #[tokio::test]
    async fn system_provider_returns_not_supported() {
        let svc = TtsService::new(Client::new());
        let result = svc.synthesize("hello", &make_system_config()).await;
        match result {
            Err(TtsError::ProviderNotSupported(p)) => assert_eq!(p, "system"),
            other => panic!("expected ProviderNotSupported, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn openai_provider_missing_config_returns_not_configured() {
        let svc = TtsService::new(Client::new());
        let config = TextToSpeechConfig {
            enabled: true,
            provider: TextToSpeechProvider::Openai,
            openai: None,
            system: None,
        };
        let result = svc.synthesize("hello", &config).await;
        assert!(matches!(result, Err(TtsError::OpenaiNotConfigured)));
    }

    #[tokio::test]
    async fn openai_empty_api_key_returns_not_configured() {
        let svc = TtsService::new(Client::new());
        let result = svc.synthesize("hello", &make_openai_config("")).await;
        assert!(matches!(result, Err(TtsError::OpenaiNotConfigured)));
    }
}
