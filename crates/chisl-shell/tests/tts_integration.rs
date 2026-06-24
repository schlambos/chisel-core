use chisl_api_types::{OpenAITextToSpeechConfig, TextToSpeechConfig, TextToSpeechProvider};
use chisl_shell::{TtsError, TtsService};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn tts_service() -> TtsService {
    TtsService::new(reqwest::Client::new())
}

fn openai_config_with_base_url(base_url: &str, api_key: &str) -> TextToSpeechConfig {
    TextToSpeechConfig {
        enabled: true,
        provider: TextToSpeechProvider::Openai,
        openai: Some(OpenAITextToSpeechConfig {
            api_key: api_key.to_owned(),
            base_url: Some(base_url.to_owned()),
            model: Some("tts-1".into()),
            voice: Some("alloy".into()),
        }),
        system: None,
    }
}

fn openai_config_with_key(api_key: &str) -> TextToSpeechConfig {
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

// ---------------------------------------------------------------------------
// TT-1: OpenAI synthesis — success
// ---------------------------------------------------------------------------
#[tokio::test]
async fn tt1_openai_synthesize_success() {
    let mock_server = MockServer::start().await;

    // Pretend audio bytes — any non-empty payload will do.
    let audio_bytes: Vec<u8> = (0..16u8).collect();
    let expected_b64 = BASE64.encode(&audio_bytes);

    Mock::given(method("POST"))
        .and(path("/v1/audio/speech"))
        .and(header("Authorization", "Bearer sk-test-key"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(audio_bytes.clone()))
        .mount(&mock_server)
        .await;

    let config = openai_config_with_base_url(&mock_server.uri(), "sk-test-key");

    let result = tts_service().synthesize("hello world", &config).await.unwrap();

    assert_eq!(result.audio, expected_b64);
    assert_eq!(result.mime, "audio/mpeg");
}

// ---------------------------------------------------------------------------
// TT-2: TTS disabled
// ---------------------------------------------------------------------------
#[tokio::test]
async fn tt2_tts_disabled() {
    let config = TextToSpeechConfig {
        enabled: false,
        provider: TextToSpeechProvider::Openai,
        openai: None,
        system: None,
    };

    let result = tts_service().synthesize("hello", &config).await;
    assert!(matches!(result, Err(TtsError::Disabled)));
}

// ---------------------------------------------------------------------------
// TT-3: OpenAI missing API key (empty string)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn tt3_openai_empty_api_key() {
    let config = openai_config_with_key("");

    let result = tts_service().synthesize("hello", &config).await;
    assert!(matches!(result, Err(TtsError::OpenaiNotConfigured)));
}

// ---------------------------------------------------------------------------
// TT-3b: OpenAI missing config section
// ---------------------------------------------------------------------------
#[tokio::test]
async fn tt3b_openai_config_section_missing() {
    let config = TextToSpeechConfig {
        enabled: true,
        provider: TextToSpeechProvider::Openai,
        openai: None,
        system: None,
    };

    let result = tts_service().synthesize("hello", &config).await;
    assert!(matches!(result, Err(TtsError::OpenaiNotConfigured)));
}

// ---------------------------------------------------------------------------
// TT-4: System provider rejected server-side
// ---------------------------------------------------------------------------
#[tokio::test]
async fn tt4_system_provider_rejected() {
    let config = TextToSpeechConfig {
        enabled: true,
        provider: TextToSpeechProvider::System,
        openai: None,
        system: Some(serde_json::json!({ "voice": "default" })),
    };

    let result = tts_service().synthesize("hello", &config).await;
    match result {
        Err(TtsError::ProviderNotSupported(p)) => assert_eq!(p, "system"),
        other => panic!("expected ProviderNotSupported, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// TT-5: Empty text
// ---------------------------------------------------------------------------
#[tokio::test]
async fn tt5_empty_text() {
    let config = openai_config_with_key("sk-test");
    let result = tts_service().synthesize("", &config).await;
    assert!(matches!(result, Err(TtsError::TextEmpty)));
}

// ---------------------------------------------------------------------------
// TT-6: Oversize text
// ---------------------------------------------------------------------------
#[tokio::test]
async fn tt6_oversize_text() {
    let config = openai_config_with_key("sk-test");
    let over = "a".repeat(8193);
    let result = tts_service().synthesize(&over, &config).await;
    assert!(matches!(result, Err(TtsError::TextTooLong(_))));
}

// ---------------------------------------------------------------------------
// TT-7: Upstream 5xx
// ---------------------------------------------------------------------------
#[tokio::test]
async fn tt7_openai_upstream_failure() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/audio/speech"))
        .respond_with(ResponseTemplate::new(500).set_body_string("internal error"))
        .mount(&mock_server)
        .await;

    let config = openai_config_with_base_url(&mock_server.uri(), "sk-fake");

    let result = tts_service().synthesize("hello", &config).await;
    match result {
        Err(TtsError::RequestFailed(msg)) => {
            assert!(msg.contains("500"), "expected 500 in error: {msg}");
            // Sanity check: the upstream body must NOT leak into the message.
            assert!(
                !msg.contains("internal error"),
                "upstream body leaked into error: {msg}"
            );
        }
        other => panic!("expected RequestFailed, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// TT-7b: Upstream 401 — key rejected
// ---------------------------------------------------------------------------
#[tokio::test]
async fn tt7b_openai_unauthorized() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/audio/speech"))
        .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
            "error": {
                "message": "Incorrect API key provided: sk-secret-LEAK",
                "type": "invalid_request_error"
            }
        })))
        .mount(&mock_server)
        .await;

    let config = openai_config_with_base_url(&mock_server.uri(), "sk-secret-LEAK");

    let result = tts_service().synthesize("hello", &config).await;
    match result {
        Err(TtsError::RequestFailed(msg)) => {
            assert!(msg.contains("401"), "expected 401 in error: {msg}");
            // The upstream body (which echoes the key) must NOT appear in the
            // error message we propagate.
            assert!(
                !msg.contains("sk-secret-LEAK"),
                "API key leaked into error message: {msg}"
            );
        }
        other => panic!("expected RequestFailed, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// TT-8: Default model + voice when fields are None
// ---------------------------------------------------------------------------
#[tokio::test]
async fn tt8_default_model_and_voice() {
    let mock_server = MockServer::start().await;

    let mock_guard = mock_server
        .register_as_scoped(
            Mock::given(method("POST"))
                .and(path("/v1/audio/speech"))
                .and(header("Authorization", "Bearer sk-test"))
                .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![0u8; 4])),
        )
        .await;

    let config = TextToSpeechConfig {
        enabled: true,
        provider: TextToSpeechProvider::Openai,
        openai: Some(OpenAITextToSpeechConfig {
            api_key: "sk-test".to_owned(),
            base_url: Some(mock_server.uri()),
            model: None,
            voice: None,
        }),
        system: None,
    };

    let _ = tts_service().synthesize("hello", &config).await.unwrap();

    // Inspect the recorded request body to confirm defaults were applied.
    let received = mock_guard.received_requests().await;
    assert!(!received.is_empty(), "expected at least one request");
    let body: serde_json::Value = serde_json::from_slice(&received.last().unwrap().body).unwrap();
    assert_eq!(body["model"], "tts-1");
    assert_eq!(body["voice"], "alloy");
    assert_eq!(body["input"], "hello");
}

// ---------------------------------------------------------------------------
// TT-9: Text exactly at the cap does NOT trigger TextTooLong
// ---------------------------------------------------------------------------
#[tokio::test]
async fn tt9_text_at_limit_passes_validation() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/audio/speech"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![1u8, 2, 3, 4]))
        .mount(&mock_server)
        .await;

    let config = openai_config_with_base_url(&mock_server.uri(), "sk-test");
    let at_limit = "a".repeat(8192);
    let result = tts_service().synthesize(&at_limit, &config).await;
    assert!(result.is_ok());
}

// ---------------------------------------------------------------------------
// TtsError → AppError conversion (black-box integration test)
// ---------------------------------------------------------------------------
#[test]
fn tts_error_to_app_error_mapping() {
    use chisl_common::AppError;

    let err: AppError = TtsError::Disabled.into();
    assert!(matches!(err, AppError::BadRequest(_)));

    let err: AppError = TtsError::OpenaiNotConfigured.into();
    assert!(matches!(err, AppError::BadRequest(_)));

    let err: AppError = TtsError::ProviderNotSupported("system".into()).into();
    assert!(matches!(err, AppError::BadRequest(_)));

    let err: AppError = TtsError::TextEmpty.into();
    assert!(matches!(err, AppError::BadRequest(_)));

    let err: AppError = TtsError::TextTooLong(8192).into();
    assert!(matches!(err, AppError::BadRequest(_)));

    let err: AppError = TtsError::RequestFailed("upstream".into()).into();
    assert!(matches!(err, AppError::BadGateway(_)));

    let err: AppError = TtsError::Unknown("bug".into()).into();
    assert!(matches!(err, AppError::Internal(_)));
}
