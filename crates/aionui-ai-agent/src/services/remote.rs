use std::sync::Arc;
use std::time::Duration;

use aionui_api_types::{
    CreateRemoteAgentRequest, HandshakeResponse, ModelInfoEntry, ModelInfoPayload, RemoteAgentListItem,
    RemoteAgentResponse, TestRemoteAgentConnectionRequest, UpdateRemoteAgentRequest,
};
use aionui_common::{
    AppError, RemoteAgentAuthType, RemoteAgentProtocol, RemoteAgentStatus, decrypt_string, encrypt_string,
};
use aionui_db::models::RemoteAgentRow;
use aionui_db::{IRemoteAgentRepository, UpdateRemoteAgentParams};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use ed25519_dalek::SigningKey;
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use tokio_tungstenite::tungstenite;
use tracing::warn;

/// Service layer for Remote Agent CRUD and connection management.
#[derive(Clone)]
pub struct RemoteAgentService {
    repo: Arc<dyn IRemoteAgentRepository>,
    encryption_key: [u8; 32],
}

impl RemoteAgentService {
    pub fn new(repo: Arc<dyn IRemoteAgentRepository>, encryption_key: [u8; 32]) -> Self {
        Self { repo, encryption_key }
    }

    /// List all remote agents (auth_token omitted).
    pub async fn list(&self) -> Result<Vec<RemoteAgentListItem>, AppError> {
        let rows = self.repo.list().await.map_err(db_err)?;
        rows.into_iter().map(|r| self.row_to_list_item(r)).collect()
    }

    /// Get a single remote agent by ID (auth_token masked).
    pub async fn get(&self, id: &str) -> Result<RemoteAgentResponse, AppError> {
        let row = self
            .repo
            .find_by_id(id)
            .await
            .map_err(db_err)?
            .ok_or_else(|| AppError::NotFound(format!("Remote agent '{id}' not found")))?;
        self.row_to_response(row)
    }

    /// Create a new remote agent. OpenClaw protocol auto-generates Ed25519 keys.
    pub async fn create(&self, req: CreateRemoteAgentRequest) -> Result<RemoteAgentResponse, AppError> {
        validate_create_request(&req)?;

        let encrypted_token = req
            .auth_token
            .as_deref()
            .map(|t| encrypt_string(t, &self.encryption_key))
            .transpose()?;

        let (device_id, device_public_key, device_private_key) = if req.protocol == RemoteAgentProtocol::OpenClaw {
            let (id, pub_key, priv_key) = generate_device_keypair(&self.encryption_key)?;
            (Some(id), Some(pub_key), Some(priv_key))
        } else {
            (None, None, None)
        };

        let row = self
            .repo
            .create(aionui_db::CreateRemoteAgentParams {
                name: &req.name,
                protocol: &enum_to_str(&req.protocol),
                url: &req.url,
                auth_type: &enum_to_str(&req.auth_type),
                auth_token: encrypted_token.as_deref(),
                allow_insecure: req.allow_insecure,
                avatar: req.avatar.as_deref(),
                description: req.description.as_deref(),
                device_id: device_id.as_deref(),
                device_public_key: device_public_key.as_deref(),
                device_private_key: device_private_key.as_deref(),
                device_token: None,
            })
            .await
            .map_err(db_err)?;

        self.row_to_response(row)
    }

    /// Update an existing remote agent.
    pub async fn update(&self, id: &str, req: UpdateRemoteAgentRequest) -> Result<RemoteAgentResponse, AppError> {
        let existing = self
            .repo
            .find_by_id(id)
            .await
            .map_err(db_err)?
            .ok_or_else(|| AppError::NotFound(format!("Remote agent '{id}' not found")))?;
        validate_protocol_url(
            req.protocol.unwrap_or_else(|| parse_protocol(&existing.protocol)),
            req.url.as_deref().unwrap_or(&existing.url),
        )?;

        let encrypted_token = match &req.auth_token {
            Some(Some(t)) => Some(Some(encrypt_string(t, &self.encryption_key)?)),
            Some(None) => Some(None),
            None => None,
        };

        let protocol_str = req.protocol.map(|p| enum_to_str(&p));
        let auth_type_str = req.auth_type.map(|a| enum_to_str(&a));

        let params = UpdateRemoteAgentParams {
            name: req.name.as_deref(),
            protocol: protocol_str.as_deref(),
            url: req.url.as_deref(),
            auth_type: auth_type_str.as_deref(),
            auth_token: encrypted_token.as_ref().map(|o| o.as_deref()),
            allow_insecure: req.allow_insecure,
            avatar: req.avatar.as_ref().map(|o| o.as_deref()),
            description: req.description.as_ref().map(|o| o.as_deref()),
        };

        let row = self.repo.update(id, params).await.map_err(|e| match e {
            aionui_db::DbError::NotFound(msg) => AppError::NotFound(msg),
            other => AppError::Internal(other.to_string()),
        })?;

        self.row_to_response(row)
    }

    /// Delete a remote agent.
    pub async fn delete(&self, id: &str) -> Result<(), AppError> {
        self.repo.delete(id).await.map_err(|e| match e {
            aionui_db::DbError::NotFound(msg) => AppError::NotFound(msg),
            other => AppError::Internal(other.to_string()),
        })
    }

    /// Test a remote agent connection using its protocol-specific transport.
    pub async fn test_connection(&self, req: TestRemoteAgentConnectionRequest) -> Result<(), AppError> {
        match req.protocol {
            RemoteAgentProtocol::OpenCode => {
                test_opencode_health(
                    &req.url,
                    req.auth_type.unwrap_or(RemoteAgentAuthType::None),
                    req.auth_token.as_deref(),
                    req.allow_insecure,
                )
                .await
            }
            RemoteAgentProtocol::OpenClaw | RemoteAgentProtocol::Acp => test_websocket_connection(&req.url).await,
            RemoteAgentProtocol::ZeroClaw => Err(AppError::BadRequest(
                "ZeroClaw remote protocol is not supported yet".into(),
            )),
        }
    }

    /// Protocol-specific handshake / health verification.
    pub async fn handshake(&self, id: &str) -> Result<HandshakeResponse, AppError> {
        let row = self
            .repo
            .find_by_id(id)
            .await
            .map_err(db_err)?
            .ok_or_else(|| AppError::NotFound(format!("Remote agent '{id}' not found")))?;

        let protocol = parse_protocol(&row.protocol);
        if protocol == RemoteAgentProtocol::OpenCode {
            let auth_type = parse_auth_type(&row.auth_type);
            let auth_token = decrypt_optional_token(row.auth_token.as_deref(), &self.encryption_key)?;
            let health_result =
                test_opencode_health(&row.url, auth_type, auth_token.as_deref(), row.allow_insecure).await;
            match health_result {
                Ok(()) => {
                    let now = aionui_common::now_ms();
                    let _ = self.repo.update_status(id, "connected", Some(now)).await;
                    return Ok(HandshakeResponse {
                        status: "ok".to_string(),
                    });
                }
                Err(e) => {
                    let _ = self.repo.update_status(id, "error", None).await;
                    return Err(e);
                }
            }
        }

        if protocol != RemoteAgentProtocol::OpenClaw {
            return Err(AppError::BadRequest(
                "Handshake is not supported for this protocol".into(),
            ));
        }

        validate_ws_url(&row.url)?;

        let url = row.url.clone();
        let connect_result = tokio::time::timeout(Duration::from_secs(15), async {
            tokio::task::spawn_blocking(move || {
                tungstenite::connect(&url)
                    .map(|_| ())
                    .map_err(|e| AppError::BadGateway(format!("Handshake connection failed: {e}")))
            })
            .await
            .map_err(|e| AppError::Internal(format!("Join error: {e}")))?
        })
        .await;

        match connect_result {
            Ok(Ok(_)) => {
                let now = aionui_common::now_ms();
                let _ = self.repo.update_status(id, "connected", Some(now)).await;
                Ok(HandshakeResponse {
                    status: "ok".to_string(),
                })
            }
            Ok(Err(e)) => {
                let _ = self.repo.update_status(id, "error", None).await;
                Err(e)
            }
            Err(_) => {
                let _ = self.repo.update_status(id, "error", None).await;
                Err(AppError::Timeout("Handshake timed out after 15 seconds".into()))
            }
        }
    }

    /// Fetch available models from an OpenCode remote agent's `/provider`
    /// endpoint.  Used by the Guid (New Chat) page to populate the model
    /// selector without requiring an active session.
    ///
    /// Mirrors the [`handshake`] pattern: reads the row by id, decrypts the
    /// auth token using the service's encryption key (the plaintext never
    /// leaves the Rust process), and returns the snake_case `ModelInfoPayload`
    /// shape the renderer expects.  Model ids are encoded as
    /// `"<providerID>::<modelID>"` to match the format the live session path
    /// in `manager/remote/agent.rs` already splits on.
    pub async fn fetch_models(&self, id: &str) -> Result<ModelInfoPayload, AppError> {
        let row = self
            .repo
            .find_by_id(id)
            .await
            .map_err(db_err)?
            .ok_or_else(|| AppError::NotFound(format!("Remote agent '{id}' not found")))?;

        let protocol = parse_protocol(&row.protocol);
        if protocol != RemoteAgentProtocol::OpenCode {
            return Err(AppError::BadRequest(
                "Model fetch is only supported for OpenCode remote agents".into(),
            ));
        }

        let auth_type = parse_auth_type(&row.auth_type);
        let auth_token = decrypt_optional_token(row.auth_token.as_deref(), &self.encryption_key)?;
        fetch_opencode_model_info(&row.url, auth_type, auth_token.as_deref(), row.allow_insecure).await
    }

    // ── Private helpers ──────────────────────────────────────────

    fn row_to_list_item(&self, row: RemoteAgentRow) -> Result<RemoteAgentListItem, AppError> {
        Ok(RemoteAgentListItem {
            id: row.id,
            name: row.name,
            protocol: parse_protocol(&row.protocol),
            url: row.url,
            auth_type: parse_auth_type(&row.auth_type),
            allow_insecure: row.allow_insecure,
            avatar: row.avatar,
            description: row.description,
            status: parse_status(&row.status),
            last_connected_at: row.last_connected_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }

    fn row_to_response(&self, row: RemoteAgentRow) -> Result<RemoteAgentResponse, AppError> {
        let masked_token =
            row.auth_token
                .as_deref()
                .map(|encrypted| match decrypt_string(encrypted, &self.encryption_key) {
                    Ok(plain) => mask_token(&plain),
                    Err(e) => {
                        warn!("Failed to decrypt auth_token for agent {}: {e}", row.id);
                        "***".to_string()
                    }
                });

        let device_public_key = row
            .device_public_key
            .as_deref()
            .map(|encrypted| decrypt_string(encrypted, &self.encryption_key).unwrap_or_else(|_| "***".to_string()));

        Ok(RemoteAgentResponse {
            id: row.id,
            name: row.name,
            protocol: parse_protocol(&row.protocol),
            url: row.url,
            auth_type: parse_auth_type(&row.auth_type),
            auth_token: masked_token,
            allow_insecure: row.allow_insecure,
            avatar: row.avatar,
            description: row.description,
            device_id: row.device_id,
            device_public_key,
            status: parse_status(&row.status),
            last_connected_at: row.last_connected_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

// ── Validation ──────────────────────────────────────────────────

fn validate_create_request(req: &CreateRemoteAgentRequest) -> Result<(), AppError> {
    if req.name.trim().is_empty() {
        return Err(AppError::BadRequest("name must not be empty".into()));
    }
    if req.url.trim().is_empty() {
        return Err(AppError::BadRequest("url must not be empty".into()));
    }
    validate_protocol_url(req.protocol, &req.url)
}

fn validate_protocol_url(protocol: RemoteAgentProtocol, url: &str) -> Result<(), AppError> {
    match protocol {
        RemoteAgentProtocol::OpenCode => validate_http_url(url),
        RemoteAgentProtocol::OpenClaw | RemoteAgentProtocol::Acp | RemoteAgentProtocol::ZeroClaw => {
            validate_ws_url(url)
        }
    }
}

fn validate_ws_url(url: &str) -> Result<(), AppError> {
    if !url.starts_with("ws://") && !url.starts_with("wss://") {
        return Err(AppError::BadRequest("URL must use ws:// or wss:// protocol".into()));
    }
    Ok(())
}

fn validate_http_url(url: &str) -> Result<(), AppError> {
    let normalized = normalize_opencode_base_url(url)?;
    if !normalized.starts_with("http://") && !normalized.starts_with("https://") {
        return Err(AppError::BadRequest("URL must use http:// or https:// protocol".into()));
    }
    Ok(())
}

fn normalize_opencode_base_url(url: &str) -> Result<String, AppError> {
    let trimmed = url.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err(AppError::BadRequest("url must not be empty".into()));
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return Ok(trimmed.to_string());
    }
    if trimmed.contains("://") {
        return Err(AppError::BadRequest("URL must use http:// or https:// protocol".into()));
    }
    Ok(format!("http://{trimmed}"))
}

async fn test_websocket_connection(url: &str) -> Result<(), AppError> {
    validate_ws_url(url)?;

    let url = url.to_string();
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::task::spawn_blocking(move || {
            tungstenite::connect(&url)
                .map(|_| ())
                .map_err(|e| AppError::BadGateway(format!("WebSocket connection failed: {e}")))
        })
        .await
        .map_err(|e| AppError::Internal(format!("Join error: {e}")))?
    })
    .await;

    match result {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(AppError::Timeout("Connection timed out after 10 seconds".into())),
    }
}

async fn test_opencode_health(
    url: &str,
    auth_type: RemoteAgentAuthType,
    auth_token: Option<&str>,
    allow_insecure: bool,
) -> Result<(), AppError> {
    let base_url = normalize_opencode_base_url(url)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .danger_accept_invalid_certs(allow_insecure)
        .build()
        .map_err(|e| AppError::Internal(format!("Failed to build HTTP client: {e}")))?;
    let response = client
        .get(format!("{base_url}/global/health"))
        .headers(build_opencode_auth_headers(auth_type, auth_token)?)
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode health check failed: {e}")))?;

    if !response.status().is_success() {
        return Err(AppError::BadGateway(format!(
            "OpenCode health check failed: {}",
            response.status()
        )));
    }

    let body: serde_json::Value = response
        .json()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode health response was not JSON: {e}")))?;
    if body.get("healthy").and_then(|v| v.as_bool()) == Some(true) {
        return Ok(());
    }

    Err(AppError::BadGateway(
        "OpenCode health endpoint returned unhealthy status".into(),
    ))
}

/// Fetch the OpenCode `/provider` listing and convert it into the renderer's
/// `ModelInfoPayload` shape.  Encodes ids as `"<providerID>::<modelID>"` to
/// match the format the live session parser at
/// `manager/remote/agent.rs::set_model` already understands.
async fn fetch_opencode_model_info(
    url: &str,
    auth_type: RemoteAgentAuthType,
    auth_token: Option<&str>,
    allow_insecure: bool,
) -> Result<ModelInfoPayload, AppError> {
    let base_url = normalize_opencode_base_url(url)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .danger_accept_invalid_certs(allow_insecure)
        .build()
        .map_err(|e| AppError::Internal(format!("Failed to build HTTP client: {e}")))?;
    let response = client
        .get(format!("{base_url}/provider"))
        .headers(build_opencode_auth_headers(auth_type, auth_token)?)
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode provider fetch failed: {e}")))?;

    if !response.status().is_success() {
        return Err(AppError::BadGateway(format!(
            "OpenCode /provider returned {}",
            response.status()
        )));
    }

    let body: serde_json::Value = response
        .json()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode /provider response was not JSON: {e}")))?;

    let connected: std::collections::HashSet<&str> = body
        .get("connected")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();

    let defaults: std::collections::HashMap<&str, &str> = body
        .get("default")
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.as_str(), s)))
                .collect()
        })
        .unwrap_or_default();

    let mut available_models: Vec<ModelInfoEntry> = Vec::new();
    let mut current_model_id: Option<String> = None;
    let mut current_model_label: Option<String> = None;

    if let Some(all) = body.get("all").and_then(|v| v.as_array()) {
        for provider in all {
            let provider_id = match provider.get("id").and_then(|v| v.as_str()) {
                // Only surface models from connected (authenticated) providers
                // when the response includes a `connected` list.  If the list
                // is empty (older OpenCode builds), fall through and include
                // everything.
                Some(id) if connected.is_empty() || connected.contains(id) => id,
                _ => continue,
            };
            let provider_default = defaults.get(provider_id).copied();
            if let Some(models) = provider.get("models").and_then(|v| v.as_object()) {
                for (model_id, model) in models {
                    let label = model.get("name").and_then(|v| v.as_str()).unwrap_or(model_id);
                    let qualified_id = format!("{provider_id}::{model_id}");
                    let qualified_label = format!("[{provider_id}] {label}");
                    if current_model_id.is_none() && provider_default == Some(model_id.as_str()) {
                        current_model_id = Some(qualified_id.clone());
                        current_model_label = Some(qualified_label.clone());
                    }
                    available_models.push(ModelInfoEntry {
                        id: qualified_id,
                        label: qualified_label,
                    });
                }
            }
        }
    }

    if current_model_id.is_none()
        && let Some(first) = available_models.first()
    {
        current_model_id = Some(first.id.clone());
        current_model_label = Some(first.label.clone());
    }

    Ok(ModelInfoPayload {
        current_model_id,
        current_model_label,
        available_models,
    })
}

fn build_opencode_auth_headers(
    auth_type: RemoteAgentAuthType,
    auth_token: Option<&str>,
) -> Result<HeaderMap, AppError> {
    let mut headers = HeaderMap::new();
    let Some(token) = auth_token.filter(|t| !t.is_empty()) else {
        return Ok(headers);
    };

    let value = match auth_type {
        RemoteAgentAuthType::Bearer => format!("Bearer {token}"),
        RemoteAgentAuthType::Password => format!("Basic {}", BASE64.encode(format!("opencode:{token}"))),
        RemoteAgentAuthType::None => return Ok(headers),
    };
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&value).map_err(|e| AppError::BadRequest(format!("Invalid auth token: {e}")))?,
    );
    Ok(headers)
}

fn decrypt_optional_token(token: Option<&str>, key: &[u8; 32]) -> Result<Option<String>, AppError> {
    token.map(|encrypted| decrypt_string(encrypted, key)).transpose()
}

// ── Token masking ───────────────────────────────────────────────

fn mask_token(token: &str) -> String {
    if token.len() <= 4 {
        "***".to_string()
    } else {
        format!("***{}", &token[token.len() - 4..])
    }
}

// ── Ed25519 key generation ──────────────────────────────────────

fn generate_device_keypair(encryption_key: &[u8; 32]) -> Result<(String, String, String), AppError> {
    let mut rng_bytes = [0u8; 32];
    getrandom::getrandom(&mut rng_bytes).map_err(|e| AppError::Internal(format!("RNG failure: {e}")))?;

    let signing_key = SigningKey::from_bytes(&rng_bytes);
    let verifying_key = signing_key.verifying_key();

    let device_id = aionui_common::generate_prefixed_id("dev");

    // Encode keys as base64 before encrypting
    let pub_b64 = BASE64.encode(verifying_key.as_bytes());
    let priv_b64 = BASE64.encode(signing_key.to_bytes());

    let encrypted_pub = encrypt_string(&pub_b64, encryption_key)?;
    let encrypted_priv = encrypt_string(&priv_b64, encryption_key)?;

    Ok((device_id, encrypted_pub, encrypted_priv))
}

// ── Enum serialization helpers ──────────────────────────────────

fn enum_to_str<T: serde::Serialize>(val: &T) -> String {
    serde_json::to_string(val)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string()
}

fn enum_from_str<T: serde::de::DeserializeOwned>(s: &str) -> Option<T> {
    serde_json::from_str(&format!("\"{s}\"")).ok()
}

fn parse_protocol(s: &str) -> RemoteAgentProtocol {
    enum_from_str(s).unwrap_or(RemoteAgentProtocol::Acp)
}

fn parse_auth_type(s: &str) -> RemoteAgentAuthType {
    enum_from_str(s).unwrap_or(RemoteAgentAuthType::None)
}

fn parse_status(s: &str) -> RemoteAgentStatus {
    enum_from_str(s).unwrap_or(RemoteAgentStatus::Unknown)
}

fn db_err(e: aionui_db::DbError) -> AppError {
    AppError::Internal(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mask_token_long() {
        assert_eq!(mask_token("my-secret-token-1234"), "***1234");
    }

    #[test]
    fn mask_token_short() {
        assert_eq!(mask_token("ab"), "***");
    }

    #[test]
    fn mask_token_exactly_four() {
        assert_eq!(mask_token("abcd"), "***");
    }

    #[test]
    fn mask_token_five() {
        assert_eq!(mask_token("abcde"), "***bcde");
    }

    #[test]
    fn validate_ws_url_accepts_ws() {
        assert!(validate_ws_url("ws://localhost:8080").is_ok());
    }

    #[test]
    fn validate_ws_url_accepts_wss() {
        assert!(validate_ws_url("wss://remote.example.com").is_ok());
    }

    #[test]
    fn validate_ws_url_rejects_http() {
        assert!(validate_ws_url("http://example.com").is_err());
    }

    #[test]
    fn validate_ws_url_rejects_https() {
        assert!(validate_ws_url("https://example.com").is_err());
    }

    #[test]
    fn validate_http_url_accepts_http() {
        assert!(validate_http_url("http://127.0.0.1:4096").is_ok());
    }

    #[test]
    fn validate_http_url_accepts_bare_host_port() {
        assert!(validate_http_url("127.0.0.1:4096").is_ok());
    }

    #[test]
    fn validate_http_url_rejects_websocket() {
        assert!(validate_http_url("wss://example.com/gateway").is_err());
    }

    #[test]
    fn normalize_opencode_base_url_trims_and_defaults_to_http() {
        assert_eq!(
            normalize_opencode_base_url(" 127.0.0.1:4096/ ").unwrap(),
            "http://127.0.0.1:4096"
        );
    }

    #[test]
    fn generate_device_keypair_produces_valid_output() {
        let key = [0x42u8; 32];
        let (id, pub_key, priv_key) = generate_device_keypair(&key).unwrap();

        assert!(id.starts_with("dev_"));
        assert!(!pub_key.is_empty());
        assert!(!priv_key.is_empty());

        // Decrypt and verify the keys decode correctly
        let pub_b64 = decrypt_string(&pub_key, &key).unwrap();
        let priv_b64 = decrypt_string(&priv_key, &key).unwrap();

        let pub_bytes = BASE64.decode(&pub_b64).unwrap();
        let priv_bytes = BASE64.decode(&priv_b64).unwrap();
        assert_eq!(pub_bytes.len(), 32);
        assert_eq!(priv_bytes.len(), 32);

        // Verify the keypair is consistent
        let signing = SigningKey::from_bytes(&priv_bytes.try_into().unwrap());
        let verifying = signing.verifying_key();
        assert_eq!(verifying.as_bytes(), pub_bytes.as_slice());
    }

    #[test]
    fn enum_to_str_protocol() {
        assert_eq!(enum_to_str(&RemoteAgentProtocol::OpenClaw), "openclaw");
        assert_eq!(enum_to_str(&RemoteAgentProtocol::OpenCode), "opencode");
        assert_eq!(enum_to_str(&RemoteAgentProtocol::ZeroClaw), "zeroclaw");
        assert_eq!(enum_to_str(&RemoteAgentProtocol::Acp), "acp");
    }

    #[test]
    fn enum_to_str_auth_type() {
        assert_eq!(enum_to_str(&RemoteAgentAuthType::Bearer), "bearer");
        assert_eq!(enum_to_str(&RemoteAgentAuthType::Password), "password");
        assert_eq!(enum_to_str(&RemoteAgentAuthType::None), "none");
    }

    #[test]
    fn parse_protocol_known_values() {
        assert_eq!(parse_protocol("openclaw"), RemoteAgentProtocol::OpenClaw);
        assert_eq!(parse_protocol("opencode"), RemoteAgentProtocol::OpenCode);
        assert_eq!(parse_protocol("zeroclaw"), RemoteAgentProtocol::ZeroClaw);
        assert_eq!(parse_protocol("acp"), RemoteAgentProtocol::Acp);
    }

    #[test]
    fn build_opencode_password_auth_uses_basic_with_default_username() {
        let headers = build_opencode_auth_headers(RemoteAgentAuthType::Password, Some("secret")).unwrap();

        assert_eq!(
            headers.get(AUTHORIZATION).unwrap().to_str().unwrap(),
            format!("Basic {}", BASE64.encode("opencode:secret"))
        );
    }

    #[test]
    fn parse_protocol_unknown_defaults() {
        assert_eq!(parse_protocol("unknown_proto"), RemoteAgentProtocol::Acp);
    }

    #[test]
    fn parse_auth_type_known_values() {
        assert_eq!(parse_auth_type("bearer"), RemoteAgentAuthType::Bearer);
        assert_eq!(parse_auth_type("password"), RemoteAgentAuthType::Password);
        assert_eq!(parse_auth_type("none"), RemoteAgentAuthType::None);
    }

    #[test]
    fn parse_status_known_values() {
        assert_eq!(parse_status("unknown"), RemoteAgentStatus::Unknown);
        assert_eq!(parse_status("connected"), RemoteAgentStatus::Connected);
        assert_eq!(parse_status("pending"), RemoteAgentStatus::Pending);
        assert_eq!(parse_status("error"), RemoteAgentStatus::Error);
    }
}
