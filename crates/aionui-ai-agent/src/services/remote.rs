use std::sync::Arc;
use std::time::Duration;

use aionui_api_types::{
    CreateRemoteAgentRequest, HandshakeResponse, ModelInfoPayload, RemoteAgentListItem, RemoteAgentResponse,
    RemoteSessionInfo, RemoteSkillInfo, TestRemoteAgentConnectionRequest, UpdateRemoteAgentRequest,
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
                tool_host: Some(req.tool_host.as_str()),
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
            tool_host: req.tool_host.as_deref(),
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

    /// Fetch the OpenCode agent catalog (`GET /agent`) and map it to the
    /// selectable session modes the renderer's mode picker consumes.  Used by
    /// the Guid (New Chat) page, which has no conversation/agent task yet and
    /// therefore cannot go through the per-conversation `/mode` endpoint.
    ///
    /// Mirrors [`fetch_models`]: reads the row, decrypts the auth token (the
    /// plaintext never leaves the Rust process), and returns the
    /// `AgentModeOption` list with `build`/`plan` defaults merged in.
    pub async fn fetch_agents(&self, id: &str) -> Result<Vec<aionui_api_types::AgentModeOption>, AppError> {
        let row = self
            .repo
            .find_by_id(id)
            .await
            .map_err(db_err)?
            .ok_or_else(|| AppError::NotFound(format!("Remote agent '{id}' not found")))?;

        let protocol = parse_protocol(&row.protocol);
        if protocol != RemoteAgentProtocol::OpenCode {
            return Err(AppError::BadRequest(
                "Agent list is only supported for OpenCode remote agents".into(),
            ));
        }

        let auth_type = parse_auth_type(&row.auth_type);
        let auth_token = decrypt_optional_token(row.auth_token.as_deref(), &self.encryption_key)?;
        fetch_opencode_agents(&row.url, auth_type, auth_token.as_deref(), row.allow_insecure).await
    }

    /// M10: fetch the OpenCode skill catalog (`GET /skill`) and map it to the
    /// selectable list the renderer's skill picker on the Guid (New Chat) page
    /// consumes. Used before any conversation is created — so we cannot go
    /// through the per-conversation `/skills` endpoint. Mirrors
    /// [`fetch_agents`]: reads the row, decrypts the auth token (the
    /// plaintext never leaves the Rust process), and returns a normalised
    /// skill list. OpenCode-only.
    pub async fn fetch_skills(&self, id: &str) -> Result<Vec<RemoteSkillInfo>, AppError> {
        let row = self
            .repo
            .find_by_id(id)
            .await
            .map_err(db_err)?
            .ok_or_else(|| AppError::NotFound(format!("Remote agent '{id}' not found")))?;

        let protocol = parse_protocol(&row.protocol);
        if protocol != RemoteAgentProtocol::OpenCode {
            return Err(AppError::BadRequest(
                "Skill list is only supported for OpenCode remote agents".into(),
            ));
        }

        let auth_type = parse_auth_type(&row.auth_type);
        let auth_token = decrypt_optional_token(row.auth_token.as_deref(), &self.encryption_key)?;
        fetch_opencode_skills(&row.url, auth_type, auth_token.as_deref(), row.allow_insecure).await
    }

    /// List active sessions on a remote OpenCode agent.  Proxies the
    /// upstream `GET /session` call so the renderer can offer an
    /// "Attach to existing session" picker (Phase 4 cross-device
    /// handoff).  Mirrors [`fetch_models`]: reads the row, decrypts the
    /// auth token (plaintext never leaves the Rust process), and
    /// returns a normalised list.
    ///
    /// OpenCode-only.  Other protocols return `BadRequest` rather than
    /// silently returning an empty list, so the UI can surface a
    /// meaningful error.
    pub async fn list_sessions(&self, id: &str) -> Result<Vec<RemoteSessionInfo>, AppError> {
        let row = self
            .repo
            .find_by_id(id)
            .await
            .map_err(db_err)?
            .ok_or_else(|| AppError::NotFound(format!("Remote agent '{id}' not found")))?;

        let protocol = parse_protocol(&row.protocol);
        if protocol != RemoteAgentProtocol::OpenCode {
            return Err(AppError::BadRequest(
                "Session list is only supported for OpenCode remote agents".into(),
            ));
        }

        let auth_type = parse_auth_type(&row.auth_type);
        let auth_token = decrypt_optional_token(row.auth_token.as_deref(), &self.encryption_key)?;
        fetch_opencode_sessions(&row.url, auth_type, auth_token.as_deref(), row.allow_insecure).await
    }

    /// Fetch the historical message transcript of a remote OpenCode
    /// session and convert it into Chisl `MessageRow` entries ready to
    /// insert under `conversation_id`. Phase 4b backfill: the first
    /// time the user opens a sync-discovered conversation, we hit
    /// `GET /session/{id}/message` once and write the rows so the
    /// chat view shows the prior turns.
    ///
    /// Returns the rows in chronological order. Caller is responsible
    /// for persisting them (the service layer deliberately doesn't
    /// hold a conversation repo).
    pub async fn fetch_session_messages(
        &self,
        remote_agent_id: &str,
        conversation_id: &str,
        session_id: &str,
    ) -> Result<Vec<aionui_db::models::MessageRow>, AppError> {
        let row = self
            .repo
            .find_by_id(remote_agent_id)
            .await
            .map_err(db_err)?
            .ok_or_else(|| AppError::NotFound(format!("Remote agent '{remote_agent_id}' not found")))?;

        let protocol = parse_protocol(&row.protocol);
        if protocol != RemoteAgentProtocol::OpenCode {
            return Err(AppError::BadRequest(
                "History backfill is only supported for OpenCode remote agents".into(),
            ));
        }

        let auth_type = parse_auth_type(&row.auth_type);
        let auth_token = decrypt_optional_token(row.auth_token.as_deref(), &self.encryption_key)?;
        fetch_opencode_messages(
            &row.url,
            auth_type,
            auth_token.as_deref(),
            row.allow_insecure,
            session_id,
            conversation_id,
        )
        .await
    }

    /// A02: lightweight health probe. Calls `GET /global/health` on the
    /// upstream server and returns `{ healthy, latency_ms, error? }` without
    /// updating the agent row's status (so the 60 s poll can't flap the
    /// handshake-driven status indicator).
    pub async fn ping_health(&self, id: &str) -> Result<aionui_api_types::RemoteAgentHealthResponse, AppError> {
        let row = self
            .repo
            .find_by_id(id)
            .await
            .map_err(db_err)?
            .ok_or_else(|| AppError::NotFound(format!("Remote agent '{id}' not found")))?;

        let protocol = parse_protocol(&row.protocol);
        let start = std::time::Instant::now();

        match protocol {
            RemoteAgentProtocol::OpenCode => {
                let auth_type = parse_auth_type(&row.auth_type);
                let auth_token = decrypt_optional_token(row.auth_token.as_deref(), &self.encryption_key)?;
                let result = test_opencode_health(&row.url, auth_type, auth_token.as_deref(), row.allow_insecure).await;
                let latency_ms = start.elapsed().as_millis() as u64;
                match result {
                    Ok(()) => Ok(aionui_api_types::RemoteAgentHealthResponse {
                        healthy: true,
                        latency_ms,
                        error: None,
                    }),
                    Err(e) => Ok(aionui_api_types::RemoteAgentHealthResponse {
                        healthy: false,
                        latency_ms,
                        error: Some(e.to_string()),
                    }),
                }
            }
            RemoteAgentProtocol::OpenClaw | RemoteAgentProtocol::Acp => {
                let result = test_websocket_connection(&row.url).await;
                let latency_ms = start.elapsed().as_millis() as u64;
                match result {
                    Ok(()) => Ok(aionui_api_types::RemoteAgentHealthResponse {
                        healthy: true,
                        latency_ms,
                        error: None,
                    }),
                    Err(e) => Ok(aionui_api_types::RemoteAgentHealthResponse {
                        healthy: false,
                        latency_ms,
                        error: Some(e.to_string()),
                    }),
                }
            }
            RemoteAgentProtocol::ZeroClaw => Ok(aionui_api_types::RemoteAgentHealthResponse {
                healthy: false,
                latency_ms: 0,
                error: Some("ZeroClaw health probe is not supported".into()),
            }),
        }
    }

    /// Propagate a rename / archive of an OpenCode-bound conversation to its
    /// server session (M06) via `PATCH /session/{sessionID}`. Best-effort: a
    /// no-op patch (`title` and `archived` both `None`) returns `Ok(())`
    /// without a network call. Only valid for OpenCode remote agents.
    pub async fn update_session(
        &self,
        remote_agent_id: &str,
        session_id: &str,
        patch: RemoteSessionPatch,
    ) -> Result<(), AppError> {
        let row = self
            .repo
            .find_by_id(remote_agent_id)
            .await
            .map_err(db_err)?
            .ok_or_else(|| AppError::NotFound(format!("Remote agent '{remote_agent_id}' not found")))?;

        let protocol = parse_protocol(&row.protocol);
        if protocol != RemoteAgentProtocol::OpenCode {
            return Err(AppError::BadRequest(
                "Session update is only supported for OpenCode remote agents".into(),
            ));
        }

        let Some(body) = build_session_patch_body(&patch, aionui_common::now_ms()) else {
            return Ok(());
        };

        let auth_type = parse_auth_type(&row.auth_type);
        let auth_token = decrypt_optional_token(row.auth_token.as_deref(), &self.encryption_key)?;
        patch_opencode_session(
            &row.url,
            auth_type,
            auth_token.as_deref(),
            row.allow_insecure,
            session_id,
            &body,
        )
        .await
    }

    /// M12: list OpenCode providers with auth state for the settings UI (`GET /provider`, §9).
    pub async fn fetch_provider_catalog(&self, remote_agent_id: &str) -> Result<serde_json::Value, AppError> {
        let (client, cfg) = self.opencode_client_config(remote_agent_id).await?;
        crate::manager::remote::opencode_provider_auth::list_providers(&client, &cfg).await
    }

    /// M12: auth methods per provider (`GET /provider/auth`, §8).
    pub async fn fetch_provider_auth_methods(&self, remote_agent_id: &str) -> Result<serde_json::Value, AppError> {
        let (client, cfg) = self.opencode_client_config(remote_agent_id).await?;
        crate::manager::remote::opencode_provider_auth::list_provider_auth_methods(&client, &cfg).await
    }

    /// M12: set provider API credentials (`PUT /auth/{id}` with `ApiAuth`, §8).
    pub async fn set_provider_credentials(
        &self,
        remote_agent_id: &str,
        provider_id: &str,
        api_key: &str,
    ) -> Result<(), AppError> {
        let (client, cfg) = self.opencode_client_config(remote_agent_id).await?;
        crate::manager::remote::opencode_provider_auth::set_api_key(&client, &cfg, provider_id, api_key).await
    }

    /// M12: set arbitrary provider auth payload (`PUT /auth/{id}`, §8 `Auth` union).
    pub async fn set_provider_auth_payload(
        &self,
        remote_agent_id: &str,
        provider_id: &str,
        payload: serde_json::Value,
    ) -> Result<(), AppError> {
        let (client, cfg) = self.opencode_client_config(remote_agent_id).await?;
        crate::manager::remote::opencode_provider_auth::set_provider_auth(&client, &cfg, provider_id, payload).await
    }

    /// M12: set WellKnown credentials (`PUT /auth/{id}` with `WellKnownAuth`, §8).
    pub async fn set_provider_wellknown(
        &self,
        remote_agent_id: &str,
        provider_id: &str,
        key: &str,
        token: &str,
    ) -> Result<(), AppError> {
        let (client, cfg) = self.opencode_client_config(remote_agent_id).await?;
        crate::manager::remote::opencode_provider_auth::set_wellknown_auth(&client, &cfg, provider_id, key, token).await
    }

    /// M12: clear provider credentials on the remote OpenCode server.
    pub async fn delete_provider_credentials(&self, remote_agent_id: &str, provider_id: &str) -> Result<(), AppError> {
        let (client, cfg) = self.opencode_client_config(remote_agent_id).await?;
        crate::manager::remote::opencode_provider_auth::delete_provider_auth(&client, &cfg, provider_id).await
    }

    /// M12: start provider OAuth — returns `ProviderAuthAuthorization` (§8).
    pub async fn start_provider_oauth(
        &self,
        remote_agent_id: &str,
        provider_id: &str,
        method_index: u32,
        inputs: Option<std::collections::HashMap<String, String>>,
    ) -> Result<serde_json::Value, AppError> {
        let (client, cfg) = self.opencode_client_config(remote_agent_id).await?;
        crate::manager::remote::opencode_provider_auth::start_provider_oauth(
            &client,
            &cfg,
            provider_id,
            method_index,
            inputs.as_ref(),
        )
        .await
    }

    /// M12: complete provider OAuth (`POST .../oauth/callback`, §8).
    pub async fn complete_provider_oauth(
        &self,
        remote_agent_id: &str,
        provider_id: &str,
        method_index: u32,
        code: Option<&str>,
    ) -> Result<(), AppError> {
        let (client, cfg) = self.opencode_client_config(remote_agent_id).await?;
        crate::manager::remote::opencode_provider_auth::complete_provider_oauth(
            &client,
            &cfg,
            provider_id,
            method_index,
            code,
        )
        .await
    }

    // ── Private helpers ──────────────────────────────────────────

    async fn opencode_client_config(
        &self,
        remote_agent_id: &str,
    ) -> Result<(reqwest::Client, crate::manager::remote::RemoteAgentConfig), AppError> {
        let row = self
            .repo
            .find_by_id(remote_agent_id)
            .await
            .map_err(db_err)?
            .ok_or_else(|| AppError::NotFound(format!("Remote agent '{remote_agent_id}' not found")))?;
        if parse_protocol(&row.protocol) != RemoteAgentProtocol::OpenCode {
            return Err(AppError::BadRequest(
                "Provider auth is only supported for OpenCode remote agents".into(),
            ));
        }
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .danger_accept_invalid_certs(row.allow_insecure)
            .build()
            .map_err(|e| AppError::Internal(format!("Failed to build HTTP client: {e}")))?;
        let cfg = crate::manager::remote::RemoteAgentConfig {
            remote_agent_id: row.id.clone(),
            protocol: row.protocol.clone(),
            url: row.url.clone(),
            auth_type: row.auth_type.clone(),
            auth_token: decrypt_optional_token(row.auth_token.as_deref(), &self.encryption_key)?,
            allow_insecure: row.allow_insecure,
            tool_host: row.tool_host.clone(),
        };
        Ok((client, cfg))
    }

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
            tool_host: row.tool_host,
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
            tool_host: row.tool_host,
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

/// Stable machine-readable code for a connect failure, embedded in the
/// error message as a `[code:<x>]` prefix that the renderer parses to
/// show an actionable, localized message (Bet A5).
///
/// The classifier is pure-text: reqwest/hyper errors carry the DNS / TLS /
/// IO detail in the `source()` chain rather than `Display`, so callers
/// pass the flattened chain (see [`error_chain_text`]) rather than the
/// top-level `Display`. The order of branches matters — DNS / TLS markers
/// are checked first so a `dns error: ... self-signed certificate` chain
/// classifies as DNS, not TLS, matching the user-visible root cause.
pub(crate) fn connect_error_code_from_text(detail: &str) -> &'static str {
    let d = detail.to_ascii_lowercase();
    if d.contains("dns error")
        || d.contains("failed to lookup address")
        || d.contains("name or service not known")
        || d.contains("nodename nor servname")
        || d.contains("no such host")
    {
        "dns_failure"
    } else if d.contains("certificate")
        || d.contains("self-signed")
        || d.contains("self signed")
        || d.contains("unknownissuer")
        || d.contains("tls")
        || d.contains("ssl")
    {
        "tls_failure"
    } else if d.contains("connection refused") {
        "connection_refused"
    } else if d.contains("timed out") || d.contains("timeout") {
        "timeout"
    } else {
        "unreachable"
    }
}

/// Flatten an error and its full `source()` chain into one string —
/// reqwest's Display often hides the io/dns/tls detail in the chain.
fn error_chain_text(e: &(dyn std::error::Error + 'static)) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let _ = write!(out, "{e}");
    let mut current = e.source();
    while let Some(cause) = current {
        let _ = write!(out, " | {cause}");
        current = cause.source();
    }
    out
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
    let response = match client
        .get(format!("{base_url}/global/health"))
        .headers(build_opencode_auth_headers(auth_type, auth_token)?)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            let chain = error_chain_text(&e);
            let code = if e.is_timeout() {
                "timeout"
            } else {
                connect_error_code_from_text(&chain)
            };
            return Err(AppError::BadGateway(format!(
                "[code:{code}] OpenCode health check failed: {chain}"
            )));
        }
    };

    if !response.status().is_success() {
        let status = response.status();
        let code = match status.as_u16() {
            401 | 403 => "auth_failure",
            404 => "not_opencode",
            _ => "server_error",
        };
        let suffix = if code == "not_opencode" {
            " (endpoint does not look like an OpenCode server: /global/health missing)"
        } else {
            ""
        };
        return Err(AppError::BadGateway(format!(
            "[code:{code}] OpenCode health check failed: {status}{suffix}"
        )));
    }

    let body: serde_json::Value = match response.json().await {
        Ok(v) => v,
        Err(_) => {
            return Err(AppError::BadGateway(
                "[code:not_opencode] OpenCode health check failed: endpoint did not return OpenCode health JSON".into(),
            ));
        }
    };
    if body.get("healthy").and_then(|v| v.as_bool()) == Some(true) {
        return Ok(());
    }

    Err(AppError::BadGateway(
        "[code:server_error] OpenCode health check failed: server reports unhealthy".into(),
    ))
}

/// Fetch the OpenCode `/agent` catalog and map it to selectable session
/// modes. Reuses the same primary/all + hidden filtering the live session
/// path applies (see `manager/remote/agent.rs::parse_opencode_agent_modes`).
/// On any failure returns the `build`/`plan` defaults so the picker is never
/// empty.
async fn fetch_opencode_agents(
    url: &str,
    auth_type: RemoteAgentAuthType,
    auth_token: Option<&str>,
    allow_insecure: bool,
) -> Result<Vec<aionui_api_types::AgentModeOption>, AppError> {
    use crate::manager::remote::agent::{default_opencode_agent_modes, parse_opencode_agent_modes};

    let base_url = normalize_opencode_base_url(url)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .danger_accept_invalid_certs(allow_insecure)
        .build()
        .map_err(|e| AppError::Internal(format!("Failed to build HTTP client: {e}")))?;
    let response = client
        .get(format!("{base_url}/agent"))
        .headers(build_opencode_auth_headers(auth_type, auth_token)?)
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode agent fetch failed: {e}")))?;

    if !response.status().is_success() {
        // Older builds without `/agent` (or a transient error): fall back to
        // the canonical defaults rather than failing the New Chat page.
        warn!("OpenCode agent fetch returned {}", response.status());
        return Ok(default_opencode_agent_modes());
    }

    let body: serde_json::Value = response
        .json()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode agent response was not JSON: {e}")))?;
    Ok(parse_opencode_agent_modes(&body))
}

/// Fetch the OpenCode `/skill` catalog and map it to the selectable skill
/// list the renderer's Guid-page skill picker consumes. Mirrors
/// [`fetch_opencode_agents`]: returns an empty list (not an error) on
/// transient failure so the New Chat page degrades gracefully when the skill
/// catalog is unreachable or the server is an older build without `/skill`.
async fn fetch_opencode_skills(
    url: &str,
    auth_type: RemoteAgentAuthType,
    auth_token: Option<&str>,
    allow_insecure: bool,
) -> Result<Vec<RemoteSkillInfo>, AppError> {
    use crate::manager::remote::agent::parse_opencode_skills;

    let base_url = normalize_opencode_base_url(url)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .danger_accept_invalid_certs(allow_insecure)
        .build()
        .map_err(|e| AppError::Internal(format!("Failed to build HTTP client: {e}")))?;
    let response = client
        .get(format!("{base_url}/skill"))
        .headers(build_opencode_auth_headers(auth_type, auth_token)?)
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode skill fetch failed: {e}")))?;

    if !response.status().is_success() {
        warn!("OpenCode skill fetch returned {}", response.status());
        return Ok(Vec::new());
    }

    let body: serde_json::Value = response
        .json()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode skill response was not JSON: {e}")))?;
    Ok(parse_opencode_skills(&body))
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

    let available_models = crate::manager::remote::opencode_models::parse_provider_model_entries(&body);
    let mut current_model_id: Option<String> = None;
    let mut current_model_label: Option<String> = None;

    if let Some(all) = body.get("all").and_then(|v| v.as_array()) {
        for provider in all {
            let provider_id = match provider.get("id").and_then(|v| v.as_str()) {
                Some(id) if connected.is_empty() || connected.contains(id) => id,
                _ => continue,
            };
            let provider_default = defaults.get(provider_id).copied();
            if let Some(models) = provider.get("models").and_then(|v| v.as_object()) {
                for (model_id, model) in models {
                    if current_model_id.is_none() && provider_default == Some(model_id.as_str()) {
                        let qualified_id = format!("{provider_id}::{model_id}");
                        let label = model.get("name").and_then(|v| v.as_str()).unwrap_or(model_id);
                        current_model_id = Some(qualified_id);
                        current_model_label = Some(format!("[{provider_id}] {label}"));
                    }
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

/// Fetch the OpenCode `/session` listing and normalise it into
/// `RemoteSessionInfo` rows. OpenCode returns an array of session
/// objects whose interesting fields are `id`, `title`, and
/// `time.updated` (timestamp). Older builds may return `updated_at`
/// at the top level; both shapes are accepted.
/// Patch fields for a remote OpenCode session (M06). All optional; only
/// present fields are sent. `archived: Some(true)` sets `time.archived` to the
/// current ms timestamp; `Some(false)` clears it (`0`); `None` leaves the
/// server's archive state untouched.
#[derive(Debug, Clone, Default)]
pub struct RemoteSessionPatch {
    pub title: Option<String>,
    pub archived: Option<bool>,
}

/// Build the JSON body for `PATCH /session/{id}` from a [`RemoteSessionPatch`].
/// Returns `None` when there is nothing to send (caller skips the request).
fn build_session_patch_body(patch: &RemoteSessionPatch, now_ms: i64) -> Option<serde_json::Value> {
    let mut body = serde_json::Map::new();
    if let Some(title) = &patch.title {
        body.insert("title".to_string(), serde_json::Value::String(title.clone()));
    }
    if let Some(archived) = patch.archived {
        let ts = if archived { now_ms } else { 0 };
        body.insert("time".to_string(), serde_json::json!({ "archived": ts }));
    }
    if body.is_empty() {
        None
    } else {
        Some(serde_json::Value::Object(body))
    }
}

/// `PATCH /session/{sessionID}` with the given body (M06). Best-effort title /
/// archive sync — mirrors [`fetch_opencode_sessions`]'s client construction.
async fn patch_opencode_session(
    url: &str,
    auth_type: RemoteAgentAuthType,
    auth_token: Option<&str>,
    allow_insecure: bool,
    session_id: &str,
    body: &serde_json::Value,
) -> Result<(), AppError> {
    let base_url = normalize_opencode_base_url(url)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .danger_accept_invalid_certs(allow_insecure)
        .build()
        .map_err(|e| AppError::Internal(format!("Failed to build HTTP client: {e}")))?;
    let response = client
        .patch(format!("{base_url}/session/{session_id}"))
        .headers(build_opencode_auth_headers(auth_type, auth_token)?)
        .json(body)
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode session update failed: {e}")))?;

    if !response.status().is_success() {
        return Err(AppError::BadGateway(format!(
            "OpenCode PATCH /session/{session_id} returned {}",
            response.status()
        )));
    }
    Ok(())
}

async fn fetch_opencode_sessions(
    url: &str,
    auth_type: RemoteAgentAuthType,
    auth_token: Option<&str>,
    allow_insecure: bool,
) -> Result<Vec<RemoteSessionInfo>, AppError> {
    let base_url = normalize_opencode_base_url(url)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .danger_accept_invalid_certs(allow_insecure)
        .build()
        .map_err(|e| AppError::Internal(format!("Failed to build HTTP client: {e}")))?;
    let response = client
        .get(format!("{base_url}/session"))
        .headers(build_opencode_auth_headers(auth_type, auth_token)?)
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode session list failed: {e}")))?;

    if !response.status().is_success() {
        return Err(AppError::BadGateway(format!(
            "OpenCode /session returned {}",
            response.status()
        )));
    }

    let body: serde_json::Value = response
        .json()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode /session response was not JSON: {e}")))?;

    let array = body
        .as_array()
        .ok_or_else(|| AppError::BadGateway("OpenCode /session response was not a JSON array".into()))?;

    let mut sessions: Vec<RemoteSessionInfo> = array.iter().filter_map(parse_opencode_session).collect();

    // Most recent first so the "Attach" picker surfaces the user's
    // current work without scrolling. Sessions without a timestamp
    // (older builds) sort to the bottom.
    sessions.sort_by_key(|s| std::cmp::Reverse(s.updated_at.unwrap_or(0)));
    Ok(sessions)
}

/// Extract `{ id, title, directory, created_at, updated_at }` from a
/// single OpenCode session object. Returns `None` if `id` is missing —
/// every other field is optional. `time.created` / `time.updated` may
/// be expressed in seconds (older builds) or milliseconds (current); we
/// lift the raw number and only normalise to ms when the value looks
/// like a seconds-since-epoch.
fn parse_opencode_session(value: &serde_json::Value) -> Option<RemoteSessionInfo> {
    let id = value.get("id").and_then(|v| v.as_str())?.to_string();
    let title = value.get("title").and_then(|v| v.as_str()).map(String::from);
    let directory = value.get("directory").and_then(|v| v.as_str()).map(String::from);

    let raw_updated = value
        .get("time")
        .and_then(|t| t.get("updated"))
        .and_then(|v| v.as_f64())
        .or_else(|| value.get("updated_at").and_then(|v| v.as_f64()))
        .or_else(|| value.get("updatedAt").and_then(|v| v.as_f64()));
    let raw_created = value
        .get("time")
        .and_then(|t| t.get("created"))
        .and_then(|v| v.as_f64())
        .or_else(|| value.get("created_at").and_then(|v| v.as_f64()))
        .or_else(|| value.get("createdAt").and_then(|v| v.as_f64()));

    Some(RemoteSessionInfo {
        id,
        title,
        directory,
        created_at: raw_created.map(normalize_ms),
        updated_at: raw_updated.map(normalize_ms),
    })
}

/// OpenCode currently emits ms-since-epoch but some older builds used
/// seconds. Anything below ~10^11 is treated as seconds and scaled. The
/// threshold (Sat Mar 03 1973 in ms) is comfortably below any plausible
/// real session time in ms and above any plausible time in seconds.
fn normalize_ms(n: f64) -> i64 {
    let as_int = n as i64;
    if as_int < 100_000_000_000 {
        as_int * 1000
    } else {
        as_int
    }
}

/// Fetch the OpenCode `/session/{id}/message` listing and convert each
/// `{info, parts}` into one or more Chisl `MessageRow` entries.
///
/// Conversion rules (matching what `stream_relay.rs` writes for live
/// turns so historical and live messages render identically):
///
/// - `user` role + `text` parts → one `text` row per text part,
///   `position = "right"`, `content = {content: <text>}`.
/// - `assistant` role + `text` parts → `text` row, `position = "left"`,
///   `content = {content: <text>, model: {...}}` when model info present.
/// - `assistant` role + `reasoning` parts → `thinking` row,
///   `content = {content, status: "done", duration_ms}`.
/// - `assistant` role + `tool` parts → `tool_call` row,
///   `content = ToolCallEventData` JSON shape (callID, name, args,
///   status, input, output).
/// - `step-start` / `step-finish` parts are skipped — they are flow
///   markers without user-visible content.
/// - `retry` parts → lightweight `opencode_retry` rows preserving part order.
async fn fetch_opencode_messages(
    url: &str,
    auth_type: RemoteAgentAuthType,
    auth_token: Option<&str>,
    allow_insecure: bool,
    session_id: &str,
    conversation_id: &str,
) -> Result<Vec<aionui_db::models::MessageRow>, AppError> {
    let base_url = normalize_opencode_base_url(url)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .danger_accept_invalid_certs(allow_insecure)
        .build()
        .map_err(|e| AppError::Internal(format!("Failed to build HTTP client: {e}")))?;
    let response = client
        .get(format!("{base_url}/session/{session_id}/message"))
        .headers(build_opencode_auth_headers(auth_type, auth_token)?)
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode message fetch failed: {e}")))?;

    if !response.status().is_success() {
        return Err(AppError::BadGateway(format!(
            "OpenCode /session/{session_id}/message returned {}",
            response.status()
        )));
    }

    let body: serde_json::Value = response
        .json()
        .await
        .map_err(|e| AppError::BadGateway(format!("OpenCode message response was not JSON: {e}")))?;
    let array = body
        .as_array()
        .ok_or_else(|| AppError::BadGateway("OpenCode message response was not a JSON array".into()))?;

    Ok(convert_opencode_messages(conversation_id, array))
}

/// Pure conversion (no I/O) so tests can drive it with synthetic JSON.
pub(crate) fn convert_opencode_messages(
    conversation_id: &str,
    array: &[serde_json::Value],
) -> Vec<aionui_db::models::MessageRow> {
    let mut rows = Vec::new();
    for msg in array {
        let info = match msg.get("info") {
            Some(v) => v,
            None => continue,
        };
        let role = info.get("role").and_then(|v| v.as_str()).unwrap_or("");
        let is_user = role == "user";
        let is_assistant = role == "assistant";
        if !is_user && !is_assistant {
            continue;
        }
        let opencode_message_id = info.get("id").and_then(|v| v.as_str()).map(String::from);
        let base_created = info
            .get("time")
            .and_then(|t| t.get("created"))
            .and_then(|v| v.as_f64())
            .map(normalize_ms)
            .unwrap_or_else(aionui_common::now_ms);
        let model = if is_assistant {
            extract_assistant_model(info)
        } else {
            None
        };

        let parts = match msg.get("parts").and_then(|v| v.as_array()) {
            Some(p) => p,
            None => continue,
        };
        for (i, part) in parts.iter().enumerate() {
            // Each part within a message gets a slight created_at bump so
            // it sorts after the previous one — OpenCode itself emits
            // parts in order but doesn't always stamp every one with a
            // distinct timestamp.
            let created_at = base_created + i as i64;
            let part_type = part.get("type").and_then(|v| v.as_str()).unwrap_or("");
            let part_id = part.get("id").and_then(|v| v.as_str()).map(String::from);
            match part_type {
                "text" => {
                    let Some(text) = part.get("text").and_then(|v| v.as_str()) else {
                        continue;
                    };
                    if text.is_empty() {
                        continue;
                    }
                    rows.push(build_text_row(
                        conversation_id,
                        part,
                        text,
                        is_user,
                        model.as_ref(),
                        created_at,
                        OpencodeIds {
                            message_id: opencode_message_id.as_deref(),
                            part_id: part_id.as_deref(),
                        },
                    ));
                }
                "reasoning" if is_assistant => {
                    let Some(text) = part.get("text").and_then(|v| v.as_str()) else {
                        continue;
                    };
                    if text.is_empty() {
                        continue;
                    }
                    rows.push(build_thinking_row(
                        conversation_id,
                        part,
                        text,
                        created_at,
                        OpencodeIds {
                            message_id: opencode_message_id.as_deref(),
                            part_id: part_id.as_deref(),
                        },
                    ));
                }
                "tool" if is_assistant => {
                    rows.push(build_tool_call_row(
                        conversation_id,
                        part,
                        created_at,
                        OpencodeIds {
                            message_id: opencode_message_id.as_deref(),
                            part_id: part_id.as_deref(),
                        },
                    ));
                }
                "retry" if is_assistant => {
                    if let Some(row) = build_retry_row(
                        conversation_id,
                        part,
                        created_at,
                        opencode_message_id.as_deref().unwrap_or(""),
                        part_id.as_deref().unwrap_or(""),
                    ) {
                        rows.push(row);
                    }
                }
                // step-start / step-finish carry no user-visible payload.
                _ => continue,
            }
        }
    }
    rows
}

fn build_retry_row(
    conversation_id: &str,
    part: &serde_json::Value,
    created_at: i64,
    message_id: &str,
    part_id: &str,
) -> Option<aionui_db::models::MessageRow> {
    if message_id.is_empty() || part_id.is_empty() {
        return None;
    }
    let reason = part
        .get("reason")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let attempt = part.get("attempt").and_then(|v| v.as_u64()).unwrap_or(1);
    let content = serde_json::json!({
        "message_id": message_id,
        "part_id": part_id,
        "attempt": attempt,
        "reason": reason,
        "retry_after": part.get("retryAfter").or_else(|| part.get("retry_after")).and_then(|v| v.as_u64()),
        "provider_hint": part.get("providerHint").or_else(|| part.get("provider_hint")).and_then(|v| v.as_str()),
        "replay": true,
    });
    Some(aionui_db::models::MessageRow {
        id: part_id.to_string(),
        conversation_id: conversation_id.to_string(),
        msg_id: Some(part_id.to_string()),
        r#type: "opencode_retry".into(),
        content: content.to_string(),
        position: Some("left".into()),
        status: Some("finish".into()),
        hidden: false,
        created_at,
    })
}

fn extract_assistant_model(info: &serde_json::Value) -> Option<(String, String)> {
    // OpenCode emits either flat `providerID`/`modelID` (live builds)
    // or nested `model: { providerID, modelID }`. Accept both.
    let provider = info.get("providerID").and_then(|v| v.as_str()).or_else(|| {
        info.get("model")
            .and_then(|m| m.get("providerID"))
            .and_then(|v| v.as_str())
    })?;
    let model = info.get("modelID").and_then(|v| v.as_str()).or_else(|| {
        info.get("model")
            .and_then(|m| m.get("modelID"))
            .and_then(|v| v.as_str())
    })?;
    Some((provider.to_string(), model.to_string()))
}

/// Bundle of OpenCode identifiers stamped into a backfilled row's
/// `content._opencode`. Lets M01/M02 (fork/revert) and M07 (edit/delete)
/// resolve a local row back to its server-side message and part ids the same
/// way live-streamed rows carry them (see `stream_relay::build_text_content_json`).
#[derive(Clone, Copy, Default)]
struct OpencodeIds<'a> {
    message_id: Option<&'a str>,
    part_id: Option<&'a str>,
}

impl OpencodeIds<'_> {
    /// Insert `_opencode: { message_id?, part_id? }` into a content object when
    /// at least one id is present. No-op for non-object content.
    fn stamp(&self, content: &mut serde_json::Value) {
        let Some(obj) = content.as_object_mut() else {
            return;
        };
        let mut opencode = serde_json::Map::new();
        if let Some(mid) = self.message_id {
            opencode.insert("message_id".to_string(), serde_json::Value::String(mid.to_string()));
        }
        if let Some(pid) = self.part_id {
            opencode.insert("part_id".to_string(), serde_json::Value::String(pid.to_string()));
        }
        if !opencode.is_empty() {
            obj.insert("_opencode".to_string(), serde_json::Value::Object(opencode));
        }
    }
}

fn build_text_row(
    conversation_id: &str,
    part: &serde_json::Value,
    text: &str,
    is_user: bool,
    model: Option<&(String, String)>,
    created_at: i64,
    opencode: OpencodeIds<'_>,
) -> aionui_db::models::MessageRow {
    let id = part_id(part);
    let mut content = match model {
        Some((provider, model_id)) if !is_user => serde_json::json!({
            "content": text,
            "model": { "provider_id": provider, "model_id": model_id },
        }),
        _ => serde_json::json!({ "content": text }),
    };
    opencode.stamp(&mut content);
    aionui_db::models::MessageRow {
        id: id.clone(),
        conversation_id: conversation_id.to_string(),
        msg_id: Some(id),
        r#type: "text".into(),
        content: content.to_string(),
        position: Some(if is_user { "right".into() } else { "left".into() }),
        status: Some("finish".into()),
        hidden: false,
        created_at,
    }
}

fn build_thinking_row(
    conversation_id: &str,
    part: &serde_json::Value,
    text: &str,
    created_at: i64,
    opencode: OpencodeIds<'_>,
) -> aionui_db::models::MessageRow {
    let id = part_id(part);
    // OpenCode reasoning parts carry `time: { start, end }` (ms) for the
    // streaming overlay timer. Lift it as duration if both are present.
    let duration_ms = part
        .get("time")
        .and_then(|t| {
            let start = t.get("start").and_then(|v| v.as_f64())?;
            let end = t.get("end").and_then(|v| v.as_f64())?;
            Some(((end - start) as i64).max(0))
        })
        .unwrap_or(0);
    let mut content = serde_json::json!({
        "content": text,
        "status": "done",
        "duration_ms": duration_ms,
    });
    opencode.stamp(&mut content);
    aionui_db::models::MessageRow {
        id: id.clone(),
        conversation_id: conversation_id.to_string(),
        msg_id: Some(id),
        r#type: "thinking".into(),
        content: content.to_string(),
        position: Some("left".into()),
        status: Some("finish".into()),
        hidden: false,
        created_at,
    }
}

fn build_tool_call_row(
    conversation_id: &str,
    part: &serde_json::Value,
    created_at: i64,
    opencode: OpencodeIds<'_>,
) -> aionui_db::models::MessageRow {
    // Mirror `stream_relay::persist_tool_call`: the row's content is the
    // serialized `ToolCallEventData`. We construct one from the OpenCode
    // tool-part state so the renderer's existing tool_call view sees the
    // historical entries identically to live ones.
    let call_id = part
        .get("callID")
        .and_then(|v| v.as_str())
        .or_else(|| part.get("call_id").and_then(|v| v.as_str()))
        .map(String::from)
        .unwrap_or_else(part_id_str_from_part);
    let name = part.get("tool").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let state = part.get("state").cloned().unwrap_or_else(|| serde_json::json!({}));
    let opencode_status = state.get("status").and_then(|v| v.as_str()).unwrap_or("");
    let (chisl_status, event_status) = match opencode_status {
        "completed" => ("finish", "completed"),
        "running" => ("work", "running"),
        "error" | "failed" => ("error", "error"),
        _ => ("finish", "completed"),
    };
    let input = state.get("input").cloned();
    let output = state
        .get("output")
        .and_then(|v| v.as_str())
        .map(String::from)
        .or_else(|| state.get("output").map(|v| v.to_string()));
    let description = state.get("title").and_then(|v| v.as_str()).map(String::from);
    let mut content = serde_json::json!({
        "call_id": call_id,
        "name": name,
        "args": input.clone().unwrap_or(serde_json::Value::Null),
        "status": event_status,
        "input": input,
        "output": output,
        "description": description,
    });
    opencode.stamp(&mut content);
    aionui_db::models::MessageRow {
        id: call_id.clone(),
        conversation_id: conversation_id.to_string(),
        msg_id: Some(call_id),
        r#type: "tool_call".into(),
        content: content.to_string(),
        position: Some("left".into()),
        status: Some(chisl_status.into()),
        hidden: false,
        created_at,
    }
}

fn part_id(part: &serde_json::Value) -> String {
    part.get("id")
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(part_id_str_from_part)
}

fn part_id_str_from_part() -> String {
    aionui_common::generate_short_id()
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
        RemoteAgentAuthType::Basic => format!("Basic {}", BASE64.encode(token)),
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
        assert_eq!(enum_to_str(&RemoteAgentAuthType::Basic), "basic");
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

    // ── M06: session patch body builder ──────────────────────────
    #[test]
    fn session_patch_body_title_only() {
        let patch = RemoteSessionPatch {
            title: Some("New title".into()),
            archived: None,
        };
        let body = build_session_patch_body(&patch, 1_700_000_000_000).unwrap();
        assert_eq!(body, serde_json::json!({ "title": "New title" }));
        assert!(body.get("time").is_none());
    }

    #[test]
    fn session_patch_body_archive_sets_timestamp() {
        let patch = RemoteSessionPatch {
            title: None,
            archived: Some(true),
        };
        let body = build_session_patch_body(&patch, 1_700_000_000_000).unwrap();
        assert_eq!(
            body,
            serde_json::json!({ "time": { "archived": 1_700_000_000_000_i64 } })
        );
    }

    #[test]
    fn session_patch_body_unarchive_clears_timestamp() {
        let patch = RemoteSessionPatch {
            title: None,
            archived: Some(false),
        };
        let body = build_session_patch_body(&patch, 1_700_000_000_000).unwrap();
        assert_eq!(body, serde_json::json!({ "time": { "archived": 0 } }));
    }

    #[test]
    fn session_patch_body_combined_title_and_archive() {
        let patch = RemoteSessionPatch {
            title: Some("Renamed".into()),
            archived: Some(true),
        };
        let body = build_session_patch_body(&patch, 42).unwrap();
        assert_eq!(body["title"], "Renamed");
        assert_eq!(body["time"]["archived"], 42);
    }

    #[test]
    fn session_patch_body_empty_is_none() {
        let patch = RemoteSessionPatch::default();
        assert!(build_session_patch_body(&patch, 1).is_none());
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
    fn build_opencode_basic_auth_uses_supplied_credentials() {
        let headers = build_opencode_auth_headers(RemoteAgentAuthType::Basic, Some("user:secret")).unwrap();

        assert_eq!(
            headers.get(AUTHORIZATION).unwrap().to_str().unwrap(),
            format!("Basic {}", BASE64.encode("user:secret"))
        );
    }

    #[test]
    fn parse_protocol_unknown_defaults() {
        assert_eq!(parse_protocol("unknown_proto"), RemoteAgentProtocol::Acp);
    }

    #[test]
    fn parse_auth_type_known_values() {
        assert_eq!(parse_auth_type("bearer"), RemoteAgentAuthType::Bearer);
        assert_eq!(parse_auth_type("basic"), RemoteAgentAuthType::Basic);
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

    #[test]
    fn parse_opencode_session_lifts_time_in_seconds() {
        // Older OpenCode shape: top-level `time.{created,updated}` in
        // seconds. We scale to ms so the renderer can format with
        // `new Date(ts)`.
        let value = serde_json::json!({
            "id": "ses_abc",
            "title": "Plan a refactor",
            "directory": "/Users/alice/proj",
            "time": { "created": 1_700_000_000, "updated": 1_700_000_500 },
        });
        let session = super::parse_opencode_session(&value).expect("session parsed");
        assert_eq!(session.id, "ses_abc");
        assert_eq!(session.title.as_deref(), Some("Plan a refactor"));
        assert_eq!(session.directory.as_deref(), Some("/Users/alice/proj"));
        assert_eq!(session.created_at, Some(1_700_000_000_000));
        assert_eq!(session.updated_at, Some(1_700_000_500_000));
    }

    #[test]
    fn parse_opencode_session_passes_through_ms_timestamps() {
        // Live builds emit ms-since-epoch directly (e.g. 1779933821994).
        // We accept verbatim without rescaling.
        let value = serde_json::json!({
            "id": "ses_xyz",
            "directory": "/app",
            "time": { "created": 1_779_933_821_994_i64, "updated": 1_779_933_836_090_i64 },
        });
        let session = super::parse_opencode_session(&value).expect("session parsed");
        assert_eq!(session.id, "ses_xyz");
        assert_eq!(session.directory.as_deref(), Some("/app"));
        assert_eq!(session.created_at, Some(1_779_933_821_994));
        assert_eq!(session.updated_at, Some(1_779_933_836_090));
    }

    #[test]
    fn parse_opencode_session_requires_id() {
        // Without an id there is nothing useful to attach to; drop the row
        // rather than surfacing a stub the UI cannot act on.
        let value = serde_json::json!({ "title": "no id" });
        assert!(super::parse_opencode_session(&value).is_none());
    }

    #[test]
    fn parse_opencode_session_handles_missing_time_and_directory() {
        let value = serde_json::json!({ "id": "ses_1" });
        let session = super::parse_opencode_session(&value).expect("session parsed");
        assert!(session.updated_at.is_none());
        assert!(session.created_at.is_none());
        assert!(session.directory.is_none());
        assert!(session.title.is_none());
    }

    #[test]
    fn convert_opencode_messages_user_text_becomes_right_position_row() {
        let array = vec![serde_json::json!({
            "info": { "role": "user", "time": { "created": 1_700_000_000_000_i64 } },
            "parts": [{ "id": "prt_a", "type": "text", "text": "hello" }],
        })];
        let rows = super::convert_opencode_messages("conv_1", &array);
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.r#type, "text");
        assert_eq!(row.position.as_deref(), Some("right"));
        assert_eq!(row.conversation_id, "conv_1");
        let parsed: serde_json::Value = serde_json::from_str(&row.content).unwrap();
        assert_eq!(parsed["content"], "hello");
        // User text rows must not carry a `model` block — the user
        // didn't generate any tokens.
        assert!(parsed.get("model").is_none());
    }

    #[test]
    fn convert_opencode_messages_assistant_text_carries_model_info() {
        let array = vec![serde_json::json!({
            "info": {
                "id": "msg_a",
                "role": "assistant",
                "time": { "created": 1_700_000_000_000_i64 },
                "model": { "providerID": "google", "modelID": "antigravity" },
            },
            "parts": [{ "id": "prt_b", "type": "text", "text": "hi" }],
        })];
        let rows = super::convert_opencode_messages("conv_1", &array);
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.position.as_deref(), Some("left"));
        let parsed: serde_json::Value = serde_json::from_str(&row.content).unwrap();
        assert_eq!(parsed["model"]["provider_id"], "google");
        assert_eq!(parsed["model"]["model_id"], "antigravity");
        // Backfilled rows must carry the OpenCode message/part ids so M01/M02
        // (fork/revert) can resolve this local row back to the server message.
        assert_eq!(parsed["_opencode"]["message_id"], "msg_a");
        assert_eq!(parsed["_opencode"]["part_id"], "prt_b");
    }

    #[test]
    fn convert_opencode_messages_skips_step_markers_and_handles_reasoning() {
        // Real OpenCode shape from the live probe: assistant turn with
        // step-start, reasoning, tool, step-finish. Reasoning becomes a
        // `thinking` row; step-* are dropped.
        let array = vec![serde_json::json!({
            "info": { "id": "msg_a", "role": "assistant", "time": { "created": 1_700_000_000_000_i64 } },
            "parts": [
                { "type": "step-start" },
                { "id": "prt_r", "type": "reasoning", "text": "thinking aloud",
                  "time": { "start": 1_700_000_000_000_i64, "end": 1_700_000_001_500_i64 } },
                { "id": "prt_t", "type": "tool", "callID": "call_1", "tool": "run_shell",
                  "state": { "status": "completed", "input": { "command": "ls" }, "output": "a b c" } },
                { "type": "step-finish" },
            ],
        })];
        let rows = super::convert_opencode_messages("conv_1", &array);
        assert_eq!(rows.len(), 2, "step-start and step-finish must be dropped");
        assert_eq!(rows[0].r#type, "thinking");
        let thinking_content: serde_json::Value = serde_json::from_str(&rows[0].content).unwrap();
        assert_eq!(thinking_content["duration_ms"], 1500);
        // Thinking rows also carry the OpenCode ids for revert-to-here.
        assert_eq!(thinking_content["_opencode"]["message_id"], "msg_a");
        assert_eq!(thinking_content["_opencode"]["part_id"], "prt_r");
        assert_eq!(rows[1].r#type, "tool_call");
        assert_eq!(rows[1].id, "call_1", "row id must come from callID");
        let tool_content: serde_json::Value = serde_json::from_str(&rows[1].content).unwrap();
        assert_eq!(tool_content["name"], "run_shell");
        assert_eq!(tool_content["status"], "completed");
        assert_eq!(tool_content["output"], "a b c");
        assert_eq!(tool_content["_opencode"]["message_id"], "msg_a");
        assert_eq!(tool_content["_opencode"]["part_id"], "prt_t");
    }

    #[test]
    fn convert_opencode_messages_skips_empty_text_parts() {
        // Defensive: OpenCode occasionally emits an empty text part
        // before the real one. We'd rather drop them than persist
        // empty user bubbles.
        let array = vec![serde_json::json!({
            "info": { "role": "user", "time": { "created": 1_700_000_000_000_i64 } },
            "parts": [
                { "id": "prt_empty", "type": "text", "text": "" },
                { "id": "prt_real", "type": "text", "text": "hi" },
            ],
        })];
        let rows = super::convert_opencode_messages("conv_1", &array);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "prt_real");
    }

    // ── Bet A5: connect-failure classifier (services/remote.rs) ──────────
    //
    // The classifier is pure-text and must remain backward compatible
    // with reqwest/hyper error text. Pin the markers the renderer will
    // parse, including the DNS-vs-TLS ordering quirk (DNS branch must be
    // checked first so a `dns error: ... self-signed ...` chain reports
    // the DNS root cause).
    #[test]
    fn connect_error_code_dns_markers() {
        assert_eq!(
            super::connect_error_code_from_text("dns error: failed to lookup address information"),
            "dns_failure"
        );
        assert_eq!(
            super::connect_error_code_from_text("failed to lookup address information: Name or service not known"),
            "dns_failure"
        );
        assert_eq!(
            super::connect_error_code_from_text("nodename nor servname provided, or not known"),
            "dns_failure"
        );
        assert_eq!(
            super::connect_error_code_from_text("no such host is known"),
            "dns_failure"
        );
    }

    #[test]
    fn connect_error_code_tls_markers() {
        assert_eq!(
            super::connect_error_code_from_text("invalid peer certificate: UnknownIssuer"),
            "tls_failure"
        );
        assert_eq!(
            super::connect_error_code_from_text("self-signed certificate in certificate chain"),
            "tls_failure"
        );
        assert_eq!(
            super::connect_error_code_from_text("self signed certificate in certificate chain"),
            "tls_failure"
        );
        // Bare `tls` and `ssl` substrings still classify as TLS.
        assert_eq!(
            super::connect_error_code_from_text("transport layer security handshake failure (TLS)"),
            "tls_failure"
        );
        assert_eq!(
            super::connect_error_code_from_text("ssl handshake failed"),
            "tls_failure"
        );
    }

    #[test]
    fn connect_error_code_connection_refused() {
        assert_eq!(
            super::connect_error_code_from_text("connection refused (os error 61)"),
            "connection_refused"
        );
    }

    #[test]
    fn connect_error_code_timeout_markers() {
        assert_eq!(super::connect_error_code_from_text("operation timed out"), "timeout");
        assert_eq!(
            super::connect_error_code_from_text("request timeout after 10s"),
            "timeout"
        );
    }

    #[test]
    fn connect_error_code_unknown_falls_through_to_unreachable() {
        assert_eq!(super::connect_error_code_from_text(""), "unreachable");
        assert_eq!(
            super::connect_error_code_from_text("network is unreachable"),
            "unreachable"
        );
    }

    // ── Bet A5: wiremock + live-failure integration tests ─────────────────
    //
    // The classifier above is pure. The tests below assert the *full
    // path*: `test_opencode_health` must build the `[code:<x>]` prefix
    // the renderer parses.

    #[tokio::test]
    async fn health_401_is_classified_as_auth_failure() {
        use wiremock::matchers::{method as wm_method, path as wm_path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(wm_method("GET"))
            .and(wm_path("/global/health"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;

        let err = super::test_opencode_health(&server.uri(), RemoteAgentAuthType::None, None, false)
            .await
            .expect_err("401 must error");
        let msg = format!("{err}");
        assert!(
            msg.contains("[code:auth_failure]"),
            "expected auth_failure code, got: {msg}"
        );
    }

    #[tokio::test]
    async fn health_404_is_classified_as_not_opencode() {
        use wiremock::matchers::{method as wm_method, path as wm_path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(wm_method("GET"))
            .and(wm_path("/global/health"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let err = super::test_opencode_health(&server.uri(), RemoteAgentAuthType::None, None, false)
            .await
            .expect_err("404 must error");
        let msg = format!("{err}");
        assert!(
            msg.contains("[code:not_opencode]"),
            "expected not_opencode code, got: {msg}"
        );
        assert!(
            msg.contains("/global/health"),
            "not_opencode message must mention the missing endpoint, got: {msg}"
        );
    }

    #[tokio::test]
    async fn health_403_is_classified_as_auth_failure() {
        use wiremock::matchers::{method as wm_method, path as wm_path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(wm_method("GET"))
            .and(wm_path("/global/health"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;

        let err = super::test_opencode_health(&server.uri(), RemoteAgentAuthType::None, None, false)
            .await
            .expect_err("403 must error");
        let msg = format!("{err}");
        assert!(
            msg.contains("[code:auth_failure]"),
            "expected auth_failure code, got: {msg}"
        );
    }

    #[tokio::test]
    async fn health_500_is_classified_as_server_error() {
        use wiremock::matchers::{method as wm_method, path as wm_path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(wm_method("GET"))
            .and(wm_path("/global/health"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let err = super::test_opencode_health(&server.uri(), RemoteAgentAuthType::None, None, false)
            .await
            .expect_err("500 must error");
        let msg = format!("{err}");
        assert!(
            msg.contains("[code:server_error]"),
            "expected server_error code, got: {msg}"
        );
    }

    #[tokio::test]
    async fn health_non_json_body_is_classified_as_not_opencode() {
        use wiremock::matchers::{method as wm_method, path as wm_path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(wm_method("GET"))
            .and(wm_path("/global/health"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
            .mount(&server)
            .await;

        let err = super::test_opencode_health(&server.uri(), RemoteAgentAuthType::None, None, false)
            .await
            .expect_err("non-JSON must error");
        let msg = format!("{err}");
        assert!(
            msg.contains("[code:not_opencode]"),
            "expected not_opencode code, got: {msg}"
        );
    }

    #[tokio::test]
    async fn health_healthy_false_is_classified_as_server_error() {
        use wiremock::matchers::{method as wm_method, path as wm_path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(wm_method("GET"))
            .and(wm_path("/global/health"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({ "healthy": false })))
            .mount(&server)
            .await;

        let err = super::test_opencode_health(&server.uri(), RemoteAgentAuthType::None, None, false)
            .await
            .expect_err("healthy:false must error");
        let msg = format!("{err}");
        assert!(
            msg.contains("[code:server_error]"),
            "expected server_error code, got: {msg}"
        );
    }

    #[tokio::test]
    async fn health_connection_refused_is_classified_as_connection_refused() {
        // Bind a TcpListener to grab a free port, drop it, then point
        // the health check at that port. The kernel responds to the
        // connect() with ECONNREFUSED immediately on loopback — the
        // classifier must label it `connection_refused` (NOT `timeout`).
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let port = listener.local_addr().expect("local_addr").port();
        drop(listener);

        let url = format!("http://127.0.0.1:{port}");
        let err = super::test_opencode_health(&url, RemoteAgentAuthType::None, None, false)
            .await
            .expect_err("closed port must error");
        let msg = format!("{err}");
        assert!(
            msg.contains("[code:connection_refused]"),
            "expected connection_refused code, got: {msg}"
        );
    }

    #[tokio::test]
    async fn health_dns_failure_is_classified_as_dns_failure() {
        // `.invalid` is reserved by RFC 2606; the resolver must fail
        // with NXDOMAIN (dns_failure), not timeout.
        let url = "http://chisl-a5-nonexistent.invalid";
        let err = super::test_opencode_health(url, RemoteAgentAuthType::None, None, false)
            .await
            .expect_err("unresolvable host must error");
        let msg = format!("{err}");
        assert!(
            msg.contains("[code:dns_failure]"),
            "expected dns_failure code, got: {msg}"
        );
    }
}
