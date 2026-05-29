//! Cross-crate lifecycle hook traits.
//!
//! Hooks defined here let lower-layer crates (e.g. `aionui-ai-agent`,
//! `aionui-cron`) react to events owned by higher-layer crates (e.g.
//! `aionui-conversation`) without forming a dependency cycle.

use async_trait::async_trait;

/// Notified when a conversation row is deleted via
/// `ConversationService::delete`.
///
/// Implementors are responsible for cleaning up their per-conversation state
/// (kill agent processes, drop cron jobs, etc.). Hooks run sequentially in
/// registration order; failures must be logged inside the hook and not
/// propagated.
#[async_trait]
pub trait OnConversationDelete: Send + Sync {
    async fn on_conversation_deleted(&self, conversation_id: &str);
}

/// Notified after a conversation row is updated via
/// `ConversationService::update` (rename, pin, archive flag, model, etc.).
///
/// Implementors react to the *post-update* state — they are expected to
/// re-read the conversation row to decide what changed. Used by the remote
/// layer (M06) to propagate a renamed/archived OpenCode-bound conversation to
/// its server session. Hooks run sequentially in registration order; failures
/// must be logged inside the hook and not propagated.
#[async_trait]
pub trait OnConversationUpdate: Send + Sync {
    async fn on_conversation_updated(&self, conversation_id: &str);
}
