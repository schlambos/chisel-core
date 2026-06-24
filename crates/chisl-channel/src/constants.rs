use std::time::Duration;

// ---------------------------------------------------------------------------
// Pairing
// ---------------------------------------------------------------------------

/// Length of the numeric pairing code (6 digits).
pub const PAIRING_CODE_LENGTH: usize = 6;

/// How long a pairing code remains valid.
pub const PAIRING_CODE_TTL: Duration = Duration::from_secs(10 * 60);

/// Interval between expired-pairing cleanup sweeps.
pub const PAIRING_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// Streaming & Throttle
// ---------------------------------------------------------------------------

/// Minimum interval between consecutive `editMessage` calls for
/// streaming responses (prevents API rate-limit errors).
pub const STREAM_THROTTLE_INTERVAL: Duration = Duration::from_millis(500);

/// Timeout for tool confirmation from the IM user.
pub const TOOL_CONFIRM_TIMEOUT: Duration = Duration::from_secs(15);

// ---------------------------------------------------------------------------
// Platform message limits
// ---------------------------------------------------------------------------

/// Maximum characters per Telegram message.
pub const TELEGRAM_MESSAGE_LIMIT: usize = 4096;

// ---------------------------------------------------------------------------
// Reconnection (Telegram long-polling)
// ---------------------------------------------------------------------------

/// Maximum reconnection attempts for Telegram long-polling.
pub const TELEGRAM_MAX_RECONNECT_ATTEMPTS: u32 = 10;

/// Maximum delay between reconnection attempts (exponential backoff cap).
pub const TELEGRAM_MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pairing_code_length_is_six() {
        assert_eq!(PAIRING_CODE_LENGTH, 6);
    }

    #[test]
    fn pairing_code_ttl_is_ten_minutes() {
        assert_eq!(PAIRING_CODE_TTL, Duration::from_secs(600));
    }

    #[test]
    fn cleanup_interval_is_sixty_seconds() {
        assert_eq!(PAIRING_CLEANUP_INTERVAL, Duration::from_secs(60));
    }

    #[test]
    fn stream_throttle_is_500ms() {
        assert_eq!(STREAM_THROTTLE_INTERVAL, Duration::from_millis(500));
    }

    #[test]
    fn tool_confirm_timeout_is_15s() {
        assert_eq!(TOOL_CONFIRM_TIMEOUT, Duration::from_secs(15));
    }

    #[test]
    fn telegram_message_limit() {
        assert_eq!(TELEGRAM_MESSAGE_LIMIT, 4096);
    }

    #[test]
    fn telegram_reconnect_limits() {
        assert_eq!(TELEGRAM_MAX_RECONNECT_ATTEMPTS, 10);
        assert_eq!(TELEGRAM_MAX_RECONNECT_DELAY, Duration::from_secs(30));
    }
}
