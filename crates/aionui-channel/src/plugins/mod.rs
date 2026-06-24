#[cfg(feature = "telegram")]
pub mod telegram;

use crate::plugin::ChannelPlugin;
use crate::types::PluginType;

/// Create a platform-specific plugin instance from a `PluginType`.
///
/// Returns `None` if the platform feature is not compiled in.
pub fn create_plugin(plugin_type: PluginType) -> Option<Box<dyn ChannelPlugin>> {
    match plugin_type {
        #[cfg(feature = "telegram")]
        PluginType::Telegram => Some(Box::new(telegram::TelegramPlugin::new())),

        #[allow(unreachable_patterns)]
        _ => None,
    }
}
