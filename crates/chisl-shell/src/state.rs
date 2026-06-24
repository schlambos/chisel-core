use std::sync::Arc;

use chisl_system::ClientPrefService;

use crate::shell::ShellService;
use crate::stt::SttService;
use crate::tts::TtsService;

#[derive(Clone)]
pub struct ShellRouterState {
    pub shell_service: Arc<ShellService>,
    pub stt_service: Arc<SttService>,
    pub tts_service: Arc<TtsService>,
    pub client_pref_service: ClientPrefService,
}
