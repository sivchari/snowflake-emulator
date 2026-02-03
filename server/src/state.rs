//! Application State

use engine::session::SessionManager;

/// アプリケーション状態
pub struct AppState {
    /// セッションマネージャー
    pub session_manager: SessionManager,
}

impl AppState {
    /// 新しいアプリケーション状態を作成
    pub fn new() -> Self {
        Self {
            session_manager: SessionManager::new(),
        }
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}
