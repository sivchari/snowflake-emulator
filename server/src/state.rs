//! Application State

use engine::session::SessionManager;

/// Application state
pub struct AppState {
    /// Session manager
    pub session_manager: SessionManager,
}

impl AppState {
    /// Create a new application state
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
