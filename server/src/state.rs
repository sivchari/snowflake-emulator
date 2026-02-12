//! Application State

use std::collections::HashMap;
use std::sync::Arc;

use engine::protocol::StatementResponse;
use engine::session::SessionManager;
use parking_lot::RwLock;
use tokio_util::sync::CancellationToken;

/// Async query execution state
#[derive(Debug, Clone)]
pub enum AsyncQueryState {
    /// Query is still running
    Running,
    /// Query completed successfully
    Completed(Box<StatementResponse>),
    /// Query failed with error
    Failed { code: String, message: String },
    /// Query was cancelled
    Cancelled,
}

/// Async query entry with cancellation support
pub struct AsyncQueryEntry {
    /// Query state
    pub state: AsyncQueryState,
    /// Cancellation token
    pub cancel_token: CancellationToken,
}

/// Application state
pub struct AppState {
    /// Session manager
    pub session_manager: SessionManager,
    /// Async query storage: statement_handle -> query state
    pub async_queries: Arc<RwLock<HashMap<String, AsyncQueryEntry>>>,
}

impl AppState {
    /// Create a new application state
    pub fn new() -> Self {
        Self {
            session_manager: SessionManager::new(),
            async_queries: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a new async query
    pub fn register_async_query(&self, statement_handle: String) -> CancellationToken {
        let cancel_token = CancellationToken::new();
        let entry = AsyncQueryEntry {
            state: AsyncQueryState::Running,
            cancel_token: cancel_token.clone(),
        };
        self.async_queries.write().insert(statement_handle, entry);
        cancel_token
    }

    /// Update async query state to completed
    pub fn complete_async_query(&self, statement_handle: &str, response: StatementResponse) {
        if let Some(entry) = self.async_queries.write().get_mut(statement_handle) {
            entry.state = AsyncQueryState::Completed(Box::new(response));
        }
    }

    /// Update async query state to failed
    pub fn fail_async_query(&self, statement_handle: &str, code: String, message: String) {
        if let Some(entry) = self.async_queries.write().get_mut(statement_handle) {
            entry.state = AsyncQueryState::Failed { code, message };
        }
    }

    /// Cancel an async query
    pub fn cancel_async_query(&self, statement_handle: &str) -> bool {
        if let Some(entry) = self.async_queries.write().get_mut(statement_handle) {
            entry.cancel_token.cancel();
            entry.state = AsyncQueryState::Cancelled;
            true
        } else {
            false
        }
    }

    /// Get async query state
    pub fn get_async_query_state(&self, statement_handle: &str) -> Option<AsyncQueryState> {
        self.async_queries
            .read()
            .get(statement_handle)
            .map(|e| e.state.clone())
    }

    /// Remove completed/failed/cancelled query (cleanup)
    pub fn remove_async_query(&self, statement_handle: &str) {
        self.async_queries.write().remove(statement_handle);
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}
