//! Session Management
//!
//! Snowflake session management

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use uuid::Uuid;

use crate::executor::Executor;
use crate::protocol::StatementResponse;
use crate::Result;

/// Session
pub struct Session {
    /// Session ID
    pub id: String,

    /// Current database
    pub database: Option<String>,

    /// Current schema
    pub schema: Option<String>,

    /// Current warehouse (ignored by emulator)
    pub warehouse: Option<String>,

    /// Current role (ignored by emulator)
    pub role: Option<String>,

    /// Session parameters
    pub parameters: HashMap<String, String>,

    /// SQL execution engine
    executor: Arc<Executor>,
}

impl Session {
    /// Create a new session
    pub fn new(executor: Arc<Executor>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            database: None,
            schema: None,
            warehouse: None,
            role: None,
            parameters: HashMap::new(),
            executor,
        }
    }

    /// Execute SQL
    pub async fn execute(&self, sql: &str) -> Result<StatementResponse> {
        self.executor.execute(sql).await
    }

    /// Set database
    pub fn use_database(&mut self, database: &str) {
        self.database = Some(database.to_uppercase());
    }

    /// Set schema
    pub fn use_schema(&mut self, schema: &str) {
        self.schema = Some(schema.to_uppercase());
    }

    /// Set warehouse
    pub fn use_warehouse(&mut self, warehouse: &str) {
        self.warehouse = Some(warehouse.to_uppercase());
    }

    /// Set role
    pub fn use_role(&mut self, role: &str) {
        self.role = Some(role.to_uppercase());
    }

    /// Set parameter
    pub fn set_parameter(&mut self, key: &str, value: &str) {
        self.parameters
            .insert(key.to_uppercase(), value.to_string());
    }

    /// Get parameter
    pub fn get_parameter(&self, key: &str) -> Option<&String> {
        self.parameters.get(&key.to_uppercase())
    }
}

/// Session manager
pub struct SessionManager {
    /// Session list
    sessions: RwLock<HashMap<String, Arc<RwLock<Session>>>>,

    /// Shared executor
    executor: Arc<Executor>,
}

impl SessionManager {
    /// Create a new session manager
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            executor: Arc::new(Executor::new()),
        }
    }

    /// Create a new session
    pub fn create_session(&self) -> Arc<RwLock<Session>> {
        let session = Session::new(self.executor.clone());
        let session_id = session.id.clone();
        let session = Arc::new(RwLock::new(session));

        self.sessions.write().insert(session_id, session.clone());

        session
    }

    /// Get session
    pub fn get_session(&self, session_id: &str) -> Option<Arc<RwLock<Session>>> {
        self.sessions.read().get(session_id).cloned()
    }

    /// Remove session
    pub fn remove_session(&self, session_id: &str) {
        self.sessions.write().remove(session_id);
    }

    /// Get executor reference
    pub fn executor(&self) -> &Arc<Executor> {
        &self.executor
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}
