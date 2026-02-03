//! Session Management
//!
//! Snowflake セッションの管理

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use uuid::Uuid;

use crate::executor::Executor;
use crate::protocol::StatementResponse;
use crate::Result;

/// セッション
pub struct Session {
    /// セッション ID
    pub id: String,

    /// 現在のデータベース
    pub database: Option<String>,

    /// 現在のスキーマ
    pub schema: Option<String>,

    /// 現在のウェアハウス（エミュレーターでは無視）
    pub warehouse: Option<String>,

    /// 現在のロール（エミュレーターでは無視）
    pub role: Option<String>,

    /// セッションパラメータ
    pub parameters: HashMap<String, String>,

    /// SQL 実行エンジン
    executor: Arc<Executor>,
}

impl Session {
    /// 新しいセッションを作成
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

    /// SQL を実行
    pub async fn execute(&self, sql: &str) -> Result<StatementResponse> {
        self.executor.execute(sql).await
    }

    /// データベースを設定
    pub fn use_database(&mut self, database: &str) {
        self.database = Some(database.to_uppercase());
    }

    /// スキーマを設定
    pub fn use_schema(&mut self, schema: &str) {
        self.schema = Some(schema.to_uppercase());
    }

    /// ウェアハウスを設定
    pub fn use_warehouse(&mut self, warehouse: &str) {
        self.warehouse = Some(warehouse.to_uppercase());
    }

    /// ロールを設定
    pub fn use_role(&mut self, role: &str) {
        self.role = Some(role.to_uppercase());
    }

    /// パラメータを設定
    pub fn set_parameter(&mut self, key: &str, value: &str) {
        self.parameters
            .insert(key.to_uppercase(), value.to_string());
    }

    /// パラメータを取得
    pub fn get_parameter(&self, key: &str) -> Option<&String> {
        self.parameters.get(&key.to_uppercase())
    }
}

/// セッションマネージャー
pub struct SessionManager {
    /// セッション一覧
    sessions: RwLock<HashMap<String, Arc<RwLock<Session>>>>,

    /// 共有エグゼキューター
    executor: Arc<Executor>,
}

impl SessionManager {
    /// 新しいセッションマネージャーを作成
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            executor: Arc::new(Executor::new()),
        }
    }

    /// 新しいセッションを作成
    pub fn create_session(&self) -> Arc<RwLock<Session>> {
        let session = Session::new(self.executor.clone());
        let session_id = session.id.clone();
        let session = Arc::new(RwLock::new(session));

        self.sessions.write().insert(session_id, session.clone());

        session
    }

    /// セッションを取得
    pub fn get_session(&self, session_id: &str) -> Option<Arc<RwLock<Session>>> {
        self.sessions.read().get(session_id).cloned()
    }

    /// セッションを削除
    pub fn remove_session(&self, session_id: &str) {
        self.sessions.write().remove(session_id);
    }

    /// エグゼキューターへの参照を取得
    pub fn executor(&self) -> &Arc<Executor> {
        &self.executor
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}
