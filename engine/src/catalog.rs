//! Snowflake Catalog Management
//!
//! データベース、スキーマ、テーブルの管理

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::prelude::*;
use parking_lot::RwLock;

use crate::error::{Error, Result};

/// Snowflake カタログ
///
/// DataFusion のカタログシステムをラップし、Snowflake の
/// DATABASE.SCHEMA.TABLE 構造を提供する
pub struct SnowflakeCatalog {
    /// データベース一覧（データベース名 → DataFusion カタログ名）
    databases: RwLock<HashMap<String, String>>,

    /// 現在のデータベース
    current_database: RwLock<Option<String>>,

    /// 現在のスキーマ
    current_schema: RwLock<Option<String>>,
}

impl SnowflakeCatalog {
    /// 新しいカタログを作成
    pub fn new() -> Self {
        Self {
            databases: RwLock::new(HashMap::new()),
            current_database: RwLock::new(None),
            current_schema: RwLock::new(None),
        }
    }

    /// データベースを作成
    pub fn create_database(&self, ctx: &SessionContext, name: &str) -> Result<()> {
        let name_upper = name.to_uppercase();

        // DataFusion のカタログとして登録
        // DataFusion はデフォルトで "datafusion" カタログを持つ
        // Snowflake の DATABASE は DataFusion の catalog に対応
        let catalog_name = format!("sf_{}", name_upper.to_lowercase());

        let catalog = datafusion::catalog::MemoryCatalogProvider::new();
        ctx.register_catalog(&catalog_name, Arc::new(catalog));

        self.databases.write().insert(name_upper, catalog_name);
        Ok(())
    }

    /// データベースを削除
    pub fn drop_database(&self, name: &str) -> Result<()> {
        let name_upper = name.to_uppercase();
        self.databases.write().remove(&name_upper);
        Ok(())
    }

    /// データベースが存在するか確認
    pub fn database_exists(&self, name: &str) -> bool {
        self.databases.read().contains_key(&name.to_uppercase())
    }

    /// 現在のデータベースを設定
    pub fn use_database(&self, name: &str) -> Result<()> {
        let name_upper = name.to_uppercase();
        if !self.database_exists(&name_upper) {
            return Err(Error::DatabaseNotFound(name_upper));
        }
        *self.current_database.write() = Some(name_upper);
        Ok(())
    }

    /// 現在のスキーマを設定
    pub fn use_schema(&self, name: &str) -> Result<()> {
        *self.current_schema.write() = Some(name.to_uppercase());
        Ok(())
    }

    /// 現在のデータベースを取得
    pub fn current_database(&self) -> Option<String> {
        self.current_database.read().clone()
    }

    /// 現在のスキーマを取得
    pub fn current_schema(&self) -> Option<String> {
        self.current_schema.read().clone()
    }

    /// DataFusion カタログ名を取得
    pub fn get_catalog_name(&self, database: &str) -> Option<String> {
        self.databases.read().get(&database.to_uppercase()).cloned()
    }

    /// データベース一覧を取得
    pub fn list_databases(&self) -> Vec<String> {
        self.databases.read().keys().cloned().collect()
    }
}

impl Default for SnowflakeCatalog {
    fn default() -> Self {
        Self::new()
    }
}
