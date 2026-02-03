//! Snowflake Catalog Management
//!
//! Database, schema, and table management

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::prelude::*;
use parking_lot::RwLock;

use crate::error::{Error, Result};

/// Snowflake catalog
///
/// Wraps DataFusion's catalog system to provide Snowflake's
/// DATABASE.SCHEMA.TABLE structure
pub struct SnowflakeCatalog {
    /// Database list (database name -> DataFusion catalog name)
    databases: RwLock<HashMap<String, String>>,

    /// Current database
    current_database: RwLock<Option<String>>,

    /// Current schema
    current_schema: RwLock<Option<String>>,
}

impl SnowflakeCatalog {
    /// Create a new catalog
    pub fn new() -> Self {
        Self {
            databases: RwLock::new(HashMap::new()),
            current_database: RwLock::new(None),
            current_schema: RwLock::new(None),
        }
    }

    /// Create a database
    pub fn create_database(&self, ctx: &SessionContext, name: &str) -> Result<()> {
        let name_upper = name.to_uppercase();

        // Register as DataFusion catalog
        // DataFusion has a default "datafusion" catalog
        // Snowflake DATABASE maps to DataFusion catalog
        let catalog_name = format!("sf_{}", name_upper.to_lowercase());

        let catalog = datafusion::catalog::MemoryCatalogProvider::new();
        ctx.register_catalog(&catalog_name, Arc::new(catalog));

        self.databases.write().insert(name_upper, catalog_name);
        Ok(())
    }

    /// Drop a database
    pub fn drop_database(&self, name: &str) -> Result<()> {
        let name_upper = name.to_uppercase();
        self.databases.write().remove(&name_upper);
        Ok(())
    }

    /// Check if database exists
    pub fn database_exists(&self, name: &str) -> bool {
        self.databases.read().contains_key(&name.to_uppercase())
    }

    /// Set current database
    pub fn use_database(&self, name: &str) -> Result<()> {
        let name_upper = name.to_uppercase();
        if !self.database_exists(&name_upper) {
            return Err(Error::DatabaseNotFound(name_upper));
        }
        *self.current_database.write() = Some(name_upper);
        Ok(())
    }

    /// Set current schema
    pub fn use_schema(&self, name: &str) -> Result<()> {
        *self.current_schema.write() = Some(name.to_uppercase());
        Ok(())
    }

    /// Get current database
    pub fn current_database(&self) -> Option<String> {
        self.current_database.read().clone()
    }

    /// Get current schema
    pub fn current_schema(&self) -> Option<String> {
        self.current_schema.read().clone()
    }

    /// Get DataFusion catalog name
    pub fn get_catalog_name(&self, database: &str) -> Option<String> {
        self.databases.read().get(&database.to_uppercase()).cloned()
    }

    /// List databases
    pub fn list_databases(&self) -> Vec<String> {
        self.databases.read().keys().cloned().collect()
    }
}

impl Default for SnowflakeCatalog {
    fn default() -> Self {
        Self::new()
    }
}
