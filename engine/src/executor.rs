//! SQL Executor powered by DataFusion
//!
//! Execute Snowflake SQL using DataFusion

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, RwLock};

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use uuid::Uuid;

use crate::catalog::SnowflakeCatalog;
use crate::error::Result;
use crate::functions;
use crate::protocol::{ColumnMetaData, StatementResponse};
use crate::sql_rewriter;

/// Sequence configuration
#[derive(Debug)]
pub struct Sequence {
    /// Current value (atomic for thread safety)
    current_value: AtomicI64,
    /// Start value (stored for potential RESET functionality)
    #[allow(dead_code)]
    start: i64,
    /// Increment value
    increment: i64,
}

impl Sequence {
    /// Create a new sequence
    pub fn new(start: i64, increment: i64) -> Self {
        Self {
            // Initialize to start - increment so first NEXTVAL returns start
            current_value: AtomicI64::new(start - increment),
            start,
            increment,
        }
    }

    /// Get next value
    pub fn next_val(&self) -> i64 {
        self.current_value
            .fetch_add(self.increment, Ordering::SeqCst)
            + self.increment
    }

    /// Get current value (without incrementing)
    pub fn curr_val(&self) -> i64 {
        self.current_value.load(Ordering::SeqCst)
    }
}

/// SQL execution engine
pub struct Executor {
    /// DataFusion session context
    ctx: SessionContext,

    /// Snowflake catalog
    catalog: Arc<SnowflakeCatalog>,

    /// Sequences storage
    sequences: Arc<RwLock<HashMap<String, Arc<Sequence>>>>,

    /// View definitions storage (view_name -> SELECT statement)
    views: Arc<RwLock<HashMap<String, String>>>,

    /// Current database name (session state)
    current_database: Arc<RwLock<String>>,

    /// Current schema name (session state)
    current_schema: Arc<RwLock<String>>,

    /// Transaction state: true if in a transaction
    in_transaction: Arc<RwLock<bool>>,

    /// Transaction snapshot: table data at BEGIN time for ROLLBACK support
    transaction_snapshot: Arc<RwLock<HashMap<String, Vec<RecordBatch>>>>,

    /// Stages storage: stage_name -> directory path
    stages: Arc<RwLock<HashMap<String, String>>>,
}

impl Executor {
    /// Create a new executor
    pub fn new() -> Self {
        let ctx = SessionContext::new();
        let catalog = Arc::new(SnowflakeCatalog::new());

        // Register Snowflake-compatible UDFs

        // Conditional functions
        ctx.register_udf(functions::iff());
        ctx.register_udf(functions::nvl());
        ctx.register_udf(functions::nvl2());
        ctx.register_udf(functions::decode());

        // JSON functions
        ctx.register_udf(functions::parse_json());
        ctx.register_udf(functions::to_json());

        // Date/Time functions
        ctx.register_udf(functions::dateadd());
        ctx.register_udf(functions::datediff());
        ctx.register_udf(functions::to_date());
        ctx.register_udf(functions::to_timestamp_udf());
        ctx.register_udf(functions::last_day());
        ctx.register_udf(functions::dayname());
        ctx.register_udf(functions::monthname());

        // TRY_* functions
        ctx.register_udf(functions::try_parse_json());
        ctx.register_udf(functions::try_to_number());
        ctx.register_udf(functions::try_to_date());
        ctx.register_udf(functions::try_to_boolean());

        // Array/Object functions (FLATTEN helpers)
        ctx.register_udf(functions::flatten_array());
        ctx.register_udf(functions::array_size());
        ctx.register_udf(functions::get_path());
        ctx.register_udf(functions::get());
        ctx.register_udf(functions::object_keys());

        // Array functions
        ctx.register_udf(functions::array_construct());
        ctx.register_udf(functions::array_construct_compact());
        ctx.register_udf(functions::array_append());
        ctx.register_udf(functions::array_prepend());
        ctx.register_udf(functions::array_cat());
        ctx.register_udf(functions::array_slice());
        ctx.register_udf(functions::array_contains());
        ctx.register_udf(functions::array_position());
        ctx.register_udf(functions::array_distinct());
        ctx.register_udf(functions::array_flatten());

        // Object functions
        ctx.register_udf(functions::object_construct());
        ctx.register_udf(functions::object_construct_keep_null());
        ctx.register_udf(functions::object_insert());
        ctx.register_udf(functions::object_delete());
        ctx.register_udf(functions::object_pick());

        // Type checking functions
        ctx.register_udf(functions::is_array());
        ctx.register_udf(functions::is_object());
        ctx.register_udf(functions::is_null_value());
        ctx.register_udf(functions::is_boolean());
        ctx.register_udf(functions::is_integer());
        ctx.register_udf(functions::is_decimal());
        ctx.register_udf(functions::typeof_func());

        // Conversion functions
        ctx.register_udf(functions::to_variant());
        ctx.register_udf(functions::to_array());
        ctx.register_udf(functions::to_object());

        // String functions
        ctx.register_udf(functions::split());
        ctx.register_udf(functions::strtok());
        ctx.register_udf(functions::strtok_to_array());
        ctx.register_udf(functions::regexp_like());
        ctx.register_udf(functions::regexp_substr());
        ctx.register_udf(functions::regexp_replace());
        ctx.register_udf(functions::regexp_count());
        ctx.register_udf(functions::contains());
        ctx.register_udf(functions::startswith());
        ctx.register_udf(functions::endswith());
        ctx.register_udf(functions::charindex());
        ctx.register_udf(functions::reverse());
        ctx.register_udf(functions::lpad());
        ctx.register_udf(functions::rpad());
        ctx.register_udf(functions::translate());

        // Aggregate functions
        ctx.register_udaf(functions::array_agg());
        ctx.register_udaf(functions::object_agg());
        ctx.register_udaf(functions::listagg());

        // Numeric functions
        ctx.register_udf(functions::div0());
        ctx.register_udf(functions::div0null());

        // Hash functions
        ctx.register_udf(functions::sha1_hex());
        ctx.register_udf(functions::sha2());

        // Context functions
        ctx.register_udf(functions::current_user());
        ctx.register_udf(functions::current_role());
        ctx.register_udf(functions::current_database());
        ctx.register_udf(functions::current_schema());
        ctx.register_udf(functions::current_warehouse());

        // Window functions
        ctx.register_udwf(functions::conditional_true_event());
        ctx.register_udwf(functions::conditional_change_event());

        // Register _NUMBERS table for FLATTEN support (0-999)
        let numbers_schema = Arc::new(Schema::new(vec![Field::new("idx", DataType::Int64, false)]));
        let numbers: Vec<i64> = (0..1000).collect();
        let numbers_array = Int64Array::from(numbers);
        let numbers_batch =
            RecordBatch::try_new(numbers_schema.clone(), vec![Arc::new(numbers_array)])
                .expect("Failed to create numbers batch");
        let numbers_table = MemTable::try_new(numbers_schema, vec![vec![numbers_batch]])
            .expect("Failed to create numbers table");
        ctx.register_table("_numbers", Arc::new(numbers_table))
            .expect("Failed to register _NUMBERS table");

        Self {
            ctx,
            catalog,
            sequences: Arc::new(RwLock::new(HashMap::new())),
            views: Arc::new(RwLock::new(HashMap::new())),
            current_database: Arc::new(RwLock::new("TESTDB".to_string())),
            current_schema: Arc::new(RwLock::new("PUBLIC".to_string())),
            in_transaction: Arc::new(RwLock::new(false)),
            transaction_snapshot: Arc::new(RwLock::new(HashMap::new())),
            stages: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Execute SQL
    pub async fn execute(&self, sql: &str) -> Result<StatementResponse> {
        let statement_handle = Uuid::new_v4().to_string();

        // Handle SHOW commands specially
        let sql_upper = sql.trim().to_uppercase();
        if sql_upper.starts_with("SHOW ") {
            return self.handle_show_command(sql, statement_handle).await;
        }

        // Handle INFORMATION_SCHEMA queries
        if sql_upper.contains("INFORMATION_SCHEMA.") {
            return self.handle_information_schema(sql, statement_handle).await;
        }

        // Handle DESCRIBE/DESC commands specially
        if sql_upper.starts_with("DESCRIBE ") || sql_upper.starts_with("DESC ") {
            return self.handle_describe_command(sql, statement_handle).await;
        }

        // Handle MERGE INTO command specially
        if sql_upper.starts_with("MERGE INTO ") || sql_upper.starts_with("MERGE ") {
            return self.handle_merge(sql, statement_handle).await;
        }

        // Handle CREATE SEQUENCE
        if sql_upper.starts_with("CREATE SEQUENCE ") {
            return self.handle_create_sequence(sql, statement_handle);
        }

        // Handle DROP SEQUENCE
        if sql_upper.starts_with("DROP SEQUENCE ") {
            return self.handle_drop_sequence(sql, statement_handle);
        }

        // Handle NEXTVAL/CURRVAL in SELECT
        if sql_upper.contains(".NEXTVAL") || sql_upper.contains(".CURRVAL") {
            return self.handle_sequence_select(sql, statement_handle);
        }

        // Handle UPDATE statement
        if sql_upper.starts_with("UPDATE ") {
            return self.handle_update(sql, statement_handle).await;
        }

        // Handle DELETE statement
        if sql_upper.starts_with("DELETE ") {
            return self.handle_delete(sql, statement_handle).await;
        }

        // Handle TRUNCATE statement
        if sql_upper.starts_with("TRUNCATE ") {
            return self.handle_truncate(sql, statement_handle).await;
        }

        // Handle ALTER TABLE statement
        if sql_upper.starts_with("ALTER TABLE ") {
            return self.handle_alter_table(sql, statement_handle).await;
        }

        // Handle CREATE VIEW statement
        if sql_upper.starts_with("CREATE VIEW ") || sql_upper.starts_with("CREATE OR REPLACE VIEW ")
        {
            return self.handle_create_view(sql, statement_handle);
        }

        // Handle DROP VIEW statement
        if sql_upper.starts_with("DROP VIEW ") {
            return self.handle_drop_view(sql, statement_handle);
        }

        // Handle USE DATABASE/SCHEMA statement
        if sql_upper.starts_with("USE ") {
            return self.handle_use(sql, statement_handle);
        }

        // Handle transaction statements
        if sql_upper.starts_with("BEGIN")
            || sql_upper.starts_with("START TRANSACTION")
            || sql_upper == "COMMIT"
            || sql_upper == "ROLLBACK"
        {
            return self.handle_transaction(sql, statement_handle).await;
        }

        // Handle CREATE STAGE
        if sql_upper.starts_with("CREATE STAGE ")
            || sql_upper.starts_with("CREATE OR REPLACE STAGE ")
        {
            return self.handle_create_stage(sql, statement_handle);
        }

        // Handle DROP STAGE
        if sql_upper.starts_with("DROP STAGE ") {
            return self.handle_drop_stage(sql, statement_handle);
        }

        // Handle COPY INTO
        if sql_upper.starts_with("COPY INTO ") {
            return self.handle_copy_into(sql, statement_handle).await;
        }

        // Handle CREATE DATABASE
        if sql_upper.starts_with("CREATE DATABASE ")
            || sql_upper.starts_with("CREATE OR REPLACE DATABASE ")
        {
            return self.handle_create_database(sql, statement_handle);
        }

        // Handle DROP DATABASE
        if sql_upper.starts_with("DROP DATABASE ") {
            return self.handle_drop_database(sql, statement_handle);
        }

        // Handle CREATE SCHEMA
        if sql_upper.starts_with("CREATE SCHEMA ")
            || sql_upper.starts_with("CREATE OR REPLACE SCHEMA ")
        {
            return self.handle_create_schema(sql, statement_handle);
        }

        // Handle DROP SCHEMA
        if sql_upper.starts_with("DROP SCHEMA ") {
            return self.handle_drop_schema(sql, statement_handle);
        }

        // Handle CURRENT_DATABASE() and CURRENT_SCHEMA() context functions
        // These need special handling to return the session state
        if sql_upper.contains("CURRENT_DATABASE()") || sql_upper.contains("CURRENT_SCHEMA()") {
            return self.handle_context_function(sql, statement_handle);
        }

        // Rewrite Snowflake-specific SQL constructs
        let rewritten_sql = sql_rewriter::rewrite(sql);

        // Expand view references in the SQL
        let rewritten_sql = self.expand_views(&rewritten_sql);

        // Execute SQL with DataFusion
        let df = self.ctx.sql(&rewritten_sql).await?;

        // Collect results
        let batches = df.collect().await?;

        // Build response
        let response = self.batches_to_response(batches, statement_handle)?;

        Ok(response)
    }

    /// Handle SHOW commands (SHOW TABLES, SHOW SCHEMAS, SHOW DATABASES)
    async fn handle_show_command(
        &self,
        sql: &str,
        statement_handle: String,
    ) -> Result<StatementResponse> {
        let sql_upper = sql.trim().to_uppercase();

        if sql_upper.starts_with("SHOW TABLES") {
            self.handle_show_tables(statement_handle).await
        } else if sql_upper.starts_with("SHOW SCHEMAS") {
            self.handle_show_schemas(statement_handle).await
        } else if sql_upper.starts_with("SHOW DATABASES") {
            self.handle_show_databases(statement_handle).await
        } else {
            Err(crate::error::Error::ExecutionError(format!(
                "Unsupported SHOW command: {sql}"
            )))
        }
    }

    /// Handle SHOW TABLES command
    async fn handle_show_tables(&self, statement_handle: String) -> Result<StatementResponse> {
        // Get all table names from DataFusion catalog
        let catalog = self.ctx.catalog("datafusion").unwrap();
        let schema = catalog.schema("public").unwrap();
        let table_names = schema.table_names();

        // Build column metadata
        let columns = vec![ColumnMetaData {
            name: "TABLE_NAME".to_string(),
            r#type: "TEXT".to_string(),
            nullable: false,
            precision: None,
            scale: None,
            length: None,
        }];

        // Build data rows
        let data: Vec<Vec<Option<String>>> = table_names
            .iter()
            .map(|name| vec![Some(name.clone())])
            .collect();

        Ok(StatementResponse::success(data, columns, statement_handle))
    }

    /// Handle SHOW SCHEMAS command
    async fn handle_show_schemas(&self, statement_handle: String) -> Result<StatementResponse> {
        // DataFusion uses a flat namespace, return "public" as default schema
        let columns = vec![ColumnMetaData {
            name: "SCHEMA_NAME".to_string(),
            r#type: "TEXT".to_string(),
            nullable: false,
            precision: None,
            scale: None,
            length: None,
        }];

        let data: Vec<Vec<Option<String>>> = vec![
            vec![Some("public".to_string())],
            vec![Some("information_schema".to_string())],
        ];

        Ok(StatementResponse::success(data, columns, statement_handle))
    }

    /// Handle SHOW DATABASES command
    async fn handle_show_databases(&self, statement_handle: String) -> Result<StatementResponse> {
        let columns = vec![ColumnMetaData {
            name: "name".to_string(),
            r#type: "TEXT".to_string(),
            nullable: false,
            precision: None,
            scale: None,
            length: None,
        }];

        // Get databases from catalog
        let mut databases = self.catalog.list_databases();

        // Always include a default database for compatibility
        if databases.is_empty() {
            databases.push("EMULATOR_DB".to_string());
        }

        let data: Vec<Vec<Option<String>>> =
            databases.into_iter().map(|name| vec![Some(name)]).collect();

        Ok(StatementResponse::success(data, columns, statement_handle))
    }

    // =========================================================================
    // INFORMATION_SCHEMA Support
    // =========================================================================

    /// Handle INFORMATION_SCHEMA queries
    ///
    /// Supports:
    /// - INFORMATION_SCHEMA.TABLES
    /// - INFORMATION_SCHEMA.COLUMNS
    /// - INFORMATION_SCHEMA.SCHEMATA
    async fn handle_information_schema(
        &self,
        sql: &str,
        statement_handle: String,
    ) -> Result<StatementResponse> {
        let sql_upper = sql.trim().to_uppercase();

        if sql_upper.contains("INFORMATION_SCHEMA.TABLES") {
            return self
                .handle_information_schema_tables(statement_handle)
                .await;
        }

        if sql_upper.contains("INFORMATION_SCHEMA.COLUMNS") {
            return self
                .handle_information_schema_columns(sql, statement_handle)
                .await;
        }

        if sql_upper.contains("INFORMATION_SCHEMA.SCHEMATA") {
            return self.handle_information_schema_schemata(statement_handle);
        }

        Err(crate::error::Error::ExecutionError(format!(
            "Unsupported INFORMATION_SCHEMA query: {}",
            sql
        )))
    }

    /// Handle SELECT FROM INFORMATION_SCHEMA.TABLES
    async fn handle_information_schema_tables(
        &self,
        statement_handle: String,
    ) -> Result<StatementResponse> {
        let catalog = self.ctx.catalog("datafusion").unwrap();
        let schema = catalog.schema("public").unwrap();
        let table_names = schema.table_names();

        let columns = vec![
            ColumnMetaData {
                name: "TABLE_CATALOG".to_string(),
                r#type: "TEXT".to_string(),
                nullable: true,
                precision: None,
                scale: None,
                length: None,
            },
            ColumnMetaData {
                name: "TABLE_SCHEMA".to_string(),
                r#type: "TEXT".to_string(),
                nullable: true,
                precision: None,
                scale: None,
                length: None,
            },
            ColumnMetaData {
                name: "TABLE_NAME".to_string(),
                r#type: "TEXT".to_string(),
                nullable: false,
                precision: None,
                scale: None,
                length: None,
            },
            ColumnMetaData {
                name: "TABLE_TYPE".to_string(),
                r#type: "TEXT".to_string(),
                nullable: true,
                precision: None,
                scale: None,
                length: None,
            },
        ];

        let current_db = self.get_current_database();
        let current_schema = self.get_current_schema();

        let data: Vec<Vec<Option<String>>> = table_names
            .iter()
            .map(|name| {
                vec![
                    Some(current_db.clone()),
                    Some(current_schema.clone()),
                    Some(name.clone()),
                    Some("BASE TABLE".to_string()),
                ]
            })
            .collect();

        Ok(StatementResponse::success(data, columns, statement_handle))
    }

    /// Handle SELECT FROM INFORMATION_SCHEMA.COLUMNS
    async fn handle_information_schema_columns(
        &self,
        sql: &str,
        statement_handle: String,
    ) -> Result<StatementResponse> {
        let columns = vec![
            ColumnMetaData {
                name: "TABLE_CATALOG".to_string(),
                r#type: "TEXT".to_string(),
                nullable: true,
                precision: None,
                scale: None,
                length: None,
            },
            ColumnMetaData {
                name: "TABLE_SCHEMA".to_string(),
                r#type: "TEXT".to_string(),
                nullable: true,
                precision: None,
                scale: None,
                length: None,
            },
            ColumnMetaData {
                name: "TABLE_NAME".to_string(),
                r#type: "TEXT".to_string(),
                nullable: false,
                precision: None,
                scale: None,
                length: None,
            },
            ColumnMetaData {
                name: "COLUMN_NAME".to_string(),
                r#type: "TEXT".to_string(),
                nullable: false,
                precision: None,
                scale: None,
                length: None,
            },
            ColumnMetaData {
                name: "ORDINAL_POSITION".to_string(),
                r#type: "NUMBER".to_string(),
                nullable: false,
                precision: None,
                scale: None,
                length: None,
            },
            ColumnMetaData {
                name: "DATA_TYPE".to_string(),
                r#type: "TEXT".to_string(),
                nullable: true,
                precision: None,
                scale: None,
                length: None,
            },
            ColumnMetaData {
                name: "IS_NULLABLE".to_string(),
                r#type: "TEXT".to_string(),
                nullable: true,
                precision: None,
                scale: None,
                length: None,
            },
        ];

        let catalog = self.ctx.catalog("datafusion").unwrap();
        let schema = catalog.schema("public").unwrap();

        let current_db = self.get_current_database();
        let current_schema = self.get_current_schema();

        // Check if filtering by table name
        let sql_upper = sql.to_uppercase();
        let filter_table: Option<String> = if sql_upper.contains("WHERE") {
            // Simple pattern matching for WHERE TABLE_NAME = 'xxx'
            let pattern = regex::Regex::new(r"(?i)TABLE_NAME\s*=\s*'([^']+)'").unwrap();
            pattern
                .captures(&sql_upper)
                .and_then(|c| c.get(1))
                .map(|m| m.as_str().to_lowercase())
        } else {
            None
        };

        let mut data: Vec<Vec<Option<String>>> = Vec::new();

        for table_name in schema.table_names() {
            // Apply filter if present
            if let Some(ref filter) = filter_table {
                if table_name.to_lowercase() != *filter {
                    continue;
                }
            }

            if let Ok(Some(table)) = schema.table(&table_name).await {
                let table_schema = table.schema();
                for (idx, field) in table_schema.fields().iter().enumerate() {
                    let (data_type, _, _, _) = self.arrow_type_to_snowflake(field.data_type());
                    let is_nullable = if field.is_nullable() { "YES" } else { "NO" };

                    data.push(vec![
                        Some(current_db.clone()),
                        Some(current_schema.clone()),
                        Some(table_name.clone()),
                        Some(field.name().clone()),
                        Some((idx + 1).to_string()),
                        Some(data_type),
                        Some(is_nullable.to_string()),
                    ]);
                }
            }
        }

        Ok(StatementResponse::success(data, columns, statement_handle))
    }

    /// Handle SELECT FROM INFORMATION_SCHEMA.SCHEMATA
    fn handle_information_schema_schemata(
        &self,
        statement_handle: String,
    ) -> Result<StatementResponse> {
        let columns = vec![
            ColumnMetaData {
                name: "CATALOG_NAME".to_string(),
                r#type: "TEXT".to_string(),
                nullable: true,
                precision: None,
                scale: None,
                length: None,
            },
            ColumnMetaData {
                name: "SCHEMA_NAME".to_string(),
                r#type: "TEXT".to_string(),
                nullable: false,
                precision: None,
                scale: None,
                length: None,
            },
            ColumnMetaData {
                name: "SCHEMA_OWNER".to_string(),
                r#type: "TEXT".to_string(),
                nullable: true,
                precision: None,
                scale: None,
                length: None,
            },
        ];

        let current_db = self.get_current_database();

        let data: Vec<Vec<Option<String>>> = vec![
            vec![
                Some(current_db.clone()),
                Some("PUBLIC".to_string()),
                Some("EMULATOR_USER".to_string()),
            ],
            vec![
                Some(current_db.clone()),
                Some("INFORMATION_SCHEMA".to_string()),
                Some("EMULATOR_USER".to_string()),
            ],
        ];

        Ok(StatementResponse::success(data, columns, statement_handle))
    }

    /// Handle DESCRIBE/DESC commands
    async fn handle_describe_command(
        &self,
        sql: &str,
        statement_handle: String,
    ) -> Result<StatementResponse> {
        let sql_upper = sql.trim().to_uppercase();

        // Parse table name from: DESCRIBE TABLE name, DESCRIBE name, DESC TABLE name, DESC name
        let table_name =
            if sql_upper.starts_with("DESCRIBE TABLE ") || sql_upper.starts_with("DESC TABLE ") {
                // DESCRIBE TABLE tablename or DESC TABLE tablename
                sql.split_whitespace().nth(2).map(|s| s.to_string())
            } else {
                // DESCRIBE tablename or DESC tablename
                sql.split_whitespace().nth(1).map(|s| s.to_string())
            };

        let table_name = table_name.ok_or_else(|| {
            crate::error::Error::ExecutionError("DESCRIBE requires a table name".to_string())
        })?;

        self.handle_describe_table(&table_name, statement_handle)
            .await
    }

    /// Handle DESCRIBE TABLE command
    async fn handle_describe_table(
        &self,
        table_name: &str,
        statement_handle: String,
    ) -> Result<StatementResponse> {
        // Get table from DataFusion catalog
        let catalog = self.ctx.catalog("datafusion").unwrap();
        let schema = catalog.schema("public").unwrap();

        let table = schema
            .table(table_name)
            .await
            .map_err(|_| crate::error::Error::TableNotFound(table_name.to_string()))?;

        let table =
            table.ok_or_else(|| crate::error::Error::TableNotFound(table_name.to_string()))?;

        let arrow_schema = table.schema();

        // Build column metadata for result
        let columns = vec![
            ColumnMetaData {
                name: "name".to_string(),
                r#type: "TEXT".to_string(),
                nullable: false,
                precision: None,
                scale: None,
                length: None,
            },
            ColumnMetaData {
                name: "type".to_string(),
                r#type: "TEXT".to_string(),
                nullable: false,
                precision: None,
                scale: None,
                length: None,
            },
            ColumnMetaData {
                name: "kind".to_string(),
                r#type: "TEXT".to_string(),
                nullable: false,
                precision: None,
                scale: None,
                length: None,
            },
            ColumnMetaData {
                name: "null".to_string(),
                r#type: "TEXT".to_string(),
                nullable: false,
                precision: None,
                scale: None,
                length: None,
            },
        ];

        // Build data rows - one row per column in the table
        let data: Vec<Vec<Option<String>>> = arrow_schema
            .fields()
            .iter()
            .map(|field| {
                vec![
                    Some(field.name().clone()),
                    Some(self.arrow_type_to_snowflake_type(field.data_type())),
                    Some("COLUMN".to_string()),
                    Some(if field.is_nullable() { "Y" } else { "N" }.to_string()),
                ]
            })
            .collect();

        Ok(StatementResponse::success(data, columns, statement_handle))
    }

    /// Convert Arrow data type to Snowflake type string
    fn arrow_type_to_snowflake_type(&self, data_type: &DataType) -> String {
        match data_type {
            DataType::Boolean => "BOOLEAN".to_string(),
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                "NUMBER".to_string()
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                "NUMBER".to_string()
            }
            DataType::Float32 | DataType::Float64 => "FLOAT".to_string(),
            DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR".to_string(),
            DataType::Date32 | DataType::Date64 => "DATE".to_string(),
            DataType::Timestamp(_, _) => "TIMESTAMP".to_string(),
            _ => format!("{data_type:?}"),
        }
    }

    /// Convert Arrow DataType to DataFusion SQL type for CAST operations
    fn arrow_type_to_sql_type(&self, data_type: &DataType) -> String {
        match data_type {
            DataType::Boolean => "BOOLEAN".to_string(),
            DataType::Int8 => "TINYINT".to_string(),
            DataType::Int16 => "SMALLINT".to_string(),
            DataType::Int32 => "INT".to_string(),
            DataType::Int64 => "BIGINT".to_string(),
            DataType::UInt8 => "TINYINT UNSIGNED".to_string(),
            DataType::UInt16 => "SMALLINT UNSIGNED".to_string(),
            DataType::UInt32 => "INT UNSIGNED".to_string(),
            DataType::UInt64 => "BIGINT UNSIGNED".to_string(),
            DataType::Float32 => "FLOAT".to_string(),
            DataType::Float64 => "DOUBLE".to_string(),
            DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR".to_string(),
            DataType::Date32 | DataType::Date64 => "DATE".to_string(),
            DataType::Timestamp(_, _) => "TIMESTAMP".to_string(),
            _ => "VARCHAR".to_string(), // Default fallback
        }
    }

    /// Handle MERGE INTO command
    ///
    /// Syntax:
    /// ```sql
    /// MERGE INTO target_table [AS alias]
    /// USING source_table [AS alias]
    /// ON condition
    /// WHEN MATCHED THEN UPDATE SET col1 = val1, ...
    /// WHEN NOT MATCHED THEN INSERT (col1, ...) VALUES (val1, ...)
    /// ```
    async fn handle_merge(&self, sql: &str, statement_handle: String) -> Result<StatementResponse> {
        // Parse MERGE statement
        let merge_pattern = regex::Regex::new(
            r"(?is)MERGE\s+INTO\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?\s+USING\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?\s+ON\s+(.+?)\s+(WHEN\s+.+)",
        ).unwrap();

        let captures = merge_pattern.captures(sql).ok_or_else(|| {
            crate::error::Error::ExecutionError("Invalid MERGE INTO syntax".to_string())
        })?;

        let target_table = captures.get(1).unwrap().as_str();
        let target_alias = captures.get(2).map(|m| m.as_str()).unwrap_or(target_table);
        let source_table = captures.get(3).unwrap().as_str();
        let source_alias = captures.get(4).map(|m| m.as_str()).unwrap_or(source_table);
        let on_condition = captures.get(5).unwrap().as_str().trim();
        let when_clauses = captures.get(6).unwrap().as_str();

        // Parse WHEN clauses (without look-ahead since Rust regex doesn't support it)
        let when_matched_update =
            regex::Regex::new(r"(?is)WHEN\s+MATCHED\s+THEN\s+UPDATE\s+SET\s+([^W]+)").unwrap();
        let when_not_matched_insert = regex::Regex::new(
            r"(?is)WHEN\s+NOT\s+MATCHED\s+THEN\s+INSERT\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)",
        )
        .unwrap();

        let mut rows_updated = 0i64;
        let mut rows_inserted = 0i64;

        // Handle WHEN MATCHED THEN UPDATE
        if let Some(matched_cap) = when_matched_update.captures(when_clauses) {
            let set_clause = matched_cap.get(1).unwrap().as_str().trim();

            // Remove trailing WHEN if present
            let set_clause = set_clause
                .trim_end_matches(|c: char| c.is_whitespace())
                .trim_end_matches("WHEN")
                .trim_end_matches(|c: char| c.is_whitespace());

            // Build UPDATE with JOIN
            // We need to update rows in target_table where the ON condition matches
            // DataFusion doesn't support UPDATE with JOIN directly, so we'll do:
            // 1. Delete matched rows
            // 2. Insert updated rows

            // Parse SET clause: "col1 = val1, col2 = val2"
            let set_pairs: Vec<(&str, &str)> = set_clause
                .split(',')
                .filter_map(|pair| {
                    let parts: Vec<&str> = pair.splitn(2, '=').map(|s| s.trim()).collect();
                    if parts.len() == 2 {
                        // Remove table alias prefix from column name
                        let col = parts[0]
                            .trim_start_matches(&format!("{target_alias}."))
                            .trim_start_matches(&format!("{target_table}."));
                        Some((col, parts[1]))
                    } else {
                        None
                    }
                })
                .collect();

            // Get column names from target table
            let catalog = self.ctx.catalog("datafusion").unwrap();
            let schema = catalog.schema("public").unwrap();
            let table = schema
                .table(target_table)
                .await
                .map_err(|_| crate::error::Error::TableNotFound(target_table.to_string()))?;
            let table = table
                .ok_or_else(|| crate::error::Error::TableNotFound(target_table.to_string()))?;
            let table_schema = table.schema();
            let column_names: Vec<&str> = table_schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect();

            // Build SELECT for matched rows with updated values
            let select_cols: Vec<String> = column_names
                .iter()
                .map(|col| {
                    // Check if this column is in the SET clause
                    if let Some((_, val)) = set_pairs.iter().find(|(c, _)| *c == *col) {
                        format!("{val} AS {col}")
                    } else {
                        format!("{target_alias}.{col}")
                    }
                })
                .collect();

            // Normalize condition to use aliases
            let normalized_condition = on_condition
                .replace(&format!("{target_table}."), &format!("{target_alias}."))
                .replace(&format!("{source_table}."), &format!("{source_alias}."));

            // Get updated rows
            let update_query = format!(
                "SELECT {} FROM {} AS {} INNER JOIN {} AS {} ON {}",
                select_cols.join(", "),
                target_table,
                target_alias,
                source_table,
                source_alias,
                normalized_condition
            );
            let update_df = self.ctx.sql(&update_query).await?;
            let update_batches = update_df.collect().await?;

            rows_updated = update_batches.iter().map(|b| b.num_rows() as i64).sum();

            // If we have matched rows, delete them and insert updated versions
            if rows_updated > 0 {
                // Create a temporary table with matched IDs
                // For simplicity, we'll use a subquery approach

                // Get non-matched rows from target
                let keep_query = format!(
                    "SELECT * FROM {} WHERE NOT EXISTS (SELECT 1 FROM {} WHERE {})",
                    target_table,
                    source_table,
                    on_condition
                        .replace(&format!("{target_alias}."), &format!("{target_table}."))
                        .replace(&format!("{source_alias}."), &format!("{source_table}."))
                );
                let keep_df = self.ctx.sql(&keep_query).await?;
                let keep_batches = keep_df.collect().await?;

                // Combine kept rows with updated rows
                // Drop and recreate the table
                let drop_sql = format!("DROP TABLE {target_table}");
                let _ = self.ctx.sql(&drop_sql).await?.collect().await;

                // Create table from combined batches
                let all_batches: Vec<RecordBatch> =
                    keep_batches.into_iter().chain(update_batches).collect();

                if !all_batches.is_empty() {
                    let mem_table =
                        datafusion::datasource::MemTable::try_new(table_schema, vec![all_batches])?;
                    self.ctx.register_table(target_table, Arc::new(mem_table))?;
                } else {
                    // Recreate empty table
                    let col_defs: Vec<String> = column_names
                        .iter()
                        .zip(table_schema.fields().iter())
                        .map(|(name, field)| {
                            format!(
                                "{} {}",
                                name,
                                self.arrow_type_to_snowflake_type(field.data_type())
                            )
                        })
                        .collect();
                    let create_sql =
                        format!("CREATE TABLE {} ({})", target_table, col_defs.join(", "));
                    let _ = self.ctx.sql(&create_sql).await?.collect().await;
                }
            }
        }

        // Handle WHEN NOT MATCHED THEN INSERT
        if let Some(insert_cap) = when_not_matched_insert.captures(when_clauses) {
            let insert_cols = insert_cap.get(1).unwrap().as_str().trim();
            let insert_vals = insert_cap.get(2).unwrap().as_str().trim();

            // Normalize condition to use aliases
            let normalized_condition = on_condition
                .replace(&format!("{target_table}."), &format!("{target_alias}."))
                .replace(&format!("{source_table}."), &format!("{source_alias}."));

            // Build INSERT for non-matched rows using LEFT JOIN
            let insert_query = format!(
                "INSERT INTO {} ({}) SELECT {} FROM {} AS {} LEFT JOIN {} AS {} ON {} WHERE {}.{} IS NULL",
                target_table,
                insert_cols,
                insert_vals,
                source_table,
                source_alias,
                target_table,
                target_alias,
                normalized_condition,
                target_alias,
                // Use first column from ON condition for NULL check
                on_condition.split('=').next().unwrap_or("id").split('.').next_back().unwrap_or("id").trim()
            );

            let insert_df = self.ctx.sql(&insert_query).await?;
            let insert_batches = insert_df.collect().await?;
            rows_inserted = insert_batches.iter().map(|b| b.num_rows() as i64).sum();
        }

        // Build response
        let columns = vec![ColumnMetaData {
            name: "number of rows merged".to_string(),
            r#type: "NUMBER".to_string(),
            nullable: false,
            precision: None,
            scale: None,
            length: None,
        }];

        let data = vec![vec![Some((rows_updated + rows_inserted).to_string())]];

        Ok(StatementResponse::success(data, columns, statement_handle))
    }

    /// Handle UPDATE statement
    ///
    /// Syntax:
    /// ```sql
    /// UPDATE table_name SET col1 = val1, col2 = val2, ... [WHERE condition]
    /// ```
    async fn handle_update(
        &self,
        sql: &str,
        statement_handle: String,
    ) -> Result<StatementResponse> {
        // Parse UPDATE statement
        let update_pattern =
            regex::Regex::new(r"(?is)UPDATE\s+(\w+)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?$").unwrap();

        let captures = update_pattern.captures(sql).ok_or_else(|| {
            crate::error::Error::ExecutionError("Invalid UPDATE syntax".to_string())
        })?;

        let table_name = captures.get(1).unwrap().as_str();
        let set_clause = captures.get(2).unwrap().as_str().trim();
        let where_clause = captures.get(3).map(|m| m.as_str().trim());

        // Get table schema
        let catalog = self.ctx.catalog("datafusion").unwrap();
        let schema = catalog.schema("public").unwrap();
        let table = schema
            .table(table_name)
            .await
            .map_err(|_| crate::error::Error::TableNotFound(table_name.to_string()))?;
        let table =
            table.ok_or_else(|| crate::error::Error::TableNotFound(table_name.to_string()))?;
        let table_schema = table.schema();
        let column_names: Vec<&str> = table_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();

        // Parse SET clause: "col1 = val1, col2 = val2"
        let set_pairs: Vec<(&str, &str)> = set_clause
            .split(',')
            .filter_map(|pair| {
                let parts: Vec<&str> = pair.splitn(2, '=').map(|s| s.trim()).collect();
                if parts.len() == 2 {
                    Some((parts[0], parts[1]))
                } else {
                    None
                }
            })
            .collect();

        if set_pairs.is_empty() {
            return Err(crate::error::Error::ExecutionError(
                "UPDATE SET clause is empty or invalid".to_string(),
            ));
        }

        // Build SELECT for updated rows with new values (with CAST to match original types)
        let select_cols: Vec<String> = column_names
            .iter()
            .zip(table_schema.fields().iter())
            .map(|(col, field)| {
                if let Some((_, val)) = set_pairs.iter().find(|(c, _)| c.eq_ignore_ascii_case(col))
                {
                    let sql_type = self.arrow_type_to_sql_type(field.data_type());
                    format!("CAST({val} AS {sql_type}) AS {col}")
                } else {
                    col.to_string()
                }
            })
            .collect();

        // Build condition for rows to update
        let update_condition = where_clause.unwrap_or("1=1");

        // Get updated rows
        let update_query = format!(
            "SELECT {} FROM {} WHERE {}",
            select_cols.join(", "),
            table_name,
            update_condition
        );
        let update_df = self.ctx.sql(&update_query).await?;
        let update_batches = update_df.collect().await?;
        let rows_updated: i64 = update_batches.iter().map(|b| b.num_rows() as i64).sum();

        // Get non-updated rows (rows that don't match WHERE condition)
        let keep_query = if where_clause.is_some() {
            format!("SELECT * FROM {table_name} WHERE NOT ({update_condition})")
        } else {
            // If no WHERE clause, all rows are updated, no rows to keep
            format!("SELECT * FROM {table_name} WHERE 1=0")
        };
        let keep_df = self.ctx.sql(&keep_query).await?;
        let keep_batches = keep_df.collect().await?;

        // Drop and recreate the table
        let drop_sql = format!("DROP TABLE {table_name}");
        let _ = self.ctx.sql(&drop_sql).await?.collect().await;

        // Combine kept rows with updated rows
        let all_batches: Vec<RecordBatch> =
            keep_batches.into_iter().chain(update_batches).collect();

        if !all_batches.is_empty() {
            let mem_table =
                datafusion::datasource::MemTable::try_new(table_schema, vec![all_batches])?;
            self.ctx.register_table(table_name, Arc::new(mem_table))?;
        } else {
            // Recreate empty table with same schema using MemTable
            // Use vec![vec![]] to create one empty partition (required for INSERT support)
            let empty_mem_table =
                datafusion::datasource::MemTable::try_new(table_schema.clone(), vec![vec![]])?;
            self.ctx
                .register_table(table_name, Arc::new(empty_mem_table))?;
        }

        // Build response
        let columns = vec![ColumnMetaData {
            name: "number of rows updated".to_string(),
            r#type: "NUMBER".to_string(),
            nullable: false,
            precision: None,
            scale: None,
            length: None,
        }];

        let data = vec![vec![Some(rows_updated.to_string())]];

        Ok(StatementResponse::success(data, columns, statement_handle))
    }

    /// Handle DELETE statement
    ///
    /// Syntax:
    /// ```sql
    /// DELETE FROM table_name [WHERE condition]
    /// ```
    async fn handle_delete(
        &self,
        sql: &str,
        statement_handle: String,
    ) -> Result<StatementResponse> {
        // Parse DELETE statement
        let delete_pattern =
            regex::Regex::new(r"(?is)DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?$").unwrap();

        let captures = delete_pattern.captures(sql).ok_or_else(|| {
            crate::error::Error::ExecutionError("Invalid DELETE syntax".to_string())
        })?;

        let table_name = captures.get(1).unwrap().as_str();
        let where_clause = captures.get(2).map(|m| m.as_str().trim());

        // Get table schema
        let catalog = self.ctx.catalog("datafusion").unwrap();
        let schema = catalog.schema("public").unwrap();
        let table = schema
            .table(table_name)
            .await
            .map_err(|_| crate::error::Error::TableNotFound(table_name.to_string()))?;
        let table =
            table.ok_or_else(|| crate::error::Error::TableNotFound(table_name.to_string()))?;
        let table_schema = table.schema();

        // Count rows to be deleted
        let count_query = if let Some(cond) = where_clause {
            format!("SELECT COUNT(*) FROM {table_name} WHERE {cond}")
        } else {
            format!("SELECT COUNT(*) FROM {table_name}")
        };
        let count_df = self.ctx.sql(&count_query).await?;
        let count_batches = count_df.collect().await?;
        let rows_deleted: i64 = if !count_batches.is_empty() && count_batches[0].num_rows() > 0 {
            count_batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|arr| arr.value(0))
                .unwrap_or(0)
        } else {
            0
        };

        // Get rows to keep (rows that don't match WHERE condition)
        let keep_query = if let Some(cond) = where_clause {
            format!("SELECT * FROM {table_name} WHERE NOT ({cond})")
        } else {
            // If no WHERE clause, delete all rows
            format!("SELECT * FROM {table_name} WHERE 1=0")
        };
        let keep_df = self.ctx.sql(&keep_query).await?;
        let keep_batches = keep_df.collect().await?;

        // Drop and recreate the table
        let drop_sql = format!("DROP TABLE {table_name}");
        let _ = self.ctx.sql(&drop_sql).await?.collect().await;

        if !keep_batches.is_empty() && keep_batches.iter().any(|b| b.num_rows() > 0) {
            let mem_table =
                datafusion::datasource::MemTable::try_new(table_schema, vec![keep_batches])?;
            self.ctx.register_table(table_name, Arc::new(mem_table))?;
        } else {
            // Recreate empty table with same schema using MemTable
            // Use vec![vec![]] to create one empty partition (required for INSERT support)
            let empty_mem_table =
                datafusion::datasource::MemTable::try_new(table_schema.clone(), vec![vec![]])?;
            self.ctx
                .register_table(table_name, Arc::new(empty_mem_table))?;
        }

        // Build response
        let columns = vec![ColumnMetaData {
            name: "number of rows deleted".to_string(),
            r#type: "NUMBER".to_string(),
            nullable: false,
            precision: None,
            scale: None,
            length: None,
        }];

        let data = vec![vec![Some(rows_deleted.to_string())]];

        Ok(StatementResponse::success(data, columns, statement_handle))
    }

    /// Handle TRUNCATE statement
    ///
    /// Syntax:
    /// ```sql
    /// TRUNCATE [TABLE] table_name
    /// ```
    async fn handle_truncate(
        &self,
        sql: &str,
        statement_handle: String,
    ) -> Result<StatementResponse> {
        // Parse TRUNCATE statement
        let truncate_pattern = regex::Regex::new(r"(?i)TRUNCATE\s+(?:TABLE\s+)?(\w+)").unwrap();

        let captures = truncate_pattern.captures(sql).ok_or_else(|| {
            crate::error::Error::ExecutionError("Invalid TRUNCATE syntax".to_string())
        })?;

        let table_name = captures.get(1).unwrap().as_str();

        // Get table schema
        let catalog = self.ctx.catalog("datafusion").unwrap();
        let schema = catalog.schema("public").unwrap();
        let table = schema
            .table(table_name)
            .await
            .map_err(|_| crate::error::Error::TableNotFound(table_name.to_string()))?;
        let table =
            table.ok_or_else(|| crate::error::Error::TableNotFound(table_name.to_string()))?;
        let table_schema = table.schema();

        // Drop the table
        let drop_sql = format!("DROP TABLE {table_name}");
        let _ = self.ctx.sql(&drop_sql).await?.collect().await;

        // Recreate empty table with same schema using MemTable (avoids type conversion issues)
        // Use vec![vec![]] to create one empty partition (required for INSERT support)
        let empty_mem_table =
            datafusion::datasource::MemTable::try_new(table_schema.clone(), vec![vec![]])?;
        self.ctx
            .register_table(table_name, Arc::new(empty_mem_table))?;

        // Build response
        let columns = vec![ColumnMetaData {
            name: "status".to_string(),
            r#type: "TEXT".to_string(),
            nullable: false,
            precision: None,
            scale: None,
            length: None,
        }];

        let data = vec![vec![Some("Statement executed successfully.".to_string())]];

        Ok(StatementResponse::success(data, columns, statement_handle))
    }

    /// Handle ALTER TABLE statement
    ///
    /// Supported operations:
    /// - ADD [COLUMN] column_name data_type
    /// - DROP [COLUMN] column_name
    /// - RENAME COLUMN old_name TO new_name
    /// - RENAME TO new_table_name
    async fn handle_alter_table(
        &self,
        sql: &str,
        statement_handle: String,
    ) -> Result<StatementResponse> {
        // ALTER TABLE table_name RENAME TO new_name
        let rename_table_pattern =
            regex::Regex::new(r"(?i)ALTER\s+TABLE\s+(\w+)\s+RENAME\s+TO\s+(\w+)").unwrap();

        if let Some(captures) = rename_table_pattern.captures(sql) {
            let old_name = captures.get(1).unwrap().as_str();
            let new_name = captures.get(2).unwrap().as_str();

            // Get table data
            let catalog = self.ctx.catalog("datafusion").unwrap();
            let schema = catalog.schema("public").unwrap();
            let table = schema
                .table(old_name)
                .await
                .map_err(|_| crate::error::Error::TableNotFound(old_name.to_string()))?;
            let table =
                table.ok_or_else(|| crate::error::Error::TableNotFound(old_name.to_string()))?;
            let table_schema = table.schema();

            // Get all data from old table
            let select_query = format!("SELECT * FROM {old_name}");
            let df = self.ctx.sql(&select_query).await?;
            let batches = df.collect().await?;

            // Drop old table
            let drop_sql = format!("DROP TABLE {old_name}");
            let _ = self.ctx.sql(&drop_sql).await?.collect().await;

            // Create new table with new name
            if !batches.is_empty() && batches.iter().any(|b| b.num_rows() > 0) {
                let mem_table =
                    datafusion::datasource::MemTable::try_new(table_schema, vec![batches])?;
                self.ctx.register_table(new_name, Arc::new(mem_table))?;
            } else {
                let empty_mem_table =
                    datafusion::datasource::MemTable::try_new(table_schema.clone(), vec![vec![]])?;
                self.ctx
                    .register_table(new_name, Arc::new(empty_mem_table))?;
            }

            return Ok(self.alter_success_response(statement_handle));
        }

        // ALTER TABLE table_name RENAME COLUMN old_name TO new_name
        let rename_col_pattern =
            regex::Regex::new(r"(?i)ALTER\s+TABLE\s+(\w+)\s+RENAME\s+COLUMN\s+(\w+)\s+TO\s+(\w+)")
                .unwrap();

        if let Some(captures) = rename_col_pattern.captures(sql) {
            let table_name = captures.get(1).unwrap().as_str();
            let old_col = captures.get(2).unwrap().as_str();
            let new_col = captures.get(3).unwrap().as_str();

            // Get table schema
            let catalog = self.ctx.catalog("datafusion").unwrap();
            let schema = catalog.schema("public").unwrap();
            let table = schema
                .table(table_name)
                .await
                .map_err(|_| crate::error::Error::TableNotFound(table_name.to_string()))?;
            let table =
                table.ok_or_else(|| crate::error::Error::TableNotFound(table_name.to_string()))?;
            let table_schema = table.schema();

            // Build new schema with renamed column
            let new_fields: Vec<Field> = table_schema
                .fields()
                .iter()
                .map(|f| {
                    if f.name().eq_ignore_ascii_case(old_col) {
                        Field::new(new_col, f.data_type().clone(), f.is_nullable())
                    } else {
                        f.as_ref().clone()
                    }
                })
                .collect();
            let new_schema = Arc::new(Schema::new(new_fields));

            // Get all data and recreate with new schema
            let select_query = format!("SELECT * FROM {table_name}");
            let df = self.ctx.sql(&select_query).await?;
            let batches = df.collect().await?;

            // Recreate batches with new schema
            let new_batches: Vec<RecordBatch> = batches
                .into_iter()
                .map(|batch| {
                    RecordBatch::try_new(new_schema.clone(), batch.columns().to_vec())
                        .expect("Schema mismatch during column rename")
                })
                .collect();

            // Drop and recreate
            let drop_sql = format!("DROP TABLE {table_name}");
            let _ = self.ctx.sql(&drop_sql).await?.collect().await;

            if !new_batches.is_empty() && new_batches.iter().any(|b| b.num_rows() > 0) {
                let mem_table =
                    datafusion::datasource::MemTable::try_new(new_schema, vec![new_batches])?;
                self.ctx.register_table(table_name, Arc::new(mem_table))?;
            } else {
                let empty_mem_table =
                    datafusion::datasource::MemTable::try_new(new_schema, vec![vec![]])?;
                self.ctx
                    .register_table(table_name, Arc::new(empty_mem_table))?;
            }

            return Ok(self.alter_success_response(statement_handle));
        }

        // ALTER TABLE table_name ADD [COLUMN] column_name data_type
        let add_col_pattern =
            regex::Regex::new(r"(?i)ALTER\s+TABLE\s+(\w+)\s+ADD\s+(?:COLUMN\s+)?(\w+)\s+(\w+)")
                .unwrap();

        if let Some(captures) = add_col_pattern.captures(sql) {
            let table_name = captures.get(1).unwrap().as_str();
            let col_name = captures.get(2).unwrap().as_str();
            let col_type = captures.get(3).unwrap().as_str();

            // Get table schema
            let catalog = self.ctx.catalog("datafusion").unwrap();
            let schema = catalog.schema("public").unwrap();
            let table = schema
                .table(table_name)
                .await
                .map_err(|_| crate::error::Error::TableNotFound(table_name.to_string()))?;
            let table =
                table.ok_or_else(|| crate::error::Error::TableNotFound(table_name.to_string()))?;
            let table_schema = table.schema();

            // Convert type string to Arrow DataType
            let arrow_type = self.snowflake_type_to_arrow(col_type);

            // Build new schema with added column
            let mut new_fields: Vec<Field> = table_schema
                .fields()
                .iter()
                .map(|f| f.as_ref().clone())
                .collect();
            new_fields.push(Field::new(col_name, arrow_type.clone(), true)); // New columns are nullable
            let new_schema = Arc::new(Schema::new(new_fields));

            // Get all data
            let select_query = format!("SELECT * FROM {table_name}");
            let df = self.ctx.sql(&select_query).await?;
            let batches = df.collect().await?;

            // Add NULL column to each batch
            let new_batches: Vec<RecordBatch> = batches
                .into_iter()
                .map(|batch| {
                    let null_col = arrow::array::new_null_array(&arrow_type, batch.num_rows());
                    let mut columns: Vec<Arc<dyn arrow::array::Array>> = batch.columns().to_vec();
                    columns.push(null_col);
                    RecordBatch::try_new(new_schema.clone(), columns)
                        .expect("Schema mismatch during column add")
                })
                .collect();

            // Drop and recreate
            let drop_sql = format!("DROP TABLE {table_name}");
            let _ = self.ctx.sql(&drop_sql).await?.collect().await;

            if !new_batches.is_empty() && new_batches.iter().any(|b| b.num_rows() > 0) {
                let mem_table =
                    datafusion::datasource::MemTable::try_new(new_schema, vec![new_batches])?;
                self.ctx.register_table(table_name, Arc::new(mem_table))?;
            } else {
                let empty_mem_table =
                    datafusion::datasource::MemTable::try_new(new_schema, vec![vec![]])?;
                self.ctx
                    .register_table(table_name, Arc::new(empty_mem_table))?;
            }

            return Ok(self.alter_success_response(statement_handle));
        }

        // ALTER TABLE table_name DROP [COLUMN] column_name
        let drop_col_pattern =
            regex::Regex::new(r"(?i)ALTER\s+TABLE\s+(\w+)\s+DROP\s+(?:COLUMN\s+)?(\w+)").unwrap();

        if let Some(captures) = drop_col_pattern.captures(sql) {
            let table_name = captures.get(1).unwrap().as_str();
            let col_name = captures.get(2).unwrap().as_str();

            // Get table schema
            let catalog = self.ctx.catalog("datafusion").unwrap();
            let schema = catalog.schema("public").unwrap();
            let table = schema
                .table(table_name)
                .await
                .map_err(|_| crate::error::Error::TableNotFound(table_name.to_string()))?;
            let table =
                table.ok_or_else(|| crate::error::Error::TableNotFound(table_name.to_string()))?;
            let table_schema = table.schema();

            // Find column index to drop
            let col_idx = table_schema
                .fields()
                .iter()
                .position(|f| f.name().eq_ignore_ascii_case(col_name))
                .ok_or_else(|| {
                    crate::error::Error::ExecutionError(format!("Column {col_name} not found"))
                })?;

            // Build new schema without the dropped column
            let new_fields: Vec<Field> = table_schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != col_idx)
                .map(|(_, f)| f.as_ref().clone())
                .collect();
            let new_schema = Arc::new(Schema::new(new_fields));

            // Get all data and remove the column
            let select_query = format!("SELECT * FROM {table_name}");
            let df = self.ctx.sql(&select_query).await?;
            let batches = df.collect().await?;

            // Remove column from each batch
            let new_batches: Vec<RecordBatch> = batches
                .into_iter()
                .map(|batch| {
                    let columns: Vec<Arc<dyn arrow::array::Array>> = batch
                        .columns()
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| *i != col_idx)
                        .map(|(_, c)| c.clone())
                        .collect();
                    RecordBatch::try_new(new_schema.clone(), columns)
                        .expect("Schema mismatch during column drop")
                })
                .collect();

            // Drop and recreate
            let drop_sql = format!("DROP TABLE {table_name}");
            let _ = self.ctx.sql(&drop_sql).await?.collect().await;

            if !new_batches.is_empty() && new_batches.iter().any(|b| b.num_rows() > 0) {
                let mem_table =
                    datafusion::datasource::MemTable::try_new(new_schema, vec![new_batches])?;
                self.ctx.register_table(table_name, Arc::new(mem_table))?;
            } else {
                let empty_mem_table =
                    datafusion::datasource::MemTable::try_new(new_schema, vec![vec![]])?;
                self.ctx
                    .register_table(table_name, Arc::new(empty_mem_table))?;
            }

            return Ok(self.alter_success_response(statement_handle));
        }

        Err(crate::error::Error::ExecutionError(format!(
            "Unsupported ALTER TABLE syntax: {sql}"
        )))
    }

    /// Helper function for ALTER TABLE success response
    fn alter_success_response(&self, statement_handle: String) -> StatementResponse {
        let columns = vec![ColumnMetaData {
            name: "status".to_string(),
            r#type: "TEXT".to_string(),
            nullable: false,
            precision: None,
            scale: None,
            length: None,
        }];

        let data = vec![vec![Some("Statement executed successfully.".to_string())]];

        StatementResponse::success(data, columns, statement_handle)
    }

    /// Handle CREATE VIEW statement
    ///
    /// Syntax:
    /// - CREATE VIEW view_name AS select_statement
    /// - CREATE OR REPLACE VIEW view_name AS select_statement
    fn handle_create_view(&self, sql: &str, statement_handle: String) -> Result<StatementResponse> {
        // Parse CREATE VIEW statement
        let create_view_pattern =
            regex::Regex::new(r"(?is)CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(\w+)\s+AS\s+(.+)")
                .unwrap();

        let captures = create_view_pattern.captures(sql).ok_or_else(|| {
            crate::error::Error::ExecutionError("Invalid CREATE VIEW syntax".to_string())
        })?;

        let view_name = captures.get(1).unwrap().as_str().to_uppercase();
        let select_statement = captures.get(2).unwrap().as_str().to_string();

        // Store the view definition
        {
            let mut views = self.views.write().unwrap();
            views.insert(view_name.clone(), select_statement);
        }

        let columns = vec![ColumnMetaData {
            name: "status".to_string(),
            r#type: "TEXT".to_string(),
            nullable: false,
            precision: None,
            scale: None,
            length: None,
        }];

        let data = vec![vec![Some(format!(
            "View {view_name} created successfully."
        ))]];

        Ok(StatementResponse::success(data, columns, statement_handle))
    }

    /// Handle DROP VIEW statement
    ///
    /// Syntax:
    /// - DROP VIEW view_name
    /// - DROP VIEW IF EXISTS view_name
    fn handle_drop_view(&self, sql: &str, statement_handle: String) -> Result<StatementResponse> {
        // Parse DROP VIEW statement
        let drop_view_pattern =
            regex::Regex::new(r"(?i)DROP\s+VIEW\s+(?:IF\s+EXISTS\s+)?(\w+)").unwrap();

        let captures = drop_view_pattern.captures(sql).ok_or_else(|| {
            crate::error::Error::ExecutionError("Invalid DROP VIEW syntax".to_string())
        })?;

        let view_name = captures.get(1).unwrap().as_str().to_uppercase();
        let if_exists = sql.to_uppercase().contains("IF EXISTS");

        // Remove the view definition
        let removed = {
            let mut views = self.views.write().unwrap();
            views.remove(&view_name).is_some()
        };

        if !removed && !if_exists {
            return Err(crate::error::Error::ExecutionError(format!(
                "View {view_name} does not exist"
            )));
        }

        let columns = vec![ColumnMetaData {
            name: "status".to_string(),
            r#type: "TEXT".to_string(),
            nullable: false,
            precision: None,
            scale: None,
            length: None,
        }];

        let data = vec![vec![Some(format!(
            "View {view_name} dropped successfully."
        ))]];

        Ok(StatementResponse::success(data, columns, statement_handle))
    }

    /// Expand view references in SQL by replacing view names with their definitions
    fn expand_views(&self, sql: &str) -> String {
        let views = self.views.read().unwrap();
        if views.is_empty() {
            return sql.to_string();
        }

        let mut result = sql.to_string();

        // Simple view expansion: replace "FROM view_name" with "FROM (select_stmt) AS view_name"
        for (view_name, select_stmt) in views.iter() {
            // Match case-insensitive view name after FROM
            let escaped_view = regex::escape(view_name);
            let from_pattern =
                regex::Regex::new(&format!(r"(?i)\bFROM\s+{escaped_view}\b")).unwrap();

            if from_pattern.is_match(&result) {
                let replacement = format!("FROM ({select_stmt}) AS {view_name}");
                result = from_pattern
                    .replace_all(&result, replacement.as_str())
                    .to_string();
            }

            // Match JOIN view_name
            let join_pattern =
                regex::Regex::new(&format!(r"(?i)\bJOIN\s+{escaped_view}\b")).unwrap();

            if join_pattern.is_match(&result) {
                let replacement = format!("JOIN ({select_stmt}) AS {view_name}");
                result = join_pattern
                    .replace_all(&result, replacement.as_str())
                    .to_string();
            }
        }

        result
    }

    /// Handle USE DATABASE/SCHEMA statement
    ///
    /// Syntax:
    /// - USE DATABASE database_name
    /// - USE SCHEMA schema_name
    /// - USE database_name.schema_name
    fn handle_use(&self, sql: &str, statement_handle: String) -> Result<StatementResponse> {
        // USE DATABASE database_name
        let use_database_pattern = regex::Regex::new(r"(?i)USE\s+DATABASE\s+(\w+)").unwrap();

        if let Some(captures) = use_database_pattern.captures(sql) {
            let database_name = captures.get(1).unwrap().as_str().to_uppercase();

            // Update catalog state (lenient - create if not exists for emulator compatibility)
            if !self.catalog.database_exists(&database_name) {
                // Auto-create database for emulator compatibility
                let _ = self.catalog.create_database(&self.ctx, &database_name);
            }
            let _ = self.catalog.use_database(&database_name);

            // Update session state
            {
                let mut current_db = self.current_database.write().unwrap();
                *current_db = database_name.clone();
            }

            return Ok(self.use_success_response(
                format!(
                    "Statement executed successfully. Database '{database_name}' is now in use."
                ),
                statement_handle,
            ));
        }

        // USE SCHEMA schema_name
        let use_schema_pattern = regex::Regex::new(r"(?i)USE\s+SCHEMA\s+(\w+)").unwrap();

        if let Some(captures) = use_schema_pattern.captures(sql) {
            let schema_name = captures.get(1).unwrap().as_str().to_uppercase();

            // Update catalog state
            let _ = self.catalog.use_schema(&schema_name);

            // Update session state
            {
                let mut current_schema = self.current_schema.write().unwrap();
                *current_schema = schema_name.clone();
            }

            return Ok(self.use_success_response(
                format!("Statement executed successfully. Schema '{schema_name}' is now in use."),
                statement_handle,
            ));
        }

        // USE database_name.schema_name
        let use_qualified_pattern = regex::Regex::new(r"(?i)USE\s+(\w+)\.(\w+)").unwrap();

        if let Some(captures) = use_qualified_pattern.captures(sql) {
            let database_name = captures.get(1).unwrap().as_str().to_uppercase();
            let schema_name = captures.get(2).unwrap().as_str().to_uppercase();

            // Update catalog state (lenient - create if not exists)
            if !self.catalog.database_exists(&database_name) {
                let _ = self.catalog.create_database(&self.ctx, &database_name);
            }
            let _ = self.catalog.use_database(&database_name);
            let _ = self.catalog.use_schema(&schema_name);

            // Update session state
            {
                let mut current_db = self.current_database.write().unwrap();
                *current_db = database_name.clone();
            }
            {
                let mut current_schema = self.current_schema.write().unwrap();
                *current_schema = schema_name.clone();
            }

            return Ok(self.use_success_response(
                format!("Statement executed successfully. Database '{database_name}' and Schema '{schema_name}' are now in use."),
                statement_handle,
            ));
        }

        // USE database_name (shorthand for USE DATABASE)
        let use_simple_pattern = regex::Regex::new(r"(?i)USE\s+(\w+)\s*$").unwrap();

        if let Some(captures) = use_simple_pattern.captures(sql) {
            let name = captures.get(1).unwrap().as_str().to_uppercase();

            // Could be either database or schema - treat as schema for simplicity
            {
                let mut current_schema = self.current_schema.write().unwrap();
                *current_schema = name.clone();
            }

            return Ok(self.use_success_response(
                format!("Statement executed successfully. Schema '{name}' is now in use."),
                statement_handle,
            ));
        }

        Err(crate::error::Error::ExecutionError(format!(
            "Invalid USE syntax: {sql}"
        )))
    }

    /// Helper function for USE success response
    fn use_success_response(&self, message: String, statement_handle: String) -> StatementResponse {
        let columns = vec![ColumnMetaData {
            name: "status".to_string(),
            r#type: "TEXT".to_string(),
            nullable: false,
            precision: None,
            scale: None,
            length: None,
        }];

        let data = vec![vec![Some(message)]];

        StatementResponse::success(data, columns, statement_handle)
    }

    /// Get current database name
    pub fn get_current_database(&self) -> String {
        self.current_database.read().unwrap().clone()
    }

    /// Get current schema name
    pub fn get_current_schema(&self) -> String {
        self.current_schema.read().unwrap().clone()
    }

    /// Handle context function queries (CURRENT_DATABASE, CURRENT_SCHEMA)
    ///
    /// These functions need to return the session state values, not hardcoded defaults.
    fn handle_context_function(
        &self,
        sql: &str,
        statement_handle: String,
    ) -> Result<StatementResponse> {
        let sql_upper = sql.trim().to_uppercase();

        // Handle SELECT CURRENT_DATABASE()
        if sql_upper.contains("CURRENT_DATABASE()") {
            let db_name = self.get_current_database();
            let columns = vec![ColumnMetaData {
                name: "CURRENT_DATABASE()".to_string(),
                r#type: "TEXT".to_string(),
                nullable: true,
                precision: None,
                scale: None,
                length: None,
            }];
            let data = vec![vec![Some(db_name)]];
            return Ok(StatementResponse::success(data, columns, statement_handle));
        }

        // Handle SELECT CURRENT_SCHEMA()
        if sql_upper.contains("CURRENT_SCHEMA()") {
            let schema_name = self.get_current_schema();
            let columns = vec![ColumnMetaData {
                name: "CURRENT_SCHEMA()".to_string(),
                r#type: "TEXT".to_string(),
                nullable: true,
                precision: None,
                scale: None,
                length: None,
            }];
            let data = vec![vec![Some(schema_name)]];
            return Ok(StatementResponse::success(data, columns, statement_handle));
        }

        Err(crate::error::Error::ExecutionError(
            "Unknown context function".to_string(),
        ))
    }

    /// Handle transaction statements (BEGIN, COMMIT, ROLLBACK)
    ///
    /// Implements basic transaction support:
    /// - BEGIN/START TRANSACTION: Start a new transaction, snapshot current table state
    /// - COMMIT: End transaction, discard snapshot (changes are already applied)
    /// - ROLLBACK: End transaction, restore tables from snapshot
    async fn handle_transaction(
        &self,
        sql: &str,
        statement_handle: String,
    ) -> Result<StatementResponse> {
        let sql_upper = sql.trim().to_uppercase();

        if sql_upper.starts_with("BEGIN") || sql_upper.starts_with("START TRANSACTION") {
            return self.handle_begin(statement_handle).await;
        }

        if sql_upper == "COMMIT" {
            return self.handle_commit(statement_handle);
        }

        if sql_upper == "ROLLBACK" {
            return self.handle_rollback(statement_handle).await;
        }

        Err(crate::error::Error::ExecutionError(
            "Unknown transaction command".to_string(),
        ))
    }

    /// Handle BEGIN/START TRANSACTION
    async fn handle_begin(&self, statement_handle: String) -> Result<StatementResponse> {
        // Check if already in a transaction
        {
            let in_tx = self.in_transaction.read().unwrap();
            if *in_tx {
                return Err(crate::error::Error::ExecutionError(
                    "Transaction already in progress".to_string(),
                ));
            }
        }

        // Snapshot all user tables
        let mut snapshot = HashMap::new();
        let catalog = self.ctx.catalog("datafusion").unwrap();
        let schema = catalog.schema("public").unwrap();
        let table_names = schema.table_names();

        for table_name in table_names {
            // Skip internal tables
            if table_name.starts_with('_') {
                continue;
            }

            if let Ok(Some(_table)) = schema.table(&table_name).await {
                // Get all data from the table
                let select_sql = format!("SELECT * FROM {}", table_name);
                if let Ok(df) = self.ctx.sql(&select_sql).await {
                    if let Ok(batches) = df.collect().await {
                        snapshot.insert(table_name.clone(), batches);
                    }
                }
            }
        }

        // Store snapshot and mark as in transaction
        {
            let mut tx_snapshot = self.transaction_snapshot.write().unwrap();
            *tx_snapshot = snapshot;
        }
        {
            let mut in_tx = self.in_transaction.write().unwrap();
            *in_tx = true;
        }

        let columns = vec![ColumnMetaData {
            name: "status".to_string(),
            r#type: "TEXT".to_string(),
            nullable: false,
            precision: None,
            scale: None,
            length: None,
        }];
        let data = vec![vec![Some("Statement executed successfully.".to_string())]];

        Ok(StatementResponse::success(data, columns, statement_handle))
    }

    /// Handle COMMIT
    fn handle_commit(&self, statement_handle: String) -> Result<StatementResponse> {
        // Check if in a transaction
        {
            let in_tx = self.in_transaction.read().unwrap();
            if !*in_tx {
                // Snowflake allows COMMIT even without explicit BEGIN
                // Just return success
            }
        }

        // Clear transaction state
        {
            let mut tx_snapshot = self.transaction_snapshot.write().unwrap();
            tx_snapshot.clear();
        }
        {
            let mut in_tx = self.in_transaction.write().unwrap();
            *in_tx = false;
        }

        let columns = vec![ColumnMetaData {
            name: "status".to_string(),
            r#type: "TEXT".to_string(),
            nullable: false,
            precision: None,
            scale: None,
            length: None,
        }];
        let data = vec![vec![Some("Statement executed successfully.".to_string())]];

        Ok(StatementResponse::success(data, columns, statement_handle))
    }

    /// Handle ROLLBACK
    async fn handle_rollback(&self, statement_handle: String) -> Result<StatementResponse> {
        // Check if in a transaction
        let was_in_tx = {
            let in_tx = self.in_transaction.read().unwrap();
            *in_tx
        };

        if was_in_tx {
            // Restore tables from snapshot
            let snapshot = {
                let tx_snapshot = self.transaction_snapshot.read().unwrap();
                tx_snapshot.clone()
            };

            for (table_name, batches) in snapshot {
                if batches.is_empty() {
                    continue;
                }

                // Get schema from first batch
                let table_schema = batches[0].schema();

                // Recreate the table with snapshot data
                let mem_table = MemTable::try_new(table_schema, vec![batches]).map_err(|e| {
                    crate::error::Error::ExecutionError(format!(
                        "Failed to restore table {}: {}",
                        table_name, e
                    ))
                })?;

                // Re-register the table
                self.ctx
                    .deregister_table(&table_name)
                    .map_err(|e| crate::error::Error::DataFusion(e))?;
                self.ctx
                    .register_table(&table_name, Arc::new(mem_table))
                    .map_err(|e| crate::error::Error::DataFusion(e))?;
            }
        }

        // Clear transaction state
        {
            let mut tx_snapshot = self.transaction_snapshot.write().unwrap();
            tx_snapshot.clear();
        }
        {
            let mut in_tx = self.in_transaction.write().unwrap();
            *in_tx = false;
        }

        let columns = vec![ColumnMetaData {
            name: "status".to_string(),
            r#type: "TEXT".to_string(),
            nullable: false,
            precision: None,
            scale: None,
            length: None,
        }];
        let data = vec![vec![Some("Statement executed successfully.".to_string())]];

        Ok(StatementResponse::success(data, columns, statement_handle))
    }

    // =========================================================================
    // COPY INTO Support (Stages and File Loading)
    // =========================================================================

    /// Handle CREATE STAGE command
    ///
    /// Syntax:
    /// ```sql
    /// CREATE STAGE stage_name URL = 'file:///path/to/directory'
    /// CREATE OR REPLACE STAGE stage_name URL = 'file:///path/to/directory'
    /// ```
    fn handle_create_stage(
        &self,
        sql: &str,
        statement_handle: String,
    ) -> Result<StatementResponse> {
        // Parse stage name and URL
        let create_pattern = regex::Regex::new(
            r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?STAGE\s+(\w+)\s+URL\s*=\s*'([^']+)'",
        )
        .unwrap();

        let captures = create_pattern.captures(sql).ok_or_else(|| {
            crate::error::Error::ExecutionError("Invalid CREATE STAGE syntax".to_string())
        })?;

        let stage_name = captures.get(1).unwrap().as_str().to_uppercase();
        let url = captures.get(2).unwrap().as_str();

        // Extract directory path from URL (file:///path or /path)
        let dir_path = if url.starts_with("file://") {
            url.strip_prefix("file://").unwrap().to_string()
        } else {
            url.to_string()
        };

        // Store the stage
        {
            let mut stages = self.stages.write().unwrap();
            stages.insert(stage_name.clone(), dir_path);
        }

        let columns = vec![ColumnMetaData {
            name: "status".to_string(),
            r#type: "TEXT".to_string(),
            nullable: false,
            precision: None,
            scale: None,
            length: None,
        }];

        let data = vec![vec![Some(format!(
            "Stage {stage_name} successfully created."
        ))]];

        Ok(StatementResponse::success(data, columns, statement_handle))
    }

    /// Handle DROP STAGE command
    ///
    /// Syntax:
    /// ```sql
    /// DROP STAGE stage_name
    /// DROP STAGE IF EXISTS stage_name
    /// ```
    fn handle_drop_stage(&self, sql: &str, statement_handle: String) -> Result<StatementResponse> {
        let drop_pattern =
            regex::Regex::new(r"(?i)DROP\s+STAGE\s+(?:IF\s+EXISTS\s+)?(\w+)").unwrap();

        let captures = drop_pattern.captures(sql).ok_or_else(|| {
            crate::error::Error::ExecutionError("Invalid DROP STAGE syntax".to_string())
        })?;

        let stage_name = captures.get(1).unwrap().as_str().to_uppercase();
        let if_exists = sql.to_uppercase().contains("IF EXISTS");

        // Remove the stage
        let removed = {
            let mut stages = self.stages.write().unwrap();
            stages.remove(&stage_name).is_some()
        };

        if !removed && !if_exists {
            return Err(crate::error::Error::ExecutionError(format!(
                "Stage {stage_name} does not exist"
            )));
        }

        let columns = vec![ColumnMetaData {
            name: "status".to_string(),
            r#type: "TEXT".to_string(),
            nullable: false,
            precision: None,
            scale: None,
            length: None,
        }];

        let message = if removed {
            format!("Stage {stage_name} successfully dropped.")
        } else {
            "Statement executed successfully.".to_string()
        };

        let data = vec![vec![Some(message)]];

        Ok(StatementResponse::success(data, columns, statement_handle))
    }

    // =========================================================================
    // DATABASE/SCHEMA Management
    // =========================================================================

    /// Handle CREATE DATABASE command
    ///
    /// Syntax:
    /// ```sql
    /// CREATE DATABASE database_name
    /// CREATE OR REPLACE DATABASE database_name
    /// CREATE DATABASE IF NOT EXISTS database_name
    /// ```
    fn handle_create_database(
        &self,
        sql: &str,
        statement_handle: String,
    ) -> Result<StatementResponse> {
        let create_pattern = regex::Regex::new(
            r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?DATABASE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)",
        )
        .unwrap();

        let captures = create_pattern.captures(sql).ok_or_else(|| {
            crate::error::Error::ExecutionError("Invalid CREATE DATABASE syntax".to_string())
        })?;

        let db_name = captures.get(1).unwrap().as_str().to_uppercase();
        let or_replace = sql.to_uppercase().contains("OR REPLACE");
        let if_not_exists = sql.to_uppercase().contains("IF NOT EXISTS");

        // Check if database already exists
        if self.catalog.database_exists(&db_name) {
            if or_replace {
                // Drop and recreate
                self.catalog.drop_database(&db_name)?;
            } else if if_not_exists {
                // Just return success without creating
                let columns = vec![ColumnMetaData {
                    name: "status".to_string(),
                    r#type: "TEXT".to_string(),
                    nullable: false,
                    precision: None,
                    scale: None,
                    length: None,
                }];
                let data = vec![vec![Some(format!(
                    "Database '{db_name}' already exists, statement succeeded."
                ))]];
                return Ok(StatementResponse::success(data, columns, statement_handle));
            } else {
                return Err(crate::error::Error::ExecutionError(format!(
                    "Database '{db_name}' already exists."
                )));
            }
        }

        // Create the database
        self.catalog.create_database(&self.ctx, &db_name)?;

        let columns = vec![ColumnMetaData {
            name: "status".to_string(),
            r#type: "TEXT".to_string(),
            nullable: false,
            precision: None,
            scale: None,
            length: None,
        }];

        let data = vec![vec![Some(format!(
            "Database {db_name} successfully created."
        ))]];

        Ok(StatementResponse::success(data, columns, statement_handle))
    }

    /// Handle DROP DATABASE command
    ///
    /// Syntax:
    /// ```sql
    /// DROP DATABASE database_name
    /// DROP DATABASE IF EXISTS database_name
    /// ```
    fn handle_drop_database(
        &self,
        sql: &str,
        statement_handle: String,
    ) -> Result<StatementResponse> {
        let drop_pattern =
            regex::Regex::new(r"(?i)DROP\s+DATABASE\s+(?:IF\s+EXISTS\s+)?(\w+)").unwrap();

        let captures = drop_pattern.captures(sql).ok_or_else(|| {
            crate::error::Error::ExecutionError("Invalid DROP DATABASE syntax".to_string())
        })?;

        let db_name = captures.get(1).unwrap().as_str().to_uppercase();
        let if_exists = sql.to_uppercase().contains("IF EXISTS");

        // Check if database exists
        if !self.catalog.database_exists(&db_name) {
            if if_exists {
                let columns = vec![ColumnMetaData {
                    name: "status".to_string(),
                    r#type: "TEXT".to_string(),
                    nullable: false,
                    precision: None,
                    scale: None,
                    length: None,
                }];
                let data = vec![vec![Some("Statement executed successfully.".to_string())]];
                return Ok(StatementResponse::success(data, columns, statement_handle));
            } else {
                return Err(crate::error::Error::ExecutionError(format!(
                    "Database '{db_name}' does not exist."
                )));
            }
        }

        // Drop the database
        self.catalog.drop_database(&db_name)?;

        let columns = vec![ColumnMetaData {
            name: "status".to_string(),
            r#type: "TEXT".to_string(),
            nullable: false,
            precision: None,
            scale: None,
            length: None,
        }];

        let data = vec![vec![Some(format!(
            "Database {db_name} successfully dropped."
        ))]];

        Ok(StatementResponse::success(data, columns, statement_handle))
    }

    /// Handle CREATE SCHEMA command
    ///
    /// Syntax:
    /// ```sql
    /// CREATE SCHEMA schema_name
    /// CREATE OR REPLACE SCHEMA schema_name
    /// CREATE SCHEMA IF NOT EXISTS schema_name
    /// ```
    fn handle_create_schema(
        &self,
        sql: &str,
        statement_handle: String,
    ) -> Result<StatementResponse> {
        let create_pattern = regex::Regex::new(
            r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?SCHEMA\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)",
        )
        .unwrap();

        let captures = create_pattern.captures(sql).ok_or_else(|| {
            crate::error::Error::ExecutionError("Invalid CREATE SCHEMA syntax".to_string())
        })?;

        let schema_name = captures.get(1).unwrap().as_str().to_uppercase();

        // For now, just acknowledge the schema creation
        // In a full implementation, this would create a schema in the current database
        let columns = vec![ColumnMetaData {
            name: "status".to_string(),
            r#type: "TEXT".to_string(),
            nullable: false,
            precision: None,
            scale: None,
            length: None,
        }];

        let data = vec![vec![Some(format!(
            "Schema {schema_name} successfully created."
        ))]];

        Ok(StatementResponse::success(data, columns, statement_handle))
    }

    /// Handle DROP SCHEMA command
    ///
    /// Syntax:
    /// ```sql
    /// DROP SCHEMA schema_name
    /// DROP SCHEMA IF EXISTS schema_name
    /// ```
    fn handle_drop_schema(&self, sql: &str, statement_handle: String) -> Result<StatementResponse> {
        let drop_pattern =
            regex::Regex::new(r"(?i)DROP\s+SCHEMA\s+(?:IF\s+EXISTS\s+)?(\w+)").unwrap();

        let captures = drop_pattern.captures(sql).ok_or_else(|| {
            crate::error::Error::ExecutionError("Invalid DROP SCHEMA syntax".to_string())
        })?;

        let schema_name = captures.get(1).unwrap().as_str().to_uppercase();

        // For now, just acknowledge the schema drop
        let columns = vec![ColumnMetaData {
            name: "status".to_string(),
            r#type: "TEXT".to_string(),
            nullable: false,
            precision: None,
            scale: None,
            length: None,
        }];

        let data = vec![vec![Some(format!(
            "Schema {schema_name} successfully dropped."
        ))]];

        Ok(StatementResponse::success(data, columns, statement_handle))
    }

    /// Handle COPY INTO command
    ///
    /// Syntax:
    /// ```sql
    /// COPY INTO table_name FROM @stage_name FILE_FORMAT = (TYPE = 'CSV')
    /// COPY INTO table_name FROM @stage_name/file.csv FILE_FORMAT = (TYPE = 'CSV')
    /// ```
    async fn handle_copy_into(
        &self,
        sql: &str,
        statement_handle: String,
    ) -> Result<StatementResponse> {
        // Parse COPY INTO syntax
        // COPY INTO table FROM @stage[/path] [FILE_FORMAT = (TYPE = 'CSV' [SKIP_HEADER = 1])]
        let copy_pattern = regex::Regex::new(
            r"(?i)COPY\s+INTO\s+(\w+)\s+FROM\s+@(\w+)(?:/([^\s]+))?\s*(?:FILE_FORMAT\s*=\s*\(([^)]+)\))?",
        )
        .unwrap();

        let captures = copy_pattern.captures(sql).ok_or_else(|| {
            crate::error::Error::ExecutionError("Invalid COPY INTO syntax".to_string())
        })?;

        // Table names are stored in lowercase in DataFusion
        let table_name = captures.get(1).unwrap().as_str().to_lowercase();
        let stage_name = captures.get(2).unwrap().as_str().to_uppercase();
        let file_path = captures.get(3).map(|m| m.as_str().to_string());
        let file_format = captures.get(4).map(|m| m.as_str().to_string());

        // Parse file format options
        let skip_header = if let Some(ref format_str) = file_format {
            let format_upper = format_str.to_uppercase();
            if format_upper.contains("SKIP_HEADER") {
                // Extract skip_header value
                let skip_pattern = regex::Regex::new(r"(?i)SKIP_HEADER\s*=\s*(\d+)").unwrap();
                skip_pattern
                    .captures(&format_upper)
                    .and_then(|c| c.get(1))
                    .and_then(|m| m.as_str().parse::<usize>().ok())
                    .unwrap_or(0)
            } else {
                0
            }
        } else {
            0
        };

        // Get stage directory
        let stage_dir = {
            let stages = self.stages.read().unwrap();
            stages.get(&stage_name).cloned()
        }
        .ok_or_else(|| {
            crate::error::Error::ExecutionError(format!("Stage {stage_name} does not exist"))
        })?;

        // Build full file path
        let full_path = if let Some(ref file) = file_path {
            format!("{}/{}", stage_dir, file)
        } else {
            stage_dir.clone()
        };

        // Get list of CSV files to load
        let files_to_load = self.get_csv_files(&full_path)?;

        if files_to_load.is_empty() {
            return Err(crate::error::Error::ExecutionError(format!(
                "No CSV files found in {}",
                full_path
            )));
        }

        // Get target table schema
        let catalog = self.ctx.catalog("datafusion").unwrap();
        let schema = catalog.schema("public").unwrap();
        let table = schema
            .table(&table_name)
            .await
            .map_err(|e| crate::error::Error::DataFusion(e))?
            .ok_or_else(|| {
                crate::error::Error::ExecutionError(format!("Table {table_name} does not exist"))
            })?;

        let table_schema = table.schema();

        // Load and insert data from each CSV file
        let mut total_rows: usize = 0;
        for csv_file in &files_to_load {
            let rows_loaded = self
                .load_csv_into_table(&table_name, csv_file, &table_schema, skip_header)
                .await?;
            total_rows += rows_loaded;
        }

        let columns = vec![
            ColumnMetaData {
                name: "rows_loaded".to_string(),
                r#type: "NUMBER".to_string(),
                nullable: false,
                precision: None,
                scale: None,
                length: None,
            },
            ColumnMetaData {
                name: "files_loaded".to_string(),
                r#type: "NUMBER".to_string(),
                nullable: false,
                precision: None,
                scale: None,
                length: None,
            },
        ];

        let data = vec![vec![
            Some(total_rows.to_string()),
            Some(files_to_load.len().to_string()),
        ]];

        Ok(StatementResponse::success(data, columns, statement_handle))
    }

    /// Get list of CSV files from a path (file or directory)
    fn get_csv_files(&self, path: &str) -> Result<Vec<String>> {
        let path_obj = std::path::Path::new(path);

        if path_obj.is_file() {
            Ok(vec![path.to_string()])
        } else if path_obj.is_dir() {
            let mut files = Vec::new();
            let entries = std::fs::read_dir(path).map_err(|e| {
                crate::error::Error::ExecutionError(format!(
                    "Failed to read directory {}: {}",
                    path, e
                ))
            })?;

            for entry in entries {
                let entry = entry.map_err(|e| {
                    crate::error::Error::ExecutionError(format!(
                        "Failed to read directory entry: {}",
                        e
                    ))
                })?;
                let file_path = entry.path();
                if file_path.is_file() {
                    if let Some(ext) = file_path.extension() {
                        if ext.to_string_lossy().to_lowercase() == "csv" {
                            files.push(file_path.to_string_lossy().to_string());
                        }
                    }
                }
            }

            Ok(files)
        } else {
            Err(crate::error::Error::ExecutionError(format!(
                "Path does not exist: {}",
                path
            )))
        }
    }

    /// Load CSV file data into a table
    async fn load_csv_into_table(
        &self,
        table_name: &str,
        csv_path: &str,
        table_schema: &Arc<arrow::datatypes::Schema>,
        skip_header: usize,
    ) -> Result<usize> {
        use std::io::BufRead;

        let file = std::fs::File::open(csv_path).map_err(|e| {
            crate::error::Error::ExecutionError(format!("Failed to open file {}: {}", csv_path, e))
        })?;
        let reader = std::io::BufReader::new(file);

        let mut lines: Vec<String> = reader
            .lines()
            .map(|l| l.unwrap_or_default())
            .skip(skip_header)
            .filter(|l| !l.trim().is_empty())
            .collect();

        if lines.is_empty() {
            return Ok(0);
        }

        let num_rows = lines.len();

        // Build INSERT statements for each row
        let columns: Vec<String> = table_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let columns_str = columns.join(", ");

        for line in &mut lines {
            // Parse CSV line (simple parsing, doesn't handle quoted fields with commas)
            let values: Vec<String> = line
                .split(',')
                .map(|v| {
                    let trimmed = v.trim();
                    // Quote string values, keep numbers as-is
                    if trimmed.parse::<f64>().is_ok() || trimmed.to_uppercase() == "NULL" {
                        trimmed.to_string()
                    } else {
                        format!("'{}'", trimmed.replace('\'', "''"))
                    }
                })
                .collect();

            let values_str = values.join(", ");
            let insert_sql = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                table_name, columns_str, values_str
            );

            // Execute the INSERT
            self.ctx
                .sql(&insert_sql)
                .await
                .map_err(|e| crate::error::Error::DataFusion(e))?
                .collect()
                .await
                .map_err(|e| crate::error::Error::DataFusion(e))?;
        }

        Ok(num_rows)
    }

    /// Convert Snowflake type string to Arrow DataType
    fn snowflake_type_to_arrow(&self, type_str: &str) -> DataType {
        match type_str.to_uppercase().as_str() {
            "INT" | "INTEGER" | "NUMBER" | "BIGINT" => DataType::Int64,
            "SMALLINT" => DataType::Int16,
            "TINYINT" => DataType::Int8,
            "FLOAT" | "DOUBLE" | "REAL" => DataType::Float64,
            "BOOLEAN" | "BOOL" => DataType::Boolean,
            "VARCHAR" | "STRING" | "TEXT" | "CHAR" => DataType::Utf8,
            "DATE" => DataType::Date32,
            "TIMESTAMP" | "DATETIME" => {
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
            }
            _ => DataType::Utf8, // Default to string for unknown types
        }
    }

    /// Handle CREATE SEQUENCE command
    ///
    /// Syntax:
    /// ```sql
    /// CREATE SEQUENCE seq_name [START = n] [INCREMENT = n]
    /// ```
    fn handle_create_sequence(
        &self,
        sql: &str,
        statement_handle: String,
    ) -> Result<StatementResponse> {
        // Parse sequence name and options
        let create_pattern = regex::Regex::new(
            r"(?i)CREATE\s+SEQUENCE\s+(\w+)(?:\s+START\s*(?:=|WITH)\s*(\d+))?(?:\s+INCREMENT\s*(?:=|BY)\s*(\d+))?",
        )
        .unwrap();

        let captures = create_pattern.captures(sql).ok_or_else(|| {
            crate::error::Error::ExecutionError("Invalid CREATE SEQUENCE syntax".to_string())
        })?;

        let seq_name = captures.get(1).unwrap().as_str().to_uppercase();
        let start: i64 = captures
            .get(2)
            .map(|m| m.as_str().parse().unwrap_or(1))
            .unwrap_or(1);
        let increment: i64 = captures
            .get(3)
            .map(|m| m.as_str().parse().unwrap_or(1))
            .unwrap_or(1);

        // Check if sequence already exists
        {
            let sequences = self.sequences.read().unwrap();
            if sequences.contains_key(&seq_name) {
                return Err(crate::error::Error::ExecutionError(format!(
                    "Sequence {seq_name} already exists"
                )));
            }
        }

        // Create and store the sequence
        {
            let mut sequences = self.sequences.write().unwrap();
            sequences.insert(seq_name.clone(), Arc::new(Sequence::new(start, increment)));
        }

        // Build success response
        let columns = vec![ColumnMetaData {
            name: "status".to_string(),
            r#type: "TEXT".to_string(),
            nullable: false,
            precision: None,
            scale: None,
            length: None,
        }];

        let data = vec![vec![Some(format!(
            "Sequence {seq_name} successfully created."
        ))]];

        Ok(StatementResponse::success(data, columns, statement_handle))
    }

    /// Handle DROP SEQUENCE command
    fn handle_drop_sequence(
        &self,
        sql: &str,
        statement_handle: String,
    ) -> Result<StatementResponse> {
        // Parse sequence name
        let drop_pattern = regex::Regex::new(r"(?i)DROP\s+SEQUENCE\s+(\w+)").unwrap();

        let captures = drop_pattern.captures(sql).ok_or_else(|| {
            crate::error::Error::ExecutionError("Invalid DROP SEQUENCE syntax".to_string())
        })?;

        let seq_name = captures.get(1).unwrap().as_str().to_uppercase();

        // Remove sequence
        {
            let mut sequences = self.sequences.write().unwrap();
            if sequences.remove(&seq_name).is_none() {
                return Err(crate::error::Error::ExecutionError(format!(
                    "Sequence {seq_name} does not exist"
                )));
            }
        }

        // Build success response
        let columns = vec![ColumnMetaData {
            name: "status".to_string(),
            r#type: "TEXT".to_string(),
            nullable: false,
            precision: None,
            scale: None,
            length: None,
        }];

        let data = vec![vec![Some(format!(
            "Sequence {seq_name} successfully dropped."
        ))]];

        Ok(StatementResponse::success(data, columns, statement_handle))
    }

    /// Handle SELECT with NEXTVAL/CURRVAL
    ///
    /// Syntax:
    /// ```sql
    /// SELECT seq_name.NEXTVAL
    /// SELECT seq_name.CURRVAL
    /// ```
    fn handle_sequence_select(
        &self,
        sql: &str,
        statement_handle: String,
    ) -> Result<StatementResponse> {
        let sql_upper = sql.to_uppercase();

        // Pattern for seq_name.NEXTVAL or seq_name.CURRVAL
        let nextval_pattern = regex::Regex::new(r"(?i)(\w+)\.NEXTVAL").unwrap();
        let currval_pattern = regex::Regex::new(r"(?i)(\w+)\.CURRVAL").unwrap();

        let sequences = self.sequences.read().unwrap();

        // Check for NEXTVAL
        if let Some(captures) = nextval_pattern.captures(&sql_upper) {
            let seq_name = captures.get(1).unwrap().as_str();

            let sequence = sequences.get(seq_name).ok_or_else(|| {
                crate::error::Error::ExecutionError(format!("Sequence {seq_name} does not exist"))
            })?;

            let value = sequence.next_val();

            let columns = vec![ColumnMetaData {
                name: "NEXTVAL".to_string(),
                r#type: "NUMBER".to_string(),
                nullable: false,
                precision: None,
                scale: None,
                length: None,
            }];

            let data = vec![vec![Some(value.to_string())]];

            return Ok(StatementResponse::success(data, columns, statement_handle));
        }

        // Check for CURRVAL
        if let Some(captures) = currval_pattern.captures(&sql_upper) {
            let seq_name = captures.get(1).unwrap().as_str();

            let sequence = sequences.get(seq_name).ok_or_else(|| {
                crate::error::Error::ExecutionError(format!("Sequence {seq_name} does not exist"))
            })?;

            let value = sequence.curr_val();

            let columns = vec![ColumnMetaData {
                name: "CURRVAL".to_string(),
                r#type: "NUMBER".to_string(),
                nullable: false,
                precision: None,
                scale: None,
                length: None,
            }];

            let data = vec![vec![Some(value.to_string())]];

            return Ok(StatementResponse::success(data, columns, statement_handle));
        }

        Err(crate::error::Error::ExecutionError(
            "Invalid sequence reference".to_string(),
        ))
    }

    /// Convert RecordBatch to StatementResponse
    fn batches_to_response(
        &self,
        batches: Vec<RecordBatch>,
        statement_handle: String,
    ) -> Result<StatementResponse> {
        if batches.is_empty() {
            return Ok(StatementResponse::success(vec![], vec![], statement_handle));
        }

        // Create column metadata from schema
        let schema = batches[0].schema();
        let row_type = self.schema_to_column_metadata(&schema);

        // Convert data to string arrays
        let mut data: Vec<Vec<Option<String>>> = Vec::new();

        for batch in &batches {
            for row_idx in 0..batch.num_rows() {
                let mut row: Vec<Option<String>> = Vec::new();

                for col_idx in 0..batch.num_columns() {
                    let column = batch.column(col_idx);
                    let value = self.array_value_to_string(column, row_idx);
                    row.push(value);
                }

                data.push(row);
            }
        }

        Ok(StatementResponse::success(data, row_type, statement_handle))
    }

    /// Convert Arrow schema to Snowflake column metadata
    fn schema_to_column_metadata(&self, schema: &Schema) -> Vec<ColumnMetaData> {
        schema
            .fields()
            .iter()
            .map(|field| self.field_to_column_metadata(field))
            .collect()
    }

    /// Convert Arrow Field to ColumnMetaData
    fn field_to_column_metadata(&self, field: &Field) -> ColumnMetaData {
        let (sf_type, precision, scale, length) = self.arrow_type_to_snowflake(field.data_type());

        ColumnMetaData {
            name: field.name().clone(),
            r#type: sf_type,
            precision,
            scale,
            length,
            nullable: field.is_nullable(),
        }
    }

    /// Convert Arrow data type to Snowflake data type
    fn arrow_type_to_snowflake(
        &self,
        data_type: &DataType,
    ) -> (String, Option<i32>, Option<i32>, Option<i32>) {
        match data_type {
            // Integer types
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                ("FIXED".to_string(), Some(38), Some(0), None)
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                ("FIXED".to_string(), Some(38), Some(0), None)
            }

            // Floating point types
            DataType::Float32 | DataType::Float64 => ("REAL".to_string(), None, None, None),

            // String types
            DataType::Utf8 | DataType::LargeUtf8 => {
                ("TEXT".to_string(), None, None, Some(16777216))
            }

            // Binary types
            DataType::Binary | DataType::LargeBinary => {
                ("BINARY".to_string(), None, None, Some(8388608))
            }

            // Boolean type
            DataType::Boolean => ("BOOLEAN".to_string(), None, None, None),

            // Date/time types
            DataType::Date32 | DataType::Date64 => ("DATE".to_string(), None, None, None),
            DataType::Time32(_) | DataType::Time64(_) => ("TIME".to_string(), Some(9), None, None),
            DataType::Timestamp(_, _) => ("TIMESTAMP_NTZ".to_string(), Some(9), None, None),

            // Decimal types
            DataType::Decimal128(precision, scale) => (
                "FIXED".to_string(),
                Some(*precision as i32),
                Some(*scale as i32),
                None,
            ),
            DataType::Decimal256(precision, scale) => (
                "FIXED".to_string(),
                Some(*precision as i32),
                Some(*scale as i32),
                None,
            ),

            // NULL type
            DataType::Null => ("TEXT".to_string(), None, None, None),

            // Other types
            _ => ("VARIANT".to_string(), None, None, None),
        }
    }

    /// Convert Arrow array value to string
    fn array_value_to_string(
        &self,
        array: &Arc<dyn arrow::array::Array>,
        row_idx: usize,
    ) -> Option<String> {
        use arrow::array::*;

        if array.is_null(row_idx) {
            return None;
        }

        let value = match array.data_type() {
            DataType::Boolean => {
                let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                arr.value(row_idx).to_string()
            }
            DataType::Int8 => {
                let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
                arr.value(row_idx).to_string()
            }
            DataType::Int16 => {
                let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
                arr.value(row_idx).to_string()
            }
            DataType::Int32 => {
                let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
                arr.value(row_idx).to_string()
            }
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                arr.value(row_idx).to_string()
            }
            DataType::UInt8 => {
                let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
                arr.value(row_idx).to_string()
            }
            DataType::UInt16 => {
                let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
                arr.value(row_idx).to_string()
            }
            DataType::UInt32 => {
                let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
                arr.value(row_idx).to_string()
            }
            DataType::UInt64 => {
                let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                arr.value(row_idx).to_string()
            }
            DataType::Float32 => {
                let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
                arr.value(row_idx).to_string()
            }
            DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                arr.value(row_idx).to_string()
            }
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                arr.value(row_idx).to_string()
            }
            DataType::LargeUtf8 => {
                let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                arr.value(row_idx).to_string()
            }
            DataType::Date32 => {
                let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
                let days = arr.value(row_idx);
                // Date32 is days since 1970-01-01
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let date = epoch + chrono::Duration::days(days as i64);
                date.format("%Y-%m-%d").to_string()
            }
            DataType::Date64 => {
                let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
                let millis = arr.value(row_idx);
                // Date64 is milliseconds since 1970-01-01
                let datetime = chrono::DateTime::from_timestamp_millis(millis);
                match datetime {
                    Some(dt) => dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
                    None => "Invalid date".to_string(),
                }
            }
            DataType::Timestamp(unit, _) => {
                use arrow::datatypes::TimeUnit;
                let value = match unit {
                    TimeUnit::Second => {
                        let arr = array
                            .as_any()
                            .downcast_ref::<TimestampSecondArray>()
                            .unwrap();
                        chrono::DateTime::from_timestamp(arr.value(row_idx), 0)
                    }
                    TimeUnit::Millisecond => {
                        let arr = array
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .unwrap();
                        chrono::DateTime::from_timestamp_millis(arr.value(row_idx))
                    }
                    TimeUnit::Microsecond => {
                        let arr = array
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .unwrap();
                        chrono::DateTime::from_timestamp_micros(arr.value(row_idx))
                    }
                    TimeUnit::Nanosecond => {
                        let arr = array
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .unwrap();
                        let nanos = arr.value(row_idx);
                        let secs = nanos / 1_000_000_000;
                        let subsec_nanos = (nanos % 1_000_000_000) as u32;
                        chrono::DateTime::from_timestamp(secs, subsec_nanos)
                    }
                };
                match value {
                    Some(dt) => dt.format("%Y-%m-%d %H:%M:%S%.9f").to_string(),
                    None => "Invalid timestamp".to_string(),
                }
            }
            _ => format!("<unsupported type {:?}>", array.data_type()),
        };

        Some(value)
    }

    /// Get catalog reference
    pub fn catalog(&self) -> &Arc<SnowflakeCatalog> {
        &self.catalog
    }

    /// Get DataFusion context reference
    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }
}

impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_select_literal() {
        let executor = Executor::new();
        let response = executor.execute("SELECT 1").await.unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 1);
        assert_eq!(response.data.unwrap()[0][0], Some("1".to_string()));
    }

    #[tokio::test]
    async fn test_select_expression() {
        let executor = Executor::new();
        let response = executor.execute("SELECT 1 + 2 AS result").await.unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 1);
        assert_eq!(response.data.unwrap()[0][0], Some("3".to_string()));
        assert_eq!(response.result_set_meta_data.row_type[0].name, "result");
    }

    #[tokio::test]
    async fn test_create_table_insert_select() {
        let executor = Executor::new();

        // CREATE TABLE
        let result = executor
            .execute("CREATE TABLE users (id INT, name VARCHAR)")
            .await;
        assert!(result.is_ok(), "CREATE TABLE failed: {:?}", result.err());

        // INSERT
        let result = executor
            .execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
            .await;
        assert!(result.is_ok(), "INSERT failed: {:?}", result.err());

        // SELECT
        let response = executor
            .execute("SELECT id, name FROM users ORDER BY id")
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 2);
        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("1".to_string()));
        assert_eq!(data[0][1], Some("Alice".to_string()));
        assert_eq!(data[1][0], Some("2".to_string()));
        assert_eq!(data[1][1], Some("Bob".to_string()));
    }

    #[tokio::test]
    async fn test_select_with_where() {
        let executor = Executor::new();

        executor
            .execute("CREATE TABLE products (id INT, name VARCHAR, price DOUBLE)")
            .await
            .unwrap();

        executor
            .execute("INSERT INTO products VALUES (1, 'Apple', 1.5), (2, 'Banana', 0.5), (3, 'Cherry', 3.0)")
            .await
            .unwrap();

        let response = executor
            .execute("SELECT name, price FROM products WHERE price > 1.0 ORDER BY price")
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 2);
        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("Apple".to_string()));
        assert_eq!(data[1][0], Some("Cherry".to_string()));
    }

    #[tokio::test]
    async fn test_aggregate_functions() {
        let executor = Executor::new();

        executor
            .execute("CREATE TABLE sales (amount DOUBLE)")
            .await
            .unwrap();

        executor
            .execute("INSERT INTO sales VALUES (100.0), (200.0), (300.0)")
            .await
            .unwrap();

        let response = executor
            .execute("SELECT COUNT(*) as cnt, SUM(amount) as total, AVG(amount) as avg FROM sales")
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 1);
        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("3".to_string()));
        // SUM/AVG results depend on DataFusion type inference
        assert!(data[0][1] == Some("600".to_string()) || data[0][1] == Some("600.0".to_string()));
        assert!(data[0][2] == Some("200".to_string()) || data[0][2] == Some("200.0".to_string()));
    }

    #[tokio::test]
    async fn test_numbers_table() {
        let executor = Executor::new();

        // Test that _NUMBERS table exists and has expected values
        let response = executor
            .execute("SELECT COUNT(*) FROM _numbers")
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 1);
        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("1000".to_string()));

        // Test selecting specific range
        let response = executor
            .execute("SELECT idx FROM _numbers WHERE idx < 5 ORDER BY idx")
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 5);
        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("0".to_string()));
        assert_eq!(data[4][0], Some("4".to_string()));
    }

    #[tokio::test]
    async fn test_lateral_flatten() {
        let executor = Executor::new();

        // Create table with JSON array column
        executor
            .execute("CREATE TABLE test_flatten (id INT, arr VARCHAR)")
            .await
            .unwrap();

        // Insert data with JSON arrays
        executor
            .execute("INSERT INTO test_flatten VALUES (1, '[10, 20, 30]'), (2, '[40, 50]')")
            .await
            .unwrap();

        // Test LATERAL FLATTEN
        let response = executor
            .execute(
                "SELECT t.id, f.value FROM test_flatten t, LATERAL FLATTEN(input => t.arr) f ORDER BY t.id, f.index",
            )
            .await
            .unwrap();

        // Should get 5 rows: 3 from id=1, 2 from id=2
        assert_eq!(response.result_set_meta_data.num_rows, 5);
        let data = response.data.unwrap();

        // First row: id=1, value=10
        assert_eq!(data[0][0], Some("1".to_string()));
        assert_eq!(data[0][1], Some("10".to_string()));

        // Second row: id=1, value=20
        assert_eq!(data[1][0], Some("1".to_string()));
        assert_eq!(data[1][1], Some("20".to_string()));

        // Third row: id=1, value=30
        assert_eq!(data[2][0], Some("1".to_string()));
        assert_eq!(data[2][1], Some("30".to_string()));

        // Fourth row: id=2, value=40
        assert_eq!(data[3][0], Some("2".to_string()));
        assert_eq!(data[3][1], Some("40".to_string()));

        // Fifth row: id=2, value=50
        assert_eq!(data[4][0], Some("2".to_string()));
        assert_eq!(data[4][1], Some("50".to_string()));
    }

    #[tokio::test]
    async fn test_lateral_flatten_with_path() {
        let executor = Executor::new();

        // Create table with nested JSON column
        executor
            .execute("CREATE TABLE test_flatten_path (id INT, data VARCHAR)")
            .await
            .unwrap();

        // Insert data with nested JSON
        executor
            .execute(r#"INSERT INTO test_flatten_path VALUES (1, '{"items": [1, 2, 3]}'), (2, '{"items": [4, 5]}')"#)
            .await
            .unwrap();

        // Test LATERAL FLATTEN with path option
        let response = executor
            .execute(
                "SELECT t.id, f.value FROM test_flatten_path t, LATERAL FLATTEN(input => t.data, path => 'items') f ORDER BY t.id, f.index",
            )
            .await
            .unwrap();

        // Should get 5 rows
        assert_eq!(response.result_set_meta_data.num_rows, 5);
        let data = response.data.unwrap();

        // First row: id=1, value=1
        assert_eq!(data[0][0], Some("1".to_string()));
        assert_eq!(data[0][1], Some("1".to_string()));
    }

    #[tokio::test]
    async fn test_pivot() {
        let executor = Executor::new();

        // Create sales table
        executor
            .execute("CREATE TABLE test_pivot (product VARCHAR, quarter VARCHAR, amount INT)")
            .await
            .unwrap();

        // Insert test data
        executor
            .execute(
                "INSERT INTO test_pivot VALUES ('Widget', 'Q1', 100), ('Widget', 'Q2', 150), ('Gadget', 'Q1', 200), ('Gadget', 'Q2', 250)",
            )
            .await
            .unwrap();

        // Test PIVOT
        let response = executor
            .execute(
                "SELECT product FROM test_pivot PIVOT (SUM(amount) FOR quarter IN ('Q1', 'Q2')) ORDER BY product",
            )
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 2);
        let data = response.data.unwrap();

        // First row: Gadget with Q1=200, Q2=250
        assert_eq!(data[0][0], Some("Gadget".to_string()));
    }

    #[tokio::test]
    async fn test_unpivot() {
        let executor = Executor::new();

        // Create quarterly sales table
        executor
            .execute("CREATE TABLE test_unpivot (product VARCHAR, Q1 INT, Q2 INT)")
            .await
            .unwrap();

        // Insert test data
        executor
            .execute("INSERT INTO test_unpivot VALUES ('Widget', 100, 150), ('Gadget', 200, 250)")
            .await
            .unwrap();

        // Test UNPIVOT - converts columns to rows
        let response = executor
            .execute("SELECT * FROM test_unpivot UNPIVOT (amount FOR quarter IN (Q1, Q2))")
            .await
            .unwrap();

        // Should get 4 rows (2 products x 2 quarters)
        assert_eq!(response.result_set_meta_data.num_rows, 4);
        let data = response.data.unwrap();

        // Check that we have quarter and amount columns
        assert!(data[0][0].is_some()); // quarter
        assert!(data[0][1].is_some()); // amount
    }

    // =========================================================================
    // Window Function Tests
    // =========================================================================

    #[tokio::test]
    async fn test_window_row_number() {
        let executor = Executor::new();

        executor
            .execute("CREATE TABLE test_rn (id INT, category VARCHAR, value INT)")
            .await
            .unwrap();

        executor
            .execute(
                "INSERT INTO test_rn VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 30), (4, 'B', 40)",
            )
            .await
            .unwrap();

        let response = executor
            .execute(
                "SELECT id, ROW_NUMBER() OVER (PARTITION BY category ORDER BY value) as rn FROM test_rn ORDER BY id",
            )
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 4);
        let data = response.data.unwrap();
        // id=1 (category A, value 10) -> rn=1
        assert_eq!(data[0][1], Some("1".to_string()));
        // id=2 (category A, value 20) -> rn=2
        assert_eq!(data[1][1], Some("2".to_string()));
        // id=3 (category B, value 30) -> rn=1
        assert_eq!(data[2][1], Some("1".to_string()));
        // id=4 (category B, value 40) -> rn=2
        assert_eq!(data[3][1], Some("2".to_string()));
    }

    #[tokio::test]
    async fn test_window_rank_dense_rank() {
        let executor = Executor::new();

        executor
            .execute("CREATE TABLE test_rank (id INT, score INT)")
            .await
            .unwrap();

        executor
            .execute("INSERT INTO test_rank VALUES (1, 100), (2, 100), (3, 90), (4, 80)")
            .await
            .unwrap();

        let response = executor
            .execute(
                "SELECT id, RANK() OVER (ORDER BY score DESC) as rnk, DENSE_RANK() OVER (ORDER BY score DESC) as drnk FROM test_rank ORDER BY id",
            )
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 4);
        let data = response.data.unwrap();
        // id=1, score=100 -> rank=1, dense_rank=1
        assert_eq!(data[0][1], Some("1".to_string()));
        assert_eq!(data[0][2], Some("1".to_string()));
        // id=2, score=100 -> rank=1, dense_rank=1
        assert_eq!(data[1][1], Some("1".to_string()));
        assert_eq!(data[1][2], Some("1".to_string()));
        // id=3, score=90 -> rank=3, dense_rank=2
        assert_eq!(data[2][1], Some("3".to_string()));
        assert_eq!(data[2][2], Some("2".to_string()));
        // id=4, score=80 -> rank=4, dense_rank=3
        assert_eq!(data[3][1], Some("4".to_string()));
        assert_eq!(data[3][2], Some("3".to_string()));
    }

    #[tokio::test]
    async fn test_window_lag_lead() {
        let executor = Executor::new();

        executor
            .execute("CREATE TABLE test_lag (id INT, value INT)")
            .await
            .unwrap();

        executor
            .execute("INSERT INTO test_lag VALUES (1, 10), (2, 20), (3, 30)")
            .await
            .unwrap();

        let response = executor
            .execute(
                "SELECT id, LAG(value, 1) OVER (ORDER BY id) as prev_val, LEAD(value, 1) OVER (ORDER BY id) as next_val FROM test_lag ORDER BY id",
            )
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 3);
        let data = response.data.unwrap();
        // id=1 -> prev=NULL, next=20
        assert_eq!(data[0][1], None);
        assert_eq!(data[0][2], Some("20".to_string()));
        // id=2 -> prev=10, next=30
        assert_eq!(data[1][1], Some("10".to_string()));
        assert_eq!(data[1][2], Some("30".to_string()));
        // id=3 -> prev=20, next=NULL
        assert_eq!(data[2][1], Some("20".to_string()));
        assert_eq!(data[2][2], None);
    }

    #[tokio::test]
    async fn test_window_sum_avg() {
        let executor = Executor::new();

        executor
            .execute("CREATE TABLE test_wsum (category VARCHAR, value INT)")
            .await
            .unwrap();

        executor
            .execute("INSERT INTO test_wsum VALUES ('A', 10), ('A', 20), ('B', 30)")
            .await
            .unwrap();

        let response = executor
            .execute(
                "SELECT category, value, SUM(value) OVER (PARTITION BY category) as cat_sum FROM test_wsum ORDER BY category, value",
            )
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 3);
        let data = response.data.unwrap();
        // Category A sum = 30
        assert_eq!(data[0][2], Some("30".to_string()));
        assert_eq!(data[1][2], Some("30".to_string()));
        // Category B sum = 30
        assert_eq!(data[2][2], Some("30".to_string()));
    }

    #[tokio::test]
    async fn test_window_first_last_value() {
        let executor = Executor::new();
        executor
            .execute("CREATE TABLE test_fl (id INT, category VARCHAR, value INT)")
            .await
            .unwrap();
        executor
            .execute(
                "INSERT INTO test_fl VALUES (1, 'A', 10), (2, 'A', 20), (3, 'A', 30), (4, 'B', 40), (5, 'B', 50)",
            )
            .await
            .unwrap();

        let response = executor
            .execute(
                "SELECT id, FIRST_VALUE(value) OVER (PARTITION BY category ORDER BY id) as first_val, LAST_VALUE(value) OVER (PARTITION BY category ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_val FROM test_fl ORDER BY id",
            )
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 5);
        let data = response.data.unwrap();
        // First value in partition A is 10
        assert_eq!(data[0][1], Some("10".to_string()));
        assert_eq!(data[1][1], Some("10".to_string()));
        assert_eq!(data[2][1], Some("10".to_string()));
        // Last value in partition A is 30
        assert_eq!(data[0][2], Some("30".to_string()));
        assert_eq!(data[1][2], Some("30".to_string()));
        assert_eq!(data[2][2], Some("30".to_string()));
        // First value in partition B is 40
        assert_eq!(data[3][1], Some("40".to_string()));
        assert_eq!(data[4][1], Some("40".to_string()));
        // Last value in partition B is 50
        assert_eq!(data[3][2], Some("50".to_string()));
        assert_eq!(data[4][2], Some("50".to_string()));
    }

    #[tokio::test]
    async fn test_window_nth_value() {
        let executor = Executor::new();
        executor
            .execute("CREATE TABLE test_nth (id INT, value INT)")
            .await
            .unwrap();
        executor
            .execute("INSERT INTO test_nth VALUES (1, 100), (2, 200), (3, 300)")
            .await
            .unwrap();

        let response = executor
            .execute(
                "SELECT id, NTH_VALUE(value, 2) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as second_val FROM test_nth ORDER BY id",
            )
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 3);
        let data = response.data.unwrap();
        // Second value is 200 for all rows
        assert_eq!(data[0][1], Some("200".to_string()));
        assert_eq!(data[1][1], Some("200".to_string()));
        assert_eq!(data[2][1], Some("200".to_string()));
    }

    #[tokio::test]
    async fn test_window_ntile() {
        let executor = Executor::new();
        executor
            .execute("CREATE TABLE test_ntile (id INT, value INT)")
            .await
            .unwrap();
        executor
            .execute("INSERT INTO test_ntile VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
            .await
            .unwrap();

        let response = executor
            .execute("SELECT id, NTILE(2) OVER (ORDER BY id) as bucket FROM test_ntile ORDER BY id")
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 4);
        let data = response.data.unwrap();
        // First 2 rows in bucket 1, last 2 in bucket 2
        assert_eq!(data[0][1], Some("1".to_string()));
        assert_eq!(data[1][1], Some("1".to_string()));
        assert_eq!(data[2][1], Some("2".to_string()));
        assert_eq!(data[3][1], Some("2".to_string()));
    }

    #[tokio::test]
    async fn test_qualify_clause() {
        let executor = Executor::new();
        executor
            .execute("CREATE TABLE test_qualify (id INT, category VARCHAR, value INT)")
            .await
            .unwrap();
        executor
            .execute(
                "INSERT INTO test_qualify VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 30), (4, 'B', 40)",
            )
            .await
            .unwrap();

        // Get first row per category using ROW_NUMBER and QUALIFY
        let response = executor
            .execute(
                "SELECT id, category, value, ROW_NUMBER() OVER (PARTITION BY category ORDER BY id) as rn FROM test_qualify QUALIFY rn = 1 ORDER BY id",
            )
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 2);
        let data = response.data.unwrap();
        // First row of category A (id=1) and first row of category B (id=3)
        assert_eq!(data[0][0], Some("1".to_string()));
        assert_eq!(data[0][1], Some("A".to_string()));
        assert_eq!(data[1][0], Some("3".to_string()));
        assert_eq!(data[1][1], Some("B".to_string()));
    }

    #[tokio::test]
    async fn test_drop_table() {
        let executor = Executor::new();

        // Create table
        executor
            .execute("CREATE TABLE drop_test (id INT, value INT)")
            .await
            .unwrap();

        // Insert data
        executor
            .execute("INSERT INTO drop_test VALUES (1, 100)")
            .await
            .unwrap();

        // Drop table
        executor.execute("DROP TABLE drop_test").await.unwrap();

        // Verify table no longer exists (query should fail)
        let result = executor.execute("SELECT * FROM drop_test").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_drop_view() {
        let executor = Executor::new();

        // Create base table
        executor
            .execute("CREATE TABLE view_base (id INT, value INT)")
            .await
            .unwrap();

        executor
            .execute("INSERT INTO view_base VALUES (1, 10), (2, 20), (3, 30)")
            .await
            .unwrap();

        // Create view
        executor
            .execute("CREATE VIEW test_view AS SELECT id, value * 2 as doubled FROM view_base WHERE value > 10")
            .await
            .unwrap();

        // Query view
        let response = executor
            .execute("SELECT * FROM test_view ORDER BY id")
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 2);
        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("2".to_string()));
        assert_eq!(data[0][1], Some("40".to_string()));
        assert_eq!(data[1][0], Some("3".to_string()));
        assert_eq!(data[1][1], Some("60".to_string()));

        // Drop view
        executor.execute("DROP VIEW test_view").await.unwrap();

        // Verify view no longer exists
        let result = executor.execute("SELECT * FROM test_view").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_decode_function() {
        let executor = Executor::new();

        // Test DECODE with integer matching
        let response = executor
            .execute("SELECT DECODE(1, 1, 'one', 2, 'two', 'other') as result")
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 1);
        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("one".to_string()));

        // Test DECODE with second match
        let response = executor
            .execute("SELECT DECODE(2, 1, 'one', 2, 'two', 'other') as result")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("two".to_string()));

        // Test DECODE with default
        let response = executor
            .execute("SELECT DECODE(3, 1, 'one', 2, 'two', 'other') as result")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("other".to_string()));
    }

    #[tokio::test]
    async fn test_decode_with_column() {
        let executor = Executor::new();
        executor
            .execute("CREATE TABLE decode_test (id INT, status VARCHAR)")
            .await
            .unwrap();
        executor
            .execute("INSERT INTO decode_test VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D')")
            .await
            .unwrap();

        let response = executor
            .execute(
                "SELECT id, DECODE(status, 'A', 'Active', 'B', 'Blocked', 'Unknown') as status_name FROM decode_test ORDER BY id",
            )
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 4);
        let data = response.data.unwrap();
        assert_eq!(data[0][1], Some("Active".to_string()));
        assert_eq!(data[1][1], Some("Blocked".to_string()));
        assert_eq!(data[2][1], Some("Unknown".to_string()));
        assert_eq!(data[3][1], Some("Unknown".to_string()));
    }

    #[tokio::test]
    async fn test_date_trunc() {
        let executor = Executor::new();

        // Test DATE_TRUNC with month
        let response = executor
            .execute("SELECT DATE_TRUNC('month', DATE '2024-03-15') as truncated")
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 1);
        let data = response.data.unwrap();
        // Should return 2024-03-01
        assert!(data[0][0].as_ref().unwrap().contains("2024-03-01"));
    }

    #[tokio::test]
    async fn test_extract_function() {
        let executor = Executor::new();

        // Test EXTRACT YEAR
        let response = executor
            .execute("SELECT EXTRACT(YEAR FROM DATE '2024-03-15') as year")
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 1);
        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("2024".to_string()));

        // Test EXTRACT MONTH
        let response = executor
            .execute("SELECT EXTRACT(MONTH FROM DATE '2024-03-15') as month")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("3".to_string()));

        // Test EXTRACT DAY
        let response = executor
            .execute("SELECT EXTRACT(DAY FROM DATE '2024-03-15') as day")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("15".to_string()));
    }

    #[tokio::test]
    async fn test_date_part() {
        let executor = Executor::new();

        // Test DATE_PART function (should be converted to EXTRACT by SQL rewriter)
        let response = executor
            .execute("SELECT DATE_PART('year', DATE '2024-03-15') as year")
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 1);
        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("2024".to_string()));
    }

    #[tokio::test]
    async fn test_concat_ws() {
        let executor = Executor::new();

        // Test CONCAT_WS function
        let response = executor
            .execute("SELECT CONCAT_WS(',', 'a', 'b', 'c') as result")
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 1);
        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("a,b,c".to_string()));
    }

    #[tokio::test]
    async fn test_replace_function() {
        let executor = Executor::new();

        // Test REPLACE function
        let response = executor
            .execute("SELECT REPLACE('hello world', 'world', 'rust') as result")
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 1);
        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("hello rust".to_string()));
    }

    #[tokio::test]
    async fn test_substr_function() {
        let executor = Executor::new();

        // Test SUBSTR function (Snowflake-style with 1-based index)
        let response = executor
            .execute("SELECT SUBSTR('hello world', 1, 5) as result")
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 1);
        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("hello".to_string()));
    }

    #[tokio::test]
    async fn test_show_tables() {
        let executor = Executor::new();

        // Create some tables
        executor
            .execute("CREATE TABLE show_test1 (id INT)")
            .await
            .unwrap();
        executor
            .execute("CREATE TABLE show_test2 (id INT)")
            .await
            .unwrap();

        // Show tables
        let response = executor.execute("SHOW TABLES").await.unwrap();

        // Should include our tables
        let data = response.data.unwrap();
        let table_names: Vec<String> = data.iter().map(|row| row[0].clone().unwrap()).collect();
        assert!(table_names.contains(&"show_test1".to_string()));
        assert!(table_names.contains(&"show_test2".to_string()));
    }

    #[tokio::test]
    async fn test_show_schemas() {
        let executor = Executor::new();

        let response = executor.execute("SHOW SCHEMAS").await.unwrap();

        assert!(response.result_set_meta_data.num_rows >= 1);
        let data = response.data.unwrap();
        let schema_names: Vec<String> = data.iter().map(|row| row[0].clone().unwrap()).collect();
        assert!(schema_names.contains(&"public".to_string()));
    }

    #[tokio::test]
    async fn test_show_databases() {
        let executor = Executor::new();

        // Initially should show default EMULATOR_DB
        let response = executor.execute("SHOW DATABASES").await.unwrap();
        assert_eq!(response.result_set_meta_data.num_rows, 1);
        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("EMULATOR_DB".to_string()));

        // Create a database
        executor.execute("CREATE DATABASE TEST_DB").await.unwrap();

        // Should now show the created database
        let response = executor.execute("SHOW DATABASES").await.unwrap();
        assert!(response.result_set_meta_data.num_rows >= 1);
        let data = response.data.unwrap();
        let db_names: Vec<_> = data.iter().map(|row| row[0].clone()).collect();
        assert!(db_names.contains(&Some("TEST_DB".to_string())));
    }

    #[tokio::test]
    async fn test_create_drop_database() {
        let executor = Executor::new();

        // Create database
        let response = executor
            .execute("CREATE DATABASE MY_DATABASE")
            .await
            .unwrap();
        let data = response.data.unwrap();
        assert!(data[0][0]
            .as_ref()
            .unwrap()
            .contains("MY_DATABASE successfully created"));

        // Verify it exists in SHOW DATABASES
        let response = executor.execute("SHOW DATABASES").await.unwrap();
        let data = response.data.unwrap();
        let db_names: Vec<_> = data.iter().map(|row| row[0].clone()).collect();
        assert!(db_names.contains(&Some("MY_DATABASE".to_string())));

        // Drop database
        let response = executor.execute("DROP DATABASE MY_DATABASE").await.unwrap();
        let data = response.data.unwrap();
        assert!(data[0][0]
            .as_ref()
            .unwrap()
            .contains("MY_DATABASE successfully dropped"));

        // Verify it no longer exists
        let response = executor.execute("SHOW DATABASES").await.unwrap();
        let data = response.data.unwrap();
        let db_names: Vec<_> = data.iter().map(|row| row[0].clone()).collect();
        assert!(!db_names.contains(&Some("MY_DATABASE".to_string())));
    }

    #[tokio::test]
    async fn test_create_database_if_not_exists() {
        let executor = Executor::new();

        // Create database
        executor
            .execute("CREATE DATABASE TEST_IF_NOT_EXISTS")
            .await
            .unwrap();

        // Create again with IF NOT EXISTS - should succeed
        let response = executor
            .execute("CREATE DATABASE IF NOT EXISTS TEST_IF_NOT_EXISTS")
            .await
            .unwrap();
        let data = response.data.unwrap();
        assert!(data[0][0].as_ref().unwrap().contains("already exists"));
    }

    #[tokio::test]
    async fn test_drop_database_if_exists() {
        let executor = Executor::new();

        // Drop non-existent database with IF EXISTS - should succeed
        let response = executor
            .execute("DROP DATABASE IF EXISTS NONEXISTENT_DB")
            .await
            .unwrap();
        let data = response.data.unwrap();
        assert!(data[0][0]
            .as_ref()
            .unwrap()
            .contains("Statement executed successfully"));
    }

    #[tokio::test]
    async fn test_describe_table() {
        let executor = Executor::new();

        // Create a table with various column types
        executor
            .execute("CREATE TABLE describe_test (id INT, name VARCHAR, active BOOLEAN)")
            .await
            .unwrap();

        // Test DESCRIBE TABLE
        let response = executor
            .execute("DESCRIBE TABLE describe_test")
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 3);
        let data = response.data.unwrap();

        // Check column names
        assert_eq!(data[0][0], Some("id".to_string()));
        assert_eq!(data[1][0], Some("name".to_string()));
        assert_eq!(data[2][0], Some("active".to_string()));

        // Check column types
        assert_eq!(data[0][1], Some("NUMBER".to_string()));
        assert_eq!(data[1][1], Some("VARCHAR".to_string()));
        assert_eq!(data[2][1], Some("BOOLEAN".to_string()));
    }

    #[tokio::test]
    async fn test_desc_alias() {
        let executor = Executor::new();

        // Create a table
        executor
            .execute("CREATE TABLE desc_test (id INT, value FLOAT)")
            .await
            .unwrap();

        // Test DESC (short form)
        let response = executor.execute("DESC desc_test").await.unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 2);
        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("id".to_string()));
        assert_eq!(data[1][0], Some("value".to_string()));
    }

    // =========================================================================
    // SAMPLE / TABLESAMPLE Tests
    // =========================================================================

    #[tokio::test]
    async fn test_sample_row() {
        let executor = Executor::new();

        // Create a table with 100 rows
        executor
            .execute("CREATE TABLE test_sample (id INT)")
            .await
            .unwrap();

        // Insert 100 rows
        for i in 1..=100 {
            executor
                .execute(&format!("INSERT INTO test_sample VALUES ({i})"))
                .await
                .unwrap();
        }

        // Test SAMPLE ROW - should return exactly 10 rows
        let response = executor
            .execute("SELECT * FROM test_sample SAMPLE ROW (10)")
            .await
            .unwrap();

        // Should return exactly 10 rows
        assert_eq!(response.result_set_meta_data.num_rows, 10);
    }

    #[tokio::test]
    async fn test_sample_percentage() {
        let executor = Executor::new();

        // Create a table with 1000 rows
        executor
            .execute("CREATE TABLE test_sample_pct (id INT)")
            .await
            .unwrap();

        // Insert 1000 rows in batches
        for batch in 0..10 {
            let values: Vec<String> = (1..=100)
                .map(|i| format!("({})", batch * 100 + i))
                .collect();
            executor
                .execute(&format!(
                    "INSERT INTO test_sample_pct VALUES {}",
                    values.join(",")
                ))
                .await
                .unwrap();
        }

        // Test SAMPLE percentage - with 50%, we should get roughly 500 rows
        // But since it's random, we allow some variation
        let response = executor
            .execute("SELECT * FROM test_sample_pct SAMPLE (50)")
            .await
            .unwrap();

        // Should return approximately 50% (with some tolerance due to randomness)
        let num_rows = response.result_set_meta_data.num_rows;
        assert!((300..=700).contains(&num_rows)); // 50% with reasonable tolerance
    }

    #[tokio::test]
    async fn test_tablesample() {
        let executor = Executor::new();

        executor
            .execute("CREATE TABLE test_tablesample (id INT)")
            .await
            .unwrap();

        // Insert 100 rows
        let values: Vec<String> = (1..=100).map(|i| format!("({i})")).collect();
        executor
            .execute(&format!(
                "INSERT INTO test_tablesample VALUES {}",
                values.join(",")
            ))
            .await
            .unwrap();

        // Test TABLESAMPLE - should return approximately 10% (10 rows)
        let response = executor
            .execute("SELECT * FROM test_tablesample TABLESAMPLE (10)")
            .await
            .unwrap();

        // Should return approximately 10% (with reasonable tolerance)
        let num_rows = response.result_set_meta_data.num_rows;
        assert!((1..=30).contains(&num_rows)); // 10% with reasonable tolerance
    }

    // =========================================================================
    // MERGE INTO Tests
    // =========================================================================

    #[tokio::test]
    async fn test_merge_into_insert_only() {
        let executor = Executor::new();

        // Create target table
        executor
            .execute("CREATE TABLE merge_target (id INT, value VARCHAR)")
            .await
            .unwrap();

        // Create source table
        executor
            .execute("CREATE TABLE merge_source (id INT, value VARCHAR)")
            .await
            .unwrap();

        // Insert initial data into target
        executor
            .execute("INSERT INTO merge_target VALUES (1, 'old1'), (2, 'old2')")
            .await
            .unwrap();

        // Insert source data (id 3 doesn't exist in target)
        executor
            .execute("INSERT INTO merge_source VALUES (3, 'new3'), (4, 'new4')")
            .await
            .unwrap();

        // Execute MERGE - should insert 2 new rows
        let response = executor
            .execute(
                "MERGE INTO merge_target t
                 USING merge_source s
                 ON t.id = s.id
                 WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)",
            )
            .await
            .unwrap();

        // Verify merge result
        assert_eq!(response.result_set_meta_data.num_rows, 1);

        // Check final state - should have 4 rows
        let select_response = executor
            .execute("SELECT * FROM merge_target ORDER BY id")
            .await
            .unwrap();

        assert_eq!(select_response.result_set_meta_data.num_rows, 4);
    }

    #[tokio::test]
    async fn test_merge_into_update_only() {
        let executor = Executor::new();

        // Create target table
        executor
            .execute("CREATE TABLE merge_upd_target (id INT, value VARCHAR)")
            .await
            .unwrap();

        // Create source table
        executor
            .execute("CREATE TABLE merge_upd_source (id INT, value VARCHAR)")
            .await
            .unwrap();

        // Insert initial data
        executor
            .execute("INSERT INTO merge_upd_target VALUES (1, 'old1'), (2, 'old2')")
            .await
            .unwrap();

        // Insert source data with same ids
        executor
            .execute("INSERT INTO merge_upd_source VALUES (1, 'updated1'), (2, 'updated2')")
            .await
            .unwrap();

        // Execute MERGE - should update 2 rows
        let response = executor
            .execute(
                "MERGE INTO merge_upd_target t
                 USING merge_upd_source s
                 ON t.id = s.id
                 WHEN MATCHED THEN UPDATE SET t.value = s.value",
            )
            .await
            .unwrap();

        // Verify merge result
        assert_eq!(response.result_set_meta_data.num_rows, 1);
        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("2".to_string())); // 2 rows updated

        // Check final state
        let select_response = executor
            .execute("SELECT * FROM merge_upd_target ORDER BY id")
            .await
            .unwrap();

        assert_eq!(select_response.result_set_meta_data.num_rows, 2);
        let data = select_response.data.unwrap();
        assert_eq!(data[0][1], Some("updated1".to_string()));
        assert_eq!(data[1][1], Some("updated2".to_string()));
    }

    #[tokio::test]
    async fn test_merge_into_upsert() {
        let executor = Executor::new();

        // Create target table
        executor
            .execute("CREATE TABLE merge_ups_target (id INT, value VARCHAR)")
            .await
            .unwrap();

        // Create source table
        executor
            .execute("CREATE TABLE merge_ups_source (id INT, value VARCHAR)")
            .await
            .unwrap();

        // Insert initial data
        executor
            .execute("INSERT INTO merge_ups_target VALUES (1, 'old1'), (2, 'old2')")
            .await
            .unwrap();

        // Insert source data - id 1 exists, id 3 is new
        executor
            .execute("INSERT INTO merge_ups_source VALUES (1, 'updated1'), (3, 'new3')")
            .await
            .unwrap();

        // Execute MERGE - update + insert (upsert)
        let response = executor
            .execute(
                "MERGE INTO merge_ups_target t
                 USING merge_ups_source s
                 ON t.id = s.id
                 WHEN MATCHED THEN UPDATE SET t.value = s.value
                 WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)",
            )
            .await
            .unwrap();

        // Verify merge result
        assert_eq!(response.result_set_meta_data.num_rows, 1);

        // Check final state - should have 3 rows
        let select_response = executor
            .execute("SELECT * FROM merge_ups_target ORDER BY id")
            .await
            .unwrap();

        assert_eq!(select_response.result_set_meta_data.num_rows, 3);
        let data = select_response.data.unwrap();
        assert_eq!(data[0][1], Some("updated1".to_string())); // id=1 updated
        assert_eq!(data[1][1], Some("old2".to_string())); // id=2 unchanged
        assert_eq!(data[2][1], Some("new3".to_string())); // id=3 inserted
    }

    // =========================================================================
    // Sequence Tests
    // =========================================================================

    #[tokio::test]
    async fn test_sequence_create_and_nextval() {
        let executor = Executor::new();

        // Create sequence with default start and increment
        let response = executor.execute("CREATE SEQUENCE test_seq").await.unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 1);
        let data = response.data.unwrap();
        assert!(data[0][0]
            .as_ref()
            .unwrap()
            .contains("successfully created"));

        // Get NEXTVAL - should return 1 (default start)
        let response = executor.execute("SELECT test_seq.NEXTVAL").await.unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 1);
        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("1".to_string()));

        // Get another NEXTVAL - should return 2
        let response = executor.execute("SELECT test_seq.NEXTVAL").await.unwrap();
        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("2".to_string()));
    }

    #[tokio::test]
    async fn test_sequence_with_start_and_increment() {
        let executor = Executor::new();

        // Create sequence with custom start and increment
        let response = executor
            .execute("CREATE SEQUENCE custom_seq START = 100 INCREMENT = 10")
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 1);

        // Get NEXTVAL - should return 100
        let response = executor.execute("SELECT custom_seq.NEXTVAL").await.unwrap();

        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("100".to_string()));

        // Get another NEXTVAL - should return 110
        let response = executor.execute("SELECT custom_seq.NEXTVAL").await.unwrap();
        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("110".to_string()));
    }

    #[tokio::test]
    async fn test_sequence_currval() {
        let executor = Executor::new();

        // Create sequence
        executor
            .execute("CREATE SEQUENCE curr_seq START = 5")
            .await
            .unwrap();

        // Get NEXTVAL first
        executor.execute("SELECT curr_seq.NEXTVAL").await.unwrap();

        // Get CURRVAL - should return 5 (current value after NEXTVAL)
        let response = executor.execute("SELECT curr_seq.CURRVAL").await.unwrap();

        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("5".to_string()));

        // CURRVAL again should still be 5
        let response = executor.execute("SELECT curr_seq.CURRVAL").await.unwrap();
        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("5".to_string()));
    }

    #[tokio::test]
    async fn test_sequence_drop() {
        let executor = Executor::new();

        // Create sequence
        executor.execute("CREATE SEQUENCE drop_seq").await.unwrap();

        // Verify it works
        executor.execute("SELECT drop_seq.NEXTVAL").await.unwrap();

        // Drop sequence
        let response = executor.execute("DROP SEQUENCE drop_seq").await.unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 1);
        let data = response.data.unwrap();
        assert!(data[0][0]
            .as_ref()
            .unwrap()
            .contains("successfully dropped"));

        // Try to use dropped sequence - should fail
        let result = executor.execute("SELECT drop_seq.NEXTVAL").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sequence_error_cases() {
        let executor = Executor::new();

        // Try to create same sequence twice
        executor.execute("CREATE SEQUENCE dup_seq").await.unwrap();

        let result = executor.execute("CREATE SEQUENCE dup_seq").await;
        assert!(result.is_err());

        // Try to drop non-existent sequence
        let result = executor.execute("DROP SEQUENCE nonexistent_seq").await;
        assert!(result.is_err());

        // Try to get NEXTVAL from non-existent sequence
        let result = executor.execute("SELECT nonexistent.NEXTVAL").await;
        assert!(result.is_err());
    }

    // ========================================================================
    // Phase 8: Extended Window Functions Tests
    // ========================================================================

    #[tokio::test]
    async fn test_window_percent_rank_cume_dist() {
        let executor = Executor::new();

        executor
            .execute("CREATE TABLE test_pct (id INT, score INT)")
            .await
            .unwrap();

        executor
            .execute("INSERT INTO test_pct VALUES (1, 100), (2, 200), (3, 200), (4, 300), (5, 400)")
            .await
            .unwrap();

        let response = executor
            .execute(
                "SELECT id, score, PERCENT_RANK() OVER (ORDER BY score) as pct_rank, CUME_DIST() OVER (ORDER BY score) as cume FROM test_pct ORDER BY id",
            )
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 5);
        let data = response.data.unwrap();
        // score=100: percent_rank=0, cume_dist=0.2
        assert_eq!(data[0][2], Some("0".to_string()));
        // score=200: percent_rank=0.25 (1/4), cume_dist=0.6 (3/5)
        // score=300: percent_rank=0.75 (3/4), cume_dist=0.8 (4/5)
        assert_eq!(data[3][2], Some("0.75".to_string()));
        // score=400: percent_rank=1, cume_dist=1
        assert_eq!(data[4][2], Some("1".to_string()));
        assert_eq!(data[4][3], Some("1".to_string()));
    }

    #[tokio::test]
    async fn test_ratio_to_report() {
        let executor = Executor::new();

        executor
            .execute("CREATE TABLE test_ratio (id INT, region VARCHAR, amount INT)")
            .await
            .unwrap();

        executor
            .execute("INSERT INTO test_ratio VALUES (1, 'East', 100), (2, 'East', 200), (3, 'East', 200), (4, 'West', 300), (5, 'West', 100)")
            .await
            .unwrap();

        let response = executor
            .execute(
                "SELECT id, region, amount, RATIO_TO_REPORT(amount) OVER (PARTITION BY region) as ratio FROM test_ratio ORDER BY id",
            )
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 5);
        let data = response.data.unwrap();
        // East total = 500: 100/500=0.2, 200/500=0.4, 200/500=0.4
        assert_eq!(data[0][3], Some("0.2".to_string())); // id=1, 100/500
        assert_eq!(data[1][3], Some("0.4".to_string())); // id=2, 200/500
        assert_eq!(data[2][3], Some("0.4".to_string())); // id=3, 200/500
                                                         // West total = 400: 300/400=0.75, 100/400=0.25
        assert_eq!(data[3][3], Some("0.75".to_string())); // id=4, 300/400
        assert_eq!(data[4][3], Some("0.25".to_string())); // id=5, 100/400
    }

    #[tokio::test]
    async fn test_conditional_true_event() {
        let executor = Executor::new();

        executor
            .execute("CREATE TABLE test_cte (id INT, active BOOLEAN)")
            .await
            .unwrap();

        executor
            .execute("INSERT INTO test_cte VALUES (1, true), (2, false), (3, true), (4, true), (5, false)")
            .await
            .unwrap();

        let response = executor
            .execute(
                "SELECT id, active, CONDITIONAL_TRUE_EVENT(active) OVER (ORDER BY id) as event_cnt FROM test_cte ORDER BY id",
            )
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 5);
        let data = response.data.unwrap();
        // Expected: [1, 1, 2, 3, 3] - counter increments on TRUE
        assert_eq!(data[0][2], Some("1".to_string())); // id=1, true -> 1
        assert_eq!(data[1][2], Some("1".to_string())); // id=2, false -> 1
        assert_eq!(data[2][2], Some("2".to_string())); // id=3, true -> 2
        assert_eq!(data[3][2], Some("3".to_string())); // id=4, true -> 3
        assert_eq!(data[4][2], Some("3".to_string())); // id=5, false -> 3
    }

    #[tokio::test]
    async fn test_conditional_change_event() {
        let executor = Executor::new();

        executor
            .execute("CREATE TABLE test_cce (id INT, status VARCHAR)")
            .await
            .unwrap();

        executor
            .execute("INSERT INTO test_cce VALUES (1, 'A'), (2, 'A'), (3, 'B'), (4, 'B'), (5, 'C')")
            .await
            .unwrap();

        let response = executor
            .execute(
                "SELECT id, status, CONDITIONAL_CHANGE_EVENT(status) OVER (ORDER BY id) as change_cnt FROM test_cce ORDER BY id",
            )
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 5);
        let data = response.data.unwrap();
        // Expected: [0, 0, 1, 1, 2] - counter increments on value change
        assert_eq!(data[0][2], Some("0".to_string())); // id=1, first row -> 0
        assert_eq!(data[1][2], Some("0".to_string())); // id=2, A==A -> 0
        assert_eq!(data[2][2], Some("1".to_string())); // id=3, B!=A -> 1
        assert_eq!(data[3][2], Some("1".to_string())); // id=4, B==B -> 1
        assert_eq!(data[4][2], Some("2".to_string())); // id=5, C!=B -> 2
    }

    // ========================================================================
    // Phase 9: DML/DDL Tests (UPDATE, DELETE, TRUNCATE)
    // ========================================================================

    #[tokio::test]
    async fn test_update_with_where() {
        let executor = Executor::new();

        executor
            .execute("CREATE TABLE test_update (id INT, name VARCHAR, value INT)")
            .await
            .unwrap();

        executor
            .execute("INSERT INTO test_update VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)")
            .await
            .unwrap();

        // Update single row
        let response = executor
            .execute("UPDATE test_update SET value = 150 WHERE id = 1")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("1".to_string())); // 1 row updated

        // Verify the update
        let response = executor
            .execute("SELECT id, name, value FROM test_update ORDER BY id")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert_eq!(data[0][2], Some("150".to_string())); // id=1, value updated
        assert_eq!(data[1][2], Some("200".to_string())); // id=2, unchanged
        assert_eq!(data[2][2], Some("300".to_string())); // id=3, unchanged
    }

    #[tokio::test]
    async fn test_update_multiple_columns() {
        let executor = Executor::new();

        executor
            .execute("CREATE TABLE test_update2 (id INT, name VARCHAR, value INT)")
            .await
            .unwrap();

        executor
            .execute("INSERT INTO test_update2 VALUES (1, 'Alice', 100), (2, 'Bob', 200)")
            .await
            .unwrap();

        // Update multiple columns
        let response = executor
            .execute("UPDATE test_update2 SET name = 'Updated', value = 999 WHERE id = 2")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("1".to_string())); // 1 row updated

        // Verify the update
        let response = executor
            .execute("SELECT id, name, value FROM test_update2 ORDER BY id")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert_eq!(data[0][1], Some("Alice".to_string())); // id=1, unchanged
        assert_eq!(data[1][1], Some("Updated".to_string())); // id=2, name updated
        assert_eq!(data[1][2], Some("999".to_string())); // id=2, value updated
    }

    #[tokio::test]
    async fn test_update_all_rows() {
        let executor = Executor::new();

        executor
            .execute("CREATE TABLE test_update3 (id INT, value INT)")
            .await
            .unwrap();

        executor
            .execute("INSERT INTO test_update3 VALUES (1, 100), (2, 200), (3, 300)")
            .await
            .unwrap();

        // Update all rows (no WHERE clause)
        let response = executor
            .execute("UPDATE test_update3 SET value = 0")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("3".to_string())); // 3 rows updated

        // Verify all updated
        let response = executor
            .execute("SELECT id, value FROM test_update3 ORDER BY id")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert_eq!(data[0][1], Some("0".to_string()));
        assert_eq!(data[1][1], Some("0".to_string()));
        assert_eq!(data[2][1], Some("0".to_string()));
    }

    #[tokio::test]
    async fn test_delete_with_where() {
        let executor = Executor::new();

        executor
            .execute("CREATE TABLE test_delete (id INT, name VARCHAR)")
            .await
            .unwrap();

        executor
            .execute("INSERT INTO test_delete VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
            .await
            .unwrap();

        // Delete single row
        let response = executor
            .execute("DELETE FROM test_delete WHERE id = 2")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("1".to_string())); // 1 row deleted

        // Verify the deletion
        let response = executor
            .execute("SELECT id, name FROM test_delete ORDER BY id")
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 2);
        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("1".to_string())); // id=1 remains
        assert_eq!(data[1][0], Some("3".to_string())); // id=3 remains
    }

    #[tokio::test]
    async fn test_delete_multiple_rows() {
        let executor = Executor::new();

        executor
            .execute("CREATE TABLE test_delete2 (id INT, category VARCHAR)")
            .await
            .unwrap();

        executor
            .execute(
                "INSERT INTO test_delete2 VALUES (1, 'A'), (2, 'B'), (3, 'A'), (4, 'B'), (5, 'A')",
            )
            .await
            .unwrap();

        // Delete multiple rows
        let response = executor
            .execute("DELETE FROM test_delete2 WHERE category = 'A'")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("3".to_string())); // 3 rows deleted

        // Verify
        let response = executor
            .execute("SELECT id FROM test_delete2 ORDER BY id")
            .await
            .unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 2);
        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("2".to_string())); // id=2 remains
        assert_eq!(data[1][0], Some("4".to_string())); // id=4 remains
    }

    #[tokio::test]
    async fn test_delete_all_rows() {
        let executor = Executor::new();

        executor
            .execute("CREATE TABLE test_delete3 (id INT, name VARCHAR)")
            .await
            .unwrap();

        executor
            .execute("INSERT INTO test_delete3 VALUES (1, 'Alice'), (2, 'Bob')")
            .await
            .unwrap();

        // Delete all rows (no WHERE clause)
        let response = executor.execute("DELETE FROM test_delete3").await.unwrap();

        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("2".to_string())); // 2 rows deleted

        // Verify table is empty but exists
        let response = executor
            .execute("SELECT COUNT(*) FROM test_delete3")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("0".to_string()));
    }

    #[tokio::test]
    async fn test_truncate_table() {
        let executor = Executor::new();

        executor
            .execute("CREATE TABLE test_truncate (id INT, name VARCHAR)")
            .await
            .unwrap();

        executor
            .execute("INSERT INTO test_truncate VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
            .await
            .unwrap();

        // Truncate the table
        let response = executor
            .execute("TRUNCATE TABLE test_truncate")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert!(data[0][0].as_ref().unwrap().contains("successfully"));

        // Verify table is empty but exists
        let response = executor
            .execute("SELECT COUNT(*) FROM test_truncate")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("0".to_string()));

        // Verify we can still insert
        executor
            .execute("INSERT INTO test_truncate VALUES (10, 'New')")
            .await
            .unwrap();

        let response = executor
            .execute("SELECT id, name FROM test_truncate")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("10".to_string()));
        assert_eq!(data[0][1], Some("New".to_string()));
    }

    #[tokio::test]
    async fn test_truncate_without_table_keyword() {
        let executor = Executor::new();

        executor
            .execute("CREATE TABLE test_truncate2 (id INT)")
            .await
            .unwrap();

        executor
            .execute("INSERT INTO test_truncate2 VALUES (1), (2), (3)")
            .await
            .unwrap();

        // Truncate without TABLE keyword
        let response = executor.execute("TRUNCATE test_truncate2").await.unwrap();

        let data = response.data.unwrap();
        assert!(data[0][0].as_ref().unwrap().contains("successfully"));

        // Verify table is empty
        let response = executor
            .execute("SELECT COUNT(*) FROM test_truncate2")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("0".to_string()));
    }

    // =====================================================================
    // Phase 9: ALTER TABLE Tests
    // =====================================================================

    #[tokio::test]
    async fn test_alter_table_add_column() {
        let executor = Executor::new();

        executor
            .execute("CREATE TABLE test_alter (id INT, name VARCHAR)")
            .await
            .unwrap();

        executor
            .execute("INSERT INTO test_alter VALUES (1, 'Alice'), (2, 'Bob')")
            .await
            .unwrap();

        // Add a new column
        let response = executor
            .execute("ALTER TABLE test_alter ADD COLUMN age INT")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert!(data[0][0].as_ref().unwrap().contains("successfully"));

        // Verify the column was added (new column should be NULL)
        let response = executor
            .execute("SELECT id, name, age FROM test_alter ORDER BY id")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert_eq!(data.len(), 2);
        assert_eq!(data[0][0], Some("1".to_string()));
        assert_eq!(data[0][1], Some("Alice".to_string()));
        assert_eq!(data[0][2], None); // New column is NULL
    }

    #[tokio::test]
    async fn test_alter_table_drop_column() {
        let executor = Executor::new();

        executor
            .execute("CREATE TABLE test_drop_col (id INT, name VARCHAR, age INT)")
            .await
            .unwrap();

        executor
            .execute("INSERT INTO test_drop_col VALUES (1, 'Alice', 30), (2, 'Bob', 25)")
            .await
            .unwrap();

        // Drop the age column
        let response = executor
            .execute("ALTER TABLE test_drop_col DROP COLUMN age")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert!(data[0][0].as_ref().unwrap().contains("successfully"));

        // Verify the column was dropped
        let response = executor
            .execute("SELECT * FROM test_drop_col ORDER BY id")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert_eq!(data.len(), 2);
        // Should only have id and name columns now (response has exactly 2 columns)
        assert_eq!(data[0].len(), 2);
    }

    #[tokio::test]
    async fn test_alter_table_rename_column() {
        let executor = Executor::new();

        executor
            .execute("CREATE TABLE test_rename_col (id INT, old_name VARCHAR)")
            .await
            .unwrap();

        executor
            .execute("INSERT INTO test_rename_col VALUES (1, 'Alice')")
            .await
            .unwrap();

        // Rename the column
        let response = executor
            .execute("ALTER TABLE test_rename_col RENAME COLUMN old_name TO new_name")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert!(data[0][0].as_ref().unwrap().contains("successfully"));

        // Verify the column was renamed
        let response = executor
            .execute("SELECT id, new_name FROM test_rename_col")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert_eq!(data[0][1], Some("Alice".to_string()));
    }

    #[tokio::test]
    async fn test_alter_table_rename_table() {
        let executor = Executor::new();

        executor
            .execute("CREATE TABLE old_table_name (id INT)")
            .await
            .unwrap();

        executor
            .execute("INSERT INTO old_table_name VALUES (1), (2)")
            .await
            .unwrap();

        // Rename the table
        let response = executor
            .execute("ALTER TABLE old_table_name RENAME TO new_table_name")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert!(data[0][0].as_ref().unwrap().contains("successfully"));

        // Verify we can select from the new name
        let response = executor
            .execute("SELECT COUNT(*) FROM new_table_name")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("2".to_string()));
    }

    // =====================================================================
    // Phase 9: CREATE/DROP VIEW Tests
    // =====================================================================

    #[tokio::test]
    async fn test_create_view() {
        let executor = Executor::new();

        executor
            .execute("CREATE TABLE employees (id INT, name VARCHAR, dept VARCHAR)")
            .await
            .unwrap();

        executor
            .execute("INSERT INTO employees VALUES (1, 'Alice', 'Engineering'), (2, 'Bob', 'Engineering'), (3, 'Charlie', 'Sales')")
            .await
            .unwrap();

        // Create a view
        let response = executor
            .execute("CREATE VIEW engineering_employees AS SELECT * FROM employees WHERE dept = 'Engineering'")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert!(data[0][0]
            .as_ref()
            .unwrap()
            .contains("created successfully"));

        // Select from the view
        let response = executor
            .execute("SELECT id, name FROM ENGINEERING_EMPLOYEES ORDER BY id")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert_eq!(data.len(), 2);
        assert_eq!(data[0][1], Some("Alice".to_string()));
        assert_eq!(data[1][1], Some("Bob".to_string()));
    }

    #[tokio::test]
    async fn test_create_or_replace_view() {
        let executor = Executor::new();

        executor
            .execute("CREATE TABLE products (id INT, name VARCHAR, price INT)")
            .await
            .unwrap();

        executor
            .execute("INSERT INTO products VALUES (1, 'Apple', 100), (2, 'Banana', 50), (3, 'Cherry', 200)")
            .await
            .unwrap();

        // Create a view
        executor
            .execute("CREATE VIEW cheap_products AS SELECT * FROM products WHERE price < 100")
            .await
            .unwrap();

        // Replace the view
        let response = executor
            .execute("CREATE OR REPLACE VIEW cheap_products AS SELECT * FROM products WHERE price <= 100")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert!(data[0][0]
            .as_ref()
            .unwrap()
            .contains("created successfully"));

        // Verify the view was replaced
        let response = executor
            .execute("SELECT COUNT(*) FROM CHEAP_PRODUCTS")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("2".to_string())); // Now includes Apple (100)
    }

    #[tokio::test]
    async fn test_drop_view() {
        let executor = Executor::new();

        executor
            .execute("CREATE TABLE items (id INT)")
            .await
            .unwrap();

        executor
            .execute("INSERT INTO items VALUES (1), (2)")
            .await
            .unwrap();

        // Create a view
        executor
            .execute("CREATE VIEW item_view AS SELECT * FROM items")
            .await
            .unwrap();

        // Drop the view
        let response = executor.execute("DROP VIEW item_view").await.unwrap();

        let data = response.data.unwrap();
        assert!(data[0][0]
            .as_ref()
            .unwrap()
            .contains("dropped successfully"));
    }

    #[tokio::test]
    async fn test_drop_view_if_exists() {
        let executor = Executor::new();

        // Drop non-existent view with IF EXISTS should succeed
        let response = executor
            .execute("DROP VIEW IF EXISTS nonexistent_view")
            .await
            .unwrap();

        let data = response.data.unwrap();
        assert!(data[0][0]
            .as_ref()
            .unwrap()
            .contains("dropped successfully"));
    }

    // =====================================================================
    // Phase 9: USE DATABASE/SCHEMA Tests
    // =====================================================================

    #[tokio::test]
    async fn test_use_database() {
        let executor = Executor::new();

        // Verify initial database
        assert_eq!(executor.get_current_database(), "TESTDB");

        // Change database
        let response = executor.execute("USE DATABASE mydb").await.unwrap();

        let data = response.data.unwrap();
        assert!(data[0][0].as_ref().unwrap().contains("MYDB"));
        assert!(data[0][0].as_ref().unwrap().contains("is now in use"));

        // Verify database was changed
        assert_eq!(executor.get_current_database(), "MYDB");
    }

    #[tokio::test]
    async fn test_use_schema() {
        let executor = Executor::new();

        // Verify initial schema
        assert_eq!(executor.get_current_schema(), "PUBLIC");

        // Change schema
        let response = executor.execute("USE SCHEMA sales").await.unwrap();

        let data = response.data.unwrap();
        assert!(data[0][0].as_ref().unwrap().contains("SALES"));
        assert!(data[0][0].as_ref().unwrap().contains("is now in use"));

        // Verify schema was changed
        assert_eq!(executor.get_current_schema(), "SALES");
    }

    #[tokio::test]
    async fn test_use_qualified_name() {
        let executor = Executor::new();

        // Change both database and schema
        let response = executor.execute("USE prod.analytics").await.unwrap();

        let data = response.data.unwrap();
        assert!(data[0][0].as_ref().unwrap().contains("PROD"));
        assert!(data[0][0].as_ref().unwrap().contains("ANALYTICS"));

        // Verify both were changed
        assert_eq!(executor.get_current_database(), "PROD");
        assert_eq!(executor.get_current_schema(), "ANALYTICS");
    }

    #[tokio::test]
    async fn test_use_simple() {
        let executor = Executor::new();

        // Simple USE (treated as schema)
        let response = executor.execute("USE staging").await.unwrap();

        let data = response.data.unwrap();
        assert!(data[0][0].as_ref().unwrap().contains("STAGING"));

        // Verify schema was changed
        assert_eq!(executor.get_current_schema(), "STAGING");
    }

    // =========================================================================
    // Transaction Tests
    // =========================================================================

    #[tokio::test]
    async fn test_begin_commit() {
        let executor = Executor::new();

        // Create table and insert data
        executor
            .execute("CREATE TABLE tx_test (id INT, value INT)")
            .await
            .unwrap();

        executor
            .execute("INSERT INTO tx_test VALUES (1, 100)")
            .await
            .unwrap();

        // Begin transaction
        let response = executor.execute("BEGIN").await.unwrap();
        let data = response.data.unwrap();
        assert!(data[0][0].as_ref().unwrap().contains("successfully"));

        // Modify data
        executor
            .execute("UPDATE tx_test SET value = 200 WHERE id = 1")
            .await
            .unwrap();

        // Commit
        let response = executor.execute("COMMIT").await.unwrap();
        let data = response.data.unwrap();
        assert!(data[0][0].as_ref().unwrap().contains("successfully"));

        // Verify data persists
        let response = executor
            .execute("SELECT value FROM tx_test WHERE id = 1")
            .await
            .unwrap();
        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("200".to_string()));
    }

    #[tokio::test]
    async fn test_begin_rollback() {
        let executor = Executor::new();

        // Create table and insert data
        executor
            .execute("CREATE TABLE rollback_test (id INT, value INT)")
            .await
            .unwrap();

        executor
            .execute("INSERT INTO rollback_test VALUES (1, 100)")
            .await
            .unwrap();

        // Begin transaction
        executor.execute("BEGIN").await.unwrap();

        // Modify data
        executor
            .execute("UPDATE rollback_test SET value = 999 WHERE id = 1")
            .await
            .unwrap();

        // Rollback
        let response = executor.execute("ROLLBACK").await.unwrap();
        let data = response.data.unwrap();
        assert!(data[0][0].as_ref().unwrap().contains("successfully"));

        // Verify data was restored
        let response = executor
            .execute("SELECT value FROM rollback_test WHERE id = 1")
            .await
            .unwrap();
        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("100".to_string()));
    }

    #[tokio::test]
    async fn test_start_transaction() {
        let executor = Executor::new();

        // START TRANSACTION is an alias for BEGIN
        let response = executor.execute("START TRANSACTION").await.unwrap();
        let data = response.data.unwrap();
        assert!(data[0][0].as_ref().unwrap().contains("successfully"));

        // Commit
        let response = executor.execute("COMMIT").await.unwrap();
        let data = response.data.unwrap();
        assert!(data[0][0].as_ref().unwrap().contains("successfully"));
    }

    #[tokio::test]
    async fn test_commit_without_begin() {
        let executor = Executor::new();

        // COMMIT without BEGIN should still succeed (Snowflake behavior)
        let response = executor.execute("COMMIT").await.unwrap();
        let data = response.data.unwrap();
        assert!(data[0][0].as_ref().unwrap().contains("successfully"));
    }

    #[tokio::test]
    async fn test_rollback_without_begin() {
        let executor = Executor::new();

        // ROLLBACK without BEGIN should still succeed
        let response = executor.execute("ROLLBACK").await.unwrap();
        let data = response.data.unwrap();
        assert!(data[0][0].as_ref().unwrap().contains("successfully"));
    }

    // =========================================================================
    // COPY INTO Tests
    // =========================================================================

    #[tokio::test]
    async fn test_create_stage() {
        let executor = Executor::new();

        let response = executor
            .execute("CREATE STAGE my_stage URL = 'file:///tmp/data'")
            .await
            .unwrap();
        let data = response.data.unwrap();
        assert!(data[0][0].as_ref().unwrap().contains("MY_STAGE"));
        assert!(data[0][0]
            .as_ref()
            .unwrap()
            .contains("successfully created"));
    }

    #[tokio::test]
    async fn test_create_or_replace_stage() {
        let executor = Executor::new();

        // Create stage
        executor
            .execute("CREATE STAGE test_stage URL = 'file:///tmp/old'")
            .await
            .unwrap();

        // Replace stage
        let response = executor
            .execute("CREATE OR REPLACE STAGE test_stage URL = 'file:///tmp/new'")
            .await
            .unwrap();
        let data = response.data.unwrap();
        assert!(data[0][0].as_ref().unwrap().contains("TEST_STAGE"));
    }

    #[tokio::test]
    async fn test_drop_stage() {
        let executor = Executor::new();

        // Create stage first
        executor
            .execute("CREATE STAGE drop_test URL = 'file:///tmp'")
            .await
            .unwrap();

        // Drop stage
        let response = executor.execute("DROP STAGE drop_test").await.unwrap();
        let data = response.data.unwrap();
        assert!(data[0][0]
            .as_ref()
            .unwrap()
            .contains("successfully dropped"));
    }

    #[tokio::test]
    async fn test_drop_stage_not_exists() {
        let executor = Executor::new();

        // Drop non-existent stage should fail
        let result = executor.execute("DROP STAGE nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_drop_stage_if_exists() {
        let executor = Executor::new();

        // DROP STAGE IF EXISTS on non-existent stage should succeed
        let response = executor
            .execute("DROP STAGE IF EXISTS nonexistent")
            .await
            .unwrap();
        let data = response.data.unwrap();
        assert!(data[0][0].as_ref().unwrap().contains("successfully"));
    }

    #[tokio::test]
    async fn test_copy_into_csv() {
        let executor = Executor::new();

        // Create table
        executor
            .execute("CREATE TABLE csv_test (id INTEGER, name VARCHAR)")
            .await
            .unwrap();

        // Create temp directory and CSV file
        let temp_dir = std::env::temp_dir();
        let csv_path = temp_dir.join("copy_test.csv");
        std::fs::write(&csv_path, "1,Alice\n2,Bob\n3,Charlie\n").unwrap();

        // Create stage pointing to temp directory
        let stage_sql = format!(
            "CREATE STAGE csv_stage URL = 'file://{}'",
            temp_dir.display()
        );
        executor.execute(&stage_sql).await.unwrap();

        // Copy into table
        let response = executor
            .execute(
                "COPY INTO csv_test FROM @csv_stage/copy_test.csv FILE_FORMAT = (TYPE = 'CSV')",
            )
            .await
            .unwrap();
        let data = response.data.unwrap();
        assert_eq!(data[0][0].as_ref().unwrap(), "3"); // 3 rows loaded
        assert_eq!(data[0][1].as_ref().unwrap(), "1"); // 1 file loaded

        // Verify data
        let select_response = executor
            .execute("SELECT COUNT(*) FROM csv_test")
            .await
            .unwrap();
        let select_data = select_response.data.unwrap();
        assert_eq!(select_data[0][0].as_ref().unwrap(), "3");

        // Cleanup
        std::fs::remove_file(&csv_path).ok();
    }

    #[tokio::test]
    async fn test_copy_into_with_skip_header() {
        let executor = Executor::new();

        // Create table
        executor
            .execute("CREATE TABLE header_test (id INTEGER, name VARCHAR)")
            .await
            .unwrap();

        // Create temp directory and CSV file with header
        let temp_dir = std::env::temp_dir();
        let csv_path = temp_dir.join("header_test.csv");
        std::fs::write(&csv_path, "ID,NAME\n1,Alice\n2,Bob\n").unwrap();

        // Create stage
        let stage_sql = format!(
            "CREATE STAGE header_stage URL = 'file://{}'",
            temp_dir.display()
        );
        executor.execute(&stage_sql).await.unwrap();

        // Copy with SKIP_HEADER
        let response = executor
            .execute(
                "COPY INTO header_test FROM @header_stage/header_test.csv FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)",
            )
            .await
            .unwrap();
        let data = response.data.unwrap();
        assert_eq!(data[0][0].as_ref().unwrap(), "2"); // 2 rows (excluding header)

        // Verify data
        let select_response = executor
            .execute("SELECT * FROM header_test ORDER BY id")
            .await
            .unwrap();
        let select_data = select_response.data.unwrap();
        assert_eq!(select_data[0][0].as_ref().unwrap(), "1");
        assert_eq!(select_data[0][1].as_ref().unwrap(), "Alice");
        assert_eq!(select_data[1][0].as_ref().unwrap(), "2");
        assert_eq!(select_data[1][1].as_ref().unwrap(), "Bob");

        // Cleanup
        std::fs::remove_file(&csv_path).ok();
    }

    #[tokio::test]
    async fn test_copy_into_stage_not_exists() {
        let executor = Executor::new();

        // Create table
        executor
            .execute("CREATE TABLE no_stage_test (id INTEGER)")
            .await
            .unwrap();

        // Try to copy from non-existent stage
        let result = executor
            .execute("COPY INTO no_stage_test FROM @nonexistent FILE_FORMAT = (TYPE = 'CSV')")
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_copy_into_table_not_exists() {
        let executor = Executor::new();

        // Create stage
        let temp_dir = std::env::temp_dir();
        let stage_sql = format!(
            "CREATE STAGE table_test_stage URL = 'file://{}'",
            temp_dir.display()
        );
        executor.execute(&stage_sql).await.unwrap();

        // Create a CSV file
        let csv_path = temp_dir.join("table_test.csv");
        std::fs::write(&csv_path, "1,test\n").unwrap();

        // Try to copy into non-existent table
        let result = executor
            .execute("COPY INTO nonexistent_table FROM @table_test_stage/table_test.csv FILE_FORMAT = (TYPE = 'CSV')")
            .await;
        assert!(result.is_err());

        // Cleanup
        std::fs::remove_file(&csv_path).ok();
    }

    // =========================================================================
    // INFORMATION_SCHEMA Tests
    // =========================================================================

    #[tokio::test]
    async fn test_information_schema_tables() {
        let executor = Executor::new();

        // Create a table first
        executor
            .execute("CREATE TABLE info_test (id INTEGER, name VARCHAR)")
            .await
            .unwrap();

        // Query INFORMATION_SCHEMA.TABLES
        let response = executor
            .execute("SELECT * FROM INFORMATION_SCHEMA.TABLES")
            .await
            .unwrap();

        let columns = &response.result_set_meta_data.row_type;
        assert_eq!(columns.len(), 4);
        assert_eq!(columns[0].name, "TABLE_CATALOG");
        assert_eq!(columns[1].name, "TABLE_SCHEMA");
        assert_eq!(columns[2].name, "TABLE_NAME");
        assert_eq!(columns[3].name, "TABLE_TYPE");

        let data = response.data.unwrap();
        // Find our table
        let our_table = data
            .iter()
            .find(|row| row[2].as_ref().unwrap() == "info_test");
        assert!(our_table.is_some());
        let row = our_table.unwrap();
        assert_eq!(row[3].as_ref().unwrap(), "BASE TABLE");
    }

    #[tokio::test]
    async fn test_information_schema_columns() {
        let executor = Executor::new();

        // Create a table first
        executor
            .execute("CREATE TABLE columns_test (id INTEGER, name VARCHAR, active BOOLEAN)")
            .await
            .unwrap();

        // Query INFORMATION_SCHEMA.COLUMNS
        let response = executor
            .execute("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'columns_test'")
            .await
            .unwrap();

        let columns = &response.result_set_meta_data.row_type;
        assert_eq!(columns.len(), 7);
        assert_eq!(columns[3].name, "COLUMN_NAME");
        assert_eq!(columns[4].name, "ORDINAL_POSITION");
        assert_eq!(columns[5].name, "DATA_TYPE");

        let data = response.data.unwrap();
        assert_eq!(data.len(), 3); // 3 columns

        // Check first column (id)
        assert_eq!(data[0][3].as_ref().unwrap(), "id");
        assert_eq!(data[0][4].as_ref().unwrap(), "1");

        // Check second column (name)
        assert_eq!(data[1][3].as_ref().unwrap(), "name");
        assert_eq!(data[1][4].as_ref().unwrap(), "2");

        // Check third column (active)
        assert_eq!(data[2][3].as_ref().unwrap(), "active");
        assert_eq!(data[2][4].as_ref().unwrap(), "3");
    }

    #[tokio::test]
    async fn test_information_schema_schemata() {
        let executor = Executor::new();

        // Query INFORMATION_SCHEMA.SCHEMATA
        let response = executor
            .execute("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA")
            .await
            .unwrap();

        let columns = &response.result_set_meta_data.row_type;
        assert_eq!(columns.len(), 3);
        assert_eq!(columns[0].name, "CATALOG_NAME");
        assert_eq!(columns[1].name, "SCHEMA_NAME");
        assert_eq!(columns[2].name, "SCHEMA_OWNER");

        let data = response.data.unwrap();
        assert_eq!(data.len(), 2); // PUBLIC and INFORMATION_SCHEMA

        // Check PUBLIC schema
        assert_eq!(data[0][1].as_ref().unwrap(), "PUBLIC");

        // Check INFORMATION_SCHEMA
        assert_eq!(data[1][1].as_ref().unwrap(), "INFORMATION_SCHEMA");
    }
}
