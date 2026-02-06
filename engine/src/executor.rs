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

        // Rewrite Snowflake-specific SQL constructs
        let rewritten_sql = sql_rewriter::rewrite(sql);

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
        // Return default database name
        let columns = vec![ColumnMetaData {
            name: "DATABASE_NAME".to_string(),
            r#type: "TEXT".to_string(),
            nullable: false,
            precision: None,
            scale: None,
            length: None,
        }];

        let data: Vec<Vec<Option<String>>> = vec![vec![Some("default".to_string())]];

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

        let response = executor.execute("SHOW DATABASES").await.unwrap();

        assert_eq!(response.result_set_meta_data.num_rows, 1);
        let data = response.data.unwrap();
        assert_eq!(data[0][0], Some("default".to_string()));
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
}
