//! SQL Executor powered by DataFusion
//!
//! Execute Snowflake SQL using DataFusion

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::*;
use uuid::Uuid;

use crate::catalog::SnowflakeCatalog;
use crate::error::Result;
use crate::functions;
use crate::protocol::{ColumnMetaData, StatementResponse};

/// SQL execution engine
pub struct Executor {
    /// DataFusion session context
    ctx: SessionContext,

    /// Snowflake catalog
    catalog: Arc<SnowflakeCatalog>,
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

        // JSON functions
        ctx.register_udf(functions::parse_json());
        ctx.register_udf(functions::to_json());

        // Date/Time functions
        ctx.register_udf(functions::dateadd());
        ctx.register_udf(functions::datediff());

        // TRY_* functions
        ctx.register_udf(functions::try_parse_json());
        ctx.register_udf(functions::try_to_number());
        ctx.register_udf(functions::try_to_date());
        ctx.register_udf(functions::try_to_boolean());

        // Array/Object functions (FLATTEN helpers)
        ctx.register_udf(functions::flatten_array());
        ctx.register_udf(functions::array_size());
        ctx.register_udf(functions::get_path());
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

        Self { ctx, catalog }
    }

    /// Execute SQL
    pub async fn execute(&self, sql: &str) -> Result<StatementResponse> {
        let statement_handle = Uuid::new_v4().to_string();

        // Execute SQL with DataFusion
        let df = self.ctx.sql(sql).await?;

        // Collect results
        let batches = df.collect().await?;

        // Build response
        let response = self.batches_to_response(batches, statement_handle)?;

        Ok(response)
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
}
