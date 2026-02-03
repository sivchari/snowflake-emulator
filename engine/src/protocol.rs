//! Snowflake REST API Protocol Types
//!
//! Request/Response type definitions compatible with Snowflake SQL API v2

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// SQL execution request
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatementRequest {
    /// SQL statement to execute
    pub statement: String,

    /// Database name
    #[serde(default)]
    pub database: Option<String>,

    /// Schema name
    #[serde(default)]
    pub schema: Option<String>,

    /// Warehouse name (ignored by emulator)
    #[serde(default)]
    pub warehouse: Option<String>,

    /// Role name (ignored by emulator)
    #[serde(default)]
    pub role: Option<String>,

    /// Timeout in seconds
    #[serde(default)]
    pub timeout: Option<u64>,

    /// Parameter bindings
    #[serde(default)]
    pub bindings: Option<HashMap<String, BindingValue>>,

    /// Session parameters
    #[serde(default)]
    pub parameters: Option<HashMap<String, String>>,

    /// Async execution flag
    #[serde(default)]
    pub r#async: Option<bool>,
}

/// Binding value
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BindingValue {
    /// Data type (FIXED, REAL, TEXT, DATE, TIME, TIMESTAMP, etc.)
    pub r#type: String,

    /// Value (passed as string)
    pub value: String,
}

/// SQL execution response
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatementResponse {
    /// Result data (each row is an array of strings)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Vec<Vec<Option<String>>>>,

    /// Result set metadata
    pub result_set_meta_data: ResultSetMetaData,

    /// Error code (only on error)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,

    /// Error message (only on error)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Result set metadata
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResultSetMetaData {
    /// Row count
    pub num_rows: i64,

    /// Inserted row count
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_rows_inserted: Option<i64>,

    /// Updated row count
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_rows_updated: Option<i64>,

    /// Deleted row count
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_rows_deleted: Option<i64>,

    /// Statement handle
    pub statement_handle: String,

    /// SQL State
    pub sql_state: String,

    /// Request ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,

    /// Column type information
    pub row_type: Vec<ColumnMetaData>,

    /// Format
    pub format: String,

    /// Partition information
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_info: Option<Vec<PartitionInfo>>,
}

/// Column metadata
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnMetaData {
    /// Column name
    pub name: String,

    /// Snowflake data type
    pub r#type: String,

    /// Precision (for numeric types)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub precision: Option<i32>,

    /// Scale (for numeric types)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scale: Option<i32>,

    /// Length (for string types)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub length: Option<i32>,

    /// Nullable
    pub nullable: bool,
}

/// Partition information
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionInfo {
    /// Row count
    pub row_count: i64,

    /// Uncompressed size
    pub uncompressed_size: i64,
}

/// Async execution response (HTTP 202)
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AsyncResponse {
    /// Status check URL
    pub statement_status_url: String,

    /// Request ID
    pub request_id: String,

    /// Statement handle
    pub statement_handle: String,
}

/// Error response
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorResponse {
    /// Error code
    pub code: String,

    /// Error message
    pub message: String,

    /// SQL State
    pub sql_state: String,

    /// Statement handle (if exists)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub statement_handle: Option<String>,
}

impl StatementResponse {
    /// Create success response
    pub fn success(
        data: Vec<Vec<Option<String>>>,
        row_type: Vec<ColumnMetaData>,
        statement_handle: String,
    ) -> Self {
        let num_rows = data.len() as i64;
        Self {
            data: Some(data),
            result_set_meta_data: ResultSetMetaData {
                num_rows,
                num_rows_inserted: None,
                num_rows_updated: None,
                num_rows_deleted: None,
                statement_handle,
                sql_state: "00000".to_string(),
                request_id: None,
                row_type,
                format: "jsonv2".to_string(),
                partition_info: None,
            },
            code: None,
            message: None,
        }
    }

    /// Create DML response
    pub fn dml(affected_rows: i64, statement_handle: String, operation: DmlOperation) -> Self {
        let (inserted, updated, deleted) = match operation {
            DmlOperation::Insert => (Some(affected_rows), None, None),
            DmlOperation::Update => (None, Some(affected_rows), None),
            DmlOperation::Delete => (None, None, Some(affected_rows)),
        };

        Self {
            data: Some(vec![vec![Some(affected_rows.to_string())]]),
            result_set_meta_data: ResultSetMetaData {
                num_rows: 1,
                num_rows_inserted: inserted,
                num_rows_updated: updated,
                num_rows_deleted: deleted,
                statement_handle,
                sql_state: "00000".to_string(),
                request_id: None,
                row_type: vec![ColumnMetaData {
                    name: "number of rows affected".to_string(),
                    r#type: "FIXED".to_string(),
                    precision: Some(38),
                    scale: Some(0),
                    length: None,
                    nullable: false,
                }],
                format: "jsonv2".to_string(),
                partition_info: None,
            },
            code: None,
            message: None,
        }
    }
}

/// DML operation type
pub enum DmlOperation {
    Insert,
    Update,
    Delete,
}

// =============================================================================
// Snowflake v1 API Types (used by gosnowflake driver)
// =============================================================================

/// v1 Query Request (POST /queries/v1/query-request)
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct V1QueryRequest {
    /// SQL text to execute
    #[serde(alias = "sqlText", alias = "SQL_TEXT")]
    pub sql_text: String,

    /// Async execution flag
    #[serde(default, alias = "asyncExec")]
    pub async_exec: bool,

    /// Sequence ID
    #[serde(default, alias = "sequenceId")]
    pub sequence_id: Option<u64>,

    /// Describe only (schema retrieval without execution)
    #[serde(default, alias = "describeOnly")]
    pub describe_only: Option<bool>,

    /// Query parameters
    #[serde(default)]
    pub parameters: Option<HashMap<String, serde_json::Value>>,

    /// Bind parameters
    #[serde(default)]
    pub bindings: Option<HashMap<String, serde_json::Value>>,
}

/// v1 Query Response
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct V1QueryResponse {
    /// Response data
    pub data: V1QueryResponseData,

    /// Status message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// Response code
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,

    /// Operation success
    pub success: bool,
}

/// v1 Query Response Data
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct V1QueryResponseData {
    /// Row type (column metadata)
    pub row_type: Vec<V1RowType>,

    /// Result rows (each row is array of strings)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rowset: Option<Vec<Vec<Option<String>>>>,

    /// Total rows
    pub total: i64,

    /// Returned rows
    pub returned: i64,

    /// Query ID (statement handle)
    pub query_id: String,

    /// SQL state
    pub sql_state: String,

    /// Database
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_database_name: Option<String>,

    /// Schema
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_schema_name: Option<String>,

    /// Warehouse
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_warehouse_name: Option<String>,

    /// Role
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_role_name: Option<String>,

    /// Query result format
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_result_format: Option<String>,

    /// Parameters (session parameters)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Vec<V1Parameter>>,
}

/// v1 Row Type (column metadata)
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct V1RowType {
    /// Column name
    pub name: String,

    /// Snowflake type
    pub r#type: String,

    /// Nullable
    pub nullable: bool,

    /// Precision (for numeric types)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub precision: Option<i32>,

    /// Scale (for numeric types)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scale: Option<i32>,

    /// Length (for string types)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub length: Option<i32>,

    /// Byte length
    #[serde(skip_serializing_if = "Option::is_none")]
    pub byte_length: Option<i32>,
}

/// v1 Session Parameter
#[derive(Debug, Clone, Serialize)]
pub struct V1Parameter {
    pub name: String,
    pub value: serde_json::Value,
}

impl V1QueryResponse {
    /// Create success response from StatementResponse
    pub fn from_statement_response(resp: &StatementResponse) -> Self {
        let row_type: Vec<V1RowType> = resp
            .result_set_meta_data
            .row_type
            .iter()
            .map(|col| V1RowType {
                name: col.name.clone(),
                r#type: col.r#type.clone(),
                nullable: col.nullable,
                precision: col.precision,
                scale: col.scale,
                length: col.length,
                byte_length: col.length,
            })
            .collect();

        let rowset = resp.data.clone();
        let total = resp.result_set_meta_data.num_rows;

        Self {
            data: V1QueryResponseData {
                row_type,
                rowset,
                total,
                returned: total,
                query_id: resp.result_set_meta_data.statement_handle.clone(),
                sql_state: resp.result_set_meta_data.sql_state.clone(),
                final_database_name: None,
                final_schema_name: None,
                final_warehouse_name: None,
                final_role_name: None,
                query_result_format: Some("json".to_string()),
                parameters: None,
            },
            message: None,
            code: None,
            success: true,
        }
    }

    /// Create error response
    pub fn error(code: &str, message: &str, sql_state: &str) -> Self {
        Self {
            data: V1QueryResponseData {
                row_type: vec![],
                rowset: None,
                total: 0,
                returned: 0,
                query_id: String::new(),
                sql_state: sql_state.to_string(),
                final_database_name: None,
                final_schema_name: None,
                final_warehouse_name: None,
                final_role_name: None,
                query_result_format: None,
                parameters: None,
            },
            message: Some(message.to_string()),
            code: Some(code.to_string()),
            success: false,
        }
    }
}
