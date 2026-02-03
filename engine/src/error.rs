//! Error types for Snowflake Emulator

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("SQL parse error: {0}")]
    ParseError(String),

    #[error("Execution error: {0}")]
    ExecutionError(String),

    #[error("Database '{0}' not found")]
    DatabaseNotFound(String),

    #[error("Schema '{0}' not found")]
    SchemaNotFound(String),

    #[error("Table '{0}' not found")]
    TableNotFound(String),

    #[error("Invalid statement handle: {0}")]
    InvalidStatementHandle(String),

    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Return Snowflake-compatible error code
    pub fn error_code(&self) -> &'static str {
        match self {
            Error::ParseError(_) => "001003",
            Error::ExecutionError(_) => "002001",
            Error::DatabaseNotFound(_) => "002003",
            Error::SchemaNotFound(_) => "002043",
            Error::TableNotFound(_) => "002003",
            Error::InvalidStatementHandle(_) => "002014",
            Error::DataFusion(_) => "002001",
            Error::Arrow(_) => "002001",
            Error::Json(_) => "002001",
            Error::Internal(_) => "000001",
        }
    }

    /// Return Snowflake-compatible SQL State
    pub fn sql_state(&self) -> &'static str {
        match self {
            Error::ParseError(_) => "42000",
            Error::ExecutionError(_) => "42000",
            Error::DatabaseNotFound(_) => "42000",
            Error::SchemaNotFound(_) => "42000",
            Error::TableNotFound(_) => "42S02",
            Error::InvalidStatementHandle(_) => "42000",
            Error::DataFusion(_) => "42000",
            Error::Arrow(_) => "42000",
            Error::Json(_) => "22000",
            Error::Internal(_) => "XX000",
        }
    }
}
