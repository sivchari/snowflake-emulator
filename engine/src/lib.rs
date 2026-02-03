//! Snowflake Emulator Engine
//!
//! DataFusion をベースとした Snowflake 互換の SQL 実行エンジン

pub mod catalog;
pub mod error;
pub mod executor;
pub mod protocol;
pub mod session;

pub use error::{Error, Result};
pub use executor::Executor;
pub use session::Session;
