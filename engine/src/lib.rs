//! Snowflake Emulator Engine
//!
//! Snowflake-compatible SQL execution engine based on DataFusion

pub mod catalog;
pub mod error;
pub mod executor;
pub mod functions;
pub mod protocol;
pub mod session;
pub mod sql_rewriter;

pub use error::{Error, Result};
pub use executor::Executor;
pub use session::Session;
