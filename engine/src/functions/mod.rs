//! Snowflake-compatible UDF implementations
//!
//! This module provides User Defined Functions that are compatible with Snowflake SQL.
//!
//! ## Available Functions
//!
//! ### Conditional Functions
//! - `IFF(condition, true_value, false_value)` - Inline IF expression
//! - `NVL(expr1, expr2)` - Return expr2 if expr1 is NULL
//! - `NVL2(expr1, expr2, expr3)` - Return expr2 if expr1 is NOT NULL, else expr3
//!
//! ### JSON Functions
//! - `PARSE_JSON(string)` - Parse string as JSON
//! - `TO_JSON(variant)` - Convert value to JSON string
//!
//! ### Date/Time Functions
//! - `DATEADD(part, value, date)` - Add interval to date/time
//! - `DATEDIFF(part, date1, date2)` - Calculate difference between dates
//!
//! ### TRY_* Functions (return NULL on error)
//! - `TRY_PARSE_JSON(string)` - Parse JSON, return NULL on error
//! - `TRY_TO_NUMBER(string)` - Convert to number, return NULL on error
//! - `TRY_TO_DATE(string)` - Convert to date, return NULL on error
//! - `TRY_TO_BOOLEAN(string)` - Convert to boolean, return NULL on error
//!
//! ### Array/Object Functions
//! - `FLATTEN_ARRAY(array, index)` - Get element at index from JSON array
//! - `ARRAY_SIZE(array)` - Get size of JSON array
//! - `GET_PATH(json, path)` - Extract value using dot notation path
//! - `OBJECT_KEYS(object)` - Get all keys from JSON object

mod conditional;
mod datetime;
mod flatten;
mod json;
mod try_functions;

// Conditional functions
pub use conditional::{iff, nvl, nvl2};

// JSON functions
pub use json::{parse_json, to_json};

// Date/Time functions
pub use datetime::{dateadd, datediff};

// TRY_* functions
pub use try_functions::{try_parse_json, try_to_boolean, try_to_date, try_to_number};

// Array/Object functions (FLATTEN helpers)
pub use flatten::{array_size, flatten_array, get_path, object_keys};
