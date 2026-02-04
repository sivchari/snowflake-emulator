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
//! ### Array Functions
//! - `ARRAY_CONSTRUCT(...)` - Construct array from arguments
//! - `ARRAY_CONSTRUCT_COMPACT(...)` - Construct array, excluding NULLs
//! - `ARRAY_APPEND(array, element)` - Append element to array
//! - `ARRAY_PREPEND(array, element)` - Prepend element to array
//! - `ARRAY_CAT(array1, array2)` - Concatenate two arrays
//! - `ARRAY_SLICE(array, from, to)` - Get slice of array
//! - `ARRAY_CONTAINS(element, array)` - Check if array contains element
//! - `ARRAY_POSITION(element, array)` - Get position of element
//! - `ARRAY_DISTINCT(array)` - Remove duplicates
//! - `ARRAY_FLATTEN(array)` - Flatten nested arrays
//! - `FLATTEN_ARRAY(array, index)` - Get element at index from JSON array
//! - `ARRAY_SIZE(array)` - Get size of JSON array
//!
//! ### Object Functions
//! - `OBJECT_CONSTRUCT(...)` - Construct object from key-value pairs
//! - `OBJECT_CONSTRUCT_KEEP_NULL(...)` - Construct object, keeping NULLs
//! - `OBJECT_INSERT(object, key, value)` - Insert key-value pair
//! - `OBJECT_DELETE(object, key)` - Delete key from object
//! - `OBJECT_PICK(object, keys...)` - Pick specific keys
//! - `OBJECT_KEYS(object)` - Get all keys from JSON object
//! - `GET_PATH(json, path)` - Extract value using dot notation path
//!
//! ### Type Checking Functions
//! - `IS_ARRAY(variant)` - Check if value is array
//! - `IS_OBJECT(variant)` - Check if value is object
//! - `IS_NULL_VALUE(variant)` - Check if value is JSON null
//! - `IS_BOOLEAN(variant)` - Check if value is boolean
//! - `IS_INTEGER(variant)` - Check if value is integer
//! - `IS_DECIMAL(variant)` - Check if value is decimal
//! - `TYPEOF(variant)` - Return type name
//!
//! ### Conversion Functions
//! - `TO_VARIANT(value)` - Convert to VARIANT
//! - `TO_ARRAY(variant)` - Convert to ARRAY
//! - `TO_OBJECT(variant)` - Convert to OBJECT

mod array;
mod conditional;
mod datetime;
mod flatten;
mod helpers;
mod json;
mod object;
mod try_functions;
mod type_check;

// Conditional functions
pub use conditional::{iff, nvl, nvl2};

// JSON functions
pub use json::{parse_json, to_json};

// Date/Time functions
pub use datetime::{dateadd, datediff};

// TRY_* functions
pub use try_functions::{try_parse_json, try_to_boolean, try_to_date, try_to_number};

// Array functions
pub use array::{
    array_append, array_cat, array_construct, array_construct_compact, array_contains,
    array_distinct, array_flatten, array_position, array_prepend, array_slice,
};

// Array/Object functions (FLATTEN helpers)
pub use flatten::{array_size, flatten_array, get_path, object_keys};

// Object functions
pub use object::{
    object_construct, object_construct_keep_null, object_delete, object_insert, object_pick,
};

// Type checking functions
pub use type_check::{
    is_array, is_boolean, is_decimal, is_integer, is_null_value, is_object, to_array, to_object,
    to_variant, typeof_func,
};
