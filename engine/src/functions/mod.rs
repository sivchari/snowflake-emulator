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
//! - `DECODE(expr, search1, result1, ..., default)` - Equivalent to CASE WHEN
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
//!
//! ### String Functions
//! - `SPLIT(string, delimiter)` - Split string into array
//! - `STRTOK(string, delimiters, part)` - Extract token at position
//! - `STRTOK_TO_ARRAY(string, delimiters)` - Split into array of tokens
//! - `REGEXP_LIKE(string, pattern)` - Check if string matches pattern
//! - `REGEXP_SUBSTR(string, pattern)` - Extract substring matching pattern
//! - `REGEXP_REPLACE(string, pattern, replacement)` - Replace matches
//! - `REGEXP_COUNT(string, pattern)` - Count pattern matches
//! - `CONTAINS(string, substring)` - Check if string contains substring
//! - `STARTSWITH(string, prefix)` - Check if string starts with prefix
//! - `ENDSWITH(string, suffix)` - Check if string ends with suffix
//!
//! ### Aggregate Functions
//! - `ARRAY_AGG(value)` - Collect values into JSON array
//! - `OBJECT_AGG(key, value)` - Collect key-value pairs into JSON object
//! - `LISTAGG(value, delimiter)` - Concatenate values with delimiter
//!
//! ### Window Functions
//! - `CONDITIONAL_TRUE_EVENT(condition)` - Counter increments when condition is TRUE
//! - `CONDITIONAL_CHANGE_EVENT(expr)` - Counter increments when value changes

mod aggregate;
mod array;
mod conditional;
mod context;
mod datetime;
mod flatten;
mod hash;
mod helpers;
mod json;
mod numeric;
mod object;
mod string;
mod try_functions;
mod type_check;
mod window;

// Conditional functions
pub use conditional::{decode, iff, nvl, nvl2};

// JSON functions
pub use json::{parse_json, to_json};

// Date/Time functions
pub use datetime::{dateadd, datediff, dayname, last_day, monthname, to_date, to_timestamp_udf};

// TRY_* functions
pub use try_functions::{try_parse_json, try_to_boolean, try_to_date, try_to_number};

// Array functions
pub use array::{
    array_append, array_cat, array_construct, array_construct_compact, array_contains,
    array_distinct, array_flatten, array_position, array_prepend, array_slice,
};

// Array/Object functions (FLATTEN helpers)
pub use flatten::{array_size, flatten_array, get, get_path, object_keys};

// Object functions
pub use object::{
    object_construct, object_construct_keep_null, object_delete, object_insert, object_pick,
};

// Type checking functions
pub use type_check::{
    is_array, is_boolean, is_decimal, is_integer, is_null_value, is_object, to_array, to_object,
    to_variant, typeof_func,
};

// String functions
pub use string::{
    charindex, contains, endswith, lpad, regexp_count, regexp_like, regexp_replace, regexp_substr,
    reverse, rpad, split, startswith, strtok, strtok_to_array, translate,
};

// Aggregate functions
pub use aggregate::{array_agg, listagg, object_agg};

// Numeric functions
pub use numeric::{div0, div0null};

// Hash functions
pub use hash::{sha1_hex, sha2};

// Context functions
pub use context::{
    current_database, current_role, current_schema, current_user, current_warehouse,
};

// Window functions
pub use window::{conditional_change_event, conditional_true_event};
