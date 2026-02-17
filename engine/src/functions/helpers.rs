//! Common helper functions for UDFs
//!
//! This module provides shared utilities for Snowflake-compatible UDF implementations.

use std::sync::Arc;

use chrono::{Datelike, NaiveDate};
use datafusion::arrow::array::{Array, StringArray};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::ColumnarValue;

// ============================================================================
// Array Processing Helpers
// ============================================================================

/// Process a ColumnarValue containing strings, applying a transformation function.
///
/// This helper reduces code duplication across UDF implementations by providing
/// a common pattern for processing both scalar and array inputs.
///
/// # Arguments
/// * `input` - The input ColumnarValue (scalar or array)
/// * `num_rows` - Number of rows for array expansion
/// * `func_name` - Name of the calling function (for error messages)
/// * `f` - Transformation function: Option<&str> -> Option<String>
///
/// # Returns
/// A ColumnarValue containing the transformed strings
#[allow(dead_code)]
pub fn process_string_input<F>(
    input: &ColumnarValue,
    num_rows: usize,
    func_name: &str,
    f: F,
) -> Result<ColumnarValue>
where
    F: Fn(Option<&str>) -> Option<String>,
{
    match input {
        ColumnarValue::Scalar(ScalarValue::Utf8(opt_s)) => {
            let result = f(opt_s.as_deref());
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
        }
        ColumnarValue::Scalar(ScalarValue::LargeUtf8(opt_s)) => {
            let result = f(opt_s.as_deref());
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
        }
        ColumnarValue::Array(arr) => {
            let result = process_string_array(arr, &f)?;
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        _ => {
            let arr = input.to_array(num_rows)?;
            if let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() {
                let result: StringArray = str_arr.iter().map(&f).collect();
                Ok(ColumnarValue::Array(Arc::new(result)))
            } else {
                Err(datafusion::error::DataFusionError::Execution(format!(
                    "{func_name} argument must be a string"
                )))
            }
        }
    }
}

/// Process a StringArray, applying a transformation function to each element.
#[allow(dead_code)]
fn process_string_array<F>(arr: &Arc<dyn Array>, f: &F) -> Result<StringArray>
where
    F: Fn(Option<&str>) -> Option<String>,
{
    let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution("Expected string array".to_string())
    })?;

    Ok(str_arr.iter().map(f).collect())
}

/// Process a ColumnarValue, applying a transformation that may fail.
///
/// Unlike `process_string_input`, this variant allows the transformation
/// function to return a Result, propagating errors for scalar inputs
/// while converting them to NULL for array inputs.
///
/// # Arguments
/// * `input` - The input ColumnarValue (scalar or array)
/// * `num_rows` - Number of rows for array expansion
/// * `func_name` - Name of the calling function (for error messages)
/// * `scalar_f` - Transformation for scalar: &str -> Result<String>
/// * `array_f` - Transformation for array elements: Option<&str> -> Option<String>
#[allow(dead_code)]
pub fn process_string_input_with_error<SF, AF>(
    input: &ColumnarValue,
    num_rows: usize,
    func_name: &str,
    scalar_f: SF,
    array_f: AF,
) -> Result<ColumnarValue>
where
    SF: Fn(&str) -> Result<String>,
    AF: Fn(Option<&str>) -> Option<String>,
{
    match input {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
            let result = scalar_f(s)?;
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
        }
        ColumnarValue::Array(arr) => {
            let result = process_string_array(arr, &array_f)?;
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        _ => {
            let arr = input.to_array(num_rows)?;
            if let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() {
                let result: StringArray = str_arr.iter().map(array_f).collect();
                Ok(ColumnarValue::Array(Arc::new(result)))
            } else {
                Err(datafusion::error::DataFusionError::Execution(format!(
                    "{func_name} argument must be a string"
                )))
            }
        }
    }
}

// ============================================================================
// Date/Time Helpers
// ============================================================================

/// Calculate the last day of a given month.
///
/// # Arguments
/// * `year` - The year
/// * `month` - The month (1-12)
///
/// # Returns
/// The day number of the last day of the month (28, 29, 30, or 31)
pub fn last_day_of_month(year: i32, month: u32) -> u32 {
    // Get the first day of the next month and subtract one day
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };

    NaiveDate::from_ymd_opt(next_year, next_month, 1)
        .and_then(|d| d.pred_opt())
        .map(|d| d.day())
        .unwrap_or(28) // Fallback to 28 if calculation fails
}

/// Clamp a day to the valid range for a given month.
///
/// # Arguments
/// * `year` - The year
/// * `month` - The month (1-12)
/// * `day` - The day to clamp
///
/// # Returns
/// The day clamped to the valid range for the month
pub fn clamp_day_to_month(year: i32, month: u32, day: u32) -> u32 {
    let max_day = last_day_of_month(year, month);
    day.min(max_day)
}

/// Safely convert nanoseconds to seconds and subsecond nanoseconds.
///
/// This function handles negative nanosecond values correctly using
/// Euclidean division/remainder.
///
/// # Arguments
/// * `ns` - Nanoseconds (can be negative)
///
/// # Returns
/// A tuple of (seconds, subsecond_nanoseconds)
pub fn nanos_to_components(ns: i64) -> (i64, u32) {
    let secs = ns.div_euclid(1_000_000_000);
    let subsec_nanos = ns.rem_euclid(1_000_000_000) as u32;
    (secs, subsec_nanos)
}

// ============================================================================
// Numeric Helpers
// ============================================================================

/// Safely convert an i64 index to usize, returning None for negative values.
///
/// # Arguments
/// * `index` - The index as i64
///
/// # Returns
/// Some(usize) if index >= 0, None otherwise
pub fn safe_index(index: i64) -> Option<usize> {
    if index >= 0 {
        Some(index as usize)
    } else {
        None
    }
}

/// Safely convert an i32 index to usize, returning None for negative values.
#[allow(dead_code)]
pub fn safe_index_i32(index: i32) -> Option<usize> {
    if index >= 0 {
        Some(index as usize)
    } else {
        None
    }
}

// ============================================================================
// Error Message Helpers
// ============================================================================

/// Truncate a string for error messages to avoid excessive output.
///
/// # Arguments
/// * `s` - The string to truncate
/// * `max_len` - Maximum length (default 100)
///
/// # Returns
/// The truncated string with "..." appended if truncated
#[allow(dead_code)]
pub fn truncate_for_error(s: &str, max_len: usize) -> String {
    if s.len() > max_len {
        format!("{}...", &s[..max_len])
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_last_day_of_month() {
        // January - 31 days
        assert_eq!(last_day_of_month(2024, 1), 31);
        // February - leap year
        assert_eq!(last_day_of_month(2024, 2), 29);
        // February - non-leap year
        assert_eq!(last_day_of_month(2023, 2), 28);
        // April - 30 days
        assert_eq!(last_day_of_month(2024, 4), 30);
        // December - 31 days
        assert_eq!(last_day_of_month(2024, 12), 31);
    }

    #[test]
    fn test_clamp_day_to_month() {
        // Day within range
        assert_eq!(clamp_day_to_month(2024, 1, 15), 15);
        // Day exceeds February in leap year
        assert_eq!(clamp_day_to_month(2024, 2, 31), 29);
        // Day exceeds February in non-leap year
        assert_eq!(clamp_day_to_month(2023, 2, 31), 28);
        // Day exceeds April
        assert_eq!(clamp_day_to_month(2024, 4, 31), 30);
    }

    #[test]
    fn test_nanos_to_components() {
        // Positive nanoseconds
        let (secs, nanos) = nanos_to_components(1_500_000_000);
        assert_eq!(secs, 1);
        assert_eq!(nanos, 500_000_000);

        // Negative nanoseconds
        let (secs, nanos) = nanos_to_components(-500_000_000);
        assert_eq!(secs, -1);
        assert_eq!(nanos, 500_000_000);

        // Zero
        let (secs, nanos) = nanos_to_components(0);
        assert_eq!(secs, 0);
        assert_eq!(nanos, 0);
    }

    #[test]
    fn test_safe_index() {
        assert_eq!(safe_index(0), Some(0));
        assert_eq!(safe_index(10), Some(10));
        assert_eq!(safe_index(-1), None);
        assert_eq!(safe_index(-100), None);
    }

    #[test]
    fn test_truncate_for_error() {
        let short = "hello";
        assert_eq!(truncate_for_error(short, 100), "hello");

        let long = "a".repeat(200);
        let truncated = truncate_for_error(&long, 100);
        assert_eq!(truncated.len(), 103); // 100 + "..."
        assert!(truncated.ends_with("..."));
    }
}
