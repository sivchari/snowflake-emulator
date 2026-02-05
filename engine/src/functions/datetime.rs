//! Date/Time functions (DATEADD, DATEDIFF)
//!
//! Snowflake-compatible date and time manipulation functions.

// Allow deprecated chrono functions until migrated to new API
#![allow(deprecated)]

use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, Date32Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, Timelike};
use datafusion::common::{Result, ScalarValue};

use super::helpers::{clamp_day_to_month, nanos_to_components};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

// ============================================================================
// DATEADD(date_or_time_part, value, date_or_time_expr)
// ============================================================================

/// DATEADD function - Add a specified value to a date/time part
///
/// Syntax: DATEADD(date_or_time_part, value, date_or_time_expr)
/// Returns a date/time with the specified value added.
///
/// Supported parts: year, month, day, hour, minute, second
#[derive(Debug)]
pub struct DateAddFunc {
    signature: Signature,
}

impl Default for DateAddFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl DateAddFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(3), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for DateAddFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "dateadd"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // Return the same type as the date/time input (third argument)
        Ok(arg_types.get(2).cloned().unwrap_or(DataType::Date32))
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _num_rows: usize) -> Result<ColumnarValue> {
        if args.len() != 3 {
            return Err(datafusion::error::DataFusionError::Execution(
                "DATEADD requires exactly 3 arguments".to_string(),
            ));
        }

        let part = &args[0];
        let value = &args[1];
        let date_expr = &args[2];

        // Get the date part string
        let part_str = match part {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.to_uppercase(),
            _ => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "DATEADD first argument must be a string literal".to_string(),
                ))
            }
        };

        // Get the value to add
        let add_value = match value {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => *v,
            ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) => *v as i64,
            _ => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "DATEADD second argument must be an integer".to_string(),
                ))
            }
        };

        // Handle the date expression
        match date_expr {
            ColumnarValue::Scalar(scalar) => {
                let result = dateadd_scalar(&part_str, add_value, scalar)?;
                Ok(ColumnarValue::Scalar(result))
            }
            ColumnarValue::Array(arr) => {
                let result = dateadd_array(&part_str, add_value, arr)?;
                Ok(ColumnarValue::Array(result))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

fn dateadd_scalar(part: &str, value: i64, scalar: &ScalarValue) -> Result<ScalarValue> {
    match scalar {
        ScalarValue::Date32(Some(days)) => {
            let date = NaiveDate::from_num_days_from_ce_opt(*days + 719163).ok_or_else(|| {
                datafusion::error::DataFusionError::Execution("Invalid date".to_string())
            })?;
            let new_date = add_to_date(&date, part, value)?;
            let new_days = new_date.num_days_from_ce() - 719163;
            Ok(ScalarValue::Date32(Some(new_days)))
        }
        ScalarValue::Date64(Some(ms)) => {
            let datetime = NaiveDateTime::from_timestamp_millis(*ms).ok_or_else(|| {
                datafusion::error::DataFusionError::Execution("Invalid datetime".to_string())
            })?;
            let new_datetime = add_to_datetime(&datetime, part, value)?;
            Ok(ScalarValue::Date64(Some(
                new_datetime.and_utc().timestamp_millis(),
            )))
        }
        ScalarValue::TimestampNanosecond(Some(ns), tz) => {
            let (secs, subsec_nanos) = nanos_to_components(*ns);
            let datetime =
                NaiveDateTime::from_timestamp_opt(secs, subsec_nanos).ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution("Invalid timestamp".to_string())
                })?;
            let new_datetime = add_to_datetime(&datetime, part, value)?;
            let result_nanos = new_datetime
                .and_utc()
                .timestamp_nanos_opt()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "Result timestamp out of range (nanoseconds overflow)".to_string(),
                    )
                })?;
            Ok(ScalarValue::TimestampNanosecond(
                Some(result_nanos),
                tz.clone(),
            ))
        }
        ScalarValue::Utf8(Some(s)) => {
            // Try to parse as date or datetime
            if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                let new_date = add_to_date(&date, part, value)?;
                Ok(ScalarValue::Utf8(Some(
                    new_date.format("%Y-%m-%d").to_string(),
                )))
            } else if let Ok(datetime) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                let new_datetime = add_to_datetime(&datetime, part, value)?;
                Ok(ScalarValue::Utf8(Some(
                    new_datetime.format("%Y-%m-%d %H:%M:%S").to_string(),
                )))
            } else {
                Err(datafusion::error::DataFusionError::Execution(format!(
                    "Cannot parse '{s}' as date or datetime"
                )))
            }
        }
        ScalarValue::Date32(None) | ScalarValue::Date64(None) | ScalarValue::Utf8(None) => {
            Ok(scalar.clone())
        }
        _ => Err(datafusion::error::DataFusionError::Execution(
            "DATEADD third argument must be a date/time value".to_string(),
        )),
    }
}

fn dateadd_array(part: &str, value: i64, array: &Arc<dyn Array>) -> Result<Arc<dyn Array>> {
    match array.data_type() {
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let result: Date32Array = arr
                .iter()
                .map(|opt| {
                    opt.and_then(|days| {
                        let date = NaiveDate::from_num_days_from_ce_opt(days + 719163)?;
                        let new_date = add_to_date(&date, part, value).ok()?;
                        Some(new_date.num_days_from_ce() - 719163)
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            let result: StringArray = arr
                .iter()
                .map(|opt| {
                    opt.and_then(|s| {
                        if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                            let new_date = add_to_date(&date, part, value).ok()?;
                            Some(new_date.format("%Y-%m-%d").to_string())
                        } else if let Ok(datetime) =
                            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                        {
                            let new_datetime = add_to_datetime(&datetime, part, value).ok()?;
                            Some(new_datetime.format("%Y-%m-%d %H:%M:%S").to_string())
                        } else {
                            None
                        }
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }
        _ => Err(datafusion::error::DataFusionError::Execution(
            "Unsupported date type for DATEADD".to_string(),
        )),
    }
}

fn add_to_date(date: &NaiveDate, part: &str, value: i64) -> Result<NaiveDate> {
    match part {
        "YEAR" | "YEARS" | "YY" | "YYYY" => {
            date.with_year(date.year() + value as i32).ok_or_else(|| {
                datafusion::error::DataFusionError::Execution("Invalid year".to_string())
            })
        }
        "MONTH" | "MONTHS" | "MM" | "MON" => {
            let total_months = date.year() * 12 + date.month() as i32 - 1 + value as i32;
            let new_year = total_months / 12;
            let new_month = (total_months % 12 + 1) as u32;
            let clamped_day = clamp_day_to_month(new_year, new_month, date.day());
            NaiveDate::from_ymd_opt(new_year, new_month, clamped_day).ok_or_else(|| {
                datafusion::error::DataFusionError::Execution("Invalid month".to_string())
            })
        }
        "DAY" | "DAYS" | "DD" | "D" => Ok(*date + Duration::days(value)),
        "WEEK" | "WEEKS" | "WK" => Ok(*date + Duration::weeks(value)),
        "QUARTER" | "QUARTERS" | "QTR" => {
            let total_months = date.year() * 12 + date.month() as i32 - 1 + (value * 3) as i32;
            let new_year = total_months / 12;
            let new_month = (total_months % 12 + 1) as u32;
            let clamped_day = clamp_day_to_month(new_year, new_month, date.day());
            NaiveDate::from_ymd_opt(new_year, new_month, clamped_day).ok_or_else(|| {
                datafusion::error::DataFusionError::Execution("Invalid quarter".to_string())
            })
        }
        _ => Err(datafusion::error::DataFusionError::Execution(format!(
            "Unknown date part: {part}"
        ))),
    }
}

fn add_to_datetime(datetime: &NaiveDateTime, part: &str, value: i64) -> Result<NaiveDateTime> {
    match part {
        "YEAR" | "YEARS" | "YY" | "YYYY" => datetime
            .with_year(datetime.year() + value as i32)
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution("Invalid year".to_string())
            }),
        "MONTH" | "MONTHS" | "MM" | "MON" => {
            let total_months = datetime.year() * 12 + datetime.month() as i32 - 1 + value as i32;
            let new_year = total_months / 12;
            let new_month = (total_months % 12 + 1) as u32;
            let clamped_day = clamp_day_to_month(new_year, new_month, datetime.day());
            let new_date =
                NaiveDate::from_ymd_opt(new_year, new_month, clamped_day).ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution("Invalid month".to_string())
                })?;
            Ok(new_date
                .and_hms_opt(datetime.hour(), datetime.minute(), datetime.second())
                .unwrap())
        }
        "DAY" | "DAYS" | "DD" | "D" => Ok(*datetime + Duration::days(value)),
        "WEEK" | "WEEKS" | "WK" => Ok(*datetime + Duration::weeks(value)),
        "HOUR" | "HOURS" | "HH" => Ok(*datetime + Duration::hours(value)),
        "MINUTE" | "MINUTES" | "MI" => Ok(*datetime + Duration::minutes(value)),
        "SECOND" | "SECONDS" | "SS" => Ok(*datetime + Duration::seconds(value)),
        "MILLISECOND" | "MILLISECONDS" | "MS" => Ok(*datetime + Duration::milliseconds(value)),
        "MICROSECOND" | "MICROSECONDS" | "US" => Ok(*datetime + Duration::microseconds(value)),
        "NANOSECOND" | "NANOSECONDS" | "NS" => Ok(*datetime + Duration::nanoseconds(value)),
        "QUARTER" | "QUARTERS" | "QTR" => {
            let total_months =
                datetime.year() * 12 + datetime.month() as i32 - 1 + (value * 3) as i32;
            let new_year = total_months / 12;
            let new_month = (total_months % 12 + 1) as u32;
            let clamped_day = clamp_day_to_month(new_year, new_month, datetime.day());
            let new_date =
                NaiveDate::from_ymd_opt(new_year, new_month, clamped_day).ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution("Invalid quarter".to_string())
                })?;
            Ok(new_date
                .and_hms_opt(datetime.hour(), datetime.minute(), datetime.second())
                .unwrap())
        }
        _ => Err(datafusion::error::DataFusionError::Execution(format!(
            "Unknown date part: {part}"
        ))),
    }
}

/// Create DATEADD scalar UDF
pub fn dateadd() -> ScalarUDF {
    ScalarUDF::from(DateAddFunc::new())
}

// ============================================================================
// DATEDIFF(date_or_time_part, date_or_time_expr1, date_or_time_expr2)
// ============================================================================

/// DATEDIFF function - Calculate the difference between two dates/times
///
/// Syntax: DATEDIFF(date_or_time_part, date_or_time_expr1, date_or_time_expr2)
/// Returns the number of date/time parts between the two expressions.
#[derive(Debug)]
pub struct DateDiffFunc {
    signature: Signature,
}

impl Default for DateDiffFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl DateDiffFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(3), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for DateDiffFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "datediff"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        if args.len() != 3 {
            return Err(datafusion::error::DataFusionError::Execution(
                "DATEDIFF requires exactly 3 arguments".to_string(),
            ));
        }

        let part = &args[0];
        let date1 = &args[1];
        let date2 = &args[2];

        // Get the date part string
        let part_str = match part {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.to_uppercase(),
            _ => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "DATEDIFF first argument must be a string literal".to_string(),
                ))
            }
        };

        // Handle scalar case
        if let (ColumnarValue::Scalar(s1), ColumnarValue::Scalar(s2)) = (date1, date2) {
            let diff = datediff_scalar(&part_str, s1, s2)?;
            return Ok(ColumnarValue::Scalar(ScalarValue::Int64(diff)));
        }

        // Handle array case
        let arr1 = date1.to_array(num_rows)?;
        let arr2 = date2.to_array(num_rows)?;
        let result = datediff_arrays(&part_str, &arr1, &arr2)?;
        Ok(ColumnarValue::Array(Arc::new(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

fn datediff_scalar(
    part: &str,
    scalar1: &ScalarValue,
    scalar2: &ScalarValue,
) -> Result<Option<i64>> {
    let dt1 = scalar_to_datetime(scalar1)?;
    let dt2 = scalar_to_datetime(scalar2)?;

    match (dt1, dt2) {
        (Some(d1), Some(d2)) => Ok(Some(calculate_diff(part, &d1, &d2)?)),
        _ => Ok(None),
    }
}

fn scalar_to_datetime(scalar: &ScalarValue) -> Result<Option<NaiveDateTime>> {
    match scalar {
        ScalarValue::Date32(Some(days)) => {
            let date = NaiveDate::from_num_days_from_ce_opt(*days + 719163).ok_or_else(|| {
                datafusion::error::DataFusionError::Execution("Invalid date".to_string())
            })?;
            Ok(Some(date.and_hms_opt(0, 0, 0).unwrap()))
        }
        ScalarValue::Date64(Some(ms)) => {
            let datetime = NaiveDateTime::from_timestamp_millis(*ms).ok_or_else(|| {
                datafusion::error::DataFusionError::Execution("Invalid datetime".to_string())
            })?;
            Ok(Some(datetime))
        }
        ScalarValue::TimestampNanosecond(Some(ns), _) => {
            let (secs, subsec_nanos) = nanos_to_components(*ns);
            let datetime =
                NaiveDateTime::from_timestamp_opt(secs, subsec_nanos).ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution("Invalid timestamp".to_string())
                })?;
            Ok(Some(datetime))
        }
        ScalarValue::Utf8(Some(s)) => {
            if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                Ok(Some(date.and_hms_opt(0, 0, 0).unwrap()))
            } else if let Ok(datetime) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                Ok(Some(datetime))
            } else {
                Err(datafusion::error::DataFusionError::Execution(format!(
                    "Cannot parse '{s}' as date or datetime"
                )))
            }
        }
        ScalarValue::Date32(None)
        | ScalarValue::Date64(None)
        | ScalarValue::TimestampNanosecond(None, _)
        | ScalarValue::Utf8(None) => Ok(None),
        _ => Err(datafusion::error::DataFusionError::Execution(
            "DATEDIFF argument must be a date/time value".to_string(),
        )),
    }
}

fn calculate_diff(part: &str, dt1: &NaiveDateTime, dt2: &NaiveDateTime) -> Result<i64> {
    let duration = *dt2 - *dt1;

    match part {
        "YEAR" | "YEARS" | "YY" | "YYYY" => Ok((dt2.year() - dt1.year()) as i64),
        "MONTH" | "MONTHS" | "MM" | "MON" => {
            let months1 = dt1.year() as i64 * 12 + dt1.month() as i64;
            let months2 = dt2.year() as i64 * 12 + dt2.month() as i64;
            Ok(months2 - months1)
        }
        "DAY" | "DAYS" | "DD" | "D" => Ok(duration.num_days()),
        "WEEK" | "WEEKS" | "WK" => Ok(duration.num_weeks()),
        "HOUR" | "HOURS" | "HH" => Ok(duration.num_hours()),
        "MINUTE" | "MINUTES" | "MI" => Ok(duration.num_minutes()),
        "SECOND" | "SECONDS" | "SS" => Ok(duration.num_seconds()),
        "MILLISECOND" | "MILLISECONDS" | "MS" => Ok(duration.num_milliseconds()),
        "MICROSECOND" | "MICROSECONDS" | "US" => Ok(duration.num_microseconds().unwrap_or(0)),
        "NANOSECOND" | "NANOSECONDS" | "NS" => Ok(duration.num_nanoseconds().unwrap_or(0)),
        "QUARTER" | "QUARTERS" | "QTR" => {
            let months1 = dt1.year() as i64 * 12 + dt1.month() as i64;
            let months2 = dt2.year() as i64 * 12 + dt2.month() as i64;
            Ok((months2 - months1) / 3)
        }
        _ => Err(datafusion::error::DataFusionError::Execution(format!(
            "Unknown date part: {part}"
        ))),
    }
}

fn datediff_arrays(part: &str, arr1: &Arc<dyn Array>, arr2: &Arc<dyn Array>) -> Result<Int64Array> {
    if arr1.len() != arr2.len() {
        return Err(datafusion::error::DataFusionError::Execution(
            "Arrays must have the same length".to_string(),
        ));
    }

    let mut results = Vec::with_capacity(arr1.len());

    for i in 0..arr1.len() {
        if arr1.is_null(i) || arr2.is_null(i) {
            results.push(None);
            continue;
        }

        let dt1 = array_value_to_datetime(arr1, i)?;
        let dt2 = array_value_to_datetime(arr2, i)?;

        match (dt1, dt2) {
            (Some(d1), Some(d2)) => results.push(Some(calculate_diff(part, &d1, &d2)?)),
            _ => results.push(None),
        }
    }

    Ok(Int64Array::from(results))
}

fn array_value_to_datetime(array: &Arc<dyn Array>, idx: usize) -> Result<Option<NaiveDateTime>> {
    match array.data_type() {
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            if arr.is_null(idx) {
                return Ok(None);
            }
            let days = arr.value(idx);
            let date = NaiveDate::from_num_days_from_ce_opt(days + 719163).ok_or_else(|| {
                datafusion::error::DataFusionError::Execution("Invalid date".to_string())
            })?;
            Ok(Some(date.and_hms_opt(0, 0, 0).unwrap()))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            if arr.is_null(idx) {
                return Ok(None);
            }
            let s = arr.value(idx);
            if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                Ok(Some(date.and_hms_opt(0, 0, 0).unwrap()))
            } else if let Ok(datetime) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                Ok(Some(datetime))
            } else {
                Err(datafusion::error::DataFusionError::Execution(format!(
                    "Cannot parse '{s}' as date or datetime"
                )))
            }
        }
        _ => Err(datafusion::error::DataFusionError::Execution(
            "Unsupported date type for DATEDIFF".to_string(),
        )),
    }
}

/// Create DATEDIFF scalar UDF
pub fn datediff() -> ScalarUDF {
    ScalarUDF::from(DateDiffFunc::new())
}

// ============================================================================
// TO_DATE(string_expr [, format])
// ============================================================================

/// TO_DATE function - Convert a string to a date
///
/// Syntax: TO_DATE(string_expr [, format])
/// Returns a date value from the input string.
#[derive(Debug)]
pub struct ToDateFunc {
    signature: Signature,
}

impl Default for ToDateFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ToDateFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ToDateFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "to_date"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _num_rows: usize) -> Result<ColumnarValue> {
        if args.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "TO_DATE requires at least 1 argument".to_string(),
            ));
        }

        let input = &args[0];

        // Handle scalar case
        match input {
            ColumnarValue::Scalar(scalar) => {
                let result = to_date_scalar(scalar)?;
                Ok(ColumnarValue::Scalar(result))
            }
            ColumnarValue::Array(arr) => {
                let result = to_date_array(arr)?;
                Ok(ColumnarValue::Array(result))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

fn to_date_scalar(scalar: &ScalarValue) -> Result<ScalarValue> {
    match scalar {
        ScalarValue::Utf8(Some(s)) => {
            // Try multiple date formats
            let formats = [
                "%Y-%m-%d",
                "%Y/%m/%d",
                "%d-%m-%Y",
                "%d/%m/%Y",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
            ];

            for fmt in &formats {
                if let Ok(date) = NaiveDate::parse_from_str(s, fmt) {
                    let days = date.num_days_from_ce() - 719163;
                    return Ok(ScalarValue::Date32(Some(days)));
                }
                if let Ok(datetime) = NaiveDateTime::parse_from_str(s, fmt) {
                    let days = datetime.date().num_days_from_ce() - 719163;
                    return Ok(ScalarValue::Date32(Some(days)));
                }
            }

            Err(datafusion::error::DataFusionError::Execution(format!(
                "Cannot parse '{s}' as date"
            )))
        }
        ScalarValue::Date32(_) => Ok(scalar.clone()),
        ScalarValue::Date64(Some(ms)) => {
            let days = (*ms / 86400000) as i32;
            Ok(ScalarValue::Date32(Some(days)))
        }
        ScalarValue::Utf8(None) | ScalarValue::Date64(None) => Ok(ScalarValue::Date32(None)),
        _ => Err(datafusion::error::DataFusionError::Execution(
            "TO_DATE argument must be a string or date".to_string(),
        )),
    }
}

fn to_date_array(array: &Arc<dyn Array>) -> Result<Arc<dyn Array>> {
    match array.data_type() {
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            let result: Date32Array = arr
                .iter()
                .map(|opt| {
                    opt.and_then(|s| {
                        let formats = [
                            "%Y-%m-%d",
                            "%Y/%m/%d",
                            "%d-%m-%Y",
                            "%d/%m/%Y",
                            "%Y-%m-%d %H:%M:%S",
                        ];
                        for fmt in &formats {
                            if let Ok(date) = NaiveDate::parse_from_str(s, fmt) {
                                return Some(date.num_days_from_ce() - 719163);
                            }
                            if let Ok(datetime) = NaiveDateTime::parse_from_str(s, fmt) {
                                return Some(datetime.date().num_days_from_ce() - 719163);
                            }
                        }
                        None
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }
        DataType::Date32 => Ok(array.clone()),
        _ => Err(datafusion::error::DataFusionError::Execution(
            "TO_DATE argument must be a string or date".to_string(),
        )),
    }
}

/// Create TO_DATE scalar UDF
pub fn to_date() -> ScalarUDF {
    ScalarUDF::from(ToDateFunc::new())
}

// ============================================================================
// TO_TIMESTAMP(string_expr [, format])
// ============================================================================

/// TO_TIMESTAMP function - Convert a string to a timestamp
///
/// Syntax: TO_TIMESTAMP(string_expr [, format])
/// Returns a timestamp value from the input string.
#[derive(Debug)]
pub struct ToTimestampFunc {
    signature: Signature,
}

impl Default for ToTimestampFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ToTimestampFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ToTimestampFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "to_timestamp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Nanosecond,
            None,
        ))
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _num_rows: usize) -> Result<ColumnarValue> {
        if args.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "TO_TIMESTAMP requires at least 1 argument".to_string(),
            ));
        }

        let input = &args[0];

        match input {
            ColumnarValue::Scalar(scalar) => {
                let result = to_timestamp_scalar(scalar)?;
                Ok(ColumnarValue::Scalar(result))
            }
            ColumnarValue::Array(arr) => {
                let result = to_timestamp_array(arr)?;
                Ok(ColumnarValue::Array(result))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

fn to_timestamp_scalar(scalar: &ScalarValue) -> Result<ScalarValue> {
    match scalar {
        ScalarValue::Utf8(Some(s)) => {
            // Try multiple datetime formats
            let formats = [
                "%Y-%m-%d %H:%M:%S%.f",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S%.f",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d",
                "%Y/%m/%d %H:%M:%S",
                "%Y/%m/%d",
            ];

            for fmt in &formats {
                if let Ok(datetime) = NaiveDateTime::parse_from_str(s, fmt) {
                    let nanos = datetime.and_utc().timestamp_nanos_opt().ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(
                            "Timestamp out of range".to_string(),
                        )
                    })?;
                    return Ok(ScalarValue::TimestampNanosecond(Some(nanos), None));
                }
                // Also try parsing as date only
                if let Ok(date) = NaiveDate::parse_from_str(s, fmt) {
                    let datetime = date.and_hms_opt(0, 0, 0).unwrap();
                    let nanos = datetime.and_utc().timestamp_nanos_opt().ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(
                            "Timestamp out of range".to_string(),
                        )
                    })?;
                    return Ok(ScalarValue::TimestampNanosecond(Some(nanos), None));
                }
            }

            Err(datafusion::error::DataFusionError::Execution(format!(
                "Cannot parse '{s}' as timestamp"
            )))
        }
        ScalarValue::Int64(Some(epoch)) => {
            // Assume seconds since Unix epoch
            let nanos = *epoch * 1_000_000_000;
            Ok(ScalarValue::TimestampNanosecond(Some(nanos), None))
        }
        ScalarValue::TimestampNanosecond(_, _) => Ok(scalar.clone()),
        ScalarValue::Utf8(None) | ScalarValue::Int64(None) => {
            Ok(ScalarValue::TimestampNanosecond(None, None))
        }
        _ => Err(datafusion::error::DataFusionError::Execution(
            "TO_TIMESTAMP argument must be a string or integer".to_string(),
        )),
    }
}

fn to_timestamp_array(array: &Arc<dyn Array>) -> Result<Arc<dyn Array>> {
    use arrow::array::TimestampNanosecondArray;

    match array.data_type() {
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            let result: TimestampNanosecondArray = arr
                .iter()
                .map(|opt| {
                    opt.and_then(|s| {
                        let formats = [
                            "%Y-%m-%d %H:%M:%S%.f",
                            "%Y-%m-%d %H:%M:%S",
                            "%Y-%m-%dT%H:%M:%S",
                            "%Y-%m-%d",
                        ];
                        for fmt in &formats {
                            if let Ok(datetime) = NaiveDateTime::parse_from_str(s, fmt) {
                                return datetime.and_utc().timestamp_nanos_opt();
                            }
                            if let Ok(date) = NaiveDate::parse_from_str(s, fmt) {
                                let datetime = date.and_hms_opt(0, 0, 0).unwrap();
                                return datetime.and_utc().timestamp_nanos_opt();
                            }
                        }
                        None
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }
        _ => Err(datafusion::error::DataFusionError::Execution(
            "TO_TIMESTAMP argument must be a string".to_string(),
        )),
    }
}

/// Create TO_TIMESTAMP scalar UDF
pub fn to_timestamp_udf() -> ScalarUDF {
    ScalarUDF::from(ToTimestampFunc::new())
}

// ============================================================================
// LAST_DAY(date_expr)
// ============================================================================

/// LAST_DAY function - Return the last day of the month
///
/// Syntax: LAST_DAY(date_expr)
/// Returns the last day of the month for the given date.
#[derive(Debug)]
pub struct LastDayFunc {
    signature: Signature,
}

impl Default for LastDayFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl LastDayFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for LastDayFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "last_day"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _num_rows: usize) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(
                "LAST_DAY requires exactly 1 argument".to_string(),
            ));
        }

        let input = &args[0];

        match input {
            ColumnarValue::Scalar(scalar) => {
                let result = last_day_scalar(scalar)?;
                Ok(ColumnarValue::Scalar(result))
            }
            ColumnarValue::Array(arr) => {
                let result = last_day_array(arr)?;
                Ok(ColumnarValue::Array(result))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

fn get_last_day_of_month(year: i32, month: u32) -> NaiveDate {
    // Get first day of next month, then subtract one day
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    NaiveDate::from_ymd_opt(next_year, next_month, 1)
        .unwrap()
        .pred_opt()
        .unwrap()
}

fn last_day_scalar(scalar: &ScalarValue) -> Result<ScalarValue> {
    match scalar {
        ScalarValue::Date32(Some(days)) => {
            let date = NaiveDate::from_num_days_from_ce_opt(*days + 719163).ok_or_else(|| {
                datafusion::error::DataFusionError::Execution("Invalid date".to_string())
            })?;
            let last_day = get_last_day_of_month(date.year(), date.month());
            let new_days = last_day.num_days_from_ce() - 719163;
            Ok(ScalarValue::Date32(Some(new_days)))
        }
        ScalarValue::Utf8(Some(s)) => {
            let date = NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|_| {
                datafusion::error::DataFusionError::Execution(format!("Cannot parse '{s}' as date"))
            })?;
            let last_day = get_last_day_of_month(date.year(), date.month());
            let new_days = last_day.num_days_from_ce() - 719163;
            Ok(ScalarValue::Date32(Some(new_days)))
        }
        ScalarValue::Date32(None) | ScalarValue::Utf8(None) => Ok(ScalarValue::Date32(None)),
        _ => Err(datafusion::error::DataFusionError::Execution(
            "LAST_DAY argument must be a date".to_string(),
        )),
    }
}

fn last_day_array(array: &Arc<dyn Array>) -> Result<Arc<dyn Array>> {
    match array.data_type() {
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let result: Date32Array = arr
                .iter()
                .map(|opt| {
                    opt.and_then(|days| {
                        let date = NaiveDate::from_num_days_from_ce_opt(days + 719163)?;
                        let last_day = get_last_day_of_month(date.year(), date.month());
                        Some(last_day.num_days_from_ce() - 719163)
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            let result: Date32Array = arr
                .iter()
                .map(|opt| {
                    opt.and_then(|s| {
                        let date = NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()?;
                        let last_day = get_last_day_of_month(date.year(), date.month());
                        Some(last_day.num_days_from_ce() - 719163)
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }
        _ => Err(datafusion::error::DataFusionError::Execution(
            "LAST_DAY argument must be a date".to_string(),
        )),
    }
}

/// Create LAST_DAY scalar UDF
pub fn last_day() -> ScalarUDF {
    ScalarUDF::from(LastDayFunc::new())
}

// ============================================================================
// DAYNAME(date_expr)
// ============================================================================

/// DAYNAME function - Return the name of the day of the week
///
/// Syntax: DAYNAME(date_expr)
/// Returns the name of the weekday (e.g., 'Mon', 'Tue', etc.)
#[derive(Debug)]
pub struct DayNameFunc {
    signature: Signature,
}

impl Default for DayNameFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl DayNameFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for DayNameFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "dayname"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _num_rows: usize) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(
                "DAYNAME requires exactly 1 argument".to_string(),
            ));
        }

        let input = &args[0];

        match input {
            ColumnarValue::Scalar(scalar) => {
                let result = dayname_scalar(scalar)?;
                Ok(ColumnarValue::Scalar(result))
            }
            ColumnarValue::Array(arr) => {
                let result = dayname_array(arr)?;
                Ok(ColumnarValue::Array(result))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

fn get_day_name(date: &NaiveDate) -> &'static str {
    match date.weekday() {
        chrono::Weekday::Mon => "Mon",
        chrono::Weekday::Tue => "Tue",
        chrono::Weekday::Wed => "Wed",
        chrono::Weekday::Thu => "Thu",
        chrono::Weekday::Fri => "Fri",
        chrono::Weekday::Sat => "Sat",
        chrono::Weekday::Sun => "Sun",
    }
}

fn dayname_scalar(scalar: &ScalarValue) -> Result<ScalarValue> {
    match scalar {
        ScalarValue::Date32(Some(days)) => {
            let date = NaiveDate::from_num_days_from_ce_opt(*days + 719163).ok_or_else(|| {
                datafusion::error::DataFusionError::Execution("Invalid date".to_string())
            })?;
            Ok(ScalarValue::Utf8(Some(get_day_name(&date).to_string())))
        }
        ScalarValue::Utf8(Some(s)) => {
            let date = NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|_| {
                datafusion::error::DataFusionError::Execution(format!("Cannot parse '{s}' as date"))
            })?;
            Ok(ScalarValue::Utf8(Some(get_day_name(&date).to_string())))
        }
        ScalarValue::Date32(None) | ScalarValue::Utf8(None) => Ok(ScalarValue::Utf8(None)),
        _ => Err(datafusion::error::DataFusionError::Execution(
            "DAYNAME argument must be a date".to_string(),
        )),
    }
}

fn dayname_array(array: &Arc<dyn Array>) -> Result<Arc<dyn Array>> {
    match array.data_type() {
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let result: StringArray = arr
                .iter()
                .map(|opt| {
                    opt.and_then(|days| {
                        let date = NaiveDate::from_num_days_from_ce_opt(days + 719163)?;
                        Some(get_day_name(&date).to_string())
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            let result: StringArray = arr
                .iter()
                .map(|opt| {
                    opt.and_then(|s| {
                        let date = NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()?;
                        Some(get_day_name(&date).to_string())
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }
        _ => Err(datafusion::error::DataFusionError::Execution(
            "DAYNAME argument must be a date".to_string(),
        )),
    }
}

/// Create DAYNAME scalar UDF
pub fn dayname() -> ScalarUDF {
    ScalarUDF::from(DayNameFunc::new())
}

// ============================================================================
// MONTHNAME(date_expr)
// ============================================================================

/// MONTHNAME function - Return the name of the month
///
/// Syntax: MONTHNAME(date_expr)
/// Returns the name of the month (e.g., 'Jan', 'Feb', etc.)
#[derive(Debug)]
pub struct MonthNameFunc {
    signature: Signature,
}

impl Default for MonthNameFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl MonthNameFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MonthNameFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "monthname"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _num_rows: usize) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(
                "MONTHNAME requires exactly 1 argument".to_string(),
            ));
        }

        let input = &args[0];

        match input {
            ColumnarValue::Scalar(scalar) => {
                let result = monthname_scalar(scalar)?;
                Ok(ColumnarValue::Scalar(result))
            }
            ColumnarValue::Array(arr) => {
                let result = monthname_array(arr)?;
                Ok(ColumnarValue::Array(result))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

fn get_month_name(month: u32) -> &'static str {
    match month {
        1 => "Jan",
        2 => "Feb",
        3 => "Mar",
        4 => "Apr",
        5 => "May",
        6 => "Jun",
        7 => "Jul",
        8 => "Aug",
        9 => "Sep",
        10 => "Oct",
        11 => "Nov",
        12 => "Dec",
        _ => "Unknown",
    }
}

fn monthname_scalar(scalar: &ScalarValue) -> Result<ScalarValue> {
    match scalar {
        ScalarValue::Date32(Some(days)) => {
            let date = NaiveDate::from_num_days_from_ce_opt(*days + 719163).ok_or_else(|| {
                datafusion::error::DataFusionError::Execution("Invalid date".to_string())
            })?;
            Ok(ScalarValue::Utf8(Some(
                get_month_name(date.month()).to_string(),
            )))
        }
        ScalarValue::Utf8(Some(s)) => {
            let date = NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|_| {
                datafusion::error::DataFusionError::Execution(format!("Cannot parse '{s}' as date"))
            })?;
            Ok(ScalarValue::Utf8(Some(
                get_month_name(date.month()).to_string(),
            )))
        }
        ScalarValue::Date32(None) | ScalarValue::Utf8(None) => Ok(ScalarValue::Utf8(None)),
        _ => Err(datafusion::error::DataFusionError::Execution(
            "MONTHNAME argument must be a date".to_string(),
        )),
    }
}

fn monthname_array(array: &Arc<dyn Array>) -> Result<Arc<dyn Array>> {
    match array.data_type() {
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let result: StringArray = arr
                .iter()
                .map(|opt| {
                    opt.and_then(|days| {
                        let date = NaiveDate::from_num_days_from_ce_opt(days + 719163)?;
                        Some(get_month_name(date.month()).to_string())
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            let result: StringArray = arr
                .iter()
                .map(|opt| {
                    opt.and_then(|s| {
                        let date = NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()?;
                        Some(get_month_name(date.month()).to_string())
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }
        _ => Err(datafusion::error::DataFusionError::Execution(
            "MONTHNAME argument must be a date".to_string(),
        )),
    }
}

/// Create MONTHNAME scalar UDF
pub fn monthname() -> ScalarUDF {
    ScalarUDF::from(MonthNameFunc::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dateadd_days() {
        let func = DateAddFunc::new();

        let part = ColumnarValue::Scalar(ScalarValue::Utf8(Some("day".to_string())));
        let value = ColumnarValue::Scalar(ScalarValue::Int64(Some(5)));
        let date = ColumnarValue::Scalar(ScalarValue::Utf8(Some("2024-01-15".to_string())));

        let result = func.invoke_batch(&[part, value, date], 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert_eq!(s, "2024-01-20");
        } else {
            panic!("Expected scalar utf8");
        }
    }

    #[test]
    fn test_dateadd_months() {
        let func = DateAddFunc::new();

        let part = ColumnarValue::Scalar(ScalarValue::Utf8(Some("month".to_string())));
        let value = ColumnarValue::Scalar(ScalarValue::Int64(Some(2)));
        let date = ColumnarValue::Scalar(ScalarValue::Utf8(Some("2024-01-15".to_string())));

        let result = func.invoke_batch(&[part, value, date], 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert_eq!(s, "2024-03-15");
        } else {
            panic!("Expected scalar utf8");
        }
    }

    #[test]
    fn test_dateadd_negative() {
        let func = DateAddFunc::new();

        let part = ColumnarValue::Scalar(ScalarValue::Utf8(Some("day".to_string())));
        let value = ColumnarValue::Scalar(ScalarValue::Int64(Some(-10)));
        let date = ColumnarValue::Scalar(ScalarValue::Utf8(Some("2024-01-15".to_string())));

        let result = func.invoke_batch(&[part, value, date], 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert_eq!(s, "2024-01-05");
        } else {
            panic!("Expected scalar utf8");
        }
    }

    #[test]
    fn test_datediff_days() {
        let func = DateDiffFunc::new();

        let part = ColumnarValue::Scalar(ScalarValue::Utf8(Some("day".to_string())));
        let date1 = ColumnarValue::Scalar(ScalarValue::Utf8(Some("2024-01-01".to_string())));
        let date2 = ColumnarValue::Scalar(ScalarValue::Utf8(Some("2024-01-15".to_string())));

        let result = func.invoke_batch(&[part, date1, date2], 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) = result {
            assert_eq!(v, 14);
        } else {
            panic!("Expected scalar int64");
        }
    }

    #[test]
    fn test_datediff_months() {
        let func = DateDiffFunc::new();

        let part = ColumnarValue::Scalar(ScalarValue::Utf8(Some("month".to_string())));
        let date1 = ColumnarValue::Scalar(ScalarValue::Utf8(Some("2024-01-15".to_string())));
        let date2 = ColumnarValue::Scalar(ScalarValue::Utf8(Some("2024-06-15".to_string())));

        let result = func.invoke_batch(&[part, date1, date2], 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) = result {
            assert_eq!(v, 5);
        } else {
            panic!("Expected scalar int64");
        }
    }

    #[test]
    fn test_datediff_years() {
        let func = DateDiffFunc::new();

        let part = ColumnarValue::Scalar(ScalarValue::Utf8(Some("year".to_string())));
        let date1 = ColumnarValue::Scalar(ScalarValue::Utf8(Some("2020-06-15".to_string())));
        let date2 = ColumnarValue::Scalar(ScalarValue::Utf8(Some("2024-06-15".to_string())));

        let result = func.invoke_batch(&[part, date1, date2], 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) = result {
            assert_eq!(v, 4);
        } else {
            panic!("Expected scalar int64");
        }
    }

    #[test]
    fn test_datediff_with_time() {
        let func = DateDiffFunc::new();

        let part = ColumnarValue::Scalar(ScalarValue::Utf8(Some("hour".to_string())));
        let dt1 = ColumnarValue::Scalar(ScalarValue::Utf8(Some("2024-01-01 10:00:00".to_string())));
        let dt2 = ColumnarValue::Scalar(ScalarValue::Utf8(Some("2024-01-01 15:30:00".to_string())));

        let result = func.invoke_batch(&[part, dt1, dt2], 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) = result {
            assert_eq!(v, 5);
        } else {
            panic!("Expected scalar int64");
        }
    }

    #[test]
    fn test_to_date() {
        let func = ToDateFunc::new();

        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("2024-01-15".to_string())));
        let result = func.invoke_batch(&[input], 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Date32(Some(days))) = result {
            let date = NaiveDate::from_num_days_from_ce_opt(days + 719163).unwrap();
            assert_eq!(date.year(), 2024);
            assert_eq!(date.month(), 1);
            assert_eq!(date.day(), 15);
        } else {
            panic!("Expected scalar Date32");
        }
    }

    #[test]
    fn test_last_day() {
        let func = LastDayFunc::new();

        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("2024-02-15".to_string())));
        let result = func.invoke_batch(&[input], 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Date32(Some(days))) = result {
            let date = NaiveDate::from_num_days_from_ce_opt(days + 719163).unwrap();
            assert_eq!(date.year(), 2024);
            assert_eq!(date.month(), 2);
            assert_eq!(date.day(), 29); // 2024 is a leap year
        } else {
            panic!("Expected scalar Date32");
        }
    }

    #[test]
    fn test_dayname() {
        let func = DayNameFunc::new();

        // 2024-01-15 is a Monday
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("2024-01-15".to_string())));
        let result = func.invoke_batch(&[input], 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(name))) = result {
            assert_eq!(name, "Mon");
        } else {
            panic!("Expected scalar Utf8");
        }
    }

    #[test]
    fn test_monthname() {
        let func = MonthNameFunc::new();

        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("2024-03-15".to_string())));
        let result = func.invoke_batch(&[input], 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(name))) = result {
            assert_eq!(name, "Mar");
        } else {
            panic!("Expected scalar Utf8");
        }
    }
}
