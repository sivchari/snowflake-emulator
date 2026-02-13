//! TRY_* functions (TRY_PARSE_JSON, TRY_TO_NUMBER, TRY_TO_DATE, etc.)
//!
//! Snowflake-compatible error-handling functions that return NULL instead of errors.

use std::any::Any;
use std::sync::Arc;

use chrono::{Datelike, NaiveDate};
use datafusion::arrow::array::{Array, Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};

// ============================================================================
// TRY_PARSE_JSON(string)
// ============================================================================

/// TRY_PARSE_JSON function - Parse a string as JSON, return NULL on error
///
/// Syntax: TRY_PARSE_JSON(string_expression)
/// Returns the input string as JSON, or NULL if parsing fails.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TryParseJsonFunc {
    signature: Signature,
}

impl Default for TryParseJsonFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl TryParseJsonFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TryParseJsonFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "try_parse_json"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(
                "TRY_PARSE_JSON requires exactly 1 argument".to_string(),
            ));
        }

        let input = &args.args[0];

        match input {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                let result = serde_json::from_str::<serde_json::Value>(s)
                    .ok()
                    .and_then(|v| serde_json::to_string(&v).ok());
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
            }
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "TRY_PARSE_JSON argument must be a string".to_string(),
                    )
                })?;

                let result: StringArray = str_arr
                    .iter()
                    .map(|opt_s| {
                        opt_s.and_then(|s| {
                            serde_json::from_str::<serde_json::Value>(s)
                                .ok()
                                .and_then(|v| serde_json::to_string(&v).ok())
                        })
                    })
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            _ => {
                let arr = input.to_array(args.number_rows)?;
                let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "TRY_PARSE_JSON argument must be a string".to_string(),
                    )
                })?;

                let result: StringArray = str_arr
                    .iter()
                    .map(|opt_s| {
                        opt_s.and_then(|s| {
                            serde_json::from_str::<serde_json::Value>(s)
                                .ok()
                                .and_then(|v| serde_json::to_string(&v).ok())
                        })
                    })
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

/// Create TRY_PARSE_JSON scalar UDF
pub fn try_parse_json() -> ScalarUDF {
    ScalarUDF::from(TryParseJsonFunc::new())
}

// ============================================================================
// TRY_TO_NUMBER(string)
// ============================================================================

/// TRY_TO_NUMBER function - Convert string to number, return NULL on error
///
/// Syntax: TRY_TO_NUMBER(string_expression)
/// Returns the numeric value, or NULL if conversion fails.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TryToNumberFunc {
    signature: Signature,
}

impl Default for TryToNumberFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl TryToNumberFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TryToNumberFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "try_to_number"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(
                "TRY_TO_NUMBER requires exactly 1 argument".to_string(),
            ));
        }

        let input = &args.args[0];

        match input {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                let result = s.trim().parse::<f64>().ok();
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(result)))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)))
            }
            ColumnarValue::Scalar(ScalarValue::Int64(v)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Float64(v.map(|n| n as f64)),
            )),
            ColumnarValue::Scalar(ScalarValue::Float64(v)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(*v)))
            }
            ColumnarValue::Array(arr) => {
                let result = try_to_number_array(arr)?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            _ => {
                let arr = input.to_array(args.number_rows)?;
                let result = try_to_number_array(&arr)?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

fn try_to_number_array(array: &Arc<dyn Array>) -> Result<Float64Array> {
    match array.data_type() {
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            let result: Float64Array = arr
                .iter()
                .map(|opt| opt.and_then(|s| s.trim().parse::<f64>().ok()))
                .collect();
            Ok(result)
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            let result: Float64Array = arr.iter().map(|opt| opt.map(|n| n as f64)).collect();
            Ok(result)
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(arr.clone())
        }
        _ => Err(datafusion::error::DataFusionError::Execution(
            "TRY_TO_NUMBER argument must be a string or number".to_string(),
        )),
    }
}

/// Create TRY_TO_NUMBER scalar UDF
pub fn try_to_number() -> ScalarUDF {
    ScalarUDF::from(TryToNumberFunc::new())
}

// ============================================================================
// TRY_TO_DATE(string)
// ============================================================================

/// TRY_TO_DATE function - Convert string to date, return NULL on error
///
/// Syntax: TRY_TO_DATE(string_expression)
/// Returns the date value, or NULL if conversion fails.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TryToDateFunc {
    signature: Signature,
}

impl Default for TryToDateFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl TryToDateFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TryToDateFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "try_to_date"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(
                "TRY_TO_DATE requires exactly 1 argument".to_string(),
            ));
        }

        let input = &args.args[0];

        match input {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                let result = try_parse_date(s);
                Ok(ColumnarValue::Scalar(ScalarValue::Date32(result)))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Date32(None)))
            }
            ColumnarValue::Scalar(ScalarValue::Date32(v)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Date32(*v)))
            }
            ColumnarValue::Array(arr) => {
                let result = try_to_date_array(arr)?;
                Ok(ColumnarValue::Array(result))
            }
            _ => {
                let arr = input.to_array(args.number_rows)?;
                let result = try_to_date_array(&arr)?;
                Ok(ColumnarValue::Array(result))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

fn try_parse_date(s: &str) -> Option<i32> {
    // Try common date formats
    let formats = [
        "%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y", "%m-%d-%Y", "%m/%d/%Y", "%Y%m%d",
    ];

    for format in formats {
        if let Ok(date) = NaiveDate::parse_from_str(s.trim(), format) {
            return Some(date.num_days_from_ce() - 719163);
        }
    }

    None
}

fn try_to_date_array(array: &Arc<dyn Array>) -> Result<Arc<dyn Array>> {
    use datafusion::arrow::array::Date32Array;

    match array.data_type() {
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            let result: Date32Array = arr.iter().map(|opt| opt.and_then(try_parse_date)).collect();
            Ok(Arc::new(result))
        }
        DataType::Date32 => Ok(array.clone()),
        _ => Err(datafusion::error::DataFusionError::Execution(
            "TRY_TO_DATE argument must be a string or date".to_string(),
        )),
    }
}

/// Create TRY_TO_DATE scalar UDF
pub fn try_to_date() -> ScalarUDF {
    ScalarUDF::from(TryToDateFunc::new())
}

// ============================================================================
// TRY_TO_BOOLEAN(string)
// ============================================================================

/// TRY_TO_BOOLEAN function - Convert string to boolean, return NULL on error
///
/// Syntax: TRY_TO_BOOLEAN(string_expression)
/// Returns true/false, or NULL if conversion fails.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TryToBooleanFunc {
    signature: Signature,
}

impl Default for TryToBooleanFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl TryToBooleanFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TryToBooleanFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "try_to_boolean"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(
                "TRY_TO_BOOLEAN requires exactly 1 argument".to_string(),
            ));
        }

        let input = &args.args[0];

        match input {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                let result = try_parse_boolean(s);
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(result)))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)))
            }
            ColumnarValue::Scalar(ScalarValue::Boolean(v)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(*v)))
            }
            ColumnarValue::Scalar(ScalarValue::Int64(Some(n))) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(*n != 0))))
            }
            ColumnarValue::Array(arr) => {
                let result = try_to_boolean_array(arr)?;
                Ok(ColumnarValue::Array(result))
            }
            _ => {
                let arr = input.to_array(args.number_rows)?;
                let result = try_to_boolean_array(&arr)?;
                Ok(ColumnarValue::Array(result))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

fn try_parse_boolean(s: &str) -> Option<bool> {
    match s.trim().to_lowercase().as_str() {
        "true" | "t" | "yes" | "y" | "on" | "1" => Some(true),
        "false" | "f" | "no" | "n" | "off" | "0" => Some(false),
        _ => None,
    }
}

fn try_to_boolean_array(array: &Arc<dyn Array>) -> Result<Arc<dyn Array>> {
    use datafusion::arrow::array::BooleanArray;

    match array.data_type() {
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            let result: BooleanArray = arr
                .iter()
                .map(|opt| opt.and_then(try_parse_boolean))
                .collect();
            Ok(Arc::new(result))
        }
        DataType::Boolean => Ok(array.clone()),
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            let result: BooleanArray = arr.iter().map(|opt| opt.map(|n| n != 0)).collect();
            Ok(Arc::new(result))
        }
        _ => Err(datafusion::error::DataFusionError::Execution(
            "TRY_TO_BOOLEAN argument must be a string, boolean, or number".to_string(),
        )),
    }
}

/// Create TRY_TO_BOOLEAN scalar UDF
pub fn try_to_boolean() -> ScalarUDF {
    ScalarUDF::from(TryToBooleanFunc::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::functions::test_utils::{
        invoke_udf_bool, invoke_udf_date32, invoke_udf_float64, invoke_udf_string,
    };

    #[test]
    fn test_try_parse_json_valid() {
        let func = TryParseJsonFunc::new();

        let input =
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(r#"{"key": "value"}"#.to_string())));

        let result = invoke_udf_string(&func, &[input]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert!(s.contains("key"));
            assert!(s.contains("value"));
        } else {
            panic!("Expected scalar utf8");
        }
    }

    #[test]
    fn test_try_parse_json_invalid() {
        let func = TryParseJsonFunc::new();

        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("not valid json".to_string())));

        let result = invoke_udf_string(&func, &[input]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(v)) = result {
            assert!(v.is_none());
        } else {
            panic!("Expected scalar utf8 null");
        }
    }

    #[test]
    fn test_try_to_number_valid() {
        let func = TryToNumberFunc::new();

        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("42.5".to_string())));

        let result = invoke_udf_float64(&func, &[input]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) = result {
            assert!((v - 42.5).abs() < 0.001);
        } else {
            panic!("Expected scalar float64");
        }
    }

    #[test]
    fn test_try_to_number_invalid() {
        let func = TryToNumberFunc::new();

        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("not a number".to_string())));

        let result = invoke_udf_float64(&func, &[input]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Float64(v)) = result {
            assert!(v.is_none());
        } else {
            panic!("Expected scalar float64 null");
        }
    }

    #[test]
    fn test_try_to_date_valid() {
        let func = TryToDateFunc::new();

        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("2024-01-15".to_string())));

        let result = invoke_udf_date32(&func, &[input]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Date32(Some(_))) = result {
            // Successfully parsed
        } else {
            panic!("Expected scalar date32");
        }
    }

    #[test]
    fn test_try_to_date_invalid() {
        let func = TryToDateFunc::new();

        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("not a date".to_string())));

        let result = invoke_udf_date32(&func, &[input]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Date32(v)) = result {
            assert!(v.is_none());
        } else {
            panic!("Expected scalar date32 null");
        }
    }

    #[test]
    fn test_try_to_boolean_true() {
        let func = TryToBooleanFunc::new();

        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("true".to_string())));

        let result = invoke_udf_bool(&func, &[input]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Boolean(Some(v))) = result {
            assert!(v);
        } else {
            panic!("Expected scalar boolean true");
        }
    }

    #[test]
    fn test_try_to_boolean_false() {
        let func = TryToBooleanFunc::new();

        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("no".to_string())));

        let result = invoke_udf_bool(&func, &[input]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Boolean(Some(v))) = result {
            assert!(!v);
        } else {
            panic!("Expected scalar boolean false");
        }
    }

    #[test]
    fn test_try_to_boolean_invalid() {
        let func = TryToBooleanFunc::new();

        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("maybe".to_string())));

        let result = invoke_udf_bool(&func, &[input]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Boolean(v)) = result {
            assert!(v.is_none());
        } else {
            panic!("Expected scalar boolean null");
        }
    }
}
