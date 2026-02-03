//! JSON functions (PARSE_JSON, TO_JSON)
//!
//! Snowflake-compatible JSON processing functions.

use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

// ============================================================================
// PARSE_JSON(string)
// ============================================================================

/// PARSE_JSON function - Parse a string as JSON and return a VARIANT
///
/// Syntax: PARSE_JSON(string_expression)
/// Returns the input string as a JSON value (VARIANT in Snowflake).
/// In this implementation, we validate JSON and return it as a string.
#[derive(Debug)]
pub struct ParseJsonFunc {
    signature: Signature,
}

impl Default for ParseJsonFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ParseJsonFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ParseJsonFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "parse_json"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // Return as Utf8 (representing VARIANT/JSON)
        Ok(DataType::Utf8)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(
                "PARSE_JSON requires exactly 1 argument".to_string(),
            ));
        }

        let input = &args[0];

        match input {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                // Validate JSON
                match serde_json::from_str::<serde_json::Value>(s) {
                    Ok(v) => {
                        // Return compact JSON
                        let json_str = serde_json::to_string(&v).map_err(|e| {
                            datafusion::error::DataFusionError::Execution(format!(
                                "JSON serialization error: {}",
                                e
                            ))
                        })?;
                        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(json_str))))
                    }
                    Err(e) => Err(datafusion::error::DataFusionError::Execution(format!(
                        "Invalid JSON: {}",
                        e
                    ))),
                }
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
            }
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "PARSE_JSON argument must be a string".to_string(),
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
                let arr = input.to_array(num_rows)?;
                let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "PARSE_JSON argument must be a string".to_string(),
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

/// Create PARSE_JSON scalar UDF
pub fn parse_json() -> ScalarUDF {
    ScalarUDF::from(ParseJsonFunc::new())
}

// ============================================================================
// TO_JSON(variant)
// ============================================================================

/// TO_JSON function - Convert a VARIANT to a JSON string
///
/// Syntax: TO_JSON(variant_expression)
/// Returns the JSON representation of the input value.
#[derive(Debug)]
pub struct ToJsonFunc {
    signature: Signature,
}

impl Default for ToJsonFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ToJsonFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ToJsonFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "to_json"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(
                "TO_JSON requires exactly 1 argument".to_string(),
            ));
        }

        let input = &args[0];

        match input {
            ColumnarValue::Scalar(scalar) => {
                let json_str = scalar_to_json(scalar)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(json_str)))
            }
            ColumnarValue::Array(arr) => {
                let result = array_to_json_strings(arr)?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

/// Convert ScalarValue to JSON string
fn scalar_to_json(scalar: &ScalarValue) -> Result<Option<String>> {
    if scalar.is_null() {
        return Ok(None);
    }

    let json_value = match scalar {
        ScalarValue::Null => return Ok(None),
        ScalarValue::Boolean(Some(b)) => serde_json::Value::Bool(*b),
        ScalarValue::Int8(Some(n)) => serde_json::Value::Number((*n).into()),
        ScalarValue::Int16(Some(n)) => serde_json::Value::Number((*n).into()),
        ScalarValue::Int32(Some(n)) => serde_json::Value::Number((*n).into()),
        ScalarValue::Int64(Some(n)) => serde_json::Value::Number((*n).into()),
        ScalarValue::UInt8(Some(n)) => serde_json::Value::Number((*n).into()),
        ScalarValue::UInt16(Some(n)) => serde_json::Value::Number((*n).into()),
        ScalarValue::UInt32(Some(n)) => serde_json::Value::Number((*n).into()),
        ScalarValue::UInt64(Some(n)) => serde_json::Value::Number((*n).into()),
        ScalarValue::Float32(Some(n)) => serde_json::Number::from_f64(*n as f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        ScalarValue::Float64(Some(n)) => serde_json::Number::from_f64(*n)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            // If the string is already valid JSON, parse and re-serialize it
            match serde_json::from_str::<serde_json::Value>(s) {
                Ok(v) => v,
                Err(_) => serde_json::Value::String(s.clone()),
            }
        }
        _ => {
            // For other types, convert to string
            serde_json::Value::String(format!("{}", scalar))
        }
    };

    let json_str = serde_json::to_string(&json_value).map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!("JSON serialization error: {}", e))
    })?;

    Ok(Some(json_str))
}

/// Convert Arrow array to JSON strings
fn array_to_json_strings(array: &ArrayRef) -> Result<StringArray> {
    use arrow::array::*;

    let result: StringArray = match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            arr.iter()
                .map(|opt| opt.map(|b| if b { "true" } else { "false" }.to_string()))
                .collect()
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            arr.iter().map(|opt| opt.map(|n| n.to_string())).collect()
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            arr.iter().map(|opt| opt.map(|n| n.to_string())).collect()
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            arr.iter().map(|opt| opt.map(|n| n.to_string())).collect()
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            arr.iter().map(|opt| opt.map(|n| n.to_string())).collect()
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            arr.iter().map(|opt| opt.map(|n| n.to_string())).collect()
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            arr.iter().map(|opt| opt.map(|n| n.to_string())).collect()
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            arr.iter().map(|opt| opt.map(|n| n.to_string())).collect()
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            arr.iter().map(|opt| opt.map(|n| n.to_string())).collect()
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            arr.iter().map(|opt| opt.map(|n| n.to_string())).collect()
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            arr.iter().map(|opt| opt.map(|n| n.to_string())).collect()
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            arr.iter()
                .map(|opt| {
                    opt.map(|s| {
                        // Try to parse as JSON first
                        match serde_json::from_str::<serde_json::Value>(s) {
                            Ok(v) => serde_json::to_string(&v).unwrap_or_else(|_| {
                                serde_json::to_string(&serde_json::Value::String(s.to_string()))
                                    .unwrap()
                            }),
                            Err(_) => {
                                serde_json::to_string(&serde_json::Value::String(s.to_string()))
                                    .unwrap()
                            }
                        }
                    })
                })
                .collect()
        }
        _ => {
            // For other types, convert to string representation
            (0..array.len())
                .map(|i| {
                    if array.is_null(i) {
                        None
                    } else {
                        // Use debug format for unsupported types
                        Some(format!("\"<unsupported type {:?}>\"", array.data_type()))
                    }
                })
                .collect()
        }
    };

    Ok(result)
}

/// Create TO_JSON scalar UDF
pub fn to_json() -> ScalarUDF {
    ScalarUDF::from(ToJsonFunc::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_json_object() {
        let func = ParseJsonFunc::new();

        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            r#"{"name": "Alice", "age": 30}"#.to_string(),
        )));

        let result = func.invoke_batch(&[input], 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            // Parse the result to verify it's valid JSON
            let v: serde_json::Value = serde_json::from_str(&s).unwrap();
            assert_eq!(v["name"], "Alice");
            assert_eq!(v["age"], 30);
        } else {
            panic!("Expected scalar utf8");
        }
    }

    #[test]
    fn test_parse_json_array() {
        let func = ParseJsonFunc::new();

        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some(r#"[1, 2, 3]"#.to_string())));

        let result = func.invoke_batch(&[input], 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            let v: serde_json::Value = serde_json::from_str(&s).unwrap();
            assert!(v.is_array());
            assert_eq!(v.as_array().unwrap().len(), 3);
        } else {
            panic!("Expected scalar utf8");
        }
    }

    #[test]
    fn test_parse_json_invalid() {
        let func = ParseJsonFunc::new();

        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("not valid json".to_string())));

        let result = func.invoke_batch(&[input], 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_to_json_number() {
        let func = ToJsonFunc::new();

        let input = ColumnarValue::Scalar(ScalarValue::Int64(Some(42)));

        let result = func.invoke_batch(&[input], 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert_eq!(s, "42");
        } else {
            panic!("Expected scalar utf8");
        }
    }

    #[test]
    fn test_to_json_string() {
        let func = ToJsonFunc::new();

        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("hello".to_string())));

        let result = func.invoke_batch(&[input], 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert_eq!(s, "\"hello\"");
        } else {
            panic!("Expected scalar utf8");
        }
    }

    #[test]
    fn test_to_json_from_json_string() {
        let func = ToJsonFunc::new();

        // When input is already JSON, it should parse and re-serialize
        let input =
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(r#"{"key": "value"}"#.to_string())));

        let result = func.invoke_batch(&[input], 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            let v: serde_json::Value = serde_json::from_str(&s).unwrap();
            assert_eq!(v["key"], "value");
        } else {
            panic!("Expected scalar utf8");
        }
    }
}
