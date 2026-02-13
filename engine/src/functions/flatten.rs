//! FLATTEN table function
//!
//! Snowflake-compatible FLATTEN function that expands arrays/objects into rows.
//!
//! FLATTEN is a table function that produces a lateral view of a VARIANT, OBJECT, or ARRAY column.
//! This implementation provides a simplified scalar version that can be used with CROSS JOIN LATERAL.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StringArray, StringViewArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};

use super::helpers::safe_index;

// ============================================================================
// Helper to extract string iterator from StringArray or StringViewArray
// ============================================================================

/// Extract string values from an array that could be StringArray or StringViewArray.
/// Returns a Vec of Option<String> for easier processing.
fn extract_strings_from_array(arr: &Arc<dyn Array>) -> Option<Vec<Option<String>>> {
    arr.as_any()
        .downcast_ref::<StringArray>()
        .map(|str_arr| {
            str_arr
                .iter()
                .map(|opt| opt.map(|s| s.to_string()))
                .collect()
        })
        .or_else(|| {
            arr.as_any()
                .downcast_ref::<StringViewArray>()
                .map(|str_arr| {
                    str_arr
                        .iter()
                        .map(|opt| opt.map(|s| s.to_string()))
                        .collect()
                })
        })
}

// ============================================================================
// FLATTEN scalar helper functions
// ============================================================================

/// FLATTEN_ARRAY function - Get array element at index
///
/// This is a helper function that extracts an element from a JSON array.
/// Syntax: FLATTEN_ARRAY(json_array, index)
/// Returns the element at the specified index, or NULL if out of bounds.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct FlattenArrayFunc {
    signature: Signature,
}

impl Default for FlattenArrayFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl FlattenArrayFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for FlattenArrayFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "flatten_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "FLATTEN_ARRAY requires exactly 2 arguments".to_string(),
            ));
        }

        let array_json = &args.args[0];
        let index = &args.args[1];

        // Handle case when index is an array (from CROSS JOIN)
        match (array_json, index) {
            // Both scalar: simple case
            (
                ColumnarValue::Scalar(ScalarValue::Utf8(json_opt)),
                ColumnarValue::Scalar(ScalarValue::Int64(idx_opt)),
            ) => {
                let result = match (json_opt, idx_opt) {
                    (Some(json), Some(i)) => {
                        if let Some(idx) = safe_index(*i) {
                            extract_array_element(json, idx)
                        } else {
                            None
                        }
                    }
                    _ => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }

            // JSON is array, index is scalar
            (
                ColumnarValue::Array(json_arr),
                ColumnarValue::Scalar(ScalarValue::Int64(idx_opt)),
            ) => {
                let strings = extract_strings_from_array(json_arr).ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "FLATTEN_ARRAY first argument must be a JSON string".to_string(),
                    )
                })?;

                let idx = idx_opt.and_then(safe_index);
                let result: StringArray = if let Some(idx) = idx {
                    strings
                        .iter()
                        .map(|opt| opt.as_ref().and_then(|s| extract_array_element(s, idx)))
                        .collect()
                } else {
                    strings.iter().map(|_| None::<String>).collect()
                };

                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            // JSON is scalar, index is array (common in CROSS JOIN FLATTEN)
            (ColumnarValue::Scalar(ScalarValue::Utf8(json_opt)), ColumnarValue::Array(idx_arr)) => {
                let int_arr = idx_arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(
                            "FLATTEN_ARRAY second argument must be an integer array".to_string(),
                        )
                    })?;

                let result: StringArray = match json_opt {
                    Some(json) => int_arr
                        .iter()
                        .map(|idx_opt| {
                            idx_opt.and_then(|i| {
                                safe_index(i).and_then(|idx| extract_array_element(json, idx))
                            })
                        })
                        .collect(),
                    None => int_arr.iter().map(|_| None::<String>).collect(),
                };

                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            // Both arrays (full row-by-row processing)
            (ColumnarValue::Array(json_arr), ColumnarValue::Array(idx_arr)) => {
                let strings = extract_strings_from_array(json_arr).ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "FLATTEN_ARRAY first argument must be a JSON string".to_string(),
                    )
                })?;

                let int_arr = idx_arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(
                            "FLATTEN_ARRAY second argument must be an integer array".to_string(),
                        )
                    })?;

                let result: StringArray = strings
                    .iter()
                    .zip(int_arr.iter())
                    .map(|(json_opt, idx_opt)| match (json_opt, idx_opt) {
                        (Some(json), Some(i)) => {
                            safe_index(i).and_then(|idx| extract_array_element(json, idx))
                        }
                        _ => None,
                    })
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            // Handle other cases by converting to arrays
            _ => {
                let json_arr = array_json.to_array(args.number_rows)?;
                let idx_arr = index.to_array(args.number_rows)?;

                let strings = extract_strings_from_array(&json_arr).ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "FLATTEN_ARRAY first argument must be a JSON string".to_string(),
                    )
                })?;

                let int_arr = idx_arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(
                            "FLATTEN_ARRAY second argument must be an integer".to_string(),
                        )
                    })?;

                let result: StringArray = strings
                    .iter()
                    .zip(int_arr.iter())
                    .map(|(json_opt, idx_opt)| match (json_opt, idx_opt) {
                        (Some(json), Some(i)) => {
                            safe_index(i).and_then(|idx| extract_array_element(json, idx))
                        }
                        _ => None,
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

fn extract_array_element(json_str: &str, index: usize) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(json_str).ok()?;
    let array = value.as_array()?;
    let element = array.get(index)?;
    Some(serde_json::to_string(element).unwrap_or_else(|_| "null".to_string()))
}

/// Create FLATTEN_ARRAY scalar UDF
pub fn flatten_array() -> ScalarUDF {
    ScalarUDF::from(FlattenArrayFunc::new())
}

// ============================================================================
// ARRAY_SIZE function
// ============================================================================

/// ARRAY_SIZE function - Get the size of a JSON array
///
/// Syntax: ARRAY_SIZE(json_array)
/// Returns the number of elements in the array.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArraySizeFunc {
    signature: Signature,
}

impl Default for ArraySizeFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ArraySizeFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArraySizeFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_size"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(
                "ARRAY_SIZE requires exactly 1 argument".to_string(),
            ));
        }

        let array_json = &args.args[0];

        match array_json {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                let size = get_array_size(s);
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(size)))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(None)))
            }
            ColumnarValue::Array(arr) => {
                let result: Int64Array =
                    if let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() {
                        str_arr
                            .iter()
                            .map(|opt| opt.and_then(get_array_size))
                            .collect()
                    } else if let Some(str_arr) = arr.as_any().downcast_ref::<StringViewArray>() {
                        str_arr
                            .iter()
                            .map(|opt| opt.and_then(get_array_size))
                            .collect()
                    } else {
                        return Err(datafusion::error::DataFusionError::Execution(
                            "ARRAY_SIZE argument must be a JSON string".to_string(),
                        ));
                    };

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            _ => {
                let arr = array_json.to_array(args.number_rows)?;
                let result: Int64Array =
                    if let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() {
                        str_arr
                            .iter()
                            .map(|opt| opt.and_then(get_array_size))
                            .collect()
                    } else if let Some(str_arr) = arr.as_any().downcast_ref::<StringViewArray>() {
                        str_arr
                            .iter()
                            .map(|opt| opt.and_then(get_array_size))
                            .collect()
                    } else {
                        return Err(datafusion::error::DataFusionError::Execution(
                            "ARRAY_SIZE argument must be a JSON string".to_string(),
                        ));
                    };

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

fn get_array_size(json_str: &str) -> Option<i64> {
    let value: serde_json::Value = serde_json::from_str(json_str).ok()?;
    let array = value.as_array()?;
    Some(array.len() as i64)
}

/// Create ARRAY_SIZE scalar UDF
pub fn array_size() -> ScalarUDF {
    ScalarUDF::from(ArraySizeFunc::new())
}

// ============================================================================
// GET_PATH function - JSON path access
// ============================================================================

/// GET_PATH function - Extract value from JSON using path
///
/// Syntax: GET_PATH(variant_expr, path)
/// This implements Snowflake's : operator functionality.
/// Example: GET_PATH('{"a": {"b": 1}}', 'a.b') returns '1'
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct GetPathFunc {
    signature: Signature,
}

impl Default for GetPathFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl GetPathFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for GetPathFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "get_path"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "GET_PATH requires exactly 2 arguments".to_string(),
            ));
        }

        let json_expr = &args.args[0];
        let path = &args.args[1];

        // Get path string
        let path_str = match path {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.clone(),
            _ => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "GET_PATH second argument must be a string path".to_string(),
                ))
            }
        };

        match json_expr {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                let result = get_json_path(s, &path_str);
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
            }
            ColumnarValue::Array(arr) => {
                let result: StringArray =
                    if let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() {
                        str_arr
                            .iter()
                            .map(|opt| opt.and_then(|s| get_json_path(s, &path_str)))
                            .collect()
                    } else if let Some(str_arr) = arr.as_any().downcast_ref::<StringViewArray>() {
                        str_arr
                            .iter()
                            .map(|opt| opt.and_then(|s| get_json_path(s, &path_str)))
                            .collect()
                    } else {
                        return Err(datafusion::error::DataFusionError::Execution(
                            "GET_PATH first argument must be a JSON string".to_string(),
                        ));
                    };

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            _ => {
                let arr = json_expr.to_array(args.number_rows)?;
                let result: StringArray =
                    if let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() {
                        str_arr
                            .iter()
                            .map(|opt| opt.and_then(|s| get_json_path(s, &path_str)))
                            .collect()
                    } else if let Some(str_arr) = arr.as_any().downcast_ref::<StringViewArray>() {
                        str_arr
                            .iter()
                            .map(|opt| opt.and_then(|s| get_json_path(s, &path_str)))
                            .collect()
                    } else {
                        return Err(datafusion::error::DataFusionError::Execution(
                            "GET_PATH first argument must be a JSON string".to_string(),
                        ));
                    };

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

fn get_json_path(json_str: &str, path: &str) -> Option<String> {
    let mut value: serde_json::Value = serde_json::from_str(json_str).ok()?;

    for part in path.split('.') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        // Check if it's an array index
        if let Ok(index) = part.parse::<usize>() {
            value = value.get(index)?.clone();
        } else {
            value = value.get(part)?.clone();
        }
    }

    // Return the value as JSON string
    match &value {
        serde_json::Value::String(s) => Some(s.clone()),
        _ => Some(serde_json::to_string(&value).unwrap_or_else(|_| "null".to_string())),
    }
}

/// Create GET_PATH scalar UDF
pub fn get_path() -> ScalarUDF {
    ScalarUDF::from(GetPathFunc::new())
}

// ============================================================================
// GET function - Array/Object element access
// ============================================================================

/// GET function - Extract element from JSON array or object by index or key
///
/// Syntax: GET(variant_expr, index_or_key)
///
/// For arrays: GET('[1, 2, 3]', 0) returns '1'
/// For objects: GET('{"a": 1}', 'a') returns '1'
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct GetFunc {
    signature: Signature,
}

impl Default for GetFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl GetFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for GetFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "get"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "GET requires exactly 2 arguments".to_string(),
            ));
        }

        match (&args.args[0], &args.args[1]) {
            (ColumnarValue::Scalar(json), ColumnarValue::Scalar(key)) => {
                let json_str = match json {
                    ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => s.clone(),
                    ScalarValue::Null => {
                        return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                    }
                    _ => {
                        return Err(datafusion::error::DataFusionError::Execution(
                            "GET first argument must be a JSON string".to_string(),
                        ));
                    }
                };

                // The key can be an integer (array index) or string (object key)
                let result = match key {
                    ScalarValue::Int64(Some(idx)) => get_by_index(&json_str, *idx as usize),
                    ScalarValue::Int32(Some(idx)) => get_by_index(&json_str, *idx as usize),
                    ScalarValue::UInt64(Some(idx)) => get_by_index(&json_str, *idx as usize),
                    ScalarValue::Utf8(Some(k)) | ScalarValue::LargeUtf8(Some(k)) => {
                        // If key is numeric string, treat as index
                        if let Ok(idx) = k.parse::<usize>() {
                            get_by_index(&json_str, idx)
                        } else {
                            get_by_key(&json_str, k)
                        }
                    }
                    ScalarValue::Null => None,
                    _ => {
                        return Err(datafusion::error::DataFusionError::Execution(
                            "GET second argument must be an integer or string".to_string(),
                        ));
                    }
                };

                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
            _ => Err(datafusion::error::DataFusionError::Execution(
                "GET requires scalar arguments".to_string(),
            )),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

fn get_by_index(json_str: &str, index: usize) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(json_str).ok()?;
    let element = value.get(index)?;
    match element {
        serde_json::Value::String(s) => Some(s.clone()),
        _ => Some(serde_json::to_string(element).unwrap_or_else(|_| "null".to_string())),
    }
}

fn get_by_key(json_str: &str, key: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(json_str).ok()?;
    let element = value.get(key)?;
    match element {
        serde_json::Value::String(s) => Some(s.clone()),
        _ => Some(serde_json::to_string(element).unwrap_or_else(|_| "null".to_string())),
    }
}

/// Create GET scalar UDF
pub fn get() -> ScalarUDF {
    ScalarUDF::from(GetFunc::new())
}

// ============================================================================
// OBJECT_KEYS function
// ============================================================================

/// OBJECT_KEYS function - Get all keys from a JSON object
///
/// Syntax: OBJECT_KEYS(object_expr)
/// Returns a JSON array of the keys in the object.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ObjectKeysFunc {
    signature: Signature,
}

impl Default for ObjectKeysFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectKeysFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ObjectKeysFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "object_keys"
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
                "OBJECT_KEYS requires exactly 1 argument".to_string(),
            ));
        }

        let json_expr = &args.args[0];

        match json_expr {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                let result = get_object_keys(s);
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
            }
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "OBJECT_KEYS argument must be a JSON string".to_string(),
                    )
                })?;

                let result: StringArray = str_arr
                    .iter()
                    .map(|opt| opt.and_then(get_object_keys))
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            _ => {
                let arr = json_expr.to_array(args.number_rows)?;
                let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "OBJECT_KEYS argument must be a JSON string".to_string(),
                    )
                })?;

                let result: StringArray = str_arr
                    .iter()
                    .map(|opt| opt.and_then(get_object_keys))
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

fn get_object_keys(json_str: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(json_str).ok()?;
    let obj = value.as_object()?;
    let keys: Vec<&str> = obj.keys().map(|k| k.as_str()).collect();
    serde_json::to_string(&keys).ok()
}

/// Create OBJECT_KEYS scalar UDF
pub fn object_keys() -> ScalarUDF {
    ScalarUDF::from(ObjectKeysFunc::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::functions::test_utils::{invoke_udf_int64, invoke_udf_string};

    #[test]
    fn test_flatten_array() {
        let func = FlattenArrayFunc::new();

        let arr = ColumnarValue::Scalar(ScalarValue::Utf8(Some(r#"[1, 2, 3]"#.to_string())));
        let idx = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));

        let result = invoke_udf_string(&func, &[arr, idx]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert_eq!(s, "2");
        } else {
            panic!("Expected scalar utf8");
        }
    }

    #[test]
    fn test_flatten_array_object() {
        let func = FlattenArrayFunc::new();

        let arr = ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            r#"[{"name": "Alice"}, {"name": "Bob"}]"#.to_string(),
        )));
        let idx = ColumnarValue::Scalar(ScalarValue::Int64(Some(0)));

        let result = invoke_udf_string(&func, &[arr, idx]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            let v: serde_json::Value = serde_json::from_str(&s).unwrap();
            assert_eq!(v["name"], "Alice");
        } else {
            panic!("Expected scalar utf8");
        }
    }

    #[test]
    fn test_array_size() {
        let func = ArraySizeFunc::new();

        let arr = ColumnarValue::Scalar(ScalarValue::Utf8(Some(r#"[1, 2, 3, 4, 5]"#.to_string())));

        let result = invoke_udf_int64(&func, &[arr]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) = result {
            assert_eq!(v, 5);
        } else {
            panic!("Expected scalar int64");
        }
    }

    #[test]
    fn test_array_size_empty() {
        let func = ArraySizeFunc::new();

        let arr = ColumnarValue::Scalar(ScalarValue::Utf8(Some(r#"[]"#.to_string())));

        let result = invoke_udf_int64(&func, &[arr]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) = result {
            assert_eq!(v, 0);
        } else {
            panic!("Expected scalar int64");
        }
    }

    #[test]
    fn test_get_path_simple() {
        let func = GetPathFunc::new();

        let json = ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            r#"{"name": "Alice", "age": 30}"#.to_string(),
        )));
        let path = ColumnarValue::Scalar(ScalarValue::Utf8(Some("name".to_string())));

        let result = invoke_udf_string(&func, &[json, path]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert_eq!(s, "Alice");
        } else {
            panic!("Expected scalar utf8");
        }
    }

    #[test]
    fn test_get_path_nested() {
        let func = GetPathFunc::new();

        let json = ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            r#"{"user": {"name": "Alice", "address": {"city": "Tokyo"}}}"#.to_string(),
        )));
        let path = ColumnarValue::Scalar(ScalarValue::Utf8(Some("user.address.city".to_string())));

        let result = invoke_udf_string(&func, &[json, path]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert_eq!(s, "Tokyo");
        } else {
            panic!("Expected scalar utf8");
        }
    }

    #[test]
    fn test_get_path_array_index() {
        let func = GetPathFunc::new();

        let json = ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            r#"{"items": ["a", "b", "c"]}"#.to_string(),
        )));
        let path = ColumnarValue::Scalar(ScalarValue::Utf8(Some("items.1".to_string())));

        let result = invoke_udf_string(&func, &[json, path]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert_eq!(s, "b");
        } else {
            panic!("Expected scalar utf8");
        }
    }

    #[test]
    fn test_object_keys() {
        let func = ObjectKeysFunc::new();

        let json = ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            r#"{"name": "Alice", "age": 30, "city": "Tokyo"}"#.to_string(),
        )));

        let result = invoke_udf_string(&func, &[json]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            let keys: Vec<String> = serde_json::from_str(&s).unwrap();
            assert!(keys.contains(&"name".to_string()));
            assert!(keys.contains(&"age".to_string()));
            assert!(keys.contains(&"city".to_string()));
            assert_eq!(keys.len(), 3);
        } else {
            panic!("Expected scalar utf8");
        }
    }

    #[test]
    fn test_get_array_index() {
        let func = GetFunc::new();

        let arr = ColumnarValue::Scalar(ScalarValue::Utf8(Some(r#"[10, 20, 30]"#.to_string())));
        let idx = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));

        let result = invoke_udf_string(&func, &[arr, idx]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert_eq!(s, "20");
        } else {
            panic!("Expected scalar utf8");
        }
    }

    #[test]
    fn test_get_object_key() {
        let func = GetFunc::new();

        let obj = ColumnarValue::Scalar(ScalarValue::Utf8(Some(r#"{"a": 1, "b": 2}"#.to_string())));
        let key = ColumnarValue::Scalar(ScalarValue::Utf8(Some("b".to_string())));

        let result = invoke_udf_string(&func, &[obj, key]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert_eq!(s, "2");
        } else {
            panic!("Expected scalar utf8");
        }
    }

    #[test]
    fn test_get_array_string_index() {
        let func = GetFunc::new();

        let arr = ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            r#"["first", "second", "third"]"#.to_string(),
        )));
        let idx = ColumnarValue::Scalar(ScalarValue::Utf8(Some("0".to_string())));

        let result = invoke_udf_string(&func, &[arr, idx]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert_eq!(s, "first");
        } else {
            panic!("Expected scalar utf8");
        }
    }
}
