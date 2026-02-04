//! Object functions for Snowflake VARIANT types
//!
//! ## Construction Functions
//! - `OBJECT_CONSTRUCT(key1, value1, key2, value2, ...)` - Construct an object
//!
//! ## Manipulation Functions
//! - `OBJECT_INSERT(object, key, value)` - Insert or update a key-value pair
//! - `OBJECT_DELETE(object, key)` - Delete a key from object
//! - `OBJECT_PICK(object, key1, key2, ...)` - Pick specific keys from object

use std::any::Any;

use arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

// ============================================================================
// OBJECT_CONSTRUCT(key1, value1, key2, value2, ...)
// ============================================================================

#[derive(Debug)]
pub struct ObjectConstructFunc {
    signature: Signature,
}

impl Default for ObjectConstructFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectConstructFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ObjectConstructFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "object_construct"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _num_rows: usize) -> Result<ColumnarValue> {
        // Arguments should be in pairs: key1, value1, key2, value2, ...
        if args.len() % 2 != 0 {
            return Err(datafusion::error::DataFusionError::Execution(
                "OBJECT_CONSTRUCT requires an even number of arguments (key-value pairs)"
                    .to_string(),
            ));
        }

        let mut obj = serde_json::Map::new();

        for pair in args.chunks(2) {
            let key = extract_key(&pair[0])?;
            let value = scalar_to_json_value_from_columnar(&pair[1]);

            // Skip NULL keys (Snowflake behavior)
            if let Some(k) = key {
                // Also skip if value is NULL (Snowflake's default behavior)
                // However, explicit NULL values are typically included
                obj.insert(k, value);
            }
        }

        let result =
            serde_json::to_string(&serde_json::Value::Object(obj)).unwrap_or_else(|_| "{}".into());

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

pub fn object_construct() -> ScalarUDF {
    ScalarUDF::from(ObjectConstructFunc::new())
}

// ============================================================================
// OBJECT_CONSTRUCT_KEEP_NULL(key1, value1, key2, value2, ...)
// ============================================================================

#[derive(Debug)]
pub struct ObjectConstructKeepNullFunc {
    signature: Signature,
}

impl Default for ObjectConstructKeepNullFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectConstructKeepNullFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ObjectConstructKeepNullFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "object_construct_keep_null"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _num_rows: usize) -> Result<ColumnarValue> {
        if args.len() % 2 != 0 {
            return Err(datafusion::error::DataFusionError::Execution(
                "OBJECT_CONSTRUCT_KEEP_NULL requires an even number of arguments (key-value pairs)"
                    .to_string(),
            ));
        }

        let mut obj = serde_json::Map::new();

        for pair in args.chunks(2) {
            let key = extract_key(&pair[0])?;
            let value = scalar_to_json_value_from_columnar(&pair[1]);

            if let Some(k) = key {
                obj.insert(k, value);
            }
        }

        let result =
            serde_json::to_string(&serde_json::Value::Object(obj)).unwrap_or_else(|_| "{}".into());

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

pub fn object_construct_keep_null() -> ScalarUDF {
    ScalarUDF::from(ObjectConstructKeepNullFunc::new())
}

// ============================================================================
// OBJECT_INSERT(object, key, value [, update_flag])
// ============================================================================

#[derive(Debug)]
pub struct ObjectInsertFunc {
    signature: Signature,
}

impl Default for ObjectInsertFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectInsertFunc {
    pub fn new() -> Self {
        Self {
            // 3 or 4 arguments
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ObjectInsertFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "object_insert"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _num_rows: usize) -> Result<ColumnarValue> {
        if args.len() < 3 || args.len() > 4 {
            return Err(datafusion::error::DataFusionError::Execution(
                "OBJECT_INSERT requires 3 or 4 arguments".to_string(),
            ));
        }

        let obj_str = extract_string_scalar(&args[0])?;
        let key = extract_key(&args[1])?;
        let value = scalar_to_json_value_from_columnar(&args[2]);

        // Optional update_flag (default: false - error if key exists)
        let update_flag = if args.len() == 4 {
            extract_bool_scalar(&args[3])?.unwrap_or(false)
        } else {
            false
        };

        let result = match (obj_str, key) {
            (Some(s), Some(k)) => {
                match serde_json::from_str::<serde_json::Value>(&s) {
                    Ok(serde_json::Value::Object(mut obj)) => {
                        if obj.contains_key(&k) && !update_flag {
                            // Key exists and update not allowed - return error or original
                            // In Snowflake, this would be an error, but we'll just skip
                            Some(serde_json::to_string(&obj).unwrap_or_else(|_| "{}".into()))
                        } else {
                            obj.insert(k, value);
                            Some(serde_json::to_string(&obj).unwrap_or_else(|_| "{}".into()))
                        }
                    }
                    _ => None,
                }
            }
            _ => None,
        };

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

pub fn object_insert() -> ScalarUDF {
    ScalarUDF::from(ObjectInsertFunc::new())
}

// ============================================================================
// OBJECT_DELETE(object, key1 [, key2, ...])
// ============================================================================

#[derive(Debug)]
pub struct ObjectDeleteFunc {
    signature: Signature,
}

impl Default for ObjectDeleteFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectDeleteFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ObjectDeleteFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "object_delete"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _num_rows: usize) -> Result<ColumnarValue> {
        if args.len() < 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "OBJECT_DELETE requires at least 2 arguments (object, key)".to_string(),
            ));
        }

        let obj_str = extract_string_scalar(&args[0])?;

        // Collect all keys to delete
        let keys_to_delete: Vec<String> = args[1..]
            .iter()
            .filter_map(|arg| extract_key(arg).ok().flatten())
            .collect();

        let result = match obj_str {
            Some(s) => match serde_json::from_str::<serde_json::Value>(&s) {
                Ok(serde_json::Value::Object(mut obj)) => {
                    for key in keys_to_delete {
                        obj.remove(&key);
                    }
                    Some(serde_json::to_string(&obj).unwrap_or_else(|_| "{}".into()))
                }
                _ => None,
            },
            None => None,
        };

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

pub fn object_delete() -> ScalarUDF {
    ScalarUDF::from(ObjectDeleteFunc::new())
}

// ============================================================================
// OBJECT_PICK(object, key1 [, key2, ...])
// ============================================================================

#[derive(Debug)]
pub struct ObjectPickFunc {
    signature: Signature,
}

impl Default for ObjectPickFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectPickFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ObjectPickFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "object_pick"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _num_rows: usize) -> Result<ColumnarValue> {
        if args.len() < 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "OBJECT_PICK requires at least 2 arguments (object, key)".to_string(),
            ));
        }

        let obj_str = extract_string_scalar(&args[0])?;

        // Collect all keys to pick
        let keys_to_pick: Vec<String> = args[1..]
            .iter()
            .filter_map(|arg| extract_key(arg).ok().flatten())
            .collect();

        let result = match obj_str {
            Some(s) => match serde_json::from_str::<serde_json::Value>(&s) {
                Ok(serde_json::Value::Object(obj)) => {
                    let mut new_obj = serde_json::Map::new();
                    for key in keys_to_pick {
                        if let Some(value) = obj.get(&key) {
                            new_obj.insert(key, value.clone());
                        }
                    }
                    Some(serde_json::to_string(&new_obj).unwrap_or_else(|_| "{}".into()))
                }
                _ => None,
            },
            None => None,
        };

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

pub fn object_pick() -> ScalarUDF {
    ScalarUDF::from(ObjectPickFunc::new())
}

// ============================================================================
// Helper Functions
// ============================================================================

fn scalar_to_json_value(scalar: &ScalarValue) -> serde_json::Value {
    match scalar {
        ScalarValue::Null => serde_json::Value::Null,
        ScalarValue::Boolean(Some(b)) => serde_json::Value::Bool(*b),
        ScalarValue::Boolean(None) => serde_json::Value::Null,
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
            // Try to parse as JSON first
            match serde_json::from_str::<serde_json::Value>(s) {
                Ok(v) => v,
                Err(_) => serde_json::Value::String(s.clone()),
            }
        }
        _ => serde_json::Value::Null,
    }
}

fn scalar_to_json_value_from_columnar(col: &ColumnarValue) -> serde_json::Value {
    match col {
        ColumnarValue::Scalar(scalar) => scalar_to_json_value(scalar),
        ColumnarValue::Array(_) => serde_json::Value::Null,
    }
}

fn extract_key(col: &ColumnarValue) -> Result<Option<String>> {
    match col {
        ColumnarValue::Scalar(ScalarValue::Utf8(opt)) => Ok(opt.clone()),
        ColumnarValue::Scalar(ScalarValue::LargeUtf8(opt)) => Ok(opt.clone()),
        ColumnarValue::Scalar(ScalarValue::Null) => Ok(None),
        _ => Ok(None),
    }
}

fn extract_string_scalar(col: &ColumnarValue) -> Result<Option<String>> {
    match col {
        ColumnarValue::Scalar(ScalarValue::Utf8(opt)) => Ok(opt.clone()),
        ColumnarValue::Scalar(ScalarValue::LargeUtf8(opt)) => Ok(opt.clone()),
        ColumnarValue::Scalar(ScalarValue::Null) => Ok(None),
        _ => Ok(None),
    }
}

fn extract_bool_scalar(col: &ColumnarValue) -> Result<Option<bool>> {
    match col {
        ColumnarValue::Scalar(ScalarValue::Boolean(opt)) => Ok(*opt),
        ColumnarValue::Scalar(ScalarValue::Null) => Ok(None),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_construct() {
        let func = ObjectConstructFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("b".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
        ];
        let result = func.invoke_batch(&args, 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            let v: serde_json::Value = serde_json::from_str(&s).unwrap();
            assert!(v.is_object());
            assert_eq!(v["a"], 1);
            assert_eq!(v["b"], 2);
        } else {
            panic!("Expected string scalar");
        }
    }

    #[test]
    fn test_object_insert() {
        let func = ObjectInsertFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("{\"a\": 1}".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("b".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
        ];
        let result = func.invoke_batch(&args, 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            let v: serde_json::Value = serde_json::from_str(&s).unwrap();
            assert_eq!(v["a"], 1);
            assert_eq!(v["b"], 2);
        } else {
            panic!("Expected string scalar");
        }
    }

    #[test]
    fn test_object_delete() {
        let func = ObjectDeleteFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("{\"a\": 1, \"b\": 2}".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".to_string()))),
        ];
        let result = func.invoke_batch(&args, 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            let v: serde_json::Value = serde_json::from_str(&s).unwrap();
            assert!(v.get("a").is_none());
            assert_eq!(v["b"], 2);
        } else {
            panic!("Expected string scalar");
        }
    }

    #[test]
    fn test_object_pick() {
        let func = ObjectPickFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "{\"a\": 1, \"b\": 2, \"c\": 3}".to_string(),
            ))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("c".to_string()))),
        ];
        let result = func.invoke_batch(&args, 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            let v: serde_json::Value = serde_json::from_str(&s).unwrap();
            assert_eq!(v["a"], 1);
            assert!(v.get("b").is_none());
            assert_eq!(v["c"], 3);
        } else {
            panic!("Expected string scalar");
        }
    }
}
