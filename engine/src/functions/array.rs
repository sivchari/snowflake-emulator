//! Array functions for Snowflake VARIANT types
//!
//! ## Construction Functions
//! - `ARRAY_CONSTRUCT(...)` - Construct an array from arguments
//! - `ARRAY_CONSTRUCT_COMPACT(...)` - Construct an array, excluding NULLs
//!
//! ## Manipulation Functions
//! - `ARRAY_APPEND(array, element)` - Append element to array
//! - `ARRAY_PREPEND(array, element)` - Prepend element to array
//! - `ARRAY_CAT(array1, array2)` - Concatenate two arrays
//! - `ARRAY_SLICE(array, from, to)` - Get a slice of the array
//! - `ARRAY_CONTAINS(element, array)` - Check if array contains element
//! - `ARRAY_POSITION(element, array)` - Get position of element in array
//! - `ARRAY_DISTINCT(array)` - Remove duplicates from array
//! - `ARRAY_FLATTEN(array)` - Flatten nested arrays

use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};

// ============================================================================
// ARRAY_CONSTRUCT(...)
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayConstructFunc {
    signature: Signature,
}

impl Default for ArrayConstructFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayConstructFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArrayConstructFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_construct"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut elements = Vec::with_capacity(args.args.len());

        for arg in &args.args {
            match arg {
                ColumnarValue::Scalar(scalar) => {
                    let json_value = scalar_to_json_value(scalar);
                    elements.push(json_value);
                }
                ColumnarValue::Array(_) => {
                    // For array inputs, we'd need to handle row-wise operations
                    // For now, treat as NULL
                    elements.push(serde_json::Value::Null);
                }
            }
        }

        let result = serde_json::to_string(&serde_json::Value::Array(elements))
            .unwrap_or_else(|_| "[]".to_string());

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

pub fn array_construct() -> ScalarUDF {
    ScalarUDF::from(ArrayConstructFunc::new())
}

// ============================================================================
// ARRAY_CONSTRUCT_COMPACT(...)
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayConstructCompactFunc {
    signature: Signature,
}

impl Default for ArrayConstructCompactFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayConstructCompactFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArrayConstructCompactFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_construct_compact"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut elements = Vec::with_capacity(args.args.len());

        for arg in &args.args {
            match arg {
                ColumnarValue::Scalar(scalar) => {
                    if !scalar.is_null() {
                        let json_value = scalar_to_json_value(scalar);
                        if !json_value.is_null() {
                            elements.push(json_value);
                        }
                    }
                }
                ColumnarValue::Array(_) => {
                    // Skip arrays for now
                }
            }
        }

        let result = serde_json::to_string(&serde_json::Value::Array(elements))
            .unwrap_or_else(|_| "[]".to_string());

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

pub fn array_construct_compact() -> ScalarUDF {
    ScalarUDF::from(ArrayConstructCompactFunc::new())
}

// ============================================================================
// ARRAY_APPEND(array, element)
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayAppendFunc {
    signature: Signature,
}

impl Default for ArrayAppendFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayAppendFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArrayAppendFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_append"
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
                "ARRAY_APPEND requires exactly 2 arguments".to_string(),
            ));
        }

        let array_str = extract_string_scalar(&args.args[0])?;
        let element = scalar_to_json_value_from_columnar(&args.args[1]);

        let result = match array_str {
            Some(s) => {
                match serde_json::from_str::<serde_json::Value>(&s) {
                    Ok(serde_json::Value::Array(mut arr)) => {
                        arr.push(element);
                        Some(serde_json::to_string(&arr).unwrap_or_else(|_| "[]".to_string()))
                    }
                    _ => None, // Not an array
                }
            }
            None => None,
        };

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

pub fn array_append() -> ScalarUDF {
    ScalarUDF::from(ArrayAppendFunc::new())
}

// ============================================================================
// ARRAY_PREPEND(array, element)
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayPrependFunc {
    signature: Signature,
}

impl Default for ArrayPrependFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayPrependFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArrayPrependFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_prepend"
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
                "ARRAY_PREPEND requires exactly 2 arguments".to_string(),
            ));
        }

        let array_str = extract_string_scalar(&args.args[0])?;
        let element = scalar_to_json_value_from_columnar(&args.args[1]);

        let result = match array_str {
            Some(s) => {
                match serde_json::from_str::<serde_json::Value>(&s) {
                    Ok(serde_json::Value::Array(mut arr)) => {
                        arr.insert(0, element);
                        Some(serde_json::to_string(&arr).unwrap_or_else(|_| "[]".to_string()))
                    }
                    _ => None, // Not an array
                }
            }
            None => None,
        };

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

pub fn array_prepend() -> ScalarUDF {
    ScalarUDF::from(ArrayPrependFunc::new())
}

// ============================================================================
// ARRAY_CAT(array1, array2)
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayCatFunc {
    signature: Signature,
}

impl Default for ArrayCatFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayCatFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArrayCatFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_cat"
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
                "ARRAY_CAT requires exactly 2 arguments".to_string(),
            ));
        }

        let arr1_str = extract_string_scalar(&args.args[0])?;
        let arr2_str = extract_string_scalar(&args.args[1])?;

        let result = match (arr1_str, arr2_str) {
            (Some(s1), Some(s2)) => {
                let arr1 = serde_json::from_str::<serde_json::Value>(&s1);
                let arr2 = serde_json::from_str::<serde_json::Value>(&s2);

                match (arr1, arr2) {
                    (Ok(serde_json::Value::Array(mut a1)), Ok(serde_json::Value::Array(a2))) => {
                        a1.extend(a2);
                        Some(serde_json::to_string(&a1).unwrap_or_else(|_| "[]".to_string()))
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

pub fn array_cat() -> ScalarUDF {
    ScalarUDF::from(ArrayCatFunc::new())
}

// ============================================================================
// ARRAY_SLICE(array, from, to)
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArraySliceFunc {
    signature: Signature,
}

impl Default for ArraySliceFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ArraySliceFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(3), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArraySliceFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_slice"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 3 {
            return Err(datafusion::error::DataFusionError::Execution(
                "ARRAY_SLICE requires exactly 3 arguments".to_string(),
            ));
        }

        let array_str = extract_string_scalar(&args.args[0])?;
        let from_idx = extract_int_scalar(&args.args[1])?;
        let to_idx = extract_int_scalar(&args.args[2])?;

        let result = match (array_str, from_idx, to_idx) {
            (Some(s), Some(from), Some(to)) => {
                match serde_json::from_str::<serde_json::Value>(&s) {
                    Ok(serde_json::Value::Array(arr)) => {
                        let len = arr.len() as i64;
                        let start = from.max(0) as usize;
                        let end = to.min(len).max(0) as usize;

                        if start >= arr.len() || start >= end {
                            Some("[]".to_string())
                        } else {
                            let slice: Vec<_> = arr[start..end.min(arr.len())].to_vec();
                            Some(serde_json::to_string(&slice).unwrap_or_else(|_| "[]".to_string()))
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

pub fn array_slice() -> ScalarUDF {
    ScalarUDF::from(ArraySliceFunc::new())
}

// ============================================================================
// ARRAY_CONTAINS(element, array)
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayContainsFunc {
    signature: Signature,
}

impl Default for ArrayContainsFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayContainsFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArrayContainsFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_contains"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "ARRAY_CONTAINS requires exactly 2 arguments".to_string(),
            ));
        }

        let element = scalar_to_json_value_from_columnar(&args.args[0]);
        let array_str = extract_string_scalar(&args.args[1])?;

        let result = match array_str {
            Some(s) => match serde_json::from_str::<serde_json::Value>(&s) {
                Ok(serde_json::Value::Array(arr)) => Some(arr.contains(&element)),
                _ => None,
            },
            None => None,
        };

        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

pub fn array_contains() -> ScalarUDF {
    ScalarUDF::from(ArrayContainsFunc::new())
}

// ============================================================================
// ARRAY_POSITION(element, array)
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayPositionFunc {
    signature: Signature,
}

impl Default for ArrayPositionFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayPositionFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArrayPositionFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_position"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "ARRAY_POSITION requires exactly 2 arguments".to_string(),
            ));
        }

        let element = scalar_to_json_value_from_columnar(&args.args[0]);
        let array_str = extract_string_scalar(&args.args[1])?;

        let result = match array_str {
            Some(s) => match serde_json::from_str::<serde_json::Value>(&s) {
                Ok(serde_json::Value::Array(arr)) => {
                    arr.iter().position(|e| e == &element).map(|p| p as i64)
                }
                _ => None,
            },
            None => None,
        };

        Ok(ColumnarValue::Scalar(ScalarValue::Int64(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

pub fn array_position() -> ScalarUDF {
    ScalarUDF::from(ArrayPositionFunc::new())
}

// ============================================================================
// ARRAY_DISTINCT(array)
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayDistinctFunc {
    signature: Signature,
}

impl Default for ArrayDistinctFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayDistinctFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArrayDistinctFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_distinct"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "ARRAY_DISTINCT requires exactly 1 argument".to_string(),
            ));
        }

        let array_str = extract_string_scalar(&args.args[0])?;

        let result = match array_str {
            Some(s) => match serde_json::from_str::<serde_json::Value>(&s) {
                Ok(serde_json::Value::Array(arr)) => {
                    let mut unique: Vec<serde_json::Value> = Vec::new();
                    for item in arr {
                        if !unique.contains(&item) {
                            unique.push(item);
                        }
                    }
                    Some(serde_json::to_string(&unique).unwrap_or_else(|_| "[]".to_string()))
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

pub fn array_distinct() -> ScalarUDF {
    ScalarUDF::from(ArrayDistinctFunc::new())
}

// ============================================================================
// ARRAY_FLATTEN(array)
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayFlattenFunc {
    signature: Signature,
}

impl Default for ArrayFlattenFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayFlattenFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArrayFlattenFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_flatten"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "ARRAY_FLATTEN requires exactly 1 argument".to_string(),
            ));
        }

        let array_str = extract_string_scalar(&args.args[0])?;

        let result = match array_str {
            Some(s) => match serde_json::from_str::<serde_json::Value>(&s) {
                Ok(serde_json::Value::Array(arr)) => {
                    let mut flat: Vec<serde_json::Value> = Vec::new();
                    for item in arr {
                        if let serde_json::Value::Array(inner) = item {
                            flat.extend(inner);
                        } else {
                            flat.push(item);
                        }
                    }
                    Some(serde_json::to_string(&flat).unwrap_or_else(|_| "[]".to_string()))
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

pub fn array_flatten() -> ScalarUDF {
    ScalarUDF::from(ArrayFlattenFunc::new())
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

fn extract_string_scalar(col: &ColumnarValue) -> Result<Option<String>> {
    match col {
        ColumnarValue::Scalar(ScalarValue::Utf8(opt)) => Ok(opt.clone()),
        ColumnarValue::Scalar(ScalarValue::LargeUtf8(opt)) => Ok(opt.clone()),
        ColumnarValue::Scalar(ScalarValue::Null) => Ok(None),
        _ => Ok(None),
    }
}

fn extract_int_scalar(col: &ColumnarValue) -> Result<Option<i64>> {
    match col {
        ColumnarValue::Scalar(ScalarValue::Int64(opt)) => Ok(*opt),
        ColumnarValue::Scalar(ScalarValue::Int32(opt)) => Ok(opt.map(|n| n as i64)),
        ColumnarValue::Scalar(ScalarValue::Int16(opt)) => Ok(opt.map(|n| n as i64)),
        ColumnarValue::Scalar(ScalarValue::Int8(opt)) => Ok(opt.map(|n| n as i64)),
        ColumnarValue::Scalar(ScalarValue::UInt64(opt)) => Ok(opt.map(|n| n as i64)),
        ColumnarValue::Scalar(ScalarValue::UInt32(opt)) => Ok(opt.map(|n| n as i64)),
        ColumnarValue::Scalar(ScalarValue::UInt16(opt)) => Ok(opt.map(|n| n as i64)),
        ColumnarValue::Scalar(ScalarValue::UInt8(opt)) => Ok(opt.map(|n| n as i64)),
        ColumnarValue::Scalar(ScalarValue::Null) => Ok(None),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::functions::test_utils::{invoke_udf_bool, invoke_udf_int64, invoke_udf_string};

    #[test]
    fn test_array_construct() {
        let func = ArrayConstructFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),
        ];
        let result = invoke_udf_string(&func, &args).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            let v: serde_json::Value = serde_json::from_str(&s).unwrap();
            assert!(v.is_array());
            assert_eq!(v.as_array().unwrap().len(), 3);
        } else {
            panic!("Expected string scalar");
        }
    }

    #[test]
    fn test_array_construct_compact() {
        let func = ArrayConstructCompactFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ColumnarValue::Scalar(ScalarValue::Null),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),
        ];
        let result = invoke_udf_string(&func, &args).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            let v: serde_json::Value = serde_json::from_str(&s).unwrap();
            assert!(v.is_array());
            assert_eq!(v.as_array().unwrap().len(), 2); // NULL excluded
        } else {
            panic!("Expected string scalar");
        }
    }

    #[test]
    fn test_array_append() {
        let func = ArrayAppendFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("[1, 2]".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),
        ];
        let result = invoke_udf_string(&func, &args).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            let v: serde_json::Value = serde_json::from_str(&s).unwrap();
            assert_eq!(v.as_array().unwrap().len(), 3);
            assert_eq!(v[2], 3);
        } else {
            panic!("Expected string scalar");
        }
    }

    #[test]
    fn test_array_cat() {
        let func = ArrayCatFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("[1, 2]".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("[3, 4]".to_string()))),
        ];
        let result = invoke_udf_string(&func, &args).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            let v: serde_json::Value = serde_json::from_str(&s).unwrap();
            assert_eq!(v.as_array().unwrap().len(), 4);
        } else {
            panic!("Expected string scalar");
        }
    }

    #[test]
    fn test_array_contains() {
        let func = ArrayContainsFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("[1, 2, 3]".to_string()))),
        ];
        let result = invoke_udf_bool(&func, &args).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) = result {
            assert!(b);
        } else {
            panic!("Expected boolean scalar");
        }
    }

    #[test]
    fn test_array_position() {
        let func = ArrayPositionFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("[1, 2, 3]".to_string()))),
        ];
        let result = invoke_udf_int64(&func, &args).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Int64(Some(p))) = result {
            assert_eq!(p, 1); // 0-indexed position
        } else {
            panic!("Expected int64 scalar");
        }
    }

    #[test]
    fn test_array_distinct() {
        let func = ArrayDistinctFunc::new();
        let args = vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "[1, 2, 2, 3, 1]".to_string(),
        )))];
        let result = invoke_udf_string(&func, &args).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            let v: serde_json::Value = serde_json::from_str(&s).unwrap();
            assert_eq!(v.as_array().unwrap().len(), 3);
        } else {
            panic!("Expected string scalar");
        }
    }

    #[test]
    fn test_array_flatten() {
        let func = ArrayFlattenFunc::new();
        let args = vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "[[1, 2], [3, 4]]".to_string(),
        )))];
        let result = invoke_udf_string(&func, &args).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            let v: serde_json::Value = serde_json::from_str(&s).unwrap();
            assert_eq!(v.as_array().unwrap().len(), 4);
        } else {
            panic!("Expected string scalar");
        }
    }
}
