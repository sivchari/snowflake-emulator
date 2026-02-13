//! Type checking and conversion functions for VARIANT types
//!
//! Snowflake-compatible type checking and conversion functions.
//!
//! ## Type Checking Functions
//! - `IS_ARRAY(variant)` - Check if value is an array
//! - `IS_OBJECT(variant)` - Check if value is an object
//! - `IS_NULL_VALUE(variant)` - Check if value is JSON null
//! - `IS_BOOLEAN(variant)` - Check if value is a boolean
//! - `IS_INTEGER(variant)` - Check if value is an integer
//! - `IS_DECIMAL(variant)` - Check if value is a decimal
//! - `TYPEOF(variant)` - Return the type name
//!
//! ## Conversion Functions
//! - `TO_VARIANT(value)` - Convert any value to VARIANT
//! - `TO_ARRAY(variant)` - Convert value to ARRAY
//! - `TO_OBJECT(variant)` - Convert value to OBJECT

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};

// ============================================================================
// Helper macro for type checking functions
// ============================================================================

macro_rules! impl_type_check_func {
    ($struct_name:ident, $func_name:literal, $check_fn:expr) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        pub struct $struct_name {
            signature: Signature,
        }

        impl Default for $struct_name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $struct_name {
            pub fn new() -> Self {
                Self {
                    signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
                }
            }
        }

        impl ScalarUDFImpl for $struct_name {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn name(&self) -> &str {
                $func_name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok(DataType::Boolean)
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                if args.args.is_empty() {
                    return Err(datafusion::error::DataFusionError::Execution(format!(
                        "{} requires exactly 1 argument",
                        $func_name.to_uppercase()
                    )));
                }

                let check_fn: fn(&serde_json::Value) -> bool = $check_fn;

                match &args.args[0] {
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                        let result = serde_json::from_str::<serde_json::Value>(s)
                            .map(|v| check_fn(&v))
                            .unwrap_or(false);
                        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(result))))
                    }
                    ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)))
                    }
                    ColumnarValue::Array(arr) => {
                        let str_arr =
                            arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                                datafusion::error::DataFusionError::Execution(format!(
                                    "{} argument must be a string",
                                    $func_name.to_uppercase()
                                ))
                            })?;
                        let result: BooleanArray = str_arr
                            .iter()
                            .map(|opt| {
                                opt.map(|s| {
                                    serde_json::from_str::<serde_json::Value>(s)
                                        .map(|v| check_fn(&v))
                                        .unwrap_or(false)
                                })
                            })
                            .collect();
                        Ok(ColumnarValue::Array(Arc::new(result)))
                    }
                    _ => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(false)))),
                }
            }

            fn documentation(&self) -> Option<&Documentation> {
                None
            }
        }
    };
}

// ============================================================================
// Type Checking Functions
// ============================================================================

impl_type_check_func!(IsArrayFunc, "is_array", |v: &serde_json::Value| v
    .is_array());
impl_type_check_func!(IsObjectFunc, "is_object", |v: &serde_json::Value| v
    .is_object());
impl_type_check_func!(IsNullValueFunc, "is_null_value", |v: &serde_json::Value| v
    .is_null());
impl_type_check_func!(IsBooleanFunc, "is_boolean", |v: &serde_json::Value| v
    .is_boolean());
impl_type_check_func!(IsIntegerFunc, "is_integer", |v: &serde_json::Value| v
    .is_i64()
    || v.is_u64());
impl_type_check_func!(IsDecimalFunc, "is_decimal", |v: &serde_json::Value| v
    .is_f64());

pub fn is_array() -> ScalarUDF {
    ScalarUDF::from(IsArrayFunc::new())
}

pub fn is_object() -> ScalarUDF {
    ScalarUDF::from(IsObjectFunc::new())
}

pub fn is_null_value() -> ScalarUDF {
    ScalarUDF::from(IsNullValueFunc::new())
}

pub fn is_boolean() -> ScalarUDF {
    ScalarUDF::from(IsBooleanFunc::new())
}

pub fn is_integer() -> ScalarUDF {
    ScalarUDF::from(IsIntegerFunc::new())
}

pub fn is_decimal() -> ScalarUDF {
    ScalarUDF::from(IsDecimalFunc::new())
}

// ============================================================================
// TYPEOF(variant)
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TypeofFunc {
    signature: Signature,
}

impl Default for TypeofFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl TypeofFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

fn get_type_name(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "NULL_VALUE",
        serde_json::Value::Bool(_) => "BOOLEAN",
        serde_json::Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                "INTEGER"
            } else {
                "DECIMAL"
            }
        }
        serde_json::Value::String(_) => "VARCHAR",
        serde_json::Value::Array(_) => "ARRAY",
        serde_json::Value::Object(_) => "OBJECT",
    }
}

impl ScalarUDFImpl for TypeofFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "typeof"
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
                "TYPEOF requires exactly 1 argument".to_string(),
            ));
        }

        match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                let type_name = match serde_json::from_str::<serde_json::Value>(s) {
                    Ok(v) => get_type_name(&v),
                    Err(_) => "VARCHAR", // Non-JSON strings are treated as VARCHAR
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    type_name.to_string(),
                ))))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
            }
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "TYPEOF argument must be a string".to_string(),
                    )
                })?;
                let result: StringArray = str_arr
                    .iter()
                    .map(|opt| {
                        opt.map(|s| match serde_json::from_str::<serde_json::Value>(s) {
                            Ok(v) => get_type_name(&v).to_string(),
                            Err(_) => "VARCHAR".to_string(),
                        })
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            _ => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "VARIANT".to_string(),
            )))),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

pub fn typeof_func() -> ScalarUDF {
    ScalarUDF::from(TypeofFunc::new())
}

// ============================================================================
// TO_VARIANT(value)
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToVariantFunc {
    signature: Signature,
}

impl Default for ToVariantFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ToVariantFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ToVariantFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "to_variant"
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
                "TO_VARIANT requires exactly 1 argument".to_string(),
            ));
        }

        match &args.args[0] {
            ColumnarValue::Scalar(scalar) => {
                let json_str = scalar_to_variant(scalar)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(json_str)))
            }
            ColumnarValue::Array(arr) => {
                let result = array_to_variant(arr)?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

fn scalar_to_variant(scalar: &ScalarValue) -> Result<Option<String>> {
    if scalar.is_null() {
        return Ok(Some("null".to_string()));
    }

    let json_value = match scalar {
        ScalarValue::Null => serde_json::Value::Null,
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
            // If already valid JSON, return as-is; otherwise wrap as string
            match serde_json::from_str::<serde_json::Value>(s) {
                Ok(v) => v,
                Err(_) => serde_json::Value::String(s.clone()),
            }
        }
        _ => {
            // For other types, convert to string
            serde_json::Value::String(format!("{scalar}"))
        }
    };

    let json_str = serde_json::to_string(&json_value).map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!("JSON serialization error: {e}"))
    })?;

    Ok(Some(json_str))
}

fn array_to_variant(array: &ArrayRef) -> Result<StringArray> {
    let result: StringArray = match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            arr.iter()
                .map(|opt| opt.map(|b| if b { "true" } else { "false" }.to_string()))
                .collect()
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
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
                    opt.map(|s| match serde_json::from_str::<serde_json::Value>(s) {
                        Ok(v) => serde_json::to_string(&v).unwrap_or_else(|_| {
                            format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
                        }),
                        Err(_) => {
                            format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
                        }
                    })
                })
                .collect()
        }
        _ => {
            // For unsupported types, return JSON null
            (0..array.len())
                .map(|i| {
                    if array.is_null(i) {
                        None
                    } else {
                        Some("null".to_string())
                    }
                })
                .collect()
        }
    };

    Ok(result)
}

pub fn to_variant() -> ScalarUDF {
    ScalarUDF::from(ToVariantFunc::new())
}

// ============================================================================
// TO_ARRAY(variant)
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToArrayFunc {
    signature: Signature,
}

impl Default for ToArrayFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ToArrayFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ToArrayFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "to_array"
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
                "TO_ARRAY requires exactly 1 argument".to_string(),
            ));
        }

        match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                let result = value_to_array(s);
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
            }
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "TO_ARRAY argument must be a string".to_string(),
                    )
                })?;
                let result: StringArray =
                    str_arr.iter().map(|opt| opt.map(value_to_array)).collect();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            _ => {
                // Non-string scalars: wrap in array
                let scalar = &args.args[0];
                if let ColumnarValue::Scalar(s) = scalar {
                    let json_str = scalar_to_variant(s)?;
                    let result = json_str.map(|s| format!("[{s}]"));
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
                } else {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                        "[]".to_string(),
                    ))))
                }
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

fn value_to_array(s: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(s) {
        Ok(v) => {
            if v.is_array() {
                serde_json::to_string(&v).unwrap_or_else(|_| "[]".to_string())
            } else {
                serde_json::to_string(&serde_json::Value::Array(vec![v]))
                    .unwrap_or_else(|_| "[]".to_string())
            }
        }
        Err(_) => {
            // Wrap plain string in array
            format!("[\"{}\"]", s.replace('\\', "\\\\").replace('"', "\\\""))
        }
    }
}

pub fn to_array() -> ScalarUDF {
    ScalarUDF::from(ToArrayFunc::new())
}

// ============================================================================
// TO_OBJECT(variant)
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToObjectFunc {
    signature: Signature,
}

impl Default for ToObjectFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ToObjectFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ToObjectFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "to_object"
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
                "TO_OBJECT requires exactly 1 argument".to_string(),
            ));
        }

        match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                let result = value_to_object(s);
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
            }
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "TO_OBJECT argument must be a string".to_string(),
                    )
                })?;
                let result: StringArray = str_arr
                    .iter()
                    .map(|opt| opt.and_then(value_to_object))
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            _ => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

fn value_to_object(s: &str) -> Option<String> {
    match serde_json::from_str::<serde_json::Value>(s) {
        Ok(v) => {
            if v.is_object() {
                Some(serde_json::to_string(&v).unwrap_or_else(|_| "{}".to_string()))
            } else {
                None // Cannot convert non-object to object
            }
        }
        Err(_) => None, // Cannot convert invalid JSON to object
    }
}

pub fn to_object() -> ScalarUDF {
    ScalarUDF::from(ToObjectFunc::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::functions::test_utils::{invoke_udf_bool, invoke_udf_string};

    #[test]
    fn test_is_array_true() {
        let func = IsArrayFunc::new();
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("[1, 2, 3]".to_string())));
        let result = invoke_udf_bool(&func, &[input]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) = result {
            assert!(b);
        } else {
            panic!("Expected boolean scalar");
        }
    }

    #[test]
    fn test_is_array_false() {
        let func = IsArrayFunc::new();
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("{\"a\": 1}".to_string())));
        let result = invoke_udf_bool(&func, &[input]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) = result {
            assert!(!b);
        } else {
            panic!("Expected boolean scalar");
        }
    }

    #[test]
    fn test_is_object_true() {
        let func = IsObjectFunc::new();
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("{\"a\": 1}".to_string())));
        let result = invoke_udf_bool(&func, &[input]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) = result {
            assert!(b);
        } else {
            panic!("Expected boolean scalar");
        }
    }

    #[test]
    fn test_typeof_array() {
        let func = TypeofFunc::new();
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("[1, 2, 3]".to_string())));
        let result = invoke_udf_string(&func, &[input]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert_eq!(s, "ARRAY");
        } else {
            panic!("Expected string scalar");
        }
    }

    #[test]
    fn test_typeof_object() {
        let func = TypeofFunc::new();
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("{\"a\": 1}".to_string())));
        let result = invoke_udf_string(&func, &[input]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert_eq!(s, "OBJECT");
        } else {
            panic!("Expected string scalar");
        }
    }

    #[test]
    fn test_typeof_integer() {
        let func = TypeofFunc::new();
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("42".to_string())));
        let result = invoke_udf_string(&func, &[input]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert_eq!(s, "INTEGER");
        } else {
            panic!("Expected string scalar");
        }
    }

    #[test]
    fn test_to_variant_int() {
        let func = ToVariantFunc::new();
        let input = ColumnarValue::Scalar(ScalarValue::Int64(Some(42)));
        let result = invoke_udf_string(&func, &[input]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert_eq!(s, "42");
        } else {
            panic!("Expected string scalar");
        }
    }

    #[test]
    fn test_to_array_non_array() {
        let func = ToArrayFunc::new();
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("42".to_string())));
        let result = invoke_udf_string(&func, &[input]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert_eq!(s, "[42]");
        } else {
            panic!("Expected string scalar");
        }
    }

    #[test]
    fn test_to_array_already_array() {
        let func = ToArrayFunc::new();
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("[1, 2, 3]".to_string())));
        let result = invoke_udf_string(&func, &[input]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            let v: serde_json::Value = serde_json::from_str(&s).unwrap();
            assert!(v.is_array());
            assert_eq!(v.as_array().unwrap().len(), 3);
        } else {
            panic!("Expected string scalar");
        }
    }

    #[test]
    fn test_to_object_valid() {
        let func = ToObjectFunc::new();
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("{\"a\": 1}".to_string())));
        let result = invoke_udf_string(&func, &[input]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            let v: serde_json::Value = serde_json::from_str(&s).unwrap();
            assert!(v.is_object());
        } else {
            panic!("Expected string scalar");
        }
    }

    #[test]
    fn test_to_object_invalid() {
        let func = ToObjectFunc::new();
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("[1, 2, 3]".to_string())));
        let result = invoke_udf_string(&func, &[input]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(opt)) = result {
            assert!(opt.is_none()); // Arrays cannot be converted to objects
        } else {
            panic!("Expected string scalar");
        }
    }
}
