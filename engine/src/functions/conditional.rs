//! Conditional functions (IFF, NVL, NVL2)
//!
//! Snowflake-compatible conditional expression functions.

use std::any::Any;

use arrow::array::{Array, BooleanArray};
use arrow::compute::kernels::zip::zip;
use arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

// ============================================================================
// IFF(condition, true_value, false_value)
// ============================================================================

/// IFF function - Snowflake's inline IF expression
///
/// Syntax: IFF(condition, true_value, false_value)
/// Returns true_value if condition is true, otherwise false_value.
#[derive(Debug)]
pub struct IffFunc {
    signature: Signature,
}

impl Default for IffFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl IffFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(3), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for IffFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "iff"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // Return type is the type of true_value (second argument)
        Ok(arg_types.get(1).cloned().unwrap_or(DataType::Null))
    }

    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        if args.len() != 3 {
            return Err(datafusion::error::DataFusionError::Execution(
                "IFF requires exactly 3 arguments".to_string(),
            ));
        }

        let condition = &args[0];
        let true_value = &args[1];
        let false_value = &args[2];

        // Convert all to arrays
        let condition_array = condition.to_array(num_rows)?;
        let true_array = true_value.to_array(num_rows)?;
        let false_array = false_value.to_array(num_rows)?;

        // Get boolean condition
        let condition_bool = condition_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "IFF condition must be boolean".to_string(),
                )
            })?;

        // Use arrow's zip function (replacement for if_then_else in arrow v54+)
        let result = zip(condition_bool, &true_array, &false_array)?;

        Ok(ColumnarValue::Array(result))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

/// Create IFF scalar UDF
pub fn iff() -> ScalarUDF {
    ScalarUDF::from(IffFunc::new())
}

// ============================================================================
// NVL(expr1, expr2)
// ============================================================================

/// NVL function - Returns expr2 if expr1 is NULL, otherwise expr1
///
/// Syntax: NVL(expr1, expr2)
/// Equivalent to COALESCE(expr1, expr2) or IFNULL(expr1, expr2)
#[derive(Debug)]
pub struct NvlFunc {
    signature: Signature,
}

impl Default for NvlFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl NvlFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NvlFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "nvl"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types.get(0).cloned().unwrap_or(DataType::Null))
    }

    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        if args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "NVL requires exactly 2 arguments".to_string(),
            ));
        }

        let expr1 = &args[0];
        let expr2 = &args[1];

        // Handle scalar case for efficiency
        if let (ColumnarValue::Scalar(s1), ColumnarValue::Scalar(s2)) = (expr1, expr2) {
            return Ok(ColumnarValue::Scalar(if s1.is_null() {
                s2.clone()
            } else {
                s1.clone()
            }));
        }

        let arr1 = expr1.to_array(num_rows)?;
        let arr2 = expr2.to_array(num_rows)?;

        // Build null mask: where arr1 is null, use arr2
        let nulls = arr1.nulls();

        if let Some(null_buffer) = nulls {
            // Create boolean array from null bitmap (true = is null)
            let is_null: BooleanArray = (0..arr1.len())
                .map(|i| Some(!null_buffer.is_valid(i)))
                .collect();

            // zip: if is_null then arr2 else arr1
            let result = zip(&is_null, &arr2, &arr1)?;
            Ok(ColumnarValue::Array(result))
        } else {
            // No nulls in arr1, return arr1 as-is
            Ok(ColumnarValue::Array(arr1))
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

/// Create NVL scalar UDF
pub fn nvl() -> ScalarUDF {
    ScalarUDF::from(NvlFunc::new())
}

// ============================================================================
// NVL2(expr1, expr2, expr3)
// ============================================================================

/// NVL2 function - Returns expr2 if expr1 is NOT NULL, otherwise expr3
///
/// Syntax: NVL2(expr1, expr2, expr3)
/// Note: This is the opposite of typical NULL handling!
/// - If expr1 is NOT NULL -> return expr2
/// - If expr1 is NULL -> return expr3
#[derive(Debug)]
pub struct Nvl2Func {
    signature: Signature,
}

impl Default for Nvl2Func {
    fn default() -> Self {
        Self::new()
    }
}

impl Nvl2Func {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(3), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Nvl2Func {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "nvl2"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // Return type is the type of expr2 (second argument)
        Ok(arg_types.get(1).cloned().unwrap_or(DataType::Null))
    }

    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        if args.len() != 3 {
            return Err(datafusion::error::DataFusionError::Execution(
                "NVL2 requires exactly 3 arguments".to_string(),
            ));
        }

        let expr1 = &args[0];
        let expr2 = &args[1]; // returned when expr1 is NOT NULL
        let expr3 = &args[2]; // returned when expr1 is NULL

        // Handle scalar case
        if let ColumnarValue::Scalar(s1) = expr1 {
            if let (ColumnarValue::Scalar(s2), ColumnarValue::Scalar(s3)) = (expr2, expr3) {
                return Ok(ColumnarValue::Scalar(if s1.is_null() {
                    s3.clone()
                } else {
                    s2.clone()
                }));
            }
        }

        let arr1 = expr1.to_array(num_rows)?;
        let arr2 = expr2.to_array(num_rows)?;
        let arr3 = expr3.to_array(num_rows)?;

        // Build condition: is NOT null
        let nulls = arr1.nulls();

        let is_not_null: BooleanArray = if let Some(null_buffer) = nulls {
            (0..arr1.len())
                .map(|i| Some(null_buffer.is_valid(i)))
                .collect()
        } else {
            // No nulls means all values are not null
            (0..arr1.len()).map(|_| Some(true)).collect()
        };

        // zip: if is_not_null then arr2 else arr3
        let result = zip(&is_not_null, &arr2, &arr3)?;
        Ok(ColumnarValue::Array(result))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

/// Create NVL2 scalar UDF
pub fn nvl2() -> ScalarUDF {
    ScalarUDF::from(Nvl2Func::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use datafusion::common::ScalarValue;

    #[test]
    fn test_iff_basic() {
        let func = IffFunc::new();

        // IFF(true, 'yes', 'no') -> 'yes'
        let condition = ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)));
        let true_val = ColumnarValue::Scalar(ScalarValue::Utf8(Some("yes".to_string())));
        let false_val = ColumnarValue::Scalar(ScalarValue::Utf8(Some("no".to_string())));

        let result = func
            .invoke_batch(&[condition, true_val, false_val], 1)
            .unwrap();

        if let ColumnarValue::Array(arr) = result {
            let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(str_arr.value(0), "yes");
        }
    }

    #[test]
    fn test_iff_false_condition() {
        let func = IffFunc::new();

        // IFF(false, 'yes', 'no') -> 'no'
        let condition = ColumnarValue::Scalar(ScalarValue::Boolean(Some(false)));
        let true_val = ColumnarValue::Scalar(ScalarValue::Utf8(Some("yes".to_string())));
        let false_val = ColumnarValue::Scalar(ScalarValue::Utf8(Some("no".to_string())));

        let result = func
            .invoke_batch(&[condition, true_val, false_val], 1)
            .unwrap();

        if let ColumnarValue::Array(arr) = result {
            let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(str_arr.value(0), "no");
        }
    }

    #[test]
    fn test_nvl_non_null() {
        let func = NvlFunc::new();

        // NVL(10, 20) -> 10
        let expr1 = ColumnarValue::Scalar(ScalarValue::Int64(Some(10)));
        let expr2 = ColumnarValue::Scalar(ScalarValue::Int64(Some(20)));

        let result = func.invoke_batch(&[expr1, expr2], 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) = result {
            assert_eq!(v, 10);
        } else {
            panic!("Expected scalar int64");
        }
    }

    #[test]
    fn test_nvl_null() {
        let func = NvlFunc::new();

        // NVL(NULL, 20) -> 20
        let expr1 = ColumnarValue::Scalar(ScalarValue::Int64(None));
        let expr2 = ColumnarValue::Scalar(ScalarValue::Int64(Some(20)));

        let result = func.invoke_batch(&[expr1, expr2], 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) = result {
            assert_eq!(v, 20);
        } else {
            panic!("Expected scalar int64");
        }
    }

    #[test]
    fn test_nvl2_not_null() {
        let func = Nvl2Func::new();

        // NVL2(10, 'has value', 'no value') -> 'has value'
        let expr1 = ColumnarValue::Scalar(ScalarValue::Int64(Some(10)));
        let expr2 = ColumnarValue::Scalar(ScalarValue::Utf8(Some("has value".to_string())));
        let expr3 = ColumnarValue::Scalar(ScalarValue::Utf8(Some("no value".to_string())));

        let result = func.invoke_batch(&[expr1, expr2, expr3], 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) = result {
            assert_eq!(v, "has value");
        } else {
            panic!("Expected scalar utf8");
        }
    }

    #[test]
    fn test_nvl2_null() {
        let func = Nvl2Func::new();

        // NVL2(NULL, 'has value', 'no value') -> 'no value'
        let expr1 = ColumnarValue::Scalar(ScalarValue::Int64(None));
        let expr2 = ColumnarValue::Scalar(ScalarValue::Utf8(Some("has value".to_string())));
        let expr3 = ColumnarValue::Scalar(ScalarValue::Utf8(Some("no value".to_string())));

        let result = func.invoke_batch(&[expr1, expr2, expr3], 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) = result {
            assert_eq!(v, "no value");
        } else {
            panic!("Expected scalar utf8");
        }
    }
}
