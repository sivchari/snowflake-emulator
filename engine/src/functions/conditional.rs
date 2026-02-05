//! Conditional functions (IFF, NVL, NVL2, DECODE)
//!
//! Snowflake-compatible conditional expression functions.

use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, StringBuilder};
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
        Ok(arg_types.first().cloned().unwrap_or(DataType::Null))
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

// ============================================================================
// DECODE(expr, search1, result1, search2, result2, ..., default)
// ============================================================================

/// DECODE function - Compares expr to search values and returns corresponding result
///
/// Syntax: DECODE(expr, search1, result1, search2, result2, ..., default)
///
/// Equivalent to:
/// ```sql
/// CASE expr
///     WHEN search1 THEN result1
///     WHEN search2 THEN result2
///     ...
///     ELSE default
/// END
/// ```
///
/// - Minimum 3 arguments: DECODE(expr, search1, result1)
/// - Optional default value at the end (when odd number of arguments after expr)
/// - Returns NULL if no match and no default provided
#[derive(Debug)]
pub struct DecodeFunc {
    signature: Signature,
}

impl Default for DecodeFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl DecodeFunc {
    pub fn new() -> Self {
        Self {
            // VariadicAny allows any number and type of arguments
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }

    /// Compare two scalar values for equality
    fn values_equal(
        val1: &datafusion::common::ScalarValue,
        val2: &datafusion::common::ScalarValue,
    ) -> bool {
        // Handle NULL comparison: NULL != NULL in DECODE
        if val1.is_null() || val2.is_null() {
            return false;
        }

        // Try to compare as strings for flexibility
        let str1 = Self::scalar_to_string(val1);
        let str2 = Self::scalar_to_string(val2);

        match (str1, str2) {
            (Some(s1), Some(s2)) => s1 == s2,
            _ => false,
        }
    }

    /// Convert scalar value to string for comparison
    fn scalar_to_string(val: &datafusion::common::ScalarValue) -> Option<String> {
        use datafusion::common::ScalarValue;

        match val {
            ScalarValue::Null => None,
            ScalarValue::Boolean(Some(b)) => Some(b.to_string()),
            ScalarValue::Int8(Some(v)) => Some(v.to_string()),
            ScalarValue::Int16(Some(v)) => Some(v.to_string()),
            ScalarValue::Int32(Some(v)) => Some(v.to_string()),
            ScalarValue::Int64(Some(v)) => Some(v.to_string()),
            ScalarValue::UInt8(Some(v)) => Some(v.to_string()),
            ScalarValue::UInt16(Some(v)) => Some(v.to_string()),
            ScalarValue::UInt32(Some(v)) => Some(v.to_string()),
            ScalarValue::UInt64(Some(v)) => Some(v.to_string()),
            ScalarValue::Float32(Some(v)) => Some(v.to_string()),
            ScalarValue::Float64(Some(v)) => Some(v.to_string()),
            ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
            ScalarValue::Utf8View(Some(s)) => Some(s.to_string()),
            _ => val.to_string().parse().ok(),
        }
    }
}

impl ScalarUDFImpl for DecodeFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "decode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // Return type is the type of the first result (third argument)
        // If not available, return Utf8
        Ok(arg_types.get(2).cloned().unwrap_or(DataType::Utf8))
    }

    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        if args.len() < 3 {
            return Err(datafusion::error::DataFusionError::Execution(
                "DECODE requires at least 3 arguments: DECODE(expr, search, result, ...)"
                    .to_string(),
            ));
        }

        let expr = &args[0];

        // Calculate pairs and default
        // After expr, we have search/result pairs and optionally a default
        // args[1], args[2] = search1, result1
        // args[3], args[4] = search2, result2
        // ...
        // If (args.len() - 1) is odd, the last argument is the default
        let remaining = args.len() - 1;
        let has_default = remaining % 2 == 1;
        let num_pairs = remaining / 2;

        // Convert expr to array for row-wise processing
        let expr_array = expr.to_array(num_rows)?;

        // Build result array
        let mut result_builder = StringBuilder::new();

        for row_idx in 0..num_rows {
            // Get expr value for this row
            let expr_scalar =
                datafusion::common::ScalarValue::try_from_array(&expr_array, row_idx)?;

            let mut matched = false;
            let mut result_value: Option<datafusion::common::ScalarValue> = None;

            // Check each search/result pair
            for pair_idx in 0..num_pairs {
                let search_idx = 1 + pair_idx * 2;
                let result_idx = 2 + pair_idx * 2;

                let search_array = args[search_idx].to_array(num_rows)?;
                let search_scalar =
                    datafusion::common::ScalarValue::try_from_array(&search_array, row_idx)?;

                if Self::values_equal(&expr_scalar, &search_scalar) {
                    let result_array = args[result_idx].to_array(num_rows)?;
                    result_value = Some(datafusion::common::ScalarValue::try_from_array(
                        &result_array,
                        row_idx,
                    )?);
                    matched = true;
                    break;
                }
            }

            // If no match, use default or NULL
            if !matched && has_default {
                let default_idx = args.len() - 1;
                let default_array = args[default_idx].to_array(num_rows)?;
                result_value = Some(datafusion::common::ScalarValue::try_from_array(
                    &default_array,
                    row_idx,
                )?);
            }

            // Append to result builder
            match result_value {
                Some(val) => {
                    if val.is_null() {
                        result_builder.append_null();
                    } else {
                        let s = Self::scalar_to_string(&val).unwrap_or_default();
                        result_builder.append_value(&s);
                    }
                }
                None => result_builder.append_null(),
            }
        }

        let result_array: ArrayRef = Arc::new(result_builder.finish());
        Ok(ColumnarValue::Array(result_array))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

/// Create DECODE scalar UDF
pub fn decode() -> ScalarUDF {
    ScalarUDF::from(DecodeFunc::new())
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

    #[test]
    fn test_decode_match_first() {
        let func = DecodeFunc::new();

        // DECODE(1, 1, 'one', 2, 'two', 'other') -> 'one'
        let expr = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        let search1 = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        let result1 = ColumnarValue::Scalar(ScalarValue::Utf8(Some("one".to_string())));
        let search2 = ColumnarValue::Scalar(ScalarValue::Int64(Some(2)));
        let result2 = ColumnarValue::Scalar(ScalarValue::Utf8(Some("two".to_string())));
        let default = ColumnarValue::Scalar(ScalarValue::Utf8(Some("other".to_string())));

        let result = func
            .invoke_batch(&[expr, search1, result1, search2, result2, default], 1)
            .unwrap();

        if let ColumnarValue::Array(arr) = result {
            let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(str_arr.value(0), "one");
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_decode_match_second() {
        let func = DecodeFunc::new();

        // DECODE(2, 1, 'one', 2, 'two', 'other') -> 'two'
        let expr = ColumnarValue::Scalar(ScalarValue::Int64(Some(2)));
        let search1 = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        let result1 = ColumnarValue::Scalar(ScalarValue::Utf8(Some("one".to_string())));
        let search2 = ColumnarValue::Scalar(ScalarValue::Int64(Some(2)));
        let result2 = ColumnarValue::Scalar(ScalarValue::Utf8(Some("two".to_string())));
        let default = ColumnarValue::Scalar(ScalarValue::Utf8(Some("other".to_string())));

        let result = func
            .invoke_batch(&[expr, search1, result1, search2, result2, default], 1)
            .unwrap();

        if let ColumnarValue::Array(arr) = result {
            let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(str_arr.value(0), "two");
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_decode_default() {
        let func = DecodeFunc::new();

        // DECODE(3, 1, 'one', 2, 'two', 'other') -> 'other'
        let expr = ColumnarValue::Scalar(ScalarValue::Int64(Some(3)));
        let search1 = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        let result1 = ColumnarValue::Scalar(ScalarValue::Utf8(Some("one".to_string())));
        let search2 = ColumnarValue::Scalar(ScalarValue::Int64(Some(2)));
        let result2 = ColumnarValue::Scalar(ScalarValue::Utf8(Some("two".to_string())));
        let default = ColumnarValue::Scalar(ScalarValue::Utf8(Some("other".to_string())));

        let result = func
            .invoke_batch(&[expr, search1, result1, search2, result2, default], 1)
            .unwrap();

        if let ColumnarValue::Array(arr) = result {
            let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(str_arr.value(0), "other");
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_decode_no_default_null() {
        let func = DecodeFunc::new();

        // DECODE(3, 1, 'one', 2, 'two') -> NULL (no default, no match)
        let expr = ColumnarValue::Scalar(ScalarValue::Int64(Some(3)));
        let search1 = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        let result1 = ColumnarValue::Scalar(ScalarValue::Utf8(Some("one".to_string())));
        let search2 = ColumnarValue::Scalar(ScalarValue::Int64(Some(2)));
        let result2 = ColumnarValue::Scalar(ScalarValue::Utf8(Some("two".to_string())));

        let result = func
            .invoke_batch(&[expr, search1, result1, search2, result2], 1)
            .unwrap();

        if let ColumnarValue::Array(arr) = result {
            let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
            assert!(str_arr.is_null(0));
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_decode_string_values() {
        let func = DecodeFunc::new();

        // DECODE('A', 'A', 'Alpha', 'B', 'Beta', 'Other') -> 'Alpha'
        let expr = ColumnarValue::Scalar(ScalarValue::Utf8(Some("A".to_string())));
        let search1 = ColumnarValue::Scalar(ScalarValue::Utf8(Some("A".to_string())));
        let result1 = ColumnarValue::Scalar(ScalarValue::Utf8(Some("Alpha".to_string())));
        let search2 = ColumnarValue::Scalar(ScalarValue::Utf8(Some("B".to_string())));
        let result2 = ColumnarValue::Scalar(ScalarValue::Utf8(Some("Beta".to_string())));
        let default = ColumnarValue::Scalar(ScalarValue::Utf8(Some("Other".to_string())));

        let result = func
            .invoke_batch(&[expr, search1, result1, search2, result2, default], 1)
            .unwrap();

        if let ColumnarValue::Array(arr) = result {
            let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(str_arr.value(0), "Alpha");
        } else {
            panic!("Expected array result");
        }
    }
}
