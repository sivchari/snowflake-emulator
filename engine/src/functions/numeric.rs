//! Numeric functions (DIV0, DIV0NULL)
//!
//! Snowflake-compatible numeric manipulation functions.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Float64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};

// ============================================================================
// DIV0(dividend, divisor)
// ============================================================================

/// DIV0 function - Division that returns 0 when dividing by zero
///
/// Syntax: DIV0(dividend, divisor)
/// Returns dividend / divisor, or 0 if divisor is 0.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Div0Func {
    signature: Signature,
}

impl Default for Div0Func {
    fn default() -> Self {
        Self::new()
    }
}

impl Div0Func {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Div0Func {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "div0"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "DIV0 requires exactly 2 arguments".to_string(),
            ));
        }

        let dividend = &args.args[0];
        let divisor = &args.args[1];

        match (dividend, divisor) {
            (ColumnarValue::Scalar(div_nd), ColumnarValue::Scalar(div_sr)) => {
                let result = div0_scalars(div_nd, div_sr)?;
                Ok(ColumnarValue::Scalar(result))
            }
            _ => {
                let dividend_arr = dividend.to_array(args.number_rows)?;
                let divisor_arr = divisor.to_array(args.number_rows)?;
                let result = div0_arrays(&dividend_arr, &divisor_arr)?;
                Ok(ColumnarValue::Array(result))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

fn scalar_to_f64(scalar: &ScalarValue) -> Option<f64> {
    match scalar {
        ScalarValue::Float64(v) => *v,
        ScalarValue::Float32(v) => v.map(|x| x as f64),
        ScalarValue::Int64(v) => v.map(|x| x as f64),
        ScalarValue::Int32(v) => v.map(|x| x as f64),
        ScalarValue::Int16(v) => v.map(|x| x as f64),
        ScalarValue::Int8(v) => v.map(|x| x as f64),
        ScalarValue::UInt64(v) => v.map(|x| x as f64),
        ScalarValue::UInt32(v) => v.map(|x| x as f64),
        ScalarValue::UInt16(v) => v.map(|x| x as f64),
        ScalarValue::UInt8(v) => v.map(|x| x as f64),
        ScalarValue::Decimal128(v, _, scale) => v.map(|x| x as f64 / 10_f64.powi(*scale as i32)),
        _ => None,
    }
}

fn div0_scalars(dividend: &ScalarValue, divisor: &ScalarValue) -> Result<ScalarValue> {
    let div_nd = scalar_to_f64(dividend);
    let div_sr = scalar_to_f64(divisor);

    match (div_nd, div_sr) {
        (Some(a), Some(b)) => {
            if b == 0.0 {
                Ok(ScalarValue::Float64(Some(0.0)))
            } else {
                Ok(ScalarValue::Float64(Some(a / b)))
            }
        }
        _ => Ok(ScalarValue::Float64(None)),
    }
}

fn array_to_f64(array: &Arc<dyn Array>, idx: usize) -> Option<f64> {
    if array.is_null(idx) {
        return None;
    }

    match array.data_type() {
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Some(arr.value(idx))
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Float32Array>()
                .unwrap();
            Some(arr.value(idx) as f64)
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .unwrap();
            Some(arr.value(idx) as f64)
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                .unwrap();
            Some(arr.value(idx) as f64)
        }
        _ => None,
    }
}

fn div0_arrays(dividend: &Arc<dyn Array>, divisor: &Arc<dyn Array>) -> Result<Arc<dyn Array>> {
    if dividend.len() != divisor.len() {
        return Err(datafusion::error::DataFusionError::Execution(
            "Arrays must have the same length".to_string(),
        ));
    }

    let mut results = Vec::with_capacity(dividend.len());

    for i in 0..dividend.len() {
        let div_nd = array_to_f64(dividend, i);
        let div_sr = array_to_f64(divisor, i);

        match (div_nd, div_sr) {
            (Some(a), Some(b)) => {
                if b == 0.0 {
                    results.push(Some(0.0));
                } else {
                    results.push(Some(a / b));
                }
            }
            _ => results.push(None),
        }
    }

    Ok(Arc::new(Float64Array::from(results)))
}

/// Create DIV0 scalar UDF
pub fn div0() -> ScalarUDF {
    ScalarUDF::from(Div0Func::new())
}

// ============================================================================
// DIV0NULL(dividend, divisor)
// ============================================================================

/// DIV0NULL function - Division that returns NULL when dividing by zero
///
/// Syntax: DIV0NULL(dividend, divisor)
/// Returns dividend / divisor, or NULL if divisor is 0.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Div0NullFunc {
    signature: Signature,
}

impl Default for Div0NullFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl Div0NullFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Div0NullFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "div0null"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "DIV0NULL requires exactly 2 arguments".to_string(),
            ));
        }

        let dividend = &args.args[0];
        let divisor = &args.args[1];

        match (dividend, divisor) {
            (ColumnarValue::Scalar(div_nd), ColumnarValue::Scalar(div_sr)) => {
                let result = div0null_scalars(div_nd, div_sr)?;
                Ok(ColumnarValue::Scalar(result))
            }
            _ => {
                let dividend_arr = dividend.to_array(args.number_rows)?;
                let divisor_arr = divisor.to_array(args.number_rows)?;
                let result = div0null_arrays(&dividend_arr, &divisor_arr)?;
                Ok(ColumnarValue::Array(result))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

fn div0null_scalars(dividend: &ScalarValue, divisor: &ScalarValue) -> Result<ScalarValue> {
    let div_nd = scalar_to_f64(dividend);
    let div_sr = scalar_to_f64(divisor);

    match (div_nd, div_sr) {
        (Some(a), Some(b)) => {
            if b == 0.0 {
                Ok(ScalarValue::Float64(None)) // Return NULL for division by zero
            } else {
                Ok(ScalarValue::Float64(Some(a / b)))
            }
        }
        _ => Ok(ScalarValue::Float64(None)),
    }
}

fn div0null_arrays(dividend: &Arc<dyn Array>, divisor: &Arc<dyn Array>) -> Result<Arc<dyn Array>> {
    if dividend.len() != divisor.len() {
        return Err(datafusion::error::DataFusionError::Execution(
            "Arrays must have the same length".to_string(),
        ));
    }

    let mut results = Vec::with_capacity(dividend.len());

    for i in 0..dividend.len() {
        let div_nd = array_to_f64(dividend, i);
        let div_sr = array_to_f64(divisor, i);

        match (div_nd, div_sr) {
            (Some(a), Some(b)) => {
                if b == 0.0 {
                    results.push(None); // NULL for division by zero
                } else {
                    results.push(Some(a / b));
                }
            }
            _ => results.push(None),
        }
    }

    Ok(Arc::new(Float64Array::from(results)))
}

/// Create DIV0NULL scalar UDF
pub fn div0null() -> ScalarUDF {
    ScalarUDF::from(Div0NullFunc::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::functions::test_utils::invoke_udf_float64;

    #[test]
    fn test_div0_normal() {
        let func = Div0Func::new();

        let dividend = ColumnarValue::Scalar(ScalarValue::Int64(Some(10)));
        let divisor = ColumnarValue::Scalar(ScalarValue::Int64(Some(2)));

        let result = invoke_udf_float64(&func, &[dividend, divisor]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) = result {
            assert_eq!(v, 5.0);
        } else {
            panic!("Expected scalar Float64");
        }
    }

    #[test]
    fn test_div0_by_zero() {
        let func = Div0Func::new();

        let dividend = ColumnarValue::Scalar(ScalarValue::Int64(Some(10)));
        let divisor = ColumnarValue::Scalar(ScalarValue::Int64(Some(0)));

        let result = invoke_udf_float64(&func, &[dividend, divisor]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) = result {
            assert_eq!(v, 0.0);
        } else {
            panic!("Expected scalar Float64 with 0.0");
        }
    }

    #[test]
    fn test_div0null_normal() {
        let func = Div0NullFunc::new();

        let dividend = ColumnarValue::Scalar(ScalarValue::Int64(Some(10)));
        let divisor = ColumnarValue::Scalar(ScalarValue::Int64(Some(2)));

        let result = invoke_udf_float64(&func, &[dividend, divisor]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) = result {
            assert_eq!(v, 5.0);
        } else {
            panic!("Expected scalar Float64");
        }
    }

    #[test]
    fn test_div0null_by_zero() {
        let func = Div0NullFunc::new();

        let dividend = ColumnarValue::Scalar(ScalarValue::Int64(Some(10)));
        let divisor = ColumnarValue::Scalar(ScalarValue::Int64(Some(0)));

        let result = invoke_udf_float64(&func, &[dividend, divisor]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Float64(None)) = result {
            // Expected NULL
        } else {
            panic!("Expected scalar Float64 NULL");
        }
    }
}
