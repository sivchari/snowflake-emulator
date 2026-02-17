//! Hash functions (SHA1, SHA2)
//!
//! Snowflake-compatible hash functions.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use sha1::Sha1;
use sha2::{Digest, Sha224, Sha256, Sha384, Sha512};

// ============================================================================
// SHA1(string)
// ============================================================================

/// SHA1 function - Calculate SHA-1 hash
///
/// Syntax: SHA1(string)
/// Returns the 40-character hex string representation of the SHA-1 hash.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Sha1Func {
    signature: Signature,
}

impl Default for Sha1Func {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha1Func {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Sha1Func {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "sha1"
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
                "SHA1 requires exactly 1 argument".to_string(),
            ));
        }

        let input = &args.args[0];

        match input {
            ColumnarValue::Scalar(scalar) => {
                let result = sha1_scalar(scalar)?;
                Ok(ColumnarValue::Scalar(result))
            }
            ColumnarValue::Array(arr) => {
                let result = sha1_array(arr)?;
                Ok(ColumnarValue::Array(result))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

fn sha1_scalar(scalar: &ScalarValue) -> Result<ScalarValue> {
    match scalar {
        ScalarValue::Utf8(Some(s)) => {
            let mut hasher = Sha1::new();
            hasher.update(s.as_bytes());
            let result = hasher.finalize();
            Ok(ScalarValue::Utf8(Some(hex::encode(result))))
        }
        ScalarValue::Utf8(None) => Ok(ScalarValue::Utf8(None)),
        _ => Err(datafusion::error::DataFusionError::Execution(
            "SHA1 argument must be a string".to_string(),
        )),
    }
}

fn sha1_array(array: &Arc<dyn Array>) -> Result<Arc<dyn Array>> {
    match array.data_type() {
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            let result: StringArray = arr
                .iter()
                .map(|opt| {
                    opt.map(|s| {
                        let mut hasher = Sha1::new();
                        hasher.update(s.as_bytes());
                        hex::encode(hasher.finalize())
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }
        _ => Err(datafusion::error::DataFusionError::Execution(
            "SHA1 argument must be a string".to_string(),
        )),
    }
}

/// Create SHA1 scalar UDF
pub fn sha1_hex() -> ScalarUDF {
    ScalarUDF::from(Sha1Func::new())
}

// ============================================================================
// SHA2(string, bit_length)
// ============================================================================

/// SHA2 function - Calculate SHA-2 hash
///
/// Syntax: SHA2(string, bit_length)
/// Returns the hex string representation of the SHA-2 hash.
/// bit_length can be 224, 256, 384, or 512 (default: 256).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Sha2Func {
    signature: Signature,
}

impl Default for Sha2Func {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha2Func {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Sha2Func {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "sha2"
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
                "SHA2 requires at least 1 argument".to_string(),
            ));
        }

        let input = &args.args[0];

        // Get bit length (default 256)
        let bit_length = if args.args.len() > 1 {
            match &args.args[1] {
                ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => *v as i32,
                ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) => *v,
                _ => 256,
            }
        } else {
            256
        };

        match input {
            ColumnarValue::Scalar(scalar) => {
                let result = sha2_scalar(scalar, bit_length)?;
                Ok(ColumnarValue::Scalar(result))
            }
            ColumnarValue::Array(arr) => {
                let result = sha2_array(arr, bit_length)?;
                Ok(ColumnarValue::Array(result))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

fn sha2_scalar(scalar: &ScalarValue, bit_length: i32) -> Result<ScalarValue> {
    match scalar {
        ScalarValue::Utf8(Some(s)) => {
            let hash = compute_sha2(s, bit_length)?;
            Ok(ScalarValue::Utf8(Some(hash)))
        }
        ScalarValue::Utf8(None) => Ok(ScalarValue::Utf8(None)),
        _ => Err(datafusion::error::DataFusionError::Execution(
            "SHA2 first argument must be a string".to_string(),
        )),
    }
}

fn sha2_array(array: &Arc<dyn Array>, bit_length: i32) -> Result<Arc<dyn Array>> {
    match array.data_type() {
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            let results: Result<Vec<Option<String>>> = arr
                .iter()
                .map(|opt| match opt {
                    Some(s) => Ok(Some(compute_sha2(s, bit_length)?)),
                    None => Ok(None),
                })
                .collect();
            let result: StringArray = results?.into_iter().collect();
            Ok(Arc::new(result))
        }
        _ => Err(datafusion::error::DataFusionError::Execution(
            "SHA2 first argument must be a string".to_string(),
        )),
    }
}

fn compute_sha2(s: &str, bit_length: i32) -> Result<String> {
    match bit_length {
        224 => {
            let mut hasher = Sha224::new();
            hasher.update(s.as_bytes());
            Ok(hex::encode(hasher.finalize()))
        }
        256 => {
            let mut hasher = Sha256::new();
            hasher.update(s.as_bytes());
            Ok(hex::encode(hasher.finalize()))
        }
        384 => {
            let mut hasher = Sha384::new();
            hasher.update(s.as_bytes());
            Ok(hex::encode(hasher.finalize()))
        }
        512 => {
            let mut hasher = Sha512::new();
            hasher.update(s.as_bytes());
            Ok(hex::encode(hasher.finalize()))
        }
        _ => Err(datafusion::error::DataFusionError::Execution(format!(
            "SHA2 bit_length must be 224, 256, 384, or 512, got {bit_length}"
        ))),
    }
}

/// Create SHA2 scalar UDF
pub fn sha2() -> ScalarUDF {
    ScalarUDF::from(Sha2Func::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::functions::test_utils::invoke_udf_string;

    #[test]
    fn test_sha1() {
        let func = Sha1Func::new();

        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("hello".to_string())));
        let result = invoke_udf_string(&func, &[input]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(hash))) = result {
            // SHA1 of "hello" is aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d
            assert_eq!(hash, "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d");
        } else {
            panic!("Expected scalar Utf8");
        }
    }

    #[test]
    fn test_sha2_256() {
        let func = Sha2Func::new();

        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("hello".to_string())));
        let bit_length = ColumnarValue::Scalar(ScalarValue::Int64(Some(256)));
        let result = invoke_udf_string(&func, &[input, bit_length]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(hash))) = result {
            // SHA256 of "hello" is 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
            assert_eq!(
                hash,
                "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
            );
        } else {
            panic!("Expected scalar Utf8");
        }
    }

    #[test]
    fn test_sha2_512() {
        let func = Sha2Func::new();

        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("hello".to_string())));
        let bit_length = ColumnarValue::Scalar(ScalarValue::Int64(Some(512)));
        let result = invoke_udf_string(&func, &[input, bit_length]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(hash))) = result {
            // SHA512 of "hello" starts with 9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043
            assert_eq!(
                hash,
                "9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043"
            );
        } else {
            panic!("Expected scalar Utf8");
        }
    }

    #[test]
    fn test_sha1_null() {
        let func = Sha1Func::new();

        let input = ColumnarValue::Scalar(ScalarValue::Utf8(None));
        let result = invoke_udf_string(&func, &[input]).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(None)) = result {
            // Expected NULL
        } else {
            panic!("Expected scalar Utf8 NULL");
        }
    }
}
