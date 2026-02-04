//! String functions for Snowflake SQL compatibility
//!
//! ## String Split Functions
//! - `SPLIT(string, delimiter)` - Split string into array
//! - `STRTOK(string, delimiters, part_number)` - Extract token at position
//! - `STRTOK_TO_ARRAY(string, delimiters)` - Split into array of tokens
//!
//! ## Regular Expression Functions
//! - `REGEXP_LIKE(string, pattern)` - Check if string matches pattern
//! - `REGEXP_SUBSTR(string, pattern)` - Extract substring matching pattern
//! - `REGEXP_REPLACE(string, pattern, replacement)` - Replace matches
//! - `REGEXP_COUNT(string, pattern)` - Count pattern matches
//!
//! ## String Check Functions
//! - `CONTAINS(string, substring)` - Check if string contains substring
//! - `STARTSWITH(string, prefix)` - Check if string starts with prefix
//! - `ENDSWITH(string, suffix)` - Check if string ends with suffix

use std::any::Any;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use regex::Regex;

// ============================================================================
// SPLIT(string, delimiter)
// ============================================================================

#[derive(Debug)]
pub struct SplitFunc {
    signature: Signature,
}

impl Default for SplitFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SplitFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SplitFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "split"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "SPLIT requires exactly 2 arguments".to_string(),
            ));
        }

        let (string_val, delimiter_val) = match (&args[0], &args[1]) {
            (ColumnarValue::Scalar(s), ColumnarValue::Scalar(d)) => {
                let string_opt = match s {
                    ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) => v.clone(),
                    ScalarValue::Null => None,
                    _ => Some(s.to_string()),
                };
                let delimiter_opt = match d {
                    ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) => v.clone(),
                    ScalarValue::Null => None,
                    _ => Some(d.to_string()),
                };
                (string_opt, delimiter_opt)
            }
            _ => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "SPLIT currently only supports scalar arguments".to_string(),
                ));
            }
        };

        let result = match (string_val, delimiter_val) {
            (Some(s), Some(d)) => {
                let parts: Vec<serde_json::Value> = s
                    .split(&d)
                    .map(|p| serde_json::Value::String(p.to_string()))
                    .collect();
                Some(serde_json::to_string(&parts).unwrap_or_else(|_| "[]".to_string()))
            }
            _ => None,
        };

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

pub fn split() -> ScalarUDF {
    ScalarUDF::from(SplitFunc::new())
}

// ============================================================================
// STRTOK(string, delimiters, part_number)
// ============================================================================

#[derive(Debug)]
pub struct StrtokFunc {
    signature: Signature,
}

impl Default for StrtokFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl StrtokFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for StrtokFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "strtok"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.is_empty() || args.len() > 3 {
            return Err(datafusion::error::DataFusionError::Execution(
                "STRTOK requires 1-3 arguments".to_string(),
            ));
        }

        let string_opt = extract_string_scalar(&args[0])?;

        let delimiters = if args.len() >= 2 {
            extract_string_scalar(&args[1])?.unwrap_or_else(|| " ".to_string())
        } else {
            " ".to_string()
        };

        let part_number = if args.len() == 3 {
            extract_int_scalar(&args[2])?.unwrap_or(1)
        } else {
            1
        };

        let result = match string_opt {
            Some(s) => {
                let tokens: Vec<&str> = s
                    .split(|c| delimiters.contains(c))
                    .filter(|t| !t.is_empty())
                    .collect();

                if part_number < 1 || part_number as usize > tokens.len() {
                    None
                } else {
                    Some(tokens[(part_number - 1) as usize].to_string())
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

pub fn strtok() -> ScalarUDF {
    ScalarUDF::from(StrtokFunc::new())
}

// ============================================================================
// STRTOK_TO_ARRAY(string, delimiters)
// ============================================================================

#[derive(Debug)]
pub struct StrtokToArrayFunc {
    signature: Signature,
}

impl Default for StrtokToArrayFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl StrtokToArrayFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for StrtokToArrayFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "strtok_to_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.is_empty() || args.len() > 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "STRTOK_TO_ARRAY requires 1-2 arguments".to_string(),
            ));
        }

        let string_opt = extract_string_scalar(&args[0])?;

        let delimiters = if args.len() == 2 {
            extract_string_scalar(&args[1])?.unwrap_or_else(|| " ".to_string())
        } else {
            " ".to_string()
        };

        let result = match string_opt {
            Some(s) => {
                let tokens: Vec<serde_json::Value> = s
                    .split(|c| delimiters.contains(c))
                    .filter(|t| !t.is_empty())
                    .map(|t| serde_json::Value::String(t.to_string()))
                    .collect();
                Some(serde_json::to_string(&tokens).unwrap_or_else(|_| "[]".to_string()))
            }
            None => None,
        };

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

pub fn strtok_to_array() -> ScalarUDF {
    ScalarUDF::from(StrtokToArrayFunc::new())
}

// ============================================================================
// REGEXP_LIKE(string, pattern)
// ============================================================================

#[derive(Debug)]
pub struct RegexpLikeFunc {
    signature: Signature,
}

impl Default for RegexpLikeFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpLikeFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpLikeFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_like"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "REGEXP_LIKE requires exactly 2 arguments".to_string(),
            ));
        }

        let string_opt = extract_string_scalar(&args[0])?;
        let pattern_opt = extract_string_scalar(&args[1])?;

        let result = match (string_opt, pattern_opt) {
            (Some(s), Some(p)) => match Regex::new(&p) {
                Ok(re) => Some(re.is_match(&s)),
                Err(_) => None,
            },
            _ => None,
        };

        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

pub fn regexp_like() -> ScalarUDF {
    ScalarUDF::from(RegexpLikeFunc::new())
}

// ============================================================================
// REGEXP_SUBSTR(string, pattern)
// ============================================================================

#[derive(Debug)]
pub struct RegexpSubstrFunc {
    signature: Signature,
}

impl Default for RegexpSubstrFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpSubstrFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpSubstrFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_substr"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "REGEXP_SUBSTR requires exactly 2 arguments".to_string(),
            ));
        }

        let string_opt = extract_string_scalar(&args[0])?;
        let pattern_opt = extract_string_scalar(&args[1])?;

        let result = match (string_opt, pattern_opt) {
            (Some(s), Some(p)) => match Regex::new(&p) {
                Ok(re) => re.find(&s).map(|m| m.as_str().to_string()),
                Err(_) => None,
            },
            _ => None,
        };

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

pub fn regexp_substr() -> ScalarUDF {
    ScalarUDF::from(RegexpSubstrFunc::new())
}

// ============================================================================
// REGEXP_REPLACE(string, pattern, replacement)
// ============================================================================

#[derive(Debug)]
pub struct RegexpReplaceFunc {
    signature: Signature,
}

impl Default for RegexpReplaceFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpReplaceFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpReplaceFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_replace"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 3 {
            return Err(datafusion::error::DataFusionError::Execution(
                "REGEXP_REPLACE requires exactly 3 arguments".to_string(),
            ));
        }

        let string_opt = extract_string_scalar(&args[0])?;
        let pattern_opt = extract_string_scalar(&args[1])?;
        let replacement_opt = extract_string_scalar(&args[2])?;

        let result = match (string_opt, pattern_opt, replacement_opt) {
            (Some(s), Some(p), Some(r)) => match Regex::new(&p) {
                Ok(re) => Some(re.replace_all(&s, r.as_str()).to_string()),
                Err(_) => None,
            },
            _ => None,
        };

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

pub fn regexp_replace() -> ScalarUDF {
    ScalarUDF::from(RegexpReplaceFunc::new())
}

// ============================================================================
// REGEXP_COUNT(string, pattern)
// ============================================================================

#[derive(Debug)]
pub struct RegexpCountFunc {
    signature: Signature,
}

impl Default for RegexpCountFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpCountFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpCountFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_count"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "REGEXP_COUNT requires exactly 2 arguments".to_string(),
            ));
        }

        let string_opt = extract_string_scalar(&args[0])?;
        let pattern_opt = extract_string_scalar(&args[1])?;

        let result = match (string_opt, pattern_opt) {
            (Some(s), Some(p)) => match Regex::new(&p) {
                Ok(re) => Some(re.find_iter(&s).count() as i64),
                Err(_) => None,
            },
            _ => None,
        };

        Ok(ColumnarValue::Scalar(ScalarValue::Int64(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

pub fn regexp_count() -> ScalarUDF {
    ScalarUDF::from(RegexpCountFunc::new())
}

// ============================================================================
// CONTAINS(string, substring)
// ============================================================================

#[derive(Debug)]
pub struct ContainsFunc {
    signature: Signature,
}

impl Default for ContainsFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ContainsFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ContainsFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "contains"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "CONTAINS requires exactly 2 arguments".to_string(),
            ));
        }

        let string_opt = extract_string_scalar(&args[0])?;
        let substring_opt = extract_string_scalar(&args[1])?;

        let result = match (string_opt, substring_opt) {
            (Some(s), Some(sub)) => Some(s.contains(&sub)),
            _ => None,
        };

        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

pub fn contains() -> ScalarUDF {
    ScalarUDF::from(ContainsFunc::new())
}

// ============================================================================
// STARTSWITH(string, prefix)
// ============================================================================

#[derive(Debug)]
pub struct StartswithFunc {
    signature: Signature,
}

impl Default for StartswithFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl StartswithFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for StartswithFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "startswith"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "STARTSWITH requires exactly 2 arguments".to_string(),
            ));
        }

        let string_opt = extract_string_scalar(&args[0])?;
        let prefix_opt = extract_string_scalar(&args[1])?;

        let result = match (string_opt, prefix_opt) {
            (Some(s), Some(prefix)) => Some(s.starts_with(&prefix)),
            _ => None,
        };

        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

pub fn startswith() -> ScalarUDF {
    ScalarUDF::from(StartswithFunc::new())
}

// ============================================================================
// ENDSWITH(string, suffix)
// ============================================================================

#[derive(Debug)]
pub struct EndswithFunc {
    signature: Signature,
}

impl Default for EndswithFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl EndswithFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for EndswithFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "endswith"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "ENDSWITH requires exactly 2 arguments".to_string(),
            ));
        }

        let string_opt = extract_string_scalar(&args[0])?;
        let suffix_opt = extract_string_scalar(&args[1])?;

        let result = match (string_opt, suffix_opt) {
            (Some(s), Some(suffix)) => Some(s.ends_with(&suffix)),
            _ => None,
        };

        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

pub fn endswith() -> ScalarUDF {
    ScalarUDF::from(EndswithFunc::new())
}

// ============================================================================
// Helper functions
// ============================================================================

fn extract_string_scalar(col: &ColumnarValue) -> Result<Option<String>> {
    match col {
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) => Ok(v.clone()),
            ScalarValue::Null => Ok(None),
            _ => Ok(Some(scalar.to_string())),
        },
        ColumnarValue::Array(arr) => {
            if arr.len() == 1 {
                if arr.is_null(0) {
                    return Ok(None);
                }
                if let Some(string_arr) = arr.as_any().downcast_ref::<StringArray>() {
                    return Ok(Some(string_arr.value(0).to_string()));
                }
            }
            Err(datafusion::error::DataFusionError::Execution(
                "Expected scalar string value".to_string(),
            ))
        }
    }
}

fn extract_int_scalar(col: &ColumnarValue) -> Result<Option<i64>> {
    match col {
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Int8(v) => Ok(v.map(|x| x as i64)),
            ScalarValue::Int16(v) => Ok(v.map(|x| x as i64)),
            ScalarValue::Int32(v) => Ok(v.map(|x| x as i64)),
            ScalarValue::Int64(v) => Ok(*v),
            ScalarValue::UInt8(v) => Ok(v.map(|x| x as i64)),
            ScalarValue::UInt16(v) => Ok(v.map(|x| x as i64)),
            ScalarValue::UInt32(v) => Ok(v.map(|x| x as i64)),
            ScalarValue::UInt64(v) => Ok(v.map(|x| x as i64)),
            ScalarValue::Null => Ok(None),
            _ => Err(datafusion::error::DataFusionError::Execution(
                "Expected integer value".to_string(),
            )),
        },
        ColumnarValue::Array(arr) => {
            if arr.len() == 1 {
                if arr.is_null(0) {
                    return Ok(None);
                }
                if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
                    return Ok(Some(int_arr.value(0)));
                }
            }
            Err(datafusion::error::DataFusionError::Execution(
                "Expected scalar integer value".to_string(),
            ))
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split() {
        let func = SplitFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("a,b,c".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(",".to_string()))),
        ];

        let result = func.invoke(&args).unwrap();
        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert_eq!(s, r#"["a","b","c"]"#);
        } else {
            panic!("Expected Utf8 scalar");
        }
    }

    #[test]
    fn test_strtok() {
        let func = StrtokFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("a.b.c".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(".".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
        ];

        let result = func.invoke(&args).unwrap();
        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert_eq!(s, "b");
        } else {
            panic!("Expected Utf8 scalar");
        }
    }

    #[test]
    fn test_strtok_to_array() {
        let func = StrtokToArrayFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("a.b.c".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(".".to_string()))),
        ];

        let result = func.invoke(&args).unwrap();
        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert_eq!(s, r#"["a","b","c"]"#);
        } else {
            panic!("Expected Utf8 scalar");
        }
    }

    #[test]
    fn test_regexp_like() {
        let func = RegexpLikeFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("abc123".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("[0-9]+".to_string()))),
        ];

        let result = func.invoke(&args).unwrap();
        if let ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) = result {
            assert!(b);
        } else {
            panic!("Expected Boolean scalar");
        }
    }

    #[test]
    fn test_regexp_substr() {
        let func = RegexpSubstrFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("abc123def".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("[0-9]+".to_string()))),
        ];

        let result = func.invoke(&args).unwrap();
        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert_eq!(s, "123");
        } else {
            panic!("Expected Utf8 scalar");
        }
    }

    #[test]
    fn test_regexp_replace() {
        let func = RegexpReplaceFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("abc123def".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("[0-9]+".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("XXX".to_string()))),
        ];

        let result = func.invoke(&args).unwrap();
        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert_eq!(s, "abcXXXdef");
        } else {
            panic!("Expected Utf8 scalar");
        }
    }

    #[test]
    fn test_regexp_count() {
        let func = RegexpCountFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("abab".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("ab".to_string()))),
        ];

        let result = func.invoke(&args).unwrap();
        if let ColumnarValue::Scalar(ScalarValue::Int64(Some(n))) = result {
            assert_eq!(n, 2);
        } else {
            panic!("Expected Int64 scalar");
        }
    }

    #[test]
    fn test_contains() {
        let func = ContainsFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("hello world".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("world".to_string()))),
        ];

        let result = func.invoke(&args).unwrap();
        if let ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) = result {
            assert!(b);
        } else {
            panic!("Expected Boolean scalar");
        }
    }

    #[test]
    fn test_startswith() {
        let func = StartswithFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("hello world".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("hello".to_string()))),
        ];

        let result = func.invoke(&args).unwrap();
        if let ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) = result {
            assert!(b);
        } else {
            panic!("Expected Boolean scalar");
        }
    }

    #[test]
    fn test_endswith() {
        let func = EndswithFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("hello world".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("world".to_string()))),
        ];

        let result = func.invoke(&args).unwrap();
        if let ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) = result {
            assert!(b);
        } else {
            panic!("Expected Boolean scalar");
        }
    }
}
