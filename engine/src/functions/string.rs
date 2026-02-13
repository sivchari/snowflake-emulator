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
//!
//! ## String Manipulation Functions
//! - `CHARINDEX(substring, string, start_pos)` - Find position of substring
//! - `REVERSE(string)` - Reverse string
//! - `LPAD(string, length, pad_string)` - Left pad string
//! - `RPAD(string, length, pad_string)` - Right pad string
//! - `TRANSLATE(string, source_chars, target_chars)` - Replace characters

use std::any::Any;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use regex::Regex;

// ============================================================================
// SPLIT(string, delimiter)
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "SPLIT requires exactly 2 arguments".to_string(),
            ));
        }

        let (string_val, delimiter_val) = match (&args.args[0], &args.args[1]) {
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

#[derive(Debug, PartialEq, Eq, Hash)]
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() || args.args.len() > 3 {
            return Err(datafusion::error::DataFusionError::Execution(
                "STRTOK requires 1-3 arguments".to_string(),
            ));
        }

        let string_opt = extract_string_scalar(&args.args[0])?;

        let delimiters = if args.args.len() >= 2 {
            extract_string_scalar(&args.args[1])?.unwrap_or_else(|| " ".to_string())
        } else {
            " ".to_string()
        };

        let part_number = if args.args.len() == 3 {
            extract_int_scalar(&args.args[2])?.unwrap_or(1)
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

#[derive(Debug, PartialEq, Eq, Hash)]
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() || args.args.len() > 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "STRTOK_TO_ARRAY requires 1-2 arguments".to_string(),
            ));
        }

        let string_opt = extract_string_scalar(&args.args[0])?;

        let delimiters = if args.args.len() == 2 {
            extract_string_scalar(&args.args[1])?.unwrap_or_else(|| " ".to_string())
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

#[derive(Debug, PartialEq, Eq, Hash)]
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "REGEXP_LIKE requires exactly 2 arguments".to_string(),
            ));
        }

        let string_opt = extract_string_scalar(&args.args[0])?;
        let pattern_opt = extract_string_scalar(&args.args[1])?;

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

#[derive(Debug, PartialEq, Eq, Hash)]
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "REGEXP_SUBSTR requires exactly 2 arguments".to_string(),
            ));
        }

        let string_opt = extract_string_scalar(&args.args[0])?;
        let pattern_opt = extract_string_scalar(&args.args[1])?;

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

#[derive(Debug, PartialEq, Eq, Hash)]
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 3 {
            return Err(datafusion::error::DataFusionError::Execution(
                "REGEXP_REPLACE requires exactly 3 arguments".to_string(),
            ));
        }

        let string_opt = extract_string_scalar(&args.args[0])?;
        let pattern_opt = extract_string_scalar(&args.args[1])?;
        let replacement_opt = extract_string_scalar(&args.args[2])?;

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

#[derive(Debug, PartialEq, Eq, Hash)]
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "REGEXP_COUNT requires exactly 2 arguments".to_string(),
            ));
        }

        let string_opt = extract_string_scalar(&args.args[0])?;
        let pattern_opt = extract_string_scalar(&args.args[1])?;

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

#[derive(Debug, PartialEq, Eq, Hash)]
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "CONTAINS requires exactly 2 arguments".to_string(),
            ));
        }

        let string_opt = extract_string_scalar(&args.args[0])?;
        let substring_opt = extract_string_scalar(&args.args[1])?;

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

#[derive(Debug, PartialEq, Eq, Hash)]
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "STARTSWITH requires exactly 2 arguments".to_string(),
            ));
        }

        let string_opt = extract_string_scalar(&args.args[0])?;
        let prefix_opt = extract_string_scalar(&args.args[1])?;

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

#[derive(Debug, PartialEq, Eq, Hash)]
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "ENDSWITH requires exactly 2 arguments".to_string(),
            ));
        }

        let string_opt = extract_string_scalar(&args.args[0])?;
        let suffix_opt = extract_string_scalar(&args.args[1])?;

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
// CHARINDEX(substring, string, [start_pos])
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CharindexFunc {
    signature: Signature,
}

impl Default for CharindexFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl CharindexFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CharindexFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "charindex"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() < 2 || args.args.len() > 3 {
            return Err(datafusion::error::DataFusionError::Execution(
                "CHARINDEX requires 2 or 3 arguments".to_string(),
            ));
        }

        let substring_opt = extract_string_scalar(&args.args[0])?;
        let string_opt = extract_string_scalar(&args.args[1])?;
        let start_pos = if args.args.len() == 3 {
            extract_int_scalar(&args.args[2])?.unwrap_or(1)
        } else {
            1
        };

        let result = match (substring_opt, string_opt) {
            (Some(sub), Some(s)) => {
                if start_pos < 1 || start_pos as usize > s.len() {
                    Some(0i64)
                } else {
                    let search_start = (start_pos - 1) as usize;
                    match s[search_start..].find(&sub) {
                        Some(pos) => Some((pos + search_start + 1) as i64),
                        None => Some(0i64),
                    }
                }
            }
            _ => None,
        };

        Ok(ColumnarValue::Scalar(ScalarValue::Int64(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

pub fn charindex() -> ScalarUDF {
    ScalarUDF::from(CharindexFunc::new())
}

// ============================================================================
// REVERSE(string)
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ReverseFunc {
    signature: Signature,
}

impl Default for ReverseFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ReverseFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ReverseFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "reverse"
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
                "REVERSE requires exactly 1 argument".to_string(),
            ));
        }

        let string_opt = extract_string_scalar(&args.args[0])?;

        let result = string_opt.map(|s| s.chars().rev().collect::<String>());

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

pub fn reverse() -> ScalarUDF {
    ScalarUDF::from(ReverseFunc::new())
}

// ============================================================================
// LPAD(string, length, pad_string)
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct LpadFunc {
    signature: Signature,
}

impl Default for LpadFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl LpadFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for LpadFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "lpad"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() < 2 || args.args.len() > 3 {
            return Err(datafusion::error::DataFusionError::Execution(
                "LPAD requires 2 or 3 arguments".to_string(),
            ));
        }

        let string_opt = extract_string_scalar(&args.args[0])?;
        let length = extract_int_scalar(&args.args[1])?.unwrap_or(0);
        let pad_string = if args.args.len() == 3 {
            extract_string_scalar(&args.args[2])?.unwrap_or_else(|| " ".to_string())
        } else {
            " ".to_string()
        };

        let result = match string_opt {
            Some(s) => {
                let current_len = s.chars().count();
                let target_len = length as usize;
                if current_len >= target_len {
                    Some(s.chars().take(target_len).collect::<String>())
                } else if pad_string.is_empty() {
                    Some(s)
                } else {
                    let pad_len = target_len - current_len;
                    let pad_chars: String = pad_string.chars().cycle().take(pad_len).collect();
                    Some(format!("{pad_chars}{s}"))
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

pub fn lpad() -> ScalarUDF {
    ScalarUDF::from(LpadFunc::new())
}

// ============================================================================
// RPAD(string, length, pad_string)
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RpadFunc {
    signature: Signature,
}

impl Default for RpadFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RpadFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for RpadFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "rpad"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() < 2 || args.args.len() > 3 {
            return Err(datafusion::error::DataFusionError::Execution(
                "RPAD requires 2 or 3 arguments".to_string(),
            ));
        }

        let string_opt = extract_string_scalar(&args.args[0])?;
        let length = extract_int_scalar(&args.args[1])?.unwrap_or(0);
        let pad_string = if args.args.len() == 3 {
            extract_string_scalar(&args.args[2])?.unwrap_or_else(|| " ".to_string())
        } else {
            " ".to_string()
        };

        let result = match string_opt {
            Some(s) => {
                let current_len = s.chars().count();
                let target_len = length as usize;
                if current_len >= target_len {
                    Some(s.chars().take(target_len).collect::<String>())
                } else if pad_string.is_empty() {
                    Some(s)
                } else {
                    let pad_len = target_len - current_len;
                    let pad_chars: String = pad_string.chars().cycle().take(pad_len).collect();
                    Some(format!("{s}{pad_chars}"))
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

pub fn rpad() -> ScalarUDF {
    ScalarUDF::from(RpadFunc::new())
}

// ============================================================================
// TRANSLATE(string, source_chars, target_chars)
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TranslateFunc {
    signature: Signature,
}

impl Default for TranslateFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl TranslateFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for TranslateFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "translate"
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
                "TRANSLATE requires exactly 3 arguments".to_string(),
            ));
        }

        let string_opt = extract_string_scalar(&args.args[0])?;
        let source_chars_opt = extract_string_scalar(&args.args[1])?;
        let target_chars_opt = extract_string_scalar(&args.args[2])?;

        let result = match (string_opt, source_chars_opt, target_chars_opt) {
            (Some(s), Some(source), Some(target)) => {
                let source_chars: Vec<char> = source.chars().collect();
                let target_chars: Vec<char> = target.chars().collect();

                let translated: String = s
                    .chars()
                    .map(|c| {
                        if let Some(idx) = source_chars.iter().position(|&sc| sc == c) {
                            if idx < target_chars.len() {
                                target_chars[idx]
                            } else {
                                // If target is shorter, remove the character
                                '\0'
                            }
                        } else {
                            c
                        }
                    })
                    .filter(|&c| c != '\0')
                    .collect();

                Some(translated)
            }
            _ => None,
        };

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

pub fn translate() -> ScalarUDF {
    ScalarUDF::from(TranslateFunc::new())
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
    use crate::functions::test_utils::{invoke_udf_bool, invoke_udf_int64, invoke_udf_string};

    #[test]
    fn test_split() {
        let func = SplitFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("a,b,c".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(",".to_string()))),
        ];

        let result = invoke_udf_string(&func, &args).unwrap();
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

        let result = invoke_udf_string(&func, &args).unwrap();
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

        let result = invoke_udf_string(&func, &args).unwrap();
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

        let result = invoke_udf_bool(&func, &args).unwrap();
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

        let result = invoke_udf_string(&func, &args).unwrap();
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

        let result = invoke_udf_string(&func, &args).unwrap();
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

        let result = invoke_udf_int64(&func, &args).unwrap();
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

        let result = invoke_udf_bool(&func, &args).unwrap();
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

        let result = invoke_udf_bool(&func, &args).unwrap();
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

        let result = invoke_udf_bool(&func, &args).unwrap();
        if let ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) = result {
            assert!(b);
        } else {
            panic!("Expected Boolean scalar");
        }
    }

    #[test]
    fn test_charindex() {
        let func = CharindexFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("bar".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("foobar".to_string()))),
        ];

        let result = invoke_udf_int64(&func, &args).unwrap();
        if let ColumnarValue::Scalar(ScalarValue::Int64(Some(n))) = result {
            assert_eq!(n, 4);
        } else {
            panic!("Expected Int64 scalar");
        }
    }

    #[test]
    fn test_charindex_with_start() {
        let func = CharindexFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("o".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("foobar".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),
        ];

        let result = invoke_udf_int64(&func, &args).unwrap();
        if let ColumnarValue::Scalar(ScalarValue::Int64(Some(n))) = result {
            assert_eq!(n, 3); // Second 'o' at position 3
        } else {
            panic!("Expected Int64 scalar");
        }
    }

    #[test]
    fn test_charindex_not_found() {
        let func = CharindexFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("xyz".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("foobar".to_string()))),
        ];

        let result = invoke_udf_int64(&func, &args).unwrap();
        if let ColumnarValue::Scalar(ScalarValue::Int64(Some(n))) = result {
            assert_eq!(n, 0);
        } else {
            panic!("Expected Int64 scalar");
        }
    }

    #[test]
    fn test_reverse() {
        let func = ReverseFunc::new();
        let args = vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "hello".to_string(),
        )))];

        let result = invoke_udf_string(&func, &args).unwrap();
        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert_eq!(s, "olleh");
        } else {
            panic!("Expected Utf8 scalar");
        }
    }

    #[test]
    fn test_lpad() {
        let func = LpadFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("123".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("0".to_string()))),
        ];

        let result = invoke_udf_string(&func, &args).unwrap();
        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert_eq!(s, "00123");
        } else {
            panic!("Expected Utf8 scalar");
        }
    }

    #[test]
    fn test_lpad_default_pad() {
        let func = LpadFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("ab".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
        ];

        let result = invoke_udf_string(&func, &args).unwrap();
        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert_eq!(s, "   ab");
        } else {
            panic!("Expected Utf8 scalar");
        }
    }

    #[test]
    fn test_rpad() {
        let func = RpadFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("123".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("0".to_string()))),
        ];

        let result = invoke_udf_string(&func, &args).unwrap();
        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert_eq!(s, "12300");
        } else {
            panic!("Expected Utf8 scalar");
        }
    }

    #[test]
    fn test_translate() {
        let func = TranslateFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("abc".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("abc".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("123".to_string()))),
        ];

        let result = invoke_udf_string(&func, &args).unwrap();
        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert_eq!(s, "123");
        } else {
            panic!("Expected Utf8 scalar");
        }
    }

    #[test]
    fn test_translate_partial() {
        let func = TranslateFunc::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("hello".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("aeiou".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("12345".to_string()))),
        ];

        let result = invoke_udf_string(&func, &args).unwrap();
        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result {
            assert_eq!(s, "h2ll4"); // e->2, o->4
        } else {
            panic!("Expected Utf8 scalar");
        }
    }
}
