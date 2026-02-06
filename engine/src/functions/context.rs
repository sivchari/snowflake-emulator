//! Context functions (CURRENT_USER, CURRENT_DATABASE, CURRENT_SCHEMA, etc.)
//!
//! Snowflake-compatible context functions that return information about the current session.
//! In this emulator, these return fixed default values.

use std::any::Any;

use arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

// ============================================================================
// CURRENT_USER()
// ============================================================================

/// CURRENT_USER function - Return the current user name
///
/// Syntax: CURRENT_USER()
/// Returns the name of the user executing the current query.
#[derive(Debug)]
pub struct CurrentUserFunc {
    signature: Signature,
}

impl Default for CurrentUserFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl CurrentUserFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Nullary, Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for CurrentUserFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "current_user"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_batch(&self, _args: &[ColumnarValue], _num_rows: usize) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "EMULATOR_USER".to_string(),
        ))))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

/// Create CURRENT_USER scalar UDF
pub fn current_user() -> ScalarUDF {
    ScalarUDF::from(CurrentUserFunc::new())
}

// ============================================================================
// CURRENT_ROLE()
// ============================================================================

/// CURRENT_ROLE function - Return the current role name
///
/// Syntax: CURRENT_ROLE()
/// Returns the name of the role in use for the current session.
#[derive(Debug)]
pub struct CurrentRoleFunc {
    signature: Signature,
}

impl Default for CurrentRoleFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl CurrentRoleFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Nullary, Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for CurrentRoleFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "current_role"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_batch(&self, _args: &[ColumnarValue], _num_rows: usize) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "ACCOUNTADMIN".to_string(),
        ))))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

/// Create CURRENT_ROLE scalar UDF
pub fn current_role() -> ScalarUDF {
    ScalarUDF::from(CurrentRoleFunc::new())
}

// ============================================================================
// CURRENT_DATABASE()
// ============================================================================

/// CURRENT_DATABASE function - Return the current database name
///
/// Syntax: CURRENT_DATABASE()
/// Returns the name of the database in use for the current session.
#[derive(Debug)]
pub struct CurrentDatabaseFunc {
    signature: Signature,
}

impl Default for CurrentDatabaseFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl CurrentDatabaseFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Nullary, Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for CurrentDatabaseFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "current_database"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_batch(&self, _args: &[ColumnarValue], _num_rows: usize) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "EMULATOR_DB".to_string(),
        ))))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

/// Create CURRENT_DATABASE scalar UDF
pub fn current_database() -> ScalarUDF {
    ScalarUDF::from(CurrentDatabaseFunc::new())
}

// ============================================================================
// CURRENT_SCHEMA()
// ============================================================================

/// CURRENT_SCHEMA function - Return the current schema name
///
/// Syntax: CURRENT_SCHEMA()
/// Returns the name of the schema in use for the current session.
#[derive(Debug)]
pub struct CurrentSchemaFunc {
    signature: Signature,
}

impl Default for CurrentSchemaFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl CurrentSchemaFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Nullary, Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for CurrentSchemaFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "current_schema"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_batch(&self, _args: &[ColumnarValue], _num_rows: usize) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "PUBLIC".to_string(),
        ))))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

/// Create CURRENT_SCHEMA scalar UDF
pub fn current_schema() -> ScalarUDF {
    ScalarUDF::from(CurrentSchemaFunc::new())
}

// ============================================================================
// CURRENT_WAREHOUSE()
// ============================================================================

/// CURRENT_WAREHOUSE function - Return the current warehouse name
///
/// Syntax: CURRENT_WAREHOUSE()
/// Returns the name of the warehouse in use for the current session.
#[derive(Debug)]
pub struct CurrentWarehouseFunc {
    signature: Signature,
}

impl Default for CurrentWarehouseFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl CurrentWarehouseFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Nullary, Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for CurrentWarehouseFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "current_warehouse"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_batch(&self, _args: &[ColumnarValue], _num_rows: usize) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "EMULATOR_WH".to_string(),
        ))))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

/// Create CURRENT_WAREHOUSE scalar UDF
pub fn current_warehouse() -> ScalarUDF {
    ScalarUDF::from(CurrentWarehouseFunc::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_current_user() {
        let func = CurrentUserFunc::new();
        let result = func.invoke_batch(&[], 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(user))) = result {
            assert_eq!(user, "EMULATOR_USER");
        } else {
            panic!("Expected scalar Utf8");
        }
    }

    #[test]
    fn test_current_role() {
        let func = CurrentRoleFunc::new();
        let result = func.invoke_batch(&[], 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(role))) = result {
            assert_eq!(role, "ACCOUNTADMIN");
        } else {
            panic!("Expected scalar Utf8");
        }
    }

    #[test]
    fn test_current_database() {
        let func = CurrentDatabaseFunc::new();
        let result = func.invoke_batch(&[], 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(db))) = result {
            assert_eq!(db, "EMULATOR_DB");
        } else {
            panic!("Expected scalar Utf8");
        }
    }

    #[test]
    fn test_current_schema() {
        let func = CurrentSchemaFunc::new();
        let result = func.invoke_batch(&[], 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(schema))) = result {
            assert_eq!(schema, "PUBLIC");
        } else {
            panic!("Expected scalar Utf8");
        }
    }

    #[test]
    fn test_current_warehouse() {
        let func = CurrentWarehouseFunc::new();
        let result = func.invoke_batch(&[], 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(wh))) = result {
            assert_eq!(wh, "EMULATOR_WH");
        } else {
            panic!("Expected scalar Utf8");
        }
    }
}
