//! Aggregate functions for Snowflake SQL compatibility
//!
//! ## Aggregate Functions
//! - `ARRAY_AGG(value)` - Collect values into a JSON array
//! - `OBJECT_AGG(key, value)` - Collect key-value pairs into a JSON object
//! - `LISTAGG(value, delimiter)` - Concatenate values with a delimiter

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{
    function::AccumulatorArgs, function::StateFieldsArgs, Accumulator, AggregateUDF,
    AggregateUDFImpl, Signature, TypeSignature, Volatility,
};

// ============================================================================
// ARRAY_AGG(value)
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayAggFunc {
    signature: Signature,
}

impl Default for ArrayAggFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayAggFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ArrayAggFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // Return as UTF8 string (JSON array)
        Ok(DataType::Utf8)
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> Result<Vec<FieldRef>> {
        // State is stored as a JSON string
        Ok(vec![Arc::new(Field::new(
            format!("{}_state", args.name),
            DataType::Utf8,
            true,
        ))])
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ArrayAggAccumulator::new()))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ArrayAggAccumulator {
    values: Vec<serde_json::Value>,
}

impl ArrayAggAccumulator {
    fn new() -> Self {
        Self { values: Vec::new() }
    }
}

impl Accumulator for ArrayAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = &values[0];
        for i in 0..array.len() {
            if array.is_null(i) {
                self.values.push(serde_json::Value::Null);
            } else {
                let value = array_value_to_json(array, i);
                self.values.push(value);
            }
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let state_array = &states[0];
        if let Some(string_array) = state_array.as_any().downcast_ref::<StringArray>() {
            for i in 0..string_array.len() {
                if !string_array.is_null(i) {
                    let state_str = string_array.value(i);
                    if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(state_str) {
                        self.values.extend(arr);
                    }
                }
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let json_str = serde_json::to_string(&self.values).unwrap_or_else(|_| "[]".to_string());
        Ok(ScalarValue::Utf8(Some(json_str)))
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let json_str = serde_json::to_string(&self.values).unwrap_or_else(|_| "[]".to_string());
        Ok(vec![ScalarValue::Utf8(Some(json_str))])
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.values.capacity() * std::mem::size_of::<serde_json::Value>()
    }
}

pub fn array_agg() -> AggregateUDF {
    AggregateUDF::from(ArrayAggFunc::new())
}

// ============================================================================
// OBJECT_AGG(key, value)
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ObjectAggFunc {
    signature: Signature,
}

impl Default for ObjectAggFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectAggFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ObjectAggFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "object_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format!("{}_state", args.name),
            DataType::Utf8,
            true,
        ))])
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ObjectAggAccumulator::new()))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ObjectAggAccumulator {
    object: serde_json::Map<String, serde_json::Value>,
}

impl ObjectAggAccumulator {
    fn new() -> Self {
        Self {
            object: serde_json::Map::new(),
        }
    }
}

impl Accumulator for ObjectAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() < 2 {
            return Ok(());
        }

        let key_array = &values[0];
        let value_array = &values[1];

        for i in 0..key_array.len() {
            if key_array.is_null(i) {
                continue; // Skip NULL keys
            }

            let key = array_value_to_string(key_array, i);
            let value = if value_array.is_null(i) {
                serde_json::Value::Null
            } else {
                array_value_to_json(value_array, i)
            };

            self.object.insert(key, value);
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let state_array = &states[0];
        if let Some(string_array) = state_array.as_any().downcast_ref::<StringArray>() {
            for i in 0..string_array.len() {
                if !string_array.is_null(i) {
                    let state_str = string_array.value(i);
                    if let Ok(obj) = serde_json::from_str::<
                        serde_json::Map<String, serde_json::Value>,
                    >(state_str)
                    {
                        self.object.extend(obj);
                    }
                }
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let json_str = serde_json::to_string(&self.object).unwrap_or_else(|_| "{}".to_string());
        Ok(ScalarValue::Utf8(Some(json_str)))
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let json_str = serde_json::to_string(&self.object).unwrap_or_else(|_| "{}".to_string());
        Ok(vec![ScalarValue::Utf8(Some(json_str))])
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.object.len() * std::mem::size_of::<(String, serde_json::Value)>()
    }
}

pub fn object_agg() -> AggregateUDF {
    AggregateUDF::from(ObjectAggFunc::new())
}

// ============================================================================
// LISTAGG(value, delimiter)
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ListaggFunc {
    signature: Signature,
}

impl Default for ListaggFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ListaggFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ListaggFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "listagg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> Result<Vec<FieldRef>> {
        // Two state fields: accumulated string and delimiter
        Ok(vec![
            Arc::new(Field::new(
                format!("{}_values", args.name),
                DataType::Utf8,
                true,
            )),
            Arc::new(Field::new(
                format!("{}_delimiter", args.name),
                DataType::Utf8,
                true,
            )),
        ])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        // Extract delimiter from second argument if provided
        let delimiter = if acc_args.exprs.len() >= 2 {
            // Default delimiter if we can't extract at compile time
            ",".to_string()
        } else {
            ",".to_string()
        };

        Ok(Box::new(ListaggAccumulator::new(delimiter)))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ListaggAccumulator {
    values: Vec<String>,
    delimiter: String,
}

impl ListaggAccumulator {
    fn new(delimiter: String) -> Self {
        Self {
            values: Vec::new(),
            delimiter,
        }
    }
}

impl Accumulator for ListaggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let value_array = &values[0];

        // Check if delimiter is provided as second argument
        if values.len() >= 2 {
            if let Some(delim_array) = values[1].as_any().downcast_ref::<StringArray>() {
                if delim_array.len() > 0 && !delim_array.is_null(0) {
                    self.delimiter = delim_array.value(0).to_string();
                }
            }
        }

        for i in 0..value_array.len() {
            if !value_array.is_null(i) {
                let value = array_value_to_string(value_array, i);
                self.values.push(value);
            }
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        // First state field: JSON array of values
        let values_array = &states[0];
        if let Some(string_array) = values_array.as_any().downcast_ref::<StringArray>() {
            for i in 0..string_array.len() {
                if !string_array.is_null(i) {
                    let state_str = string_array.value(i);
                    if let Ok(arr) = serde_json::from_str::<Vec<String>>(state_str) {
                        self.values.extend(arr);
                    }
                }
            }
        }

        // Second state field: delimiter
        if states.len() >= 2 {
            let delim_array = &states[1];
            if let Some(string_array) = delim_array.as_any().downcast_ref::<StringArray>() {
                if string_array.len() > 0 && !string_array.is_null(0) {
                    self.delimiter = string_array.value(0).to_string();
                }
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let result = self.values.join(&self.delimiter);
        Ok(ScalarValue::Utf8(Some(result)))
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let values_json = serde_json::to_string(&self.values).unwrap_or_else(|_| "[]".to_string());
        Ok(vec![
            ScalarValue::Utf8(Some(values_json)),
            ScalarValue::Utf8(Some(self.delimiter.clone())),
        ])
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.values.capacity() * std::mem::size_of::<String>()
            + self.delimiter.capacity()
    }
}

pub fn listagg() -> AggregateUDF {
    AggregateUDF::from(ListaggFunc::new())
}

// ============================================================================
// Helper functions
// ============================================================================

fn array_value_to_json(array: &ArrayRef, index: usize) -> serde_json::Value {
    use datafusion::arrow::array::*;

    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_boolean();
            serde_json::Value::Bool(arr.value(index))
        }
        DataType::Int8 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::Int8Type>();
            serde_json::Value::Number(arr.value(index).into())
        }
        DataType::Int16 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::Int16Type>();
            serde_json::Value::Number(arr.value(index).into())
        }
        DataType::Int32 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::Int32Type>();
            serde_json::Value::Number(arr.value(index).into())
        }
        DataType::Int64 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::Int64Type>();
            serde_json::Value::Number(arr.value(index).into())
        }
        DataType::UInt8 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::UInt8Type>();
            serde_json::Value::Number(arr.value(index).into())
        }
        DataType::UInt16 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::UInt16Type>();
            serde_json::Value::Number(arr.value(index).into())
        }
        DataType::UInt32 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::UInt32Type>();
            serde_json::Value::Number(arr.value(index).into())
        }
        DataType::UInt64 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::UInt64Type>();
            serde_json::Value::Number(arr.value(index).into())
        }
        DataType::Float32 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::Float32Type>();
            serde_json::Number::from_f64(arr.value(index) as f64)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        DataType::Float64 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::Float64Type>();
            serde_json::Number::from_f64(arr.value(index))
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        DataType::Utf8 => {
            let arr = array.as_string::<i32>();
            let s = arr.value(index);
            // Try to parse as JSON first
            if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(s) {
                json_val
            } else {
                serde_json::Value::String(s.to_string())
            }
        }
        DataType::LargeUtf8 => {
            let arr = array.as_string::<i64>();
            let s = arr.value(index);
            if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(s) {
                json_val
            } else {
                serde_json::Value::String(s.to_string())
            }
        }
        _ => serde_json::Value::String(format!("<unsupported: {:?}>", array.data_type())),
    }
}

fn array_value_to_string(array: &ArrayRef, index: usize) -> String {
    use datafusion::arrow::array::*;

    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_boolean();
            arr.value(index).to_string()
        }
        DataType::Int8 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::Int8Type>();
            arr.value(index).to_string()
        }
        DataType::Int16 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::Int16Type>();
            arr.value(index).to_string()
        }
        DataType::Int32 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::Int32Type>();
            arr.value(index).to_string()
        }
        DataType::Int64 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::Int64Type>();
            arr.value(index).to_string()
        }
        DataType::UInt8 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::UInt8Type>();
            arr.value(index).to_string()
        }
        DataType::UInt16 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::UInt16Type>();
            arr.value(index).to_string()
        }
        DataType::UInt32 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::UInt32Type>();
            arr.value(index).to_string()
        }
        DataType::UInt64 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::UInt64Type>();
            arr.value(index).to_string()
        }
        DataType::Float32 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::Float32Type>();
            arr.value(index).to_string()
        }
        DataType::Float64 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::Float64Type>();
            arr.value(index).to_string()
        }
        DataType::Utf8 => {
            let arr = array.as_string::<i32>();
            arr.value(index).to_string()
        }
        DataType::LargeUtf8 => {
            let arr = array.as_string::<i64>();
            arr.value(index).to_string()
        }
        _ => format!("<unsupported: {:?}>", array.data_type()),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use std::sync::Arc;

    #[test]
    fn test_array_agg_accumulator() {
        let mut acc = ArrayAggAccumulator::new();

        // Create test data
        let array: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        acc.update_batch(&[array]).unwrap();

        let result = acc.evaluate().unwrap();
        if let ScalarValue::Utf8(Some(s)) = result {
            assert_eq!(s, "[1,2,3]");
        } else {
            panic!("Expected Utf8 scalar");
        }
    }

    #[test]
    fn test_object_agg_accumulator() {
        let mut acc = ObjectAggAccumulator::new();

        // Create test data
        let keys: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
        let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        acc.update_batch(&[keys, values]).unwrap();

        let result = acc.evaluate().unwrap();
        if let ScalarValue::Utf8(Some(s)) = result {
            // Object key order may vary
            assert!(s.contains("\"a\":1") && s.contains("\"b\":2"));
        } else {
            panic!("Expected Utf8 scalar");
        }
    }

    #[test]
    fn test_listagg_accumulator() {
        let mut acc = ListaggAccumulator::new(",".to_string());

        // Create test data
        let array: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        acc.update_batch(&[array]).unwrap();

        let result = acc.evaluate().unwrap();
        if let ScalarValue::Utf8(Some(s)) = result {
            assert_eq!(s, "a,b,c");
        } else {
            panic!("Expected Utf8 scalar");
        }
    }
}
