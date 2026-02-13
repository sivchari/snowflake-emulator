//! Snowflake-compatible Window Functions
//!
//! This module implements Snowflake-specific window functions that are not
//! natively supported by DataFusion.
//!
//! ## Functions
//!
//! - `CONDITIONAL_TRUE_EVENT(condition)` - Increments counter when condition is TRUE
//! - `CONDITIONAL_CHANGE_EVENT(expr)` - Increments counter when value changes

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, Int64Builder};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::Result;
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::{
    PartitionEvaluator, Signature, Volatility, WindowUDF, WindowUDFImpl,
};

// ============================================================================
// CONDITIONAL_TRUE_EVENT
// ============================================================================

/// CONDITIONAL_TRUE_EVENT window function
///
/// Returns a counter that increments each time the condition evaluates to TRUE.
/// The counter starts at 0 and is independent for each partition.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConditionalTrueEvent {
    signature: Signature,
}

impl Default for ConditionalTrueEvent {
    fn default() -> Self {
        Self::new()
    }
}

impl ConditionalTrueEvent {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Boolean], Volatility::Immutable),
        }
    }
}

impl WindowUDFImpl for ConditionalTrueEvent {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "conditional_true_event"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(ConditionalTrueEventEvaluator::new()))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            field_args.name(),
            DataType::Int64,
            true,
        )))
    }
}

/// Partition evaluator for CONDITIONAL_TRUE_EVENT
#[derive(Debug, PartialEq, Eq, Hash)]
struct ConditionalTrueEventEvaluator {
    // No state needed - we process the entire partition at once
}

impl ConditionalTrueEventEvaluator {
    fn new() -> Self {
        Self {}
    }
}

impl PartitionEvaluator for ConditionalTrueEventEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], _num_rows: usize) -> Result<ArrayRef> {
        let condition_array = &values[0];
        let bool_array = condition_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "CONDITIONAL_TRUE_EVENT requires a boolean argument".to_string(),
                )
            })?;

        let mut builder = Int64Builder::with_capacity(bool_array.len());
        let mut counter: i64 = 0;

        for i in 0..bool_array.len() {
            // NULL is treated as FALSE
            if bool_array.is_valid(i) && bool_array.value(i) {
                counter += 1;
            }
            builder.append_value(counter);
        }

        Ok(Arc::new(builder.finish()))
    }

    fn uses_window_frame(&self) -> bool {
        false
    }

    fn supports_bounded_execution(&self) -> bool {
        false
    }
}

// ============================================================================
// CONDITIONAL_CHANGE_EVENT
// ============================================================================

/// CONDITIONAL_CHANGE_EVENT window function
///
/// Returns a counter that increments each time the value changes from the previous row.
/// The counter starts at 0 and is independent for each partition.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConditionalChangeEvent {
    signature: Signature,
}

impl Default for ConditionalChangeEvent {
    fn default() -> Self {
        Self::new()
    }
}

impl ConditionalChangeEvent {
    pub fn new() -> Self {
        Self {
            // Accept any comparable type
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl WindowUDFImpl for ConditionalChangeEvent {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "conditional_change_event"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(ConditionalChangeEventEvaluator::new()))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            field_args.name(),
            DataType::Int64,
            true,
        )))
    }
}

/// Partition evaluator for CONDITIONAL_CHANGE_EVENT
#[derive(Debug, PartialEq, Eq, Hash)]
struct ConditionalChangeEventEvaluator {
    // No state needed - we process the entire partition at once
}

impl ConditionalChangeEventEvaluator {
    fn new() -> Self {
        Self {}
    }
}

impl PartitionEvaluator for ConditionalChangeEventEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], _num_rows: usize) -> Result<ArrayRef> {
        let value_array = &values[0];
        let len = value_array.len();

        let mut builder = Int64Builder::with_capacity(len);
        let mut counter: i64 = 0;

        // First row: counter = 0 (no previous value to compare)
        if len > 0 {
            builder.append_value(counter);
        }

        // Subsequent rows: compare with previous value
        for i in 1..len {
            let current_is_null = value_array.is_null(i);
            let prev_is_null = value_array.is_null(i - 1);

            let changed = match (current_is_null, prev_is_null) {
                (true, true) => false, // Both NULL -> no change
                (true, false) => true, // NULL vs non-NULL -> change
                (false, true) => true, // Non-NULL vs NULL -> change
                (false, false) => {
                    // Compare actual values using Arrow's comparison
                    !array_values_equal(value_array, i, i - 1)
                }
            };

            if changed {
                counter += 1;
            }
            builder.append_value(counter);
        }

        Ok(Arc::new(builder.finish()))
    }

    fn uses_window_frame(&self) -> bool {
        false
    }

    fn supports_bounded_execution(&self) -> bool {
        false
    }
}

/// Compare two values in an array for equality
fn array_values_equal(array: &ArrayRef, idx1: usize, idx2: usize) -> bool {
    use datafusion::arrow::array::*;

    macro_rules! compare_primitive {
        ($array_type:ty) => {
            if let Some(arr) = array.as_any().downcast_ref::<$array_type>() {
                return arr.value(idx1) == arr.value(idx2);
            }
        };
    }

    // Handle common types
    compare_primitive!(Int8Array);
    compare_primitive!(Int16Array);
    compare_primitive!(Int32Array);
    compare_primitive!(Int64Array);
    compare_primitive!(UInt8Array);
    compare_primitive!(UInt16Array);
    compare_primitive!(UInt32Array);
    compare_primitive!(UInt64Array);
    compare_primitive!(Float32Array);
    compare_primitive!(Float64Array);
    compare_primitive!(BooleanArray);

    // String types
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return arr.value(idx1) == arr.value(idx2);
    }
    if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
        return arr.value(idx1) == arr.value(idx2);
    }
    if let Some(arr) = array.as_any().downcast_ref::<StringViewArray>() {
        return arr.value(idx1) == arr.value(idx2);
    }

    // Date/Time types
    compare_primitive!(Date32Array);
    compare_primitive!(Date64Array);
    compare_primitive!(TimestampSecondArray);
    compare_primitive!(TimestampMillisecondArray);
    compare_primitive!(TimestampMicrosecondArray);
    compare_primitive!(TimestampNanosecondArray);

    // Default: assume not equal (safer)
    false
}

// ============================================================================
// Public API
// ============================================================================

/// Create CONDITIONAL_TRUE_EVENT window UDF
pub fn conditional_true_event() -> WindowUDF {
    WindowUDF::from(ConditionalTrueEvent::new())
}

/// Create CONDITIONAL_CHANGE_EVENT window UDF
pub fn conditional_change_event() -> WindowUDF {
    WindowUDF::from(ConditionalChangeEvent::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;

    #[test]
    fn test_conditional_true_event_basic() {
        let mut evaluator = ConditionalTrueEventEvaluator::new();

        // Create boolean array: [true, false, true, true, false]
        let bool_array: ArrayRef = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(true),
            Some(true),
            Some(false),
        ]));

        let result = evaluator.evaluate_all(&[bool_array], 5).unwrap();
        let result_array = result.as_any().downcast_ref::<Int64Array>().unwrap();

        // Expected: [1, 1, 2, 3, 3]
        assert_eq!(result_array.value(0), 1); // true -> counter = 1
        assert_eq!(result_array.value(1), 1); // false -> counter = 1
        assert_eq!(result_array.value(2), 2); // true -> counter = 2
        assert_eq!(result_array.value(3), 3); // true -> counter = 3
        assert_eq!(result_array.value(4), 3); // false -> counter = 3
    }

    #[test]
    fn test_conditional_true_event_with_nulls() {
        let mut evaluator = ConditionalTrueEventEvaluator::new();

        // Create boolean array with NULLs: [true, NULL, true, NULL]
        let bool_array: ArrayRef =
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(true), None]));

        let result = evaluator.evaluate_all(&[bool_array], 4).unwrap();
        let result_array = result.as_any().downcast_ref::<Int64Array>().unwrap();

        // Expected: [1, 1, 2, 2] (NULL treated as FALSE)
        assert_eq!(result_array.value(0), 1);
        assert_eq!(result_array.value(1), 1);
        assert_eq!(result_array.value(2), 2);
        assert_eq!(result_array.value(3), 2);
    }

    #[test]
    fn test_conditional_change_event_basic() {
        let mut evaluator = ConditionalChangeEventEvaluator::new();

        // Create int array: [1, 1, 2, 2, 3]
        let int_array: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 2, 2, 3]));

        let result = evaluator.evaluate_all(&[int_array], 5).unwrap();
        let result_array = result.as_any().downcast_ref::<Int64Array>().unwrap();

        // Expected: [0, 0, 1, 1, 2]
        assert_eq!(result_array.value(0), 0); // first row
        assert_eq!(result_array.value(1), 0); // 1 == 1, no change
        assert_eq!(result_array.value(2), 1); // 2 != 1, change
        assert_eq!(result_array.value(3), 1); // 2 == 2, no change
        assert_eq!(result_array.value(4), 2); // 3 != 2, change
    }

    #[test]
    fn test_conditional_change_event_with_nulls() {
        let mut evaluator = ConditionalChangeEventEvaluator::new();

        // Create int array with NULLs: [1, NULL, NULL, 2]
        let int_array: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, None, Some(2)]));

        let result = evaluator.evaluate_all(&[int_array], 4).unwrap();
        let result_array = result.as_any().downcast_ref::<Int64Array>().unwrap();

        // Expected: [0, 1, 1, 2]
        assert_eq!(result_array.value(0), 0); // first row
        assert_eq!(result_array.value(1), 1); // NULL != 1, change
        assert_eq!(result_array.value(2), 1); // NULL == NULL, no change
        assert_eq!(result_array.value(3), 2); // 2 != NULL, change
    }

    #[test]
    fn test_conditional_change_event_strings() {
        let mut evaluator = ConditionalChangeEventEvaluator::new();

        // Create string array: ["a", "a", "b", "b", "c"]
        let str_array: ArrayRef = Arc::new(datafusion::arrow::array::StringArray::from(vec![
            "a", "a", "b", "b", "c",
        ]));

        let result = evaluator.evaluate_all(&[str_array], 5).unwrap();
        let result_array = result.as_any().downcast_ref::<Int64Array>().unwrap();

        // Expected: [0, 0, 1, 1, 2]
        assert_eq!(result_array.value(0), 0);
        assert_eq!(result_array.value(1), 0);
        assert_eq!(result_array.value(2), 1);
        assert_eq!(result_array.value(3), 1);
        assert_eq!(result_array.value(4), 2);
    }
}
