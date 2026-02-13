//! Test utilities for UDF testing
//!
//! Provides helper functions for testing scalar UDFs with the DataFusion 52+ API.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};

/// Helper function to invoke a scalar UDF for testing purposes.
///
/// This creates the necessary `ScalarFunctionArgs` struct and calls
/// `invoke_with_args` on the provided UDF implementation.
pub fn invoke_udf<T: ScalarUDFImpl>(
    udf: &T,
    args: &[ColumnarValue],
    return_type: DataType,
) -> datafusion::common::Result<ColumnarValue> {
    let args_vec: Vec<ColumnarValue> = args.to_vec();
    let arg_fields: Vec<Arc<Field>> = args
        .iter()
        .enumerate()
        .map(|(i, cv)| {
            let dt = match cv {
                ColumnarValue::Scalar(sv) => sv.data_type(),
                ColumnarValue::Array(arr) => arr.data_type().clone(),
            };
            Arc::new(Field::new(format!("arg{}", i), dt, true))
        })
        .collect();

    let function_args = ScalarFunctionArgs {
        args: args_vec,
        arg_fields,
        number_rows: 1,
        return_field: Arc::new(Field::new("result", return_type, true)),
        config_options: Arc::new(ConfigOptions::default()),
    };

    udf.invoke_with_args(function_args)
}

/// Helper function to invoke a scalar UDF with boolean return type.
pub fn invoke_udf_bool<T: ScalarUDFImpl>(
    udf: &T,
    args: &[ColumnarValue],
) -> datafusion::common::Result<ColumnarValue> {
    invoke_udf(udf, args, DataType::Boolean)
}

/// Helper function to invoke a scalar UDF with string (Utf8) return type.
pub fn invoke_udf_string<T: ScalarUDFImpl>(
    udf: &T,
    args: &[ColumnarValue],
) -> datafusion::common::Result<ColumnarValue> {
    invoke_udf(udf, args, DataType::Utf8)
}

/// Helper function to invoke a scalar UDF with Int64 return type.
pub fn invoke_udf_int64<T: ScalarUDFImpl>(
    udf: &T,
    args: &[ColumnarValue],
) -> datafusion::common::Result<ColumnarValue> {
    invoke_udf(udf, args, DataType::Int64)
}

/// Helper function to invoke a scalar UDF with Float64 return type.
pub fn invoke_udf_float64<T: ScalarUDFImpl>(
    udf: &T,
    args: &[ColumnarValue],
) -> datafusion::common::Result<ColumnarValue> {
    invoke_udf(udf, args, DataType::Float64)
}

/// Helper function to invoke a scalar UDF with Date32 return type.
pub fn invoke_udf_date32<T: ScalarUDFImpl>(
    udf: &T,
    args: &[ColumnarValue],
) -> datafusion::common::Result<ColumnarValue> {
    invoke_udf(udf, args, DataType::Date32)
}

/// Helper function to invoke a scalar UDF with TimestampNanosecond return type.
pub fn invoke_udf_timestamp<T: ScalarUDFImpl>(
    udf: &T,
    args: &[ColumnarValue],
) -> datafusion::common::Result<ColumnarValue> {
    invoke_udf(
        udf,
        args,
        DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None),
    )
}
