// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! NumberConverter helper module for primitive number type converters.
//!
//! This provides common utilities for number converters in Java Kafka Connect style.

use common_trait::config::ConfigDef;
use connect_api::data::{ConnectSchema, SchemaBuilder, SchemaType};
use connect_api::errors::ConnectError;
use connect_api::storage::ConverterConfig;
use serde_json::Value;
use std::collections::HashMap;

/// Static ConfigDef for NumberConverter.
pub struct NumberConverterConfigDef;

impl ConfigDef for NumberConverterConfigDef {
    fn config_def(&self) -> HashMap<String, common_trait::config::ConfigValueEntry> {
        ConverterConfig::new_config_def()
            .build()
            .into_iter()
            .map(|(k, v)| (k, v.to_config_value_entry()))
            .collect()
    }
}

pub static NUMBER_CONVERTER_CONFIG_DEF: NumberConverterConfigDef = NumberConverterConfigDef;

/// Creates an optional schema for the given type.
pub fn optional_schema(schema_type: SchemaType) -> ConnectSchema {
    SchemaBuilder::type_builder(schema_type).optional().build()
}

/// Extracts boolean value from JSON Value.
pub fn extract_bool(value: &Value) -> Result<bool, ConnectError> {
    match value {
        Value::Bool(b) => Ok(*b),
        _ => Err(ConnectError::data(format!(
            "BooleanConverter is not compatible with objects of type {}",
            value_type_name(value)
        ))),
    }
}

/// Extracts i64 value from JSON Value (for Int64).
pub fn extract_i64(value: &Value) -> Result<i64, ConnectError> {
    match value {
        Value::Number(n) => n
            .as_i64()
            .ok_or_else(|| ConnectError::data(format!("Cannot convert to i64: {}", value))),
        _ => Err(ConnectError::data(format!(
            "LongConverter is not compatible with objects of type {}",
            value_type_name(value)
        ))),
    }
}

/// Extracts i32 value from JSON Value (for Int32).
pub fn extract_i32(value: &Value) -> Result<i32, ConnectError> {
    match value {
        Value::Number(n) => n
            .as_i64()
            .and_then(|v| {
                if v >= i32::MIN as i64 && v <= i32::MAX as i64 {
                    Some(v as i32)
                } else {
                    None
                }
            })
            .ok_or_else(|| ConnectError::data(format!("Cannot convert to i32: {}", value))),
        _ => Err(ConnectError::data(format!(
            "IntegerConverter is not compatible with objects of type {}",
            value_type_name(value)
        ))),
    }
}

/// Extracts i16 value from JSON Value (for Int16/Short).
pub fn extract_i16(value: &Value) -> Result<i16, ConnectError> {
    match value {
        Value::Number(n) => n
            .as_i64()
            .and_then(|v| {
                if v >= i16::MIN as i64 && v <= i16::MAX as i64 {
                    Some(v as i16)
                } else {
                    None
                }
            })
            .ok_or_else(|| ConnectError::data(format!("Cannot convert to i16: {}", value))),
        _ => Err(ConnectError::data(format!(
            "ShortConverter is not compatible with objects of type {}",
            value_type_name(value)
        ))),
    }
}

/// Extracts f64 value from JSON Value (for Float64/Double).
pub fn extract_f64(value: &Value) -> Result<f64, ConnectError> {
    match value {
        Value::Number(n) => n
            .as_f64()
            .ok_or_else(|| ConnectError::data(format!("Cannot convert to f64: {}", value))),
        _ => Err(ConnectError::data(format!(
            "DoubleConverter is not compatible with objects of type {}",
            value_type_name(value)
        ))),
    }
}

/// Extracts f32 value from JSON Value (for Float32/Float).
pub fn extract_f32(value: &Value) -> Result<f32, ConnectError> {
    match value {
        Value::Number(n) => n
            .as_f64()
            .and_then(|v| {
                if v >= f32::MIN as f64 && v <= f32::MAX as f64 {
                    Some(v as f32)
                } else {
                    None
                }
            })
            .ok_or_else(|| ConnectError::data(format!("Cannot convert to f32: {}", value))),
        _ => Err(ConnectError::data(format!(
            "FloatConverter is not compatible with objects of type {}",
            value_type_name(value)
        ))),
    }
}

/// Returns the type name for a JSON value.
fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}
