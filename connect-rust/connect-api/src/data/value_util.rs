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

//! Value utilities for Kafka Connect.

use crate::data::{Schema, SchemaType};
use crate::errors::ConnectError;
use serde_json::Value;

/// Value utilities for converting and validating values.
pub struct ValueUtil;

impl ValueUtil {
    /// Validates a value against a schema type.
    pub fn validate_type(value: &Value, expected_type: SchemaType) -> Result<(), ConnectError> {
        match expected_type {
            SchemaType::Int8 | SchemaType::Int16 | SchemaType::Int32 | SchemaType::Int64 => {
                if value.as_i64().is_none() && !value.is_null() {
                    return Err(ConnectError::data(format!(
                        "Expected integer type, got: {}",
                        value
                    )));
                }
            }
            SchemaType::Float32 | SchemaType::Float64 => {
                if value.as_f64().is_none() && !value.is_null() {
                    return Err(ConnectError::data(format!(
                        "Expected float type, got: {}",
                        value
                    )));
                }
            }
            SchemaType::Boolean => {
                if !matches!(value, Value::Bool(_)) && !value.is_null() {
                    return Err(ConnectError::data(format!(
                        "Expected boolean type, got: {}",
                        value
                    )));
                }
            }
            SchemaType::String => {
                if !value.is_string() && !value.is_null() {
                    return Err(ConnectError::data(format!(
                        "Expected string type, got: {}",
                        value
                    )));
                }
            }
            SchemaType::Bytes => {
                if !matches!(value, Value::String(_) | Value::Array(_)) && !value.is_null() {
                    return Err(ConnectError::data(format!(
                        "Expected bytes type, got: {}",
                        value
                    )));
                }
            }
            SchemaType::Array => {
                if !value.is_array() && !value.is_null() {
                    return Err(ConnectError::data(format!(
                        "Expected array type, got: {}",
                        value
                    )));
                }
            }
            SchemaType::Map => {
                if !value.is_object() && !value.is_null() {
                    return Err(ConnectError::data(format!(
                        "Expected map type, got: {}",
                        value
                    )));
                }
            }
            SchemaType::Struct => {
                if !value.is_object() && !value.is_null() {
                    return Err(ConnectError::data(format!(
                        "Expected struct type, got: {}",
                        value
                    )));
                }
            }
        }
        Ok(())
    }

    /// Checks if a value is of a specific schema type.
    pub fn is_type(value: &Value, schema_type: SchemaType) -> bool {
        match schema_type {
            SchemaType::Boolean => matches!(value, Value::Bool(_)),
            SchemaType::Int8 | SchemaType::Int16 | SchemaType::Int32 | SchemaType::Int64 => {
                value.as_i64().is_some()
            }
            SchemaType::Float32 | SchemaType::Float64 => value.as_f64().is_some(),
            SchemaType::String => value.is_string(),
            SchemaType::Bytes => matches!(value, Value::String(_) | Value::Array(_)),
            SchemaType::Array => value.is_array(),
            SchemaType::Map => value.is_object(),
            SchemaType::Struct => value.is_object(),
        }
    }

    /// Converts a value to the closest JSON number representation.
    pub fn to_number(value: &Value) -> Option<Value> {
        match value {
            Value::Number(n) => Some(value.clone()),
            Value::String(s) => {
                if let Ok(i) = s.parse::<i64>() {
                    Some(Value::Number(i.into()))
                } else if let Ok(f) = s.parse::<f64>() {
                    serde_json::Number::from_f64(f).map(Value::Number)
                } else {
                    None
                }
            }
            Value::Bool(b) => Some(Value::Number(if *b { 1 } else { 0 }.into())),
            _ => None,
        }
    }

    /// Deep clones a value.
    pub fn deep_clone(value: &Value) -> Value {
        value.clone()
    }

    /// Merges two JSON values deeply.
    pub fn deep_merge(base: &Value, overlay: &Value) -> Value {
        match (base, overlay) {
            (Value::Object(base_obj), Value::Object(overlay_obj)) => {
                let mut merged = base_obj.clone();
                for (k, v) in overlay_obj {
                    if let Some(base_v) = merged.get(k) {
                        merged.insert(k.clone(), Self::deep_merge(base_v, v));
                    } else {
                        merged.insert(k.clone(), v.clone());
                    }
                }
                Value::Object(merged)
            }
            (_, overlay) => overlay.clone(),
        }
    }

    /// Returns the type of a value as a string.
    pub fn type_name(value: &Value) -> &'static str {
        match value {
            Value::Null => "null",
            Value::Bool(_) => "boolean",
            Value::Number(_) => "number",
            Value::String(_) => "string",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
        }
    }

    /// Checks if two values are deeply equal.
    pub fn deep_equals(a: &Value, b: &Value) -> bool {
        a == b
    }

    /// Parses a string value to a Connect value.
    pub fn parse_string_value(s: &str) -> Result<Value, ConnectError> {
        // Try JSON parse first
        if let Ok(v) = serde_json::from_str(s) {
            return Ok(v);
        }

        // Try quoted string
        if s.starts_with('"') && s.ends_with('"') && s.len() > 1 {
            return Ok(Value::String(s[1..s.len() - 1].to_string()));
        }

        // Try boolean
        if s == "true" {
            return Ok(Value::Bool(true));
        }
        if s == "false" {
            return Ok(Value::Bool(false));
        }

        // Try number
        if let Ok(i) = s.parse::<i64>() {
            return Ok(Value::Number(i.into()));
        }
        if let Ok(f) = s.parse::<f64>() {
            if let Some(n) = serde_json::Number::from_f64(f) {
                return Ok(Value::Number(n));
            }
        }

        // Default to string
        Ok(Value::String(s.to_string()))
    }

    /// Converts a value to its string representation.
    pub fn to_string_repr(value: &Value) -> String {
        match value {
            Value::Null => "null".to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Number(n) => n.to_string(),
            Value::String(s) => format!("\"{}\"", s),
            Value::Array(arr) => {
                let items: Vec<String> = arr.iter().map(Self::to_string_repr).collect();
                format!("[{}]", items.join(", "))
            }
            Value::Object(obj) => {
                let entries: Vec<String> = obj
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, Self::to_string_repr(v)))
                    .collect();
                format!("{{{}}}", entries.join(", "))
            }
        }
    }
}

/// ValueValidator for validating values against schemas.
pub struct ValueValidator;

impl ValueValidator {
    /// Validates a value against a schema.
    pub fn validate(schema: &dyn Schema, value: &Value) -> Result<(), ConnectError> {
        // Null check
        if value.is_null() {
            if !schema.is_optional() && schema.default_value().is_none() {
                return Err(ConnectError::data("Null value for required schema"));
            }
            return Ok(());
        }

        // Type validation
        ValueUtil::validate_type(value, schema.r#type())?;

        // Additional validation for complex types
        match schema.r#type() {
            SchemaType::Struct => Self::validate_struct(schema, value),
            SchemaType::Array => Self::validate_array(schema, value),
            SchemaType::Map => Self::validate_map(schema, value),
            _ => Ok(()),
        }
    }

    fn validate_struct(schema: &dyn Schema, value: &Value) -> Result<(), ConnectError> {
        let obj = value
            .as_object()
            .ok_or_else(|| ConnectError::data("Struct value must be an object"))?;

        // Check required fields
        for field in schema.fields() {
            if !field.is_optional() && field.default_value().is_none() {
                if !obj.contains_key(field.name()) {
                    return Err(ConnectError::data(format!(
                        "Missing required field: {}",
                        field.name()
                    )));
                }
            }
        }

        Ok(())
    }

    fn validate_array(schema: &dyn Schema, value: &Value) -> Result<(), ConnectError> {
        let arr = value
            .as_array()
            .ok_or_else(|| ConnectError::data("Array value must be an array"))?;

        if let Some(value_schema) = schema.value_schema() {
            for item in arr {
                Self::validate(value_schema, item)?;
            }
        }

        Ok(())
    }

    fn validate_map(schema: &dyn Schema, value: &Value) -> Result<(), ConnectError> {
        let obj = value
            .as_object()
            .ok_or_else(|| ConnectError::data("Map value must be an object"))?;

        if let Some(value_schema) = schema.value_schema() {
            for v in obj.values() {
                Self::validate(value_schema, v)?;
            }
        }

        Ok(())
    }
}

/// ValueComparator for comparing values.
pub struct ValueComparator;

impl ValueComparator {
    /// Compares two values and returns their ordering.
    pub fn compare(a: &Value, b: &Value) -> std::cmp::Ordering {
        match (a, b) {
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            (Value::Null, _) => std::cmp::Ordering::Less,
            (_, Value::Null) => std::cmp::Ordering::Greater,
            (Value::Bool(a_bool), Value::Bool(b_bool)) => a_bool.cmp(b_bool),
            (Value::Number(a_num), Value::Number(b_num)) => {
                if let (Some(a_i), Some(b_i)) = (a_num.as_i64(), b_num.as_i64()) {
                    a_i.cmp(&b_i)
                } else if let (Some(a_f), Some(b_f)) = (a_num.as_f64(), b_num.as_f64()) {
                    a_f.partial_cmp(&b_f).unwrap_or(std::cmp::Ordering::Equal)
                } else {
                    std::cmp::Ordering::Equal
                }
            }
            (Value::String(a_str), Value::String(b_str)) => a_str.cmp(b_str),
            (Value::Array(a_arr), Value::Array(b_arr)) => {
                for (i, a_item) in a_arr.iter().enumerate() {
                    if i >= b_arr.len() {
                        return std::cmp::Ordering::Greater;
                    }
                    let cmp = Self::compare(a_item, &b_arr[i]);
                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }
                }
                a_arr.len().cmp(&b_arr.len())
            }
            (Value::Object(a_obj), Value::Object(b_obj)) => {
                let a_keys: Vec<_> = a_obj.keys().collect();
                let b_keys: Vec<_> = b_obj.keys().collect();
                let key_cmp = a_keys.cmp(&b_keys);
                if key_cmp != std::cmp::Ordering::Equal {
                    return key_cmp;
                }
                for key in a_keys {
                    let cmp = Self::compare(&a_obj[key], &b_obj[key]);
                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }
                }
                std::cmp::Ordering::Equal
            }
            // Different types: use type ordering
            (Value::Bool(_), _) => std::cmp::Ordering::Less,
            (Value::Number(_), Value::Bool(_)) => std::cmp::Ordering::Greater,
            (Value::Number(_), _) => std::cmp::Ordering::Less,
            (Value::String(_), Value::Bool(_) | Value::Number(_)) => std::cmp::Ordering::Greater,
            (Value::String(_), _) => std::cmp::Ordering::Less,
            (Value::Array(_), Value::Bool(_) | Value::Number(_) | Value::String(_)) => {
                std::cmp::Ordering::Greater
            }
            (Value::Array(_), _) => std::cmp::Ordering::Less,
            (Value::Object(_), _) => std::cmp::Ordering::Greater,
        }
    }

    /// Returns true if a is less than b.
    pub fn less_than(a: &Value, b: &Value) -> bool {
        Self::compare(a, b) == std::cmp::Ordering::Less
    }

    /// Returns true if a is greater than b.
    pub fn greater_than(a: &Value, b: &Value) -> bool {
        Self::compare(a, b) == std::cmp::Ordering::Greater
    }

    /// Returns true if a equals b.
    pub fn equals(a: &Value, b: &Value) -> bool {
        Self::compare(a, b) == std::cmp::Ordering::Equal
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::SchemaBuilder;
    use serde_json::json;

    #[test]
    fn test_validate_type_int32() {
        let value = json!(42);
        assert!(ValueUtil::validate_type(&value, SchemaType::Int32).is_ok());
    }

    #[test]
    fn test_validate_type_string() {
        let value = json!("hello");
        assert!(ValueUtil::validate_type(&value, SchemaType::String).is_ok());
    }

    #[test]
    fn test_validate_type_boolean() {
        let value = json!(true);
        assert!(ValueUtil::validate_type(&value, SchemaType::Boolean).is_ok());
    }

    #[test]
    fn test_validate_type_failure() {
        let value = json!("not a number");
        assert!(ValueUtil::validate_type(&value, SchemaType::Int32).is_err());
    }

    #[test]
    fn test_is_type() {
        assert!(ValueUtil::is_type(&json!(true), SchemaType::Boolean));
        assert!(ValueUtil::is_type(&json!(42), SchemaType::Int32));
        assert!(ValueUtil::is_type(&json!("hello"), SchemaType::String));
    }

    #[test]
    fn test_to_number() {
        let result = ValueUtil::to_number(&json!("42")).unwrap();
        assert_eq!(result, json!(42));

        let result = ValueUtil::to_number(&json!(true)).unwrap();
        assert_eq!(result, json!(1));
    }

    #[test]
    fn test_type_name() {
        assert_eq!(ValueUtil::type_name(&json!(null)), "null");
        assert_eq!(ValueUtil::type_name(&json!(true)), "boolean");
        assert_eq!(ValueUtil::type_name(&json!(42)), "number");
        assert_eq!(ValueUtil::type_name(&json!("hello")), "string");
    }

    #[test]
    fn test_parse_string_value() {
        let result = ValueUtil::parse_string_value("42").unwrap();
        assert_eq!(result, json!(42));

        let result = ValueUtil::parse_string_value("true").unwrap();
        assert_eq!(result, json!(true));

        let result = ValueUtil::parse_string_value("\"hello\"").unwrap();
        assert_eq!(result, json!("hello"));
    }

    #[test]
    fn test_deep_merge() {
        let base = json!({"a": 1, "b": {"c": 2}});
        let overlay = json!({"b": {"d": 3}, "e": 4});

        let result = ValueUtil::deep_merge(&base, &overlay);
        assert_eq!(result, json!({"a": 1, "b": {"c": 2, "d": 3}, "e": 4}));
    }

    #[test]
    fn test_value_validator() {
        let schema = SchemaBuilder::int32().build();
        let value = json!(42);

        assert!(ValueValidator::validate(&schema, &value).is_ok());
    }

    #[test]
    fn test_value_validator_null_required() {
        let schema = SchemaBuilder::int32().build();
        let value = json!(null);

        assert!(ValueValidator::validate(&schema, &value).is_err());
    }

    #[test]
    fn test_value_validator_null_optional() {
        let schema = SchemaBuilder::int32().optional().build();
        let value = json!(null);

        assert!(ValueValidator::validate(&schema, &value).is_ok());
    }

    #[test]
    fn test_compare_numbers() {
        assert!(ValueComparator::less_than(&json!(1), &json!(2)));
        assert!(ValueComparator::greater_than(&json!(3), &json!(2)));
        assert!(ValueComparator::equals(&json!(1), &json!(1)));
    }

    #[test]
    fn test_compare_strings() {
        assert!(ValueComparator::less_than(&json!("a"), &json!("b")));
        assert!(ValueComparator::greater_than(&json!("c"), &json!("b")));
    }

    #[test]
    fn test_compare_arrays() {
        assert!(ValueComparator::less_than(&json!([1, 2]), &json!([1, 3])));
        assert!(ValueComparator::equals(&json!([1, 2]), &json!([1, 2])));
    }
}
