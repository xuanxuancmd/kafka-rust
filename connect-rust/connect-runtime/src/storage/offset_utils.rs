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

//! Utility functions for offset storage validation and processing.
//!
//! Corresponds to `org.apache.kafka.connect.storage.OffsetUtils` in Java.

use serde_json::Value;
use std::collections::HashMap;

/// Validates the format of offset data.
///
/// Offset data must be a Map with String keys and primitive-type values.
/// Both keys and values for offsets may be null.
pub fn validate_format(offset_data: &Value) -> Result<(), String> {
    if offset_data.is_null() {
        return Ok(());
    }

    if !offset_data.is_object() {
        return Err("Offsets must be specified as a Map".to_string());
    }

    let map = offset_data.as_object().unwrap();
    for (key, value) in map.iter() {
        // Keys must be strings (JSON object keys are always strings)

        if value.is_null() {
            continue;
        }

        // Values must be primitive types
        if !is_primitive_type(value) {
            return Err(format!(
                "Offsets may only contain primitive types as values, but field {} contains {}",
                key,
                get_type_name(value)
            ));
        }
    }

    Ok(())
}

/// Validates the format of offset data as a HashMap.
pub fn validate_format_map(offset_data: &HashMap<String, Value>) -> Result<(), String> {
    if offset_data.is_empty() {
        return Ok(());
    }

    for (key, value) in offset_data.iter() {
        if value.is_null() {
            continue;
        }

        if !is_primitive_type(value) {
            return Err(format!(
                "Offsets may only contain primitive types as values, but field {} contains {}",
                key,
                get_type_name(value)
            ));
        }
    }

    Ok(())
}

/// Checks if a JSON value is a primitive type.
fn is_primitive_type(value: &Value) -> bool {
    value.is_boolean()
        || value.is_i64()
        || value.is_u64()
        || value.is_f64()
        || value.is_string()
        || value.is_null()
}

/// Gets the type name of a JSON value.
fn get_type_name(value: &Value) -> String {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_validate_format_null() {
        let result = validate_format(&json!(null));
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_format_valid_map() {
        let data = json!({
            "partition": 1,
            "offset": 100,
            "topic": "test-topic"
        });
        let result = validate_format(&data);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_format_null_values() {
        let data = json!({
            "partition": null,
            "offset": null
        });
        let result = validate_format(&data);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_format_invalid_not_map() {
        let data = json!("invalid");
        let result = validate_format(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must be specified as a Map"));
    }

    #[test]
    fn test_validate_format_invalid_nested_object() {
        let data = json!({
            "partition": {
                "nested": "value"
            }
        });
        let result = validate_format(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("primitive types"));
    }

    #[test]
    fn test_validate_format_invalid_array() {
        let data = json!({
            "partition": [1, 2, 3]
        });
        let result = validate_format(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_format_map_empty() {
        let map = HashMap::new();
        let result = validate_format_map(&map);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_format_map_valid() {
        let map = HashMap::from([
            ("offset".to_string(), json!(100)),
            ("partition".to_string(), json!(1)),
        ]);
        let result = validate_format_map(&map);
        assert!(result.is_ok());
    }

    #[test]
    fn test_is_primitive_type() {
        assert!(is_primitive_type(&json!(true)));
        assert!(is_primitive_type(&json!(123)));
        assert!(is_primitive_type(&json!(3.14)));
        assert!(is_primitive_type(&json!("string")));
        assert!(is_primitive_type(&json!(null)));
        assert!(!is_primitive_type(&json!([1, 2, 3])));
        assert!(!is_primitive_type(&json!({"key": "value"})));
    }

    #[test]
    fn test_get_type_name() {
        assert_eq!(get_type_name(&json!(null)), "null");
        assert_eq!(get_type_name(&json!(true)), "boolean");
        assert_eq!(get_type_name(&json!(123)), "number");
        assert_eq!(get_type_name(&json!("string")), "string");
        assert_eq!(get_type_name(&json!([1, 2, 3])), "array");
        assert_eq!(get_type_name(&json!({"key": "value"})), "object");
    }
}
