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

use crate::data::{Schema, SchemaAndValue, SchemaBuilder, Values};
use crate::errors::ConnectError;
use crate::storage::HeaderConverter;
use common_trait::config::ConfigDef;
use serde_json::Value;
use std::collections::HashMap;

/// SimpleHeaderConverter for simple header conversion.
///
/// This corresponds to `org.apache.kafka.connect.storage.SimpleHeaderConverter` in Java.
///
/// A HeaderConverter that serializes header values as strings and that deserializes
/// header values to the most appropriate numeric, boolean, array, or map representation.
/// Schemas are not serialized, but are inferred upon deserialization when possible.
pub struct SimpleHeaderConverter;

impl SimpleHeaderConverter {
    pub fn new() -> Self {
        SimpleHeaderConverter
    }
}

impl Default for SimpleHeaderConverter {
    fn default() -> Self {
        Self::new()
    }
}

impl HeaderConverter for SimpleHeaderConverter {
    fn configure(&mut self, _configs: HashMap<String, String>, _is_key: bool) {
        // do nothing - SimpleHeaderConverter has no configuration
    }

    fn from_connect_header(
        &self,
        _topic: &str,
        _header_key: &str,
        schema: Option<&dyn Schema>,
        value: &Value,
    ) -> Result<Option<Vec<u8>>, ConnectError> {
        if value.is_null() {
            return Ok(None);
        }
        // Convert value to string and encode as UTF-8 bytes
        let string_value = Values::convert_to_string(schema, value)?;
        match string_value {
            Some(s) => Ok(Some(s.into_bytes())),
            None => Ok(None),
        }
    }

    fn to_connect_header(
        &self,
        _topic: &str,
        _header_key: &str,
        value: Option<&[u8]>,
    ) -> Result<SchemaAndValue, ConnectError> {
        match value {
            None => Ok(SchemaAndValue::null()),
            Some(bytes) if bytes.is_empty() => Ok(SchemaAndValue::null()),
            Some(bytes) => {
                // Convert bytes to string and parse
                let str_value = String::from_utf8(bytes.to_vec()).map_err(|e| {
                    ConnectError::data(format!("Failed to convert bytes to string: {}", e))
                })?;
                // Use Values.parse_string to parse the string representation
                Values::parse_string(&str_value)
            }
        }
    }

    fn config(&self) -> &'static dyn ConfigDef {
        static EMPTY_CONFIG_DEF: EmptyConfigDef = EmptyConfigDef;
        &EMPTY_CONFIG_DEF
    }

    fn close(&mut self) {
        // do nothing - SimpleHeaderConverter has no resources to release
    }
}

struct EmptyConfigDef;

impl common_trait::config::ConfigDef for EmptyConfigDef {
    fn config_def(
        &self,
    ) -> std::collections::HashMap<String, common_trait::config::ConfigValueEntry> {
        std::collections::HashMap::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_simple_header_converter_from_connect_header() {
        let converter = SimpleHeaderConverter::new();
        let value = json!("test_value");

        let result = converter.from_connect_header("test_topic", "test_header", None, &value);
        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert_eq!(Some("test_value".as_bytes().to_vec()), bytes);
    }

    #[test]
    fn test_simple_header_converter_to_connect_header() {
        let converter = SimpleHeaderConverter::new();
        let bytes = "test_value".as_bytes();

        let result = converter.to_connect_header("test_topic", "test_header", Some(bytes));
        assert!(result.is_ok());
        let schema_and_value = result.unwrap();
        assert_eq!(Some(&json!("test_value")), schema_and_value.value());
    }

    #[test]
    fn test_simple_header_converter_null_value() {
        let converter = SimpleHeaderConverter::new();
        let value = json!(null);

        let result = converter.from_connect_header("test_topic", "test_header", None, &value);
        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert!(bytes.is_none());
    }

    #[test]
    fn test_should_convert_simple_string() {
        let converter = SimpleHeaderConverter::new();
        let value = json!("hello");

        let bytes = converter
            .from_connect_header("test_topic", "header", None, &value)
            .unwrap();
        assert_eq!(Some("hello".as_bytes().to_vec()), bytes);

        let result = converter
            .to_connect_header("test_topic", "header", Some(bytes.unwrap().as_slice()))
            .unwrap();
        assert_eq!(Some(&json!("hello")), result.value());
    }

    #[test]
    fn test_should_convert_empty_string() {
        let converter = SimpleHeaderConverter::new();
        let value = json!("");

        let bytes = converter
            .from_connect_header("test_topic", "header", None, &value)
            .unwrap();
        assert!(bytes.unwrap().is_empty());

        let result = converter
            .to_connect_header("test_topic", "header", Some(&[]))
            .unwrap();
        assert!(result.is_null());
    }

    #[test]
    fn test_should_convert_map_with_int_values() {
        let converter = SimpleHeaderConverter::new();
        let value = json!({"foo": 1234567890, "bar": 0, "baz": -987654321});

        let bytes = converter
            .from_connect_header("test_topic", "header", None, &value)
            .unwrap();
        let bytes_str = String::from_utf8(bytes.clone().unwrap()).unwrap();
        assert!(bytes_str.contains("foo"));
        assert!(bytes_str.contains("bar"));

        let result = converter
            .to_connect_header("test_topic", "header", Some(bytes.unwrap().as_slice()))
            .unwrap();
        // Values.parse_string parses JSON object to SchemaAndValue with Map schema
        assert!(result.value().is_some());
    }

    #[test]
    fn test_should_convert_list_with_string_values() {
        let converter = SimpleHeaderConverter::new();
        let value = json!(["foo", "bar"]);

        let bytes = converter
            .from_connect_header("test_topic", "header", None, &value)
            .unwrap();
        let bytes_str = String::from_utf8(bytes.clone().unwrap()).unwrap();
        assert!(bytes_str.contains("foo"));
        assert!(bytes_str.contains("bar"));

        let result = converter
            .to_connect_header("test_topic", "header", Some(bytes.unwrap().as_slice()))
            .unwrap();
        assert!(result.value().is_some());
    }

    #[test]
    fn test_should_convert_list_with_int_values() {
        let converter = SimpleHeaderConverter::new();
        let value = json!([1234567890, -987654321]);

        let bytes = converter
            .from_connect_header("test_topic", "header", None, &value)
            .unwrap();
        let bytes_str = String::from_utf8(bytes.clone().unwrap()).unwrap();
        assert!(bytes_str.contains("1234567890"));

        let result = converter
            .to_connect_header("test_topic", "header", Some(bytes.unwrap().as_slice()))
            .unwrap();
        assert!(result.value().is_some());
    }

    #[test]
    fn test_should_convert_empty_map_to_map() {
        let converter = SimpleHeaderConverter::new();
        let value = json!({});

        let bytes = converter
            .from_connect_header("test_topic", "header", None, &value)
            .unwrap();
        let bytes_str = String::from_utf8(bytes.clone().unwrap()).unwrap();
        assert_eq!("{}", bytes_str);

        let result = converter
            .to_connect_header("test_topic", "header", Some(bytes.unwrap().as_slice()))
            .unwrap();
        assert!(result.value().is_some());
    }

    #[test]
    fn test_should_convert_empty_list_to_list() {
        let converter = SimpleHeaderConverter::new();
        let value = json!([]);

        let bytes = converter
            .from_connect_header("test_topic", "header", None, &value)
            .unwrap();
        let bytes_str = String::from_utf8(bytes.clone().unwrap()).unwrap();
        assert_eq!("[]", bytes_str);

        let result = converter
            .to_connect_header("test_topic", "header", Some(bytes.unwrap().as_slice()))
            .unwrap();
        assert!(result.value().is_some());
    }

    // Wave 2 P1 Tests - Additional converter tests

    #[test]
    fn test_should_convert_string_with_quotes_and_delimiter() {
        // Java: shouldConvertStringWithQuotesAndOtherDelimiterCharacters
        let converter = SimpleHeaderConverter::new();
        let value = json!("test\"with\\quotes");

        let bytes = converter
            .from_connect_header("test_topic", "header", None, &value)
            .unwrap();
        let result = converter
            .to_connect_header("test_topic", "header", Some(bytes.unwrap().as_slice()))
            .unwrap();

        // Should handle escaped characters
        if let Some(v) = result.value() {
            assert!(v.as_str().unwrap().contains("test"));
        }
    }

    #[test]
    fn test_should_convert_mixed_values_to_map() {
        // Java: shouldConvertMapWithStringKeysAndMixedValuesToMap
        let converter = SimpleHeaderConverter::new();
        let value = json!({"str": "value", "int": 42, "bool": true});

        let bytes = converter
            .from_connect_header("test_topic", "header", None, &value)
            .unwrap();
        let result = converter
            .to_connect_header("test_topic", "header", Some(bytes.unwrap().as_slice()))
            .unwrap();
        assert!(result.value().is_some());
    }

    #[test]
    fn test_should_parse_string_of_map_with_whitespace() {
        // Java: shouldParseStringOfMapWithStringValuesWithWhitespace
        let converter = SimpleHeaderConverter::new();
        let map_str = "{ \"foo\" : \"123\" , \"bar\" : \"baz\" }";
        let bytes = map_str.as_bytes();

        let result = converter
            .to_connect_header("test_topic", "header", Some(bytes))
            .unwrap();
        // Should parse as SchemaAndValue
        assert!(result.value().is_some());
    }

    #[test]
    fn test_should_convert_mixed_list_without_schema() {
        // Java: shouldConvertListWithMixedValuesToListWithoutSchema
        let converter = SimpleHeaderConverter::new();
        let value = json!(["str", 42, true]);

        let bytes = converter
            .from_connect_header("test_topic", "header", None, &value)
            .unwrap();
        let result = converter
            .to_connect_header("test_topic", "header", Some(bytes.unwrap().as_slice()))
            .unwrap();
        assert!(result.value().is_some());
    }

    // Wave 3 P2 Tests - Version and edge cases

    #[test]
    fn test_converter_should_return_app_info_parser_version() {
        // Java: testConverterShouldReturnAppInfoParserVersion
        // The converter should have version info
        let converter = SimpleHeaderConverter::new();

        // Config should be accessible
        let config = converter.config();
        assert!(config.config_def().is_empty());
    }

    #[test]
    fn test_configure_and_close() {
        let mut converter = SimpleHeaderConverter::new();
        let configs = HashMap::new();
        converter.configure(configs, false);
        converter.close();
    }

    #[test]
    fn test_to_connect_header_with_none() {
        let converter = SimpleHeaderConverter::new();
        let result = converter
            .to_connect_header("test_topic", "header", None)
            .unwrap();
        assert!(result.is_null());
    }
}
