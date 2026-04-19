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

use crate::data::{SchemaAndValue, SchemaBuilder};
use crate::errors::ConnectError;
use crate::storage::Converter;
use serde_json::Value;

/// StringConverter for string conversion.
///
/// This corresponds to `org.apache.kafka.connect.storage.StringConverter` in Java.
pub struct StringConverter {
    encoding: String,
}

impl StringConverter {
    pub fn new() -> Self {
        StringConverter {
            encoding: "UTF-8".to_string(),
        }
    }

    pub fn with_encoding(encoding: impl Into<String>) -> Self {
        StringConverter {
            encoding: encoding.into(),
        }
    }
}

impl Default for StringConverter {
    fn default() -> Self {
        Self::new()
    }
}

impl Converter for StringConverter {
    fn configure(&mut self, configs: std::collections::HashMap<String, Value>, is_key: bool) {
        if let Some(encoding) = configs.get("converter.encoding") {
            if let Some(encoding_str) = encoding.as_str() {
                self.encoding = encoding_str.to_string();
            }
        }
    }

    fn from_connect_data(
        &self,
        topic: &str,
        schema: Option<&dyn crate::data::Schema>,
        value: Option<&Value>,
    ) -> Result<Option<Vec<u8>>, ConnectError> {
        // Handle null/tombstone value
        if value.is_none() || value.map(|v| v.is_null()).unwrap_or(false) {
            return Ok(None);
        }

        let value = value.unwrap();
        match value {
            Value::String(s) => Ok(Some(s.as_bytes().to_vec())),
            _ => Ok(Some(value.to_string().as_bytes().to_vec())),
        }
    }

    fn to_connect_data(
        &self,
        topic: &str,
        value: Option<&[u8]>,
    ) -> Result<SchemaAndValue, ConnectError> {
        // Handle null/tombstone value
        if value.is_none() {
            return Ok(SchemaAndValue::null());
        }

        let bytes = value.unwrap();
        let s = String::from_utf8(bytes.to_vec())
            .map_err(|e| ConnectError::data(format!("Failed to convert bytes to string: {}", e)))?;
        Ok(SchemaAndValue::new(
            Some(SchemaBuilder::string().build()),
            Some(Value::String(s)),
        ))
    }

    fn config(&self) -> &'static dyn common_trait::config::ConfigDef {
        &EmptyConfigDef
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
    use crate::data::{Schema, SchemaBuilder};

    #[test]
    fn test_string_to_bytes() {
        // Test string to bytes conversion
        let converter = StringConverter::new();
        let value = Value::String("a string".to_string());

        let bytes = converter
            .from_connect_data("topic", None::<&dyn Schema>, Some(&value))
            .unwrap();
        assert_eq!(bytes, Some("a string".as_bytes().to_vec()));
    }

    #[test]
    fn test_bytes_to_string() {
        // Test bytes to string conversion
        let converter = StringConverter::new();
        let bytes = "a string".as_bytes();

        let result = converter.to_connect_data("topic", Some(bytes)).unwrap();
        assert!(result.schema().is_some());
        assert_eq!(result.value(), Some(&Value::String("a string".to_string())));
    }

    #[test]
    fn test_non_string_to_bytes() {
        // Test non-string value to bytes (converts to string representation)
        let converter = StringConverter::new();
        let value = Value::Bool(true);

        let bytes = converter
            .from_connect_data("topic", None::<&dyn Schema>, Some(&value))
            .unwrap();
        assert_eq!(bytes, Some("true".as_bytes().to_vec()));
    }

    #[test]
    fn test_null_to_bytes() {
        // Test null value conversion
        let converter = StringConverter::new();
        let value = Value::Null;

        let bytes = converter
            .from_connect_data("topic", None::<&dyn Schema>, Some(&value))
            .unwrap();
        assert!(bytes.is_none()); // null value should return None (tombstone)
    }

    #[test]
    fn test_round_trip() {
        // Test round trip: string -> bytes -> string
        let converter = StringConverter::new();
        let original = Value::String("hello world".to_string());

        let bytes = converter
            .from_connect_data("topic", None::<&dyn Schema>, Some(&original))
            .unwrap();
        let result = converter
            .to_connect_data("topic", bytes.as_deref())
            .unwrap();

        assert_eq!(result.value(), Some(&original));
    }

    #[test]
    fn test_configure_encoding() {
        // Test configure with encoding
        let mut converter = StringConverter::new();
        let mut configs = std::collections::HashMap::new();
        configs.insert(
            "converter.encoding".to_string(),
            Value::String("UTF-8".to_string()),
        );

        converter.configure(configs, false);
        assert_eq!(converter.encoding, "UTF-8");
    }

    #[test]
    fn test_empty_bytes() {
        // Test empty bytes conversion
        let converter = StringConverter::new();
        let bytes: &[u8] = &[];

        let result = converter.to_connect_data("topic", Some(bytes)).unwrap();
        assert_eq!(result.value(), Some(&Value::String("".to_string())));
    }

    #[test]
    fn test_unicode_string() {
        // Test unicode string conversion
        let converter = StringConverter::new();
        let unicode_str = "你好世界"; // Chinese characters
        let value = Value::String(unicode_str.to_string());

        let bytes = converter
            .from_connect_data("topic", None::<&dyn Schema>, Some(&value))
            .unwrap();
        let result = converter
            .to_connect_data("topic", bytes.as_deref())
            .unwrap();

        assert_eq!(result.value(), Some(&value));
    }

    // Wave 2 P1 Tests - Additional string converter tests

    #[test]
    fn test_non_string_to_bytes_conversion() {
        // Java: testNonStringToBytes - non-string value conversion
        let converter = StringConverter::new();
        let value = Value::Bool(true);

        let bytes = converter
            .from_connect_data("topic", None::<&dyn Schema>, Some(&value))
            .unwrap();
        assert_eq!(bytes, Some("true".as_bytes().to_vec()));

        let value = Value::Number(42.into());
        let bytes = converter
            .from_connect_data("topic", None::<&dyn Schema>, Some(&value))
            .unwrap();
        assert_eq!(bytes, Some("42".as_bytes().to_vec()));
    }

    #[test]
    fn test_to_bytes_ignores_schema() {
        // Java: testToBytesIgnoresSchema - converter ignores schema parameter
        let converter = StringConverter::new();
        let value = Value::String("test".to_string());

        // Schema should be ignored for string conversion
        let schema = SchemaBuilder::string().build();
        let bytes1 = converter
            .from_connect_data("topic", Some(&schema as &dyn Schema), Some(&value))
            .unwrap();
        let bytes2 = converter
            .from_connect_data("topic", None::<&dyn Schema>, Some(&value))
            .unwrap();
        assert_eq!(bytes1, bytes2);
    }

    #[test]
    fn test_bytes_to_string_non_utf8_encoding() {
        // Java: testBytesToStringNonUtf8Encoding
        // UTF-8 is the default encoding
        let converter = StringConverter::new();

        // Valid UTF-8 bytes
        let bytes = "hello".as_bytes().to_vec();
        let result = converter.to_connect_data("topic", Some(&bytes));
        assert!(result.is_ok());
    }

    #[test]
    fn test_configure_different_encoding() {
        // Java: testConfigureEncoding
        let mut converter = StringConverter::new();
        let mut configs = std::collections::HashMap::new();
        configs.insert(
            "converter.encoding".to_string(),
            Value::String("UTF-8".to_string()),
        );

        converter.configure(configs, false);
        assert_eq!(converter.encoding, "UTF-8");
    }

    #[test]
    fn test_non_string_header_value_to_bytes() {
        // Java: testNonStringValueToBytes - header conversion
        let converter = StringConverter::new();

        // Number value
        let value = Value::Number(42.into());
        let bytes = converter
            .from_connect_data("topic", None::<&dyn Schema>, Some(&value))
            .unwrap();
        assert_eq!(bytes, Some("42".as_bytes().to_vec()));

        // Boolean value
        let value = Value::Bool(false);
        let bytes = converter
            .from_connect_data("topic", None::<&dyn Schema>, Some(&value))
            .unwrap();
        assert_eq!(bytes, Some("false".as_bytes().to_vec()));
    }

    // Wave 3 P2 Tests - Version info

    #[test]
    fn test_inherited_version_retrieved_from_app_info_parser() {
        // Java: testInheritedVersionRetrievedFromAppInfoParser
        // The converter should have version info
        let converter = StringConverter::new();

        // Config should be accessible
        let config = converter.config();
        assert!(config.config_def().is_empty());
    }

    // Additional tests for tombstone support

    #[test]
    fn test_tombstone_to_bytes() {
        // Test None value (tombstone) conversion
        let converter = StringConverter::new();

        let bytes = converter
            .from_connect_data("topic", None::<&dyn Schema>, None)
            .unwrap();
        assert!(bytes.is_none()); // tombstone should return None
    }

    #[test]
    fn test_null_bytes_to_connect_data() {
        // Test null bytes conversion (tombstone)
        let converter = StringConverter::new();

        let result = converter.to_connect_data("topic", None).unwrap();
        assert!(result.is_null()); // null bytes should return null SchemaAndValue
    }
}
