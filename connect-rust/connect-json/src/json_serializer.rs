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

//! JSON serializer for Kafka Connect.
//!
//! Corresponds to: `org.apache.kafka.connect.json.JsonSerializer` in Java Kafka Connect.
//!
//! Source: connect/json/src/main/java/org/apache/kafka/connect/json/JsonSerializer.java

use common_trait::errors::SerializationException;
use serde_json::Value;
use std::collections::HashMap;

/// Serialize Jackson `JsonNode` tree model objects to UTF-8 JSON.
///
/// Using the tree model allows handling arbitrarily structured data without
/// corresponding Java classes. This serializer also supports Connect schemas.
///
/// Corresponds to: `org.apache.kafka.connect.json.JsonSerializer`
pub struct JsonSerializer {
    /// Whether to enable blackbird optimization.
    /// In Java, this enables BlackbirdModule for faster serialization.
    /// In Rust, this is a placeholder for future optimization.
    enable_blackbird: bool,
}

impl JsonSerializer {
    /// Default constructor needed by Kafka.
    ///
    /// Corresponds to: `JsonSerializer()` constructor in Java.
    pub fn new() -> Self {
        Self::with_blackbird(true)
    }

    /// A constructor that specifies whether to enable blackbird.
    ///
    /// Corresponds to: `JsonSerializer(Set<SerializationFeature>, JsonNodeFactory, boolean)` constructor in Java.
    /// The Rust version simplifies to just the blackbird flag.
    ///
    /// # Arguments
    /// * `enable_blackbird` - Whether to enable blackbird optimization
    pub fn with_blackbird(enable_blackbird: bool) -> Self {
        JsonSerializer { enable_blackbird }
    }

    /// Configures this serializer.
    ///
    /// Corresponds to: `configure(Map<String, ?> configs, boolean isKey)` in Java Serializer interface.
    /// This implementation is empty as JSON serializer has no configuration options.
    pub fn configure(&mut self, _configs: HashMap<String, Value>, _is_key: bool) {
        // No configuration needed for JSON serializer
    }

    /// Serialize Jackson `JsonNode` tree model object to UTF-8 JSON bytes.
    ///
    /// Returns `None` for null data (tombstone records in Kafka).
    ///
    /// Corresponds to: `serialize(String topic, JsonNode data)` in Java.
    ///
    /// # Arguments
    /// * `topic` - The topic name (unused in JSON serialization)
    /// * `data` - The JSON value to serialize, or None for tombstone
    ///
    /// # Returns
    /// * `Some(Vec<u8>)` - UTF-8 JSON bytes for valid data
    /// * `None` - For null/tombstone records
    ///
    /// # Errors
    /// Throws `SerializationException` if JSON serialization fails
    pub fn serialize(&self, topic: &str, data: Option<&Value>) -> Option<Vec<u8>> {
        if data.is_none() {
            return None;
        }

        let value = data.unwrap();
        match serde_json::to_vec(value) {
            Ok(bytes) => Some(bytes),
            Err(e) => {
                panic!(
                    "{}",
                    SerializationException::new(format!("Error serializing JSON message: {}", e))
                );
            }
        }
    }

    /// Closes this serializer.
    ///
    /// Corresponds to: `close()` in Java Serializer interface.
    /// This implementation is empty as there are no resources to release.
    pub fn close(&mut self) {
        // No resources to close
    }
}

impl Default for JsonSerializer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_new() {
        let serializer = JsonSerializer::new();
        assert!(serializer.enable_blackbird);
    }

    #[test]
    fn test_with_blackbird_true() {
        let serializer = JsonSerializer::with_blackbird(true);
        assert!(serializer.enable_blackbird);
    }

    #[test]
    fn test_with_blackbird_false() {
        let serializer = JsonSerializer::with_blackbird(false);
        assert!(!serializer.enable_blackbird);
    }

    #[test]
    fn test_serialize_null() {
        let serializer = JsonSerializer::new();
        let result = serializer.serialize("test", None);
        assert!(result.is_none());
    }

    #[test]
    fn test_serialize_string() {
        let serializer = JsonSerializer::new();
        let data = json!("hello");
        let result = serializer.serialize("test", Some(&data));
        assert!(result.is_some());
        assert_eq!(result.unwrap(), b"\"hello\"");
    }

    #[test]
    fn test_serialize_number() {
        let serializer = JsonSerializer::new();
        let data = json!(42);
        let result = serializer.serialize("test", Some(&data));
        assert!(result.is_some());
        assert_eq!(result.unwrap(), b"42");
    }

    #[test]
    fn test_serialize_object() {
        let serializer = JsonSerializer::new();
        let data = json!({"key": "value"});
        let result = serializer.serialize("test", Some(&data));
        assert!(result.is_some());
        let bytes = result.unwrap();
        let parsed: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed["key"], "value");
    }

    #[test]
    fn test_serialize_array() {
        let serializer = JsonSerializer::new();
        let data = json!([1, 2, 3]);
        let result = serializer.serialize("test", Some(&data));
        assert!(result.is_some());
        let bytes = result.unwrap();
        let parsed: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed, json!([1, 2, 3]));
    }

    #[test]
    fn test_serialize_boolean() {
        let serializer = JsonSerializer::new();
        let data = json!(true);
        let result = serializer.serialize("test", Some(&data));
        assert!(result.is_some());
        assert_eq!(result.unwrap(), b"true");
    }

    #[test]
    fn test_serialize_null_value() {
        let serializer = JsonSerializer::new();
        let data = json!(null);
        let result = serializer.serialize("test", Some(&data));
        assert!(result.is_some());
        assert_eq!(result.unwrap(), b"null");
    }

    #[test]
    fn test_configure() {
        let mut serializer = JsonSerializer::new();
        let configs = HashMap::new();
        serializer.configure(configs, false);
        assert!(serializer.enable_blackbird);
    }

    #[test]
    fn test_close() {
        let mut serializer = JsonSerializer::new();
        serializer.close();
        // No assertion needed - just verify it doesn't panic
    }

    #[test]
    fn test_default() {
        let serializer = JsonSerializer::default();
        assert!(serializer.enable_blackbird);
    }
}
