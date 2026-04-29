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

//! JSON deserializer for Jackson's JsonNode tree model.
//!
//! Using the tree model allows it to work with arbitrarily structured data
//! without having associated classes. This deserializer also supports Connect schemas.
//!
//! Corresponds to: `org.apache.kafka.connect.json.JsonDeserializer` in Java Kafka Connect.
//!
//! Source: connect/json/src/main/java/org/apache/kafka/connect/json/JsonDeserializer.java

use common_trait::errors::SerializationException;
use common_trait::serialization::Deserializer;
use serde_json::Value;
use std::collections::HashMap;

/// JSON deserializer for Jackson's JsonNode tree model.
///
/// Using the tree model allows it to work with arbitrarily structured data
/// without having associated Java classes. This deserializer also supports Connect schemas.
///
/// Corresponds to: `org.apache.kafka.connect.json.JsonDeserializer`
pub struct JsonDeserializer {
    /// Whether to enable the Blackbird module for performance optimization.
    /// In Rust, we don't have a direct equivalent to Blackbird, but this flag
    /// is preserved for API compatibility.
    enable_blackbird: bool,
}

impl JsonDeserializer {
    /// Default constructor needed by Kafka.
    ///
    /// Corresponds to Java constructor:
    /// `public JsonDeserializer() { this(Set.of(), new JsonNodeFactory(true), true); }`
    pub fn new() -> Self {
        JsonDeserializer {
            enable_blackbird: true,
        }
    }

    /// Creates a JsonDeserializer with optional Blackbird module enablement.
    ///
    /// In Java, this is part of the constructor that accepts `enableBlackbird` parameter.
    /// Blackbird is a high-performance Jackson module. In Rust, serde_json already
    /// provides good performance, but this parameter is kept for API compatibility.
    ///
    /// Corresponds to Java constructor parameter:
    /// `JsonDeserializer(Set<DeserializationFeature>, JsonNodeFactory, boolean enableBlackbird)`
    pub fn with_blackbird(enable: bool) -> Self {
        JsonDeserializer {
            enable_blackbird: enable,
        }
    }

    /// Returns whether Blackbird optimization is enabled.
    ///
    /// This getter is provided for testing and configuration inspection purposes.
    pub fn is_blackbird_enabled(&self) -> bool {
        self.enable_blackbird
    }
}

impl Default for JsonDeserializer {
    fn default() -> Self {
        Self::new()
    }
}

impl Deserializer<Option<Value>> for JsonDeserializer {
    /// Configures this deserializer.
    ///
    /// In the Java implementation, this method is inherited from the Deserializer interface
    /// with a default empty implementation. We provide the same behavior here.
    fn configure(&mut self, _configs: HashMap<String, Value>, _is_key: bool) {
        // No configuration needed for basic JSON deserializer
        // Java implementation uses default empty configure method
    }

    /// Deserializes the given bytes to a JSON Value (JsonNode equivalent).
    ///
    /// If `data` is empty (null in Kafka terms), returns `None`.
    /// Otherwise, parses the bytes as JSON using serde_json::from_slice.
    ///
    /// Corresponds to Java method:
    /// ```java
    /// @Override
    /// public JsonNode deserialize(String topic, byte[] bytes) {
    ///     if (bytes == null)
    ///         return null;
    ///     JsonNode data;
    ///     try {
    ///         data = objectMapper.readTree(bytes);
    ///     } catch (Exception e) {
    ///         throw new SerializationException(e);
    ///     }
    ///     return data;
    /// }
    /// ```
    fn deserialize(
        &self,
        _topic: &str,
        data: &[u8],
    ) -> Result<Option<Value>, SerializationException> {
        if data.is_empty() {
            // Corresponds to Java: if (bytes == null) return null;
            return Ok(None);
        }

        // Parse JSON using serde_json::from_slice
        // Corresponds to Java: objectMapper.readTree(bytes)
        let value = serde_json::from_slice::<Value>(data).map_err(|e| {
            SerializationException::new(format!("Failed to deserialize JSON: {}", e))
        })?;

        Ok(Some(value))
    }

    /// Closes this deserializer.
    ///
    /// In the Java implementation, this method is inherited from the Deserializer interface
    /// with a default empty implementation. We provide the same behavior here.
    fn close(&mut self) {
        // No resources to close
        // Java implementation uses default empty close method
    }
}
