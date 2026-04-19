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

use crate::errors::SerializationException;
use serde_json::Value;
use std::collections::HashMap;

/// Deserializer trait for deserializing data.
///
/// This corresponds to `org.apache.kafka.common.serialization.Deserializer` in Java.
pub trait Deserializer<T> {
    /// Configures this deserializer.
    fn configure(&mut self, configs: HashMap<String, Value>, is_key: bool);

    /// Deserializes the given data.
    fn deserialize(&self, topic: &str, data: &[u8]) -> Result<T, SerializationException>;

    /// Closes this deserializer.
    fn close(&mut self);
}

/// StringDeserializer for deserializing strings.
///
/// This corresponds to `org.apache.kafka.common.serialization.StringDeserializer` in Java.
pub struct StringDeserializer {
    encoding: String,
}

impl StringDeserializer {
    /// Creates a new StringDeserializer with UTF-8 encoding.
    pub fn new() -> Self {
        StringDeserializer {
            encoding: "UTF-8".to_string(),
        }
    }

    /// Creates a new StringDeserializer with the given encoding.
    pub fn with_encoding(encoding: impl Into<String>) -> Self {
        StringDeserializer {
            encoding: encoding.into(),
        }
    }
}

impl Default for StringDeserializer {
    fn default() -> Self {
        Self::new()
    }
}

impl Deserializer<String> for StringDeserializer {
    fn configure(&mut self, configs: HashMap<String, Value>, is_key: bool) {
        if let Some(encoding) = configs.get("deserializer.encoding") {
            if let Some(encoding_str) = encoding.as_str() {
                self.encoding = encoding_str.to_string();
            }
        }
    }

    fn deserialize(&self, topic: &str, data: &[u8]) -> Result<String, SerializationException> {
        String::from_utf8(data.to_vec()).map_err(|e| {
            SerializationException::new(format!("Failed to deserialize string: {}", e))
        })
    }

    fn close(&mut self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize() {
        let deserializer = StringDeserializer::new();
        let result = deserializer.deserialize("test", b"hello");
        assert_eq!(result.unwrap(), "hello");
    }

    #[test]
    fn test_deserialize_invalid() {
        let deserializer = StringDeserializer::new();
        let invalid_bytes: Vec<u8> = vec![0xFF, 0xFE];
        let result = deserializer.deserialize("test", &invalid_bytes);
        assert!(result.is_err());
    }
}
