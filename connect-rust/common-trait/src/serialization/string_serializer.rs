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

use serde_json::Value;
use std::collections::HashMap;

/// Serializer trait for serializing data.
///
/// This corresponds to `org.apache.kafka.common.serialization.Serializer` in Java.
pub trait Serializer<T> {
    /// Configures this serializer.
    fn configure(&mut self, configs: HashMap<String, Value>, is_key: bool);

    /// Serializes the given data.
    fn serialize(&self, topic: &str, data: &T) -> Vec<u8>;

    /// Closes this serializer.
    fn close(&mut self);
}

/// StringSerializer for serializing strings.
///
/// This corresponds to `org.apache.kafka.common.serialization.StringSerializer` in Java.
pub struct StringSerializer {
    encoding: String,
}

impl StringSerializer {
    /// Creates a new StringSerializer with UTF-8 encoding.
    pub fn new() -> Self {
        StringSerializer {
            encoding: "UTF-8".to_string(),
        }
    }

    /// Creates a new StringSerializer with the given encoding.
    pub fn with_encoding(encoding: impl Into<String>) -> Self {
        StringSerializer {
            encoding: encoding.into(),
        }
    }
}

impl Default for StringSerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl Serializer<String> for StringSerializer {
    fn configure(&mut self, configs: HashMap<String, Value>, is_key: bool) {
        if let Some(encoding) = configs.get("serializer.encoding") {
            if let Some(encoding_str) = encoding.as_str() {
                self.encoding = encoding_str.to_string();
            }
        }
    }

    fn serialize(&self, topic: &str, data: &String) -> Vec<u8> {
        data.as_bytes().to_vec()
    }

    fn close(&mut self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize() {
        let serializer = StringSerializer::new();
        let result = serializer.serialize("test", &"hello".to_string());
        assert_eq!(result, b"hello");
    }
}
