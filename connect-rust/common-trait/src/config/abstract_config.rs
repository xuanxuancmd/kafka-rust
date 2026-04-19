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

/// AbstractConfig is the base class for configuration objects.
///
/// This corresponds to `org.apache.kafka.common.config.AbstractConfig` in Java.
#[derive(Debug, Clone)]
pub struct AbstractConfig {
    originals: HashMap<String, Value>,
}

impl AbstractConfig {
    /// Creates a new AbstractConfig with the given configuration values.
    pub fn new(originals: HashMap<String, Value>) -> Self {
        AbstractConfig { originals }
    }

    /// Returns the raw configuration value for the given key.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.originals.get(key)
    }

    /// Returns the configuration value as a string.
    pub fn get_string(&self, key: &str) -> Option<&str> {
        self.originals.get(key).and_then(|v| v.as_str())
    }

    /// Returns the configuration value as an integer.
    pub fn get_int(&self, key: &str) -> Option<i64> {
        self.originals.get(key).and_then(|v| v.as_i64())
    }

    /// Returns the configuration value as a boolean.
    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.originals.get(key).and_then(|v| v.as_bool())
    }

    /// Returns the configuration value as a double.
    pub fn get_double(&self, key: &str) -> Option<f64> {
        self.originals.get(key).and_then(|v| v.as_f64())
    }

    /// Returns all configuration values.
    pub fn originals(&self) -> &HashMap<String, Value> {
        &self.originals
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let config = AbstractConfig::new(HashMap::new());
        assert_eq!(config.originals().len(), 0);
    }

    #[test]
    fn test_get_string() {
        let mut originals = HashMap::new();
        originals.insert("key".to_string(), Value::String("value".to_string()));
        let config = AbstractConfig::new(originals);
        assert_eq!(config.get_string("key"), Some("value"));
    }

    #[test]
    fn test_get_int() {
        let mut originals = HashMap::new();
        originals.insert("key".to_string(), Value::Number(42.into()));
        let config = AbstractConfig::new(originals);
        assert_eq!(config.get_int("key"), Some(42));
    }

    #[test]
    fn test_get_bool() {
        let mut originals = HashMap::new();
        originals.insert("key".to_string(), Value::Bool(true));
        let config = AbstractConfig::new(originals);
        assert_eq!(config.get_bool("key"), Some(true));
    }
}
