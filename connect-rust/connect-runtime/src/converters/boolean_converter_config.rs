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

//! Boolean Converter Configuration
//!
//! Configuration options for BooleanConverter instances.

use std::collections::HashMap;

/// Configuration options for BooleanConverter instances.
#[derive(Debug, Clone)]
pub struct BooleanConverterConfig {
    /// Configuration properties
    configs: HashMap<String, String>,
    /// Whether this is a key converter
    is_key: bool,
}

impl BooleanConverterConfig {
    /// Create a new BooleanConverterConfig.
    ///
    /// # Arguments
    ///
    /// * `props` - configuration properties
    pub fn new(props: HashMap<String, String>) -> Self {
        Self {
            configs: props,
            is_key: false,
        }
    }

    /// Create a new BooleanConverterConfig with is_key flag.
    ///
    /// # Arguments
    ///
    /// * `props` - configuration properties
    /// * `is_key` - whether this is a key converter
    pub fn new_with_type(props: HashMap<String, String>, is_key: bool) -> Self {
        Self {
            configs: props,
            is_key,
        }
    }

    /// Get configuration value.
    ///
    /// # Arguments
    ///
    /// * `key` - configuration key
    ///
    /// # Returns
    ///
    /// the configuration value if present
    pub fn get(&self, key: &str) -> Option<&String> {
        self.configs.get(key)
    }

    /// Check if this is a key converter.
    pub fn is_key(&self) -> bool {
        self.is_key
    }

    /// Check if this is a value converter.
    pub fn is_value(&self) -> bool {
        !self.is_key
    }
}

impl Default for BooleanConverterConfig {
    fn default() -> Self {
        Self {
            configs: HashMap::new(),
            is_key: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_boolean_converter_config_new() {
        let config = BooleanConverterConfig::new(HashMap::new());
        assert!(!config.is_key());
        assert!(config.is_value());
    }

    #[test]
    fn test_boolean_converter_config_with_type() {
        let config = BooleanConverterConfig::new_with_type(HashMap::new(), true);
        assert!(config.is_key());
        assert!(!config.is_value());
    }

    #[test]
    fn test_boolean_converter_config_get() {
        let mut props = HashMap::new();
        props.insert("test.key".to_string(), "test.value".to_string());
        let config = BooleanConverterConfig::new(props);
        assert_eq!(config.get("test.key"), Some(&"test.value".to_string()));
    }
}
