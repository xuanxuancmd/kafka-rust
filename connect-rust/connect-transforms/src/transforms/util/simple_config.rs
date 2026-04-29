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

//! SimpleConfig - A barebones concrete implementation of AbstractConfig.
//!
//! This corresponds to `org.apache.kafka.connect.transforms.util.SimpleConfig` in Java.
//! It is a simple wrapper around configuration values that provides convenient getter methods.

use common_trait::config::{AbstractConfig, ConfigDefBuilder};
use common_trait::ConfigException;
use serde_json::Value;
use std::collections::HashMap;

/// A barebones concrete implementation of AbstractConfig.
///
/// SimpleConfig is used by transformations to parse and validate configuration
/// values based on a ConfigDef definition.
///
/// # Example
///
/// ```
/// use connect_transforms::transforms::util::SimpleConfig;
/// use common_trait::config::{ConfigDefBuilder, ConfigDefType, ConfigDefImportance};
/// use serde_json::json;
/// use std::collections::HashMap;
///
/// let config_def = ConfigDefBuilder::new()
///     .define("name", ConfigDefType::String, None, ConfigDefImportance::High, "The name")
///     .define("count", ConfigDefType::Int, Some(json!(10)), ConfigDefImportance::Medium, "The count")
///     .build();
///
/// let mut props = HashMap::new();
/// props.insert("name".to_string(), json!("test"));
///
/// let config = SimpleConfig::new(config_def, props).unwrap();
/// assert_eq!(config.get_string("name"), Some("test"));
/// assert_eq!(config.get_int("count"), Some(10));
/// ```
pub struct SimpleConfig {
    /// The underlying AbstractConfig that stores parsed configuration values.
    config: AbstractConfig,
}

impl SimpleConfig {
    /// Creates a new SimpleConfig with the given ConfigDef and original properties.
    ///
    /// This corresponds to `SimpleConfig(ConfigDef configDef, Map<?, ?> originals)` in Java.
    ///
    /// # Arguments
    ///
    /// * `config_def` - A HashMap of ConfigKeyDef defining the configuration schema
    /// * `originals` - A HashMap of original configuration values
    ///
    /// # Returns
    ///
    /// Returns a Result containing the SimpleConfig or a ConfigException if validation fails.
    ///
    /// # Errors
    ///
    /// Returns ConfigException if any configuration value fails validation.
    pub fn new(
        config_def: HashMap<String, common_trait::config::ConfigKeyDef>,
        originals: HashMap<String, Value>,
    ) -> Result<Self, ConfigException> {
        // Use ConfigDefBuilder to parse and validate the configuration
        let builder = ConfigDefBuilder::from_map(config_def);
        let parsed = builder.parse(&originals)?;

        // Create AbstractConfig from parsed values
        let config = AbstractConfig::new(parsed);

        Ok(SimpleConfig { config })
    }

    /// Creates a SimpleConfig from a ConfigDefBuilder and original properties.
    ///
    /// This is a convenience method that takes a builder instead of a built HashMap.
    ///
    /// # Arguments
    ///
    /// * `builder` - A ConfigDefBuilder defining the configuration schema
    /// * `originals` - A HashMap of original configuration values
    ///
    /// # Returns
    ///
    /// Returns a Result containing the SimpleConfig or a ConfigException if validation fails.
    pub fn from_builder(
        builder: ConfigDefBuilder,
        originals: HashMap<String, Value>,
    ) -> Result<Self, ConfigException> {
        let parsed = builder.parse(&originals)?;
        let config = AbstractConfig::new(parsed);
        Ok(SimpleConfig { config })
    }

    /// Returns the raw configuration value for the given key.
    ///
    /// This corresponds to `AbstractConfig.get(String key)` in Java.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.config.get(key)
    }

    /// Returns the configuration value as a string.
    ///
    /// This corresponds to `AbstractConfig.getString(String key)` in Java.
    /// Returns None if the key does not exist or the value is not a string.
    pub fn get_string(&self, key: &str) -> Option<&str> {
        self.config.get_string(key)
    }

    /// Returns the configuration value as an integer.
    ///
    /// This corresponds to `AbstractConfig.getInt(String key)` in Java.
    /// Returns None if the key does not exist or the value is not an integer.
    pub fn get_int(&self, key: &str) -> Option<i64> {
        self.config.get_int(key)
    }

    /// Returns the configuration value as a boolean.
    ///
    /// This corresponds to `AbstractConfig.getBoolean(String key)` in Java.
    /// Returns None if the key does not exist or the value is not a boolean.
    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.config.get_bool(key)
    }

    /// Returns the configuration value as a double.
    ///
    /// This corresponds to `AbstractConfig.getDouble(String key)` in Java.
    /// Returns None if the key does not exist or the value is not a number.
    pub fn get_double(&self, key: &str) -> Option<f64> {
        self.config.get_double(key)
    }

    /// Returns the configuration value as a list of strings.
    ///
    /// This corresponds to `AbstractConfig.getList(String key)` in Java.
    /// Returns None if the key does not exist or the value is not an array.
    /// Returns an empty Vec if the value is an empty array.
    pub fn get_list(&self, key: &str) -> Option<Vec<String>> {
        self.config.get(key).and_then(|v| {
            v.as_array().map(|arr| {
                arr.iter()
                    .filter_map(|item| item.as_str().map(|s| s.to_string()))
                    .collect()
            })
        })
    }

    /// Returns the configuration value as a long (same as get_int in this implementation).
    ///
    /// This corresponds to `AbstractConfig.getLong(String key)` in Java.
    /// Returns None if the key does not exist or the value is not an integer.
    pub fn get_long(&self, key: &str) -> Option<i64> {
        self.config.get_int(key)
    }

    /// Returns all configuration values as a reference to the HashMap.
    ///
    /// This corresponds to `AbstractConfig.originals()` in Java.
    pub fn originals(&self) -> &HashMap<String, Value> {
        self.config.originals()
    }

    /// Returns all configuration values as a cloned HashMap.
    ///
    /// This corresponds to `AbstractConfig.originalsStrings()` in Java.
    pub fn originals_strings(&self) -> HashMap<String, String> {
        self.config
            .originals()
            .iter()
            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
            .collect()
    }

    /// Parses a list configuration value from a config HashMap.
    ///
    /// This is a convenience static method for parsing list configs
    /// without creating a full SimpleConfig instance.
    ///
    /// # Arguments
    /// * `configs` - The configuration HashMap
    /// * `key` - The configuration key to parse
    ///
    /// # Returns
    /// Returns an Option containing the parsed Vec<String>, or None if not found.
    pub fn parse_list_config(configs: &HashMap<String, Value>, key: &str) -> Option<Vec<String>> {
        configs.get(key).and_then(|v| {
            v.as_array().map(|arr| {
                arr.iter()
                    .filter_map(|item| item.as_str().map(|s| s.to_string()))
                    .collect()
            })
        })
    }

    /// Parses a boolean configuration value from a config HashMap.
    ///
    /// This is a convenience static method for parsing boolean configs
    /// without creating a full SimpleConfig instance.
    ///
    /// # Arguments
    /// * `configs` - The configuration HashMap
    /// * `key` - The configuration key to parse
    /// * `default` - The default value if key is not found
    ///
    /// # Returns
    /// Returns the parsed boolean value, or the default if not found.
    pub fn parse_bool_config(configs: &HashMap<String, Value>, key: &str, default: bool) -> bool {
        configs
            .get(key)
            .and_then(|v| v.as_bool())
            .unwrap_or(default)
    }
}

