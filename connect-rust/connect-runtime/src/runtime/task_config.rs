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

//! Task configuration for Kafka Connect.
//!
//! Configuration options for Tasks. These only include Kafka Connect system-level
//! configuration options.
//!
//! Corresponds to Java: `org.apache.kafka.connect.runtime.TaskConfig`

use common_trait::config::AbstractConfig;
use serde_json::Value;
use std::collections::HashMap;

/// Configuration key for task class.
///
/// Corresponds to Java: `TaskConfig.TASK_CLASS_CONFIG`
pub const TASK_CLASS_CONFIG: &str = "task.class";

/// Documentation for task class configuration.
///
/// Corresponds to Java: `TaskConfig.TASK_CLASS_DOC`
const TASK_CLASS_DOC: &str = "Name of the class for this task. Must be a subclass of org.apache.kafka.connect.connector.Task";

/// Configuration options for Tasks.
///
/// These only include Kafka Connect system-level configuration options.
/// This configuration is used to identify and instantiate task instances.
///
/// Corresponds to Java: `org.apache.kafka.connect.runtime.TaskConfig`
/// (Java: extends `AbstractConfig`)
#[derive(Debug, Clone)]
pub struct TaskConfig {
    /// The underlying configuration.
    config: AbstractConfig,
}

impl TaskConfig {
    /// Creates a new TaskConfig with empty configuration.
    ///
    /// Corresponds to Java: `TaskConfig()`
    pub fn new_empty() -> Self {
        TaskConfig {
            config: AbstractConfig::new(HashMap::new()),
        }
    }

    /// Creates a new TaskConfig with the given properties.
    ///
    /// # Arguments
    /// * `props` - Configuration properties map
    ///
    /// Corresponds to Java: `TaskConfig(Map<String, ?> props)`
    pub fn new(props: HashMap<String, Value>) -> Self {
        // In Java, this calls super(CONFIG, props, true)
        // where CONFIG is a static ConfigDef with TASK_CLASS_CONFIG defined
        TaskConfig {
            config: AbstractConfig::new(props),
        }
    }

    /// Returns the task class name.
    ///
    /// The task class must be a subclass of `Task`.
    ///
    /// Corresponds to Java: TaskConfig.getString(TASK_CLASS_CONFIG)
    pub fn task_class(&self) -> Option<&str> {
        self.config.get_string(TASK_CLASS_CONFIG)
    }

    /// Returns all original configuration values.
    ///
    /// Corresponds to Java: `AbstractConfig.originals()`
    pub fn originals(&self) -> &HashMap<String, Value> {
        self.config.originals()
    }

    /// Returns the raw configuration value for the given key.
    ///
    /// Corresponds to Java: `AbstractConfig.get(String key)`
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.config.get(key)
    }

    /// Returns the configuration value as a string.
    ///
    /// Corresponds to Java: `AbstractConfig.getString(String key)`
    pub fn get_string(&self, key: &str) -> Option<&str> {
        self.config.get_string(key)
    }

    /// Returns the configuration value as an integer.
    ///
    /// Corresponds to Java: `AbstractConfig.getInt(String key)`
    pub fn get_int(&self, key: &str) -> Option<i64> {
        self.config.get_int(key)
    }

    /// Returns the configuration value as a boolean.
    ///
    /// Corresponds to Java: `AbstractConfig.getBoolean(String key)`
    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.config.get_bool(key)
    }

    /// Returns the configuration value as a double.
    ///
    /// Corresponds to Java: `AbstractConfig.getDouble(String key)`
    pub fn get_double(&self, key: &str) -> Option<f64> {
        self.config.get_double(key)
    }
}

impl Default for TaskConfig {
    fn default() -> Self {
        Self::new_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_config_empty() {
        let config = TaskConfig::new_empty();
        assert!(config.task_class().is_none());
        assert_eq!(config.originals().len(), 0);
    }

    #[test]
    fn test_task_config_with_task_class() {
        let mut props = HashMap::new();
        props.insert(
            TASK_CLASS_CONFIG.to_string(),
            Value::String("org.apache.kafka.connect.file.FileSourceTask".to_string()),
        );

        let config = TaskConfig::new(props);
        assert_eq!(
            config.task_class(),
            Some("org.apache.kafka.connect.file.FileSourceTask")
        );
    }

    #[test]
    fn test_task_config_default() {
        let config = TaskConfig::default();
        assert!(config.task_class().is_none());
    }

    #[test]
    fn test_task_config_with_multiple_properties() {
        let mut props = HashMap::new();
        props.insert(
            TASK_CLASS_CONFIG.to_string(),
            Value::String("TestTask".to_string()),
        );
        props.insert(
            "key.converter".to_string(),
            Value::String("StringConverter".to_string()),
        );
        props.insert(
            "value.converter".to_string(),
            Value::String("JsonConverter".to_string()),
        );

        let config = TaskConfig::new(props);
        assert_eq!(config.task_class(), Some("TestTask"));
        assert_eq!(config.get_string("key.converter"), Some("StringConverter"));
        assert_eq!(config.get_string("value.converter"), Some("JsonConverter"));
    }

    #[test]
    fn test_task_config_getters() {
        let mut props = HashMap::new();
        props.insert("int.key".to_string(), Value::Number(42.into()));
        props.insert("bool.key".to_string(), Value::Bool(true));
        props.insert(
            "double.key".to_string(),
            Value::Number(serde_json::Number::from_f64(3.14).unwrap()),
        );

        let config = TaskConfig::new(props);
        assert_eq!(config.get_int("int.key"), Some(42));
        assert_eq!(config.get_bool("bool.key"), Some(true));
        assert_eq!(config.get_double("double.key"), Some(3.14));
    }
}
