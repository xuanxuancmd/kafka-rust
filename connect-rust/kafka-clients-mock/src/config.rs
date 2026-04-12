//! Mock Configuration Implementation
//!
//! This module provides a mock wrapper around ConfigDef for testing purposes.

use kafka_clients_trait::config::{ConfigDef, ConfigImportance, ConfigValue};

/// Mock configuration wrapper for testing.
pub struct MockConfigDef {
    config: ConfigDef,
}

impl MockConfigDef {
    /// Create a new mock configuration
    pub fn new() -> Self {
        MockConfigDef {
            config: ConfigDef::new(),
        }
    }

    /// Create a mock configuration with default values
    pub fn with_defaults() -> Self {
        let mut config = ConfigDef::new();

        // Common Kafka configuration defaults
        config.add(ConfigValue {
            name: "bootstrap.servers".to_string(),
            value: Some("localhost:9092".to_string()),
            default_value: Some("localhost:9092".to_string()),
            documentation: "List of brokers".to_string(),
            importance: ConfigImportance::High,
            required: true,
            error_messages: Vec::new(),
        });

        config.add(ConfigValue {
            name: "acks".to_string(),
            value: Some("all".to_string()),
            default_value: Some("all".to_string()),
            documentation: "Number of acknowledgments".to_string(),
            importance: ConfigImportance::High,
            required: false,
            error_messages: Vec::new(),
        });

        config.add(ConfigValue {
            name: "retries".to_string(),
            value: Some("3".to_string()),
            default_value: Some("3".to_string()),
            documentation: "Number of retries".to_string(),
            importance: ConfigImportance::Medium,
            required: false,
            error_messages: Vec::new(),
        });

        config.add(ConfigValue {
            name: "batch.size".to_string(),
            value: Some("16384".to_string()),
            default_value: Some("16384".to_string()),
            documentation: "Batch size in bytes".to_string(),
            importance: ConfigImportance::Medium,
            required: false,
            error_messages: Vec::new(),
        });

        config.add(ConfigValue {
            name: "linger.ms".to_string(),
            value: Some("0".to_string()),
            default_value: Some("0".to_string()),
            documentation: "Linger time in milliseconds".to_string(),
            importance: ConfigImportance::Medium,
            required: false,
            error_messages: Vec::new(),
        });

        config.add(ConfigValue {
            name: "buffer.memory".to_string(),
            value: Some("33554432".to_string()),
            default_value: Some("33554432".to_string()),
            documentation: "Buffer memory in bytes".to_string(),
            importance: ConfigImportance::Medium,
            required: false,
            error_messages: Vec::new(),
        });

        config.add(ConfigValue {
            name: "group.id".to_string(),
            value: Some("test-group".to_string()),
            default_value: None,
            documentation: "Consumer group ID".to_string(),
            importance: ConfigImportance::High,
            required: false,
            error_messages: Vec::new(),
        });

        config.add(ConfigValue {
            name: "auto.offset.reset".to_string(),
            value: Some("latest".to_string()),
            default_value: Some("latest".to_string()),
            documentation: "Auto offset reset policy".to_string(),
            importance: ConfigImportance::Medium,
            required: false,
            error_messages: Vec::new(),
        });

        config.add(ConfigValue {
            name: "enable.auto.commit".to_string(),
            value: Some("true".to_string()),
            default_value: Some("true".to_string()),
            documentation: "Enable auto commit".to_string(),
            importance: ConfigImportance::Medium,
            required: false,
            error_messages: Vec::new(),
        });

        MockConfigDef { config }
    }

    /// Get the underlying ConfigDef
    pub fn get_config(&self) -> &ConfigDef {
        &self.config
    }

    /// Get mutable reference to underlying ConfigDef
    pub fn get_config_mut(&mut self) -> &mut ConfigDef {
        &mut self.config
    }

    /// Get a configuration value by name
    pub fn get(&self, name: &str) -> Option<&ConfigValue> {
        self.config.get(name)
    }

    /// Get a configuration value as a string
    pub fn get_string(&self, name: &str) -> Option<String> {
        self.config.get_string(name)
    }

    /// Get a configuration value as an integer
    pub fn get_int(&self, name: &str) -> Option<i32> {
        self.config.get_int(name)
    }

    /// Get a configuration value as a boolean
    pub fn get_bool(&self, name: &str) -> Option<bool> {
        self.config.get_bool(name)
    }

    /// Set a configuration value
    pub fn set(&mut self, name: &str, value: String) {
        self.config.set(name, value);
    }

    /// Add a configuration value
    pub fn add(&mut self, config: ConfigValue) {
        self.config.add(config);
    }

    /// Returns all configuration names
    pub fn keys(&self) -> Vec<String> {
        self.config.keys()
    }

    /// Validates all required configurations
    pub fn validate(&self) -> Result<(), Vec<String>> {
        self.config.validate()
    }
}

impl Default for MockConfigDef {
    fn default() -> Self {
        Self::new()
    }
}
