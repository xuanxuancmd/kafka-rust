//! Kafka Configuration Trait
//!
//! Defines the core interface for Kafka configuration management.

use std::collections::HashMap;

/// Configuration definition for a Kafka client
#[derive(Debug, Clone)]
pub struct ConfigDef {
    configs: HashMap<String, ConfigValue>,
}

/// Represents a configuration value
#[derive(Debug, Clone)]
pub struct ConfigValue {
    pub name: String,
    pub value: Option<String>,
    pub default_value: Option<String>,
    pub documentation: String,
    pub importance: ConfigImportance,
    pub required: bool,
}

/// Importance level of a configuration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigImportance {
    High,
    Medium,
    Low,
}

impl ConfigDef {
    /// Creates a new empty configuration definition
    pub fn new() -> Self {
        ConfigDef {
            configs: HashMap::new(),
        }
    }

    /// Adds a configuration value
    pub fn add(&mut self, config: ConfigValue) {
        self.configs.insert(config.name.clone(), config);
    }

    /// Gets a configuration value by name
    pub fn get(&self, name: &str) -> Option<&ConfigValue> {
        self.configs.get(name)
    }

    /// Gets a configuration value as a string
    pub fn get_string(&self, name: &str) -> Option<String> {
        self.configs.get(name).and_then(|v| v.value.clone())
    }

    /// Gets a configuration value as an integer
    pub fn get_int(&self, name: &str) -> Option<i32> {
        self.configs
            .get(name)
            .and_then(|v| v.value.as_ref())
            .and_then(|s| s.parse::<i32>().ok())
    }

    /// Gets a configuration value as a boolean
    pub fn get_bool(&self, name: &str) -> Option<bool> {
        self.configs
            .get(name)
            .and_then(|v| v.value.as_ref())
            .and_then(|s| match s.to_lowercase().as_str() {
                "true" => Some(true),
                "false" => Some(false),
                _ => None,
            })
    }

    /// Sets a configuration value
    pub fn set(&mut self, name: &str, value: String) {
        if let Some(config) = self.configs.get_mut(name) {
            config.value = Some(value);
        }
    }

    /// Returns all configuration names
    pub fn keys(&self) -> Vec<String> {
        self.configs.keys().cloned().collect()
    }

    /// Validates all required configurations
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        for config in self.configs.values() {
            if config.required && config.value.is_none() {
                errors.push(format!(
                    "Required configuration '{}' is missing",
                    config.name
                ));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

impl Default for ConfigDef {
    fn default() -> Self {
        Self::new()
    }
}
