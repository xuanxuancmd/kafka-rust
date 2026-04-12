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
    pub error_messages: Vec<String>,
}

/// Importance level of a configuration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigImportance {
    High,
    Medium,
    Low,
}

/// Configuration type
///
/// Corresponds to Java's `ConfigDef.Type` enum in `clients/src/main/java/org/apache/kafka/common/config/ConfigDef.java`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigType {
    /// Boolean type
    Boolean,
    /// String type
    String,
    /// 32-bit integer type
    Int,
    /// 16-bit integer type
    Short,
    /// 64-bit integer type
    Long,
    /// Double-precision floating-point type
    Double,
    /// List of string values
    List,
    /// Java class name
    Class,
    /// Password (sensitive string)
    Password,
}

/// Configuration key definition
///
/// Corresponds to Java's `ConfigKey` inner class in `ConfigDef.java`
#[derive(Debug, Clone)]
pub struct ConfigKey {
    /// Configuration name
    pub name: String,
    /// Configuration documentation
    pub doc: String,
    /// Configuration type
    pub type_: ConfigType,
    /// Default value (optional)
    pub default_value: Option<String>,
    /// Importance level
    pub importance: ConfigImportance,
    /// Whether the configuration is required
    pub required: bool,
}

/// Configuration with validation and error collection
///
/// Corresponds to Java's `Config` class in `connect/api/src/main/java/org/apache/kafka/common/config/Config.java`
#[derive(Debug, Clone)]
pub struct Config {
    /// Configuration values
    config_values: Vec<ConfigValue>,
    /// Error messages
    error_messages: Vec<String>,
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

impl ConfigValue {
    /// Creates a new configuration value
    pub fn new(
        name: String,
        value: Option<String>,
        default_value: Option<String>,
        documentation: String,
        importance: ConfigImportance,
        required: bool,
    ) -> Self {
        ConfigValue {
            name,
            value,
            default_value,
            documentation,
            importance,
            required,
            error_messages: Vec::new(),
        }
    }

    /// Adds an error message
    pub fn add_error(&mut self, error: String) {
        self.error_messages.push(error);
    }

    /// Gets all error messages
    pub fn error_messages(&self) -> &[String] {
        &self.error_messages
    }

    /// Checks if this configuration value has errors
    pub fn has_errors(&self) -> bool {
        !self.error_messages.is_empty()
    }
}

impl ConfigKey {
    /// Creates a new configuration key
    pub fn new(
        name: String,
        doc: String,
        type_: ConfigType,
        default_value: Option<String>,
        importance: ConfigImportance,
        required: bool,
    ) -> Self {
        ConfigKey {
            name,
            doc,
            type_,
            default_value,
            importance,
            required,
        }
    }

    /// Creates a builder-style constructor
    pub fn builder(name: String) -> ConfigKeyBuilder {
        ConfigKeyBuilder::new(name)
    }
}

/// Builder for ConfigKey
pub struct ConfigKeyBuilder {
    name: String,
    doc: String,
    type_: ConfigType,
    default_value: Option<String>,
    importance: ConfigImportance,
    required: bool,
}

impl ConfigKeyBuilder {
    pub fn new(name: String) -> Self {
        ConfigKeyBuilder {
            name,
            doc: String::new(),
            type_: ConfigType::String,
            default_value: None,
            importance: ConfigImportance::Medium,
            required: false,
        }
    }

    pub fn doc(mut self, doc: String) -> Self {
        self.doc = doc;
        self
    }

    pub fn type_(mut self, type_: ConfigType) -> Self {
        self.type_ = type_;
        self
    }

    pub fn default_value(mut self, default_value: String) -> Self {
        self.default_value = Some(default_value);
        self
    }

    pub fn importance(mut self, importance: ConfigImportance) -> Self {
        self.importance = importance;
        self
    }

    pub fn required(mut self, required: bool) -> Self {
        self.required = required;
        self
    }

    pub fn build(self) -> ConfigKey {
        ConfigKey {
            name: self.name,
            doc: self.doc,
            type_: self.type_,
            default_value: self.default_value,
            importance: self.importance,
            required: self.required,
        }
    }
}

impl Config {
    /// Creates a new configuration with given config values
    pub fn new(config_values: Vec<ConfigValue>) -> Self {
        Config {
            config_values,
            error_messages: Vec::new(),
        }
    }

    /// Creates an empty configuration
    pub fn empty() -> Self {
        Config {
            config_values: Vec::new(),
            error_messages: Vec::new(),
        }
    }

    /// Checks if configuration is valid (no errors)
    pub fn is_valid(&self) -> bool {
        self.error_messages.is_empty() && self.config_values.iter().all(|v| !v.has_errors())
    }

    /// Gets all error messages
    pub fn error_messages(&self) -> &[String] {
        &self.error_messages
    }

    /// Gets all configuration values
    pub fn values(&self) -> &[ConfigValue] {
        &self.config_values
    }

    /// Adds an error message
    pub fn add_error(&mut self, error: String) {
        self.error_messages.push(error);
    }

    /// Gets configuration value by name
    pub fn get(&self, name: &str) -> Option<&ConfigValue> {
        self.config_values.iter().find(|v| v.name == name)
    }

    /// Collects all errors from config values
    pub fn collect_errors(&mut self) {
        for config_value in &self.config_values {
            for error in config_value.error_messages() {
                self.error_messages.push(error.clone());
            }
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::empty()
    }
}
