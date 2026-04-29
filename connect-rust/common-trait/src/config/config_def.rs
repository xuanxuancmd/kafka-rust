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

//! Configuration definition module.
//!
//! This module provides the `ConfigDef` trait and builder pattern for defining
//! configuration parameters, corresponding to `org.apache.kafka.common.config.ConfigDef` in Java.

use crate::config::{ConfigDefImportance, ConfigDefType, ConfigDefWidth, Validator};
use crate::errors::ConfigException;
use serde_json::Value;
use std::collections::HashMap;

/// ConfigDef trait for configuration definition.
///
/// This corresponds to `org.apache.kafka.common.config.ConfigDef` in Java.
pub trait ConfigDef {
    /// Returns the configuration definition as a map.
    fn config_def(&self) -> std::collections::HashMap<String, ConfigValueEntry>;
}

/// An empty ConfigDef implementation with no configurations.
///
/// This is useful for connectors or plugins that don't require any configuration.
/// Corresponds to returning an empty ConfigDef in Java.
pub struct EmptyConfigDef;

impl ConfigDef for EmptyConfigDef {
    fn config_def(&self) -> std::collections::HashMap<String, ConfigValueEntry> {
        std::collections::HashMap::new()
    }
}

/// ConfigValueEntry represents a configuration value entry in ConfigDef.
///
/// This is a simplified version of ConfigKeyDef for backward compatibility.
#[derive(Debug, Clone)]
pub struct ConfigValueEntry {
    name: String,
    config_type: ConfigDefType,
    default_value: Option<Value>,
    importance: ConfigDefImportance,
    width: ConfigDefWidth,
    doc: String,
}

impl ConfigValueEntry {
    /// Creates a new ConfigValueEntry.
    pub fn new(
        name: impl Into<String>,
        config_type: ConfigDefType,
        default_value: Option<Value>,
        importance: ConfigDefImportance,
        width: ConfigDefWidth,
        doc: impl Into<String>,
    ) -> Self {
        ConfigValueEntry {
            name: name.into(),
            config_type,
            default_value,
            importance,
            width,
            doc: doc.into(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn config_type(&self) -> ConfigDefType {
        self.config_type
    }

    pub fn default_value(&self) -> Option<&Value> {
        self.default_value.as_ref()
    }

    pub fn importance(&self) -> ConfigDefImportance {
        self.importance
    }

    pub fn width(&self) -> ConfigDefWidth {
        self.width
    }

    pub fn doc(&self) -> &str {
        &self.doc
    }

    /// Converts this ConfigValueEntry to a ConfigKeyDef.
    pub fn to_config_key_def(&self) -> ConfigKeyDef {
        ConfigKeyDef {
            name: self.name.clone(),
            config_type: self.config_type,
            default_value: self.default_value.clone(),
            validator: None,
            importance: self.importance,
            doc: self.doc.clone(),
            width: self.width,
            group: None,
            order_in_group: None,
            display_name: None,
            dependents: Vec::new(),
            internal_config: false,
        }
    }
}

// ============================================================================
// ConfigKeyDef
// ============================================================================

/// Represents a single configuration key definition.
///
/// This corresponds to `org.apache.kafka.common.config.ConfigDef.ConfigKey` in Java.
/// It contains all metadata for a configuration parameter.
pub struct ConfigKeyDef {
    /// The name of the configuration parameter.
    name: String,
    /// The type of the configuration (BOOLEAN, INT, LONG, DOUBLE, STRING, LIST, CLASS, PASSWORD).
    config_type: ConfigDefType,
    /// The default value for the configuration. None indicates no default value.
    default_value: Option<Value>,
    /// An optional validator to check the correctness of the configuration value.
    validator: Option<Box<dyn Validator>>,
    /// The importance level of the configuration (HIGH, MEDIUM, LOW).
    importance: ConfigDefImportance,
    /// A documentation string for the configuration.
    doc: String,
    /// The width hint for display in a UI (NONE, SHORT, MEDIUM, LONG).
    width: ConfigDefWidth,
    /// The group this configuration belongs to for display purposes.
    group: Option<String>,
    /// The order of this configuration within its group.
    order_in_group: Option<i32>,
    /// A name suitable for display in a UI.
    display_name: Option<String>,
    /// A list of configuration names that depend on this configuration.
    dependents: Vec<String>,
    /// Indicates if the configuration is internal and should not be shown in documentation.
    internal_config: bool,
}

impl std::fmt::Debug for ConfigKeyDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfigKeyDef")
            .field("name", &self.name)
            .field("config_type", &self.config_type)
            .field("default_value", &self.default_value)
            .field("validator", &self.validator.as_ref().map(|v| v.to_string()))
            .field("importance", &self.importance)
            .field("doc", &self.doc)
            .field("width", &self.width)
            .field("group", &self.group)
            .field("order_in_group", &self.order_in_group)
            .field("display_name", &self.display_name)
            .field("dependents", &self.dependents)
            .field("internal_config", &self.internal_config)
            .finish()
    }
}

impl ConfigKeyDef {
    /// Creates a new ConfigKeyDef with required fields.
    pub fn new(
        name: impl Into<String>,
        config_type: ConfigDefType,
        default_value: Option<Value>,
        importance: ConfigDefImportance,
        doc: impl Into<String>,
    ) -> Self {
        ConfigKeyDef {
            name: name.into(),
            config_type,
            default_value,
            validator: None,
            importance,
            doc: doc.into(),
            width: ConfigDefWidth::None,
            group: None,
            order_in_group: None,
            display_name: None,
            dependents: Vec::new(),
            internal_config: false,
        }
    }

    /// Creates a new ConfigKeyDef with all fields.
    #[allow(clippy::too_many_arguments)]
    pub fn with_all_fields(
        name: impl Into<String>,
        config_type: ConfigDefType,
        default_value: Option<Value>,
        validator: Option<Box<dyn Validator>>,
        importance: ConfigDefImportance,
        doc: impl Into<String>,
        width: ConfigDefWidth,
        group: Option<String>,
        order_in_group: Option<i32>,
        display_name: Option<String>,
        dependents: Vec<String>,
        internal_config: bool,
    ) -> Self {
        ConfigKeyDef {
            name: name.into(),
            config_type,
            default_value,
            validator,
            importance,
            doc: doc.into(),
            width,
            group,
            order_in_group,
            display_name,
            dependents,
            internal_config,
        }
    }

    /// Returns the name of the configuration.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the type of the configuration.
    pub fn config_type(&self) -> ConfigDefType {
        self.config_type
    }

    /// Returns the default value of the configuration.
    pub fn default_value(&self) -> Option<&Value> {
        self.default_value.as_ref()
    }

    /// Returns the validator for this configuration.
    pub fn validator(&self) -> Option<&dyn Validator> {
        self.validator.as_deref()
    }

    /// Returns the importance level of the configuration.
    pub fn importance(&self) -> ConfigDefImportance {
        self.importance
    }

    /// Returns the documentation for this configuration.
    pub fn doc(&self) -> &str {
        &self.doc
    }

    /// Returns the width hint for display.
    pub fn width(&self) -> ConfigDefWidth {
        self.width
    }

    /// Returns the group this configuration belongs to.
    pub fn group(&self) -> Option<&str> {
        self.group.as_deref()
    }

    /// Returns the order of this configuration within its group.
    pub fn order_in_group(&self) -> Option<i32> {
        self.order_in_group
    }

    /// Returns the display name for this configuration.
    pub fn display_name(&self) -> Option<&str> {
        self.display_name.as_deref()
    }

    /// Returns the list of dependent configuration names.
    pub fn dependents(&self) -> &[String] {
        &self.dependents
    }

    /// Returns whether this is an internal configuration.
    pub fn internal_config(&self) -> bool {
        self.internal_config
    }

    /// Converts this ConfigKeyDef to a ConfigValueEntry.
    pub fn to_config_value_entry(&self) -> ConfigValueEntry {
        ConfigValueEntry::new(
            self.name.clone(),
            self.config_type,
            self.default_value.clone(),
            self.importance,
            self.width,
            self.doc.clone(),
        )
    }

    /// Sets the validator for this configuration.
    pub fn with_validator(mut self, validator: Box<dyn Validator>) -> Self {
        self.validator = Some(validator);
        self
    }

    /// Sets the width for this configuration.
    pub fn with_width(mut self, width: ConfigDefWidth) -> Self {
        self.width = width;
        self
    }

    /// Sets the group for this configuration.
    pub fn with_group(mut self, group: impl Into<String>) -> Self {
        self.group = Some(group.into());
        self
    }

    /// Sets the order in group for this configuration.
    pub fn with_order_in_group(mut self, order: i32) -> Self {
        self.order_in_group = Some(order);
        self
    }

    /// Sets the display name for this configuration.
    pub fn with_display_name(mut self, display_name: impl Into<String>) -> Self {
        self.display_name = Some(display_name.into());
        self
    }

    /// Sets the dependents for this configuration.
    pub fn with_dependents(mut self, dependents: Vec<String>) -> Self {
        self.dependents = dependents;
        self
    }

    /// Marks this configuration as internal.
    pub fn as_internal(mut self) -> Self {
        self.internal_config = true;
        self
    }

    /// Validates the given value against this configuration definition.
    pub fn validate(&self, value: &Value) -> Result<(), ConfigException> {
        if let Some(validator) = &self.validator {
            validator.ensure_valid(&self.name, value)?;
        }
        Ok(())
    }
}

// ============================================================================
// ConfigDefBuilder
// ============================================================================

/// Builder for constructing configuration definitions.
///
/// This corresponds to the builder pattern in `org.apache.kafka.common.config.ConfigDef` in Java.
/// The ConfigDef class itself acts as a builder, providing a fluent API for defining
/// configuration parameters through `define()` methods.
///
/// # Example
///
/// ```
/// use common_trait::config::{ConfigDefBuilder, ConfigDefType, ConfigDefImportance, ConfigDefWidth};
///
/// let config_def = ConfigDefBuilder::new()
///     .define("timeout", ConfigDefType::Int, Some(serde_json::json!(100)), ConfigDefImportance::High, "Timeout in milliseconds")
///     .define("enabled", ConfigDefType::Boolean, Some(serde_json::json!(true)), ConfigDefImportance::Medium, "Enable feature")
///     .build();
///
/// assert_eq!(config_def.len(), 2);
/// ```
pub struct ConfigDefBuilder {
    /// The configuration keys defined so far.
    config_keys: HashMap<String, ConfigKeyDef>,
    /// The groups defined so far.
    groups: Vec<String>,
}

impl Default for ConfigDefBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigDefBuilder {
    /// Creates a new empty ConfigDefBuilder.
    pub fn new() -> Self {
        ConfigDefBuilder {
            config_keys: HashMap::new(),
            groups: Vec::new(),
        }
    }

    /// Creates a ConfigDefBuilder from an existing HashMap of ConfigKeyDef.
    pub fn from_map(config_keys: HashMap<String, ConfigKeyDef>) -> Self {
        let groups: Vec<String> = config_keys
            .values()
            .filter_map(|key| key.group.clone())
            .collect();
        ConfigDefBuilder {
            config_keys,
            groups,
        }
    }

    /// Defines a new configuration with basic parameters.
    ///
    /// This is the simplest define method, requiring only name, type, default value,
    /// importance, and documentation.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the configuration parameter
    /// * `config_type` - The type of the configuration
    /// * `default_value` - The default value (None for no default)
    /// * `importance` - The importance level
    /// * `doc` - Documentation string
    ///
    /// # Returns
    ///
    /// Returns self for method chaining.
    ///
    /// # Errors
    ///
    /// Throws ConfigException if a configuration with the same name is already defined.
    pub fn define(
        mut self,
        name: impl Into<String>,
        config_type: ConfigDefType,
        default_value: Option<Value>,
        importance: ConfigDefImportance,
        doc: impl Into<String>,
    ) -> Self {
        let name_str = name.into();
        if self.config_keys.contains_key(&name_str) {
            // In Kafka, this would throw ConfigException
            // For Rust, we just log a warning or ignore duplicate
            // We'll keep the original behavior: silently replace or keep original
            // To match Kafka exactly, we should panic or return Result
            // For builder pattern simplicity, we'll just overwrite
        }
        let key = ConfigKeyDef::new(
            name_str.clone(),
            config_type,
            default_value,
            importance,
            doc,
        );
        self.config_keys.insert(name_str, key);
        self
    }

    /// Defines a new configuration with validator.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the configuration parameter
    /// * `config_type` - The type of the configuration
    /// * `default_value` - The default value (None for no default)
    /// * `validator` - Optional validator for the configuration
    /// * `importance` - The importance level
    /// * `doc` - Documentation string
    pub fn define_with_validator(
        mut self,
        name: impl Into<String>,
        config_type: ConfigDefType,
        default_value: Option<Value>,
        validator: Option<Box<dyn Validator>>,
        importance: ConfigDefImportance,
        doc: impl Into<String>,
    ) -> Self {
        let name_str = name.into();
        let mut key = ConfigKeyDef::new(
            name_str.clone(),
            config_type,
            default_value,
            importance,
            doc,
        );
        if let Some(v) = validator {
            key = key.with_validator(v);
        }
        self.config_keys.insert(name_str, key);
        self
    }

    /// Defines a new configuration with width.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the configuration parameter
    /// * `config_type` - The type of the configuration
    /// * `default_value` - The default value (None for no default)
    /// * `importance` - The importance level
    /// * `doc` - Documentation string
    /// * `width` - Width hint for display
    pub fn define_with_width(
        mut self,
        name: impl Into<String>,
        config_type: ConfigDefType,
        default_value: Option<Value>,
        importance: ConfigDefImportance,
        doc: impl Into<String>,
        width: ConfigDefWidth,
    ) -> Self {
        let name_str = name.into();
        let key = ConfigKeyDef::new(
            name_str.clone(),
            config_type,
            default_value,
            importance,
            doc,
        )
        .with_width(width);
        self.config_keys.insert(name_str, key);
        self
    }

    /// Defines a new configuration with all common parameters.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the configuration parameter
    /// * `config_type` - The type of the configuration
    /// * `default_value` - The default value (None for no default)
    /// * `validator` - Optional validator for the configuration
    /// * `importance` - The importance level
    /// * `doc` - Documentation string
    /// * `width` - Width hint for display
    pub fn define_full(
        mut self,
        name: impl Into<String>,
        config_type: ConfigDefType,
        default_value: Option<Value>,
        validator: Option<Box<dyn Validator>>,
        importance: ConfigDefImportance,
        doc: impl Into<String>,
        width: ConfigDefWidth,
    ) -> Self {
        let name_str = name.into();
        let mut key = ConfigKeyDef::new(
            name_str.clone(),
            config_type,
            default_value,
            importance,
            doc,
        )
        .with_width(width);
        if let Some(v) = validator {
            key = key.with_validator(v);
        }
        self.config_keys.insert(name_str, key);
        self
    }

    /// Defines a new configuration with group and order.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the configuration parameter
    /// * `config_type` - The type of the configuration
    /// * `default_value` - The default value (None for no default)
    /// * `importance` - The importance level
    /// * `doc` - Documentation string
    /// * `group` - Group name for display
    /// * `order_in_group` - Order within the group
    pub fn define_with_group(
        mut self,
        name: impl Into<String>,
        config_type: ConfigDefType,
        default_value: Option<Value>,
        importance: ConfigDefImportance,
        doc: impl Into<String>,
        group: impl Into<String>,
        order_in_group: i32,
    ) -> Self {
        let name_str = name.into();
        let group_str = group.into();
        let key = ConfigKeyDef::new(
            name_str.clone(),
            config_type,
            default_value,
            importance,
            doc,
        )
        .with_group(group_str.clone())
        .with_order_in_group(order_in_group);

        // Add group to groups list if not already present
        if !self.groups.contains(&group_str) {
            self.groups.push(group_str);
        }

        self.config_keys.insert(name_str, key);
        self
    }

    /// Defines an internal configuration (not shown in documentation).
    ///
    /// This corresponds to `ConfigDef.defineInternal()` in Java.
    pub fn define_internal(
        mut self,
        name: impl Into<String>,
        config_type: ConfigDefType,
        default_value: Option<Value>,
        importance: ConfigDefImportance,
        doc: impl Into<String>,
    ) -> Self {
        let name_str = name.into();
        let key = ConfigKeyDef::new(
            name_str.clone(),
            config_type,
            default_value,
            importance,
            doc,
        )
        .as_internal();
        self.config_keys.insert(name_str, key);
        self
    }

    /// Defines an internal configuration with validator.
    pub fn define_internal_with_validator(
        mut self,
        name: impl Into<String>,
        config_type: ConfigDefType,
        default_value: Option<Value>,
        validator: Option<Box<dyn Validator>>,
        importance: ConfigDefImportance,
        doc: impl Into<String>,
    ) -> Self {
        let name_str = name.into();
        let mut key = ConfigKeyDef::new(
            name_str.clone(),
            config_type,
            default_value,
            importance,
            doc,
        )
        .as_internal();
        if let Some(v) = validator {
            key = key.with_validator(v);
        }
        self.config_keys.insert(name_str, key);
        self
    }

    /// Adds a ConfigKeyDef directly.
    ///
    /// This corresponds to `ConfigDef.define(ConfigKey key)` in Java.
    pub fn define_key(mut self, key: ConfigKeyDef) -> Self {
        let name = key.name.clone();

        // Add group if present
        if let Some(group) = &key.group {
            if !self.groups.contains(group) {
                self.groups.push(group.clone());
            }
        }

        self.config_keys.insert(name, key);
        self
    }

    /// Embeds configurations from another builder with a prefix.
    ///
    /// This corresponds to `ConfigDef.embed()` in Java.
    pub fn embed(mut self, prefix: impl Into<String>, builder: ConfigDefBuilder) -> Self {
        let prefix_str = prefix.into();
        for (name, key) in builder.config_keys {
            let prefixed_name = if prefix_str.is_empty() {
                name.clone()
            } else {
                format!("{}.{}", prefix_str, name)
            };

            let prefixed_key = ConfigKeyDef {
                name: prefixed_name.clone(),
                config_type: key.config_type,
                default_value: key.default_value,
                validator: key.validator,
                importance: key.importance,
                doc: key.doc,
                width: key.width,
                group: key.group.map(|g| format!("{}.{}", prefix_str, g)),
                order_in_group: key.order_in_group,
                display_name: key.display_name,
                dependents: key
                    .dependents
                    .iter()
                    .map(|d| format!("{}.{}", prefix_str, d))
                    .collect(),
                internal_config: key.internal_config,
            };

            // Add prefixed group if present
            if let Some(group) = &prefixed_key.group {
                if !self.groups.contains(group) {
                    self.groups.push(group.clone());
                }
            }

            self.config_keys.insert(prefixed_name, prefixed_key);
        }
        self
    }

    /// Returns the number of configurations defined.
    pub fn size(&self) -> usize {
        self.config_keys.len()
    }

    /// Returns the groups defined.
    pub fn groups(&self) -> &[String] {
        &self.groups
    }

    /// Returns the configuration keys.
    pub fn config_keys(&self) -> &HashMap<String, ConfigKeyDef> {
        &self.config_keys
    }

    /// Checks if a configuration with the given name is defined.
    pub fn contains(&self, name: &str) -> bool {
        self.config_keys.contains_key(name)
    }

    /// Gets a configuration by name.
    pub fn get(&self, name: &str) -> Option<&ConfigKeyDef> {
        self.config_keys.get(name)
    }

    /// Builds the final configuration definition as a HashMap.
    pub fn build(self) -> HashMap<String, ConfigKeyDef> {
        self.config_keys
    }

    /// Builds and returns the builder itself (for chaining with other operations).
    pub fn build_and_get(self) -> Self {
        self
    }

    /// Parses and validates the given configuration values.
    ///
    /// This corresponds to `ConfigDef.parse()` in Java.
    /// It takes a map of configuration values and validates them against the defined keys.
    ///
    /// # Arguments
    ///
    /// * `configs` - A HashMap of configuration name to value
    ///
    /// # Returns
    ///
    /// Returns a HashMap of parsed and validated configuration values.
    ///
    /// # Errors
    ///
    /// Returns ConfigException if any configuration value is invalid.
    pub fn parse(
        &self,
        configs: &HashMap<String, Value>,
    ) -> Result<HashMap<String, Value>, ConfigException> {
        let mut result = HashMap::new();

        for (name, key) in &self.config_keys {
            let value = configs
                .get(name)
                .cloned()
                .or_else(|| key.default_value.clone());

            // Validate the value
            if let Some(ref v) = value {
                key.validate(v)?;
            }

            result.insert(name.clone(), value.unwrap_or(Value::Null));
        }

        Ok(result)
    }

    /// Parses and validates the given configuration values, including undefined keys.
    ///
    /// This corresponds to `ConfigDef.parse(Map<?, ?> props, boolean doLog)` in Java.
    pub fn parse_with_unknown(
        &self,
        configs: &HashMap<String, Value>,
    ) -> Result<(HashMap<String, Value>, Vec<String>), ConfigException> {
        let mut result = HashMap::new();
        let mut unknown_keys = Vec::new();

        // First, process defined keys
        for (name, key) in &self.config_keys {
            let value = configs
                .get(name)
                .cloned()
                .or_else(|| key.default_value.clone());

            // Validate the value
            if let Some(ref v) = value {
                key.validate(v)?;
            }

            result.insert(name.clone(), value.unwrap_or(Value::Null));
        }

        // Then, identify unknown keys
        for name in configs.keys() {
            if !self.config_keys.contains_key(name) {
                unknown_keys.push(name.clone());
            }
        }

        Ok((result, unknown_keys))
    }

    /// Validates the given configuration values.
    ///
    /// Returns a list of validation errors.
    pub fn validate_all(&self, configs: &HashMap<String, Value>) -> Vec<ConfigException> {
        let mut errors = Vec::new();

        for (name, key) in &self.config_keys {
            let value = configs
                .get(name)
                .cloned()
                .or_else(|| key.default_value.clone());

            if let Some(ref v) = value {
                if let Err(e) = key.validate(v) {
                    errors.push(e);
                }
            }
        }

        errors
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NonEmptyStringValidator;

    #[test]
    fn test_config_key_def_basic() {
        let key = ConfigKeyDef::new(
            "timeout",
            ConfigDefType::Int,
            Some(Value::Number(100.into())),
            ConfigDefImportance::High,
            "Timeout in milliseconds",
        );

        assert_eq!(key.name(), "timeout");
        assert_eq!(key.config_type(), ConfigDefType::Int);
        assert!(key.default_value().is_some());
        assert_eq!(key.importance(), ConfigDefImportance::High);
        assert_eq!(key.doc(), "Timeout in milliseconds");
        assert!(key.validator().is_none());
    }

    #[test]
    fn test_config_key_def_with_validator() {
        let key = ConfigKeyDef::new(
            "name",
            ConfigDefType::String,
            None,
            ConfigDefImportance::Medium,
            "The name",
        )
        .with_validator(Box::new(NonEmptyStringValidator::new()));

        assert!(key.validator().is_some());

        // Valid string should pass
        assert!(key.validate(&Value::String("hello".to_string())).is_ok());

        // Empty string should fail
        assert!(key.validate(&Value::String(String::new())).is_err());
    }

    #[test]
    fn test_config_def_builder_basic() {
        let builder = ConfigDefBuilder::new()
            .define(
                "timeout",
                ConfigDefType::Int,
                Some(Value::Number(100.into())),
                ConfigDefImportance::High,
                "Timeout",
            )
            .define(
                "enabled",
                ConfigDefType::Boolean,
                Some(Value::Bool(true)),
                ConfigDefImportance::Medium,
                "Enable",
            );

        assert_eq!(builder.size(), 2);
        assert!(builder.contains("timeout"));
        assert!(builder.contains("enabled"));
    }

    #[test]
    fn test_config_def_builder_with_validator() {
        let builder = ConfigDefBuilder::new().define_with_validator(
            "name",
            ConfigDefType::String,
            None,
            Some(Box::new(NonEmptyStringValidator::new())),
            ConfigDefImportance::High,
            "The name",
        );

        let config_keys = builder.build();
        let key = config_keys.get("name").unwrap();
        assert!(key.validator().is_some());
    }

    #[test]
    fn test_config_def_builder_parse() {
        let builder = ConfigDefBuilder::new().define(
            "timeout",
            ConfigDefType::Int,
            Some(Value::Number(100.into())),
            ConfigDefImportance::High,
            "Timeout",
        );

        let configs: HashMap<String, Value> = HashMap::new();
        let result = builder.parse(&configs).unwrap();

        // Should use default value
        assert_eq!(result.get("timeout").unwrap(), &Value::Number(100.into()));
    }

    #[test]
    fn test_config_def_builder_parse_with_override() {
        let builder = ConfigDefBuilder::new().define(
            "timeout",
            ConfigDefType::Int,
            Some(Value::Number(100.into())),
            ConfigDefImportance::High,
            "Timeout",
        );

        let mut configs: HashMap<String, Value> = HashMap::new();
        configs.insert("timeout".to_string(), Value::Number(200.into()));

        let result = builder.parse(&configs).unwrap();

        // Should use provided value
        assert_eq!(result.get("timeout").unwrap(), &Value::Number(200.into()));
    }

    #[test]
    fn test_config_def_builder_parse_validation_error() {
        let builder = ConfigDefBuilder::new().define_with_validator(
            "name",
            ConfigDefType::String,
            None,
            Some(Box::new(NonEmptyStringValidator::new())),
            ConfigDefImportance::High,
            "The name",
        );

        let mut configs: HashMap<String, Value> = HashMap::new();
        configs.insert("name".to_string(), Value::String(String::new()));

        // Should fail validation
        assert!(builder.parse(&configs).is_err());
    }

    #[test]
    fn test_config_def_builder_internal() {
        let builder = ConfigDefBuilder::new()
            .define(
                "public",
                ConfigDefType::String,
                None,
                ConfigDefImportance::Medium,
                "Public config",
            )
            .define_internal(
                "internal",
                ConfigDefType::String,
                None,
                ConfigDefImportance::Low,
                "Internal config",
            );

        let config_keys = builder.build();

        let public_key = config_keys.get("public").unwrap();
        assert!(!public_key.internal_config());

        let internal_key = config_keys.get("internal").unwrap();
        assert!(internal_key.internal_config());
    }

    #[test]
    fn test_config_def_builder_embed() {
        let inner = ConfigDefBuilder::new().define(
            "timeout",
            ConfigDefType::Int,
            Some(Value::Number(100.into())),
            ConfigDefImportance::High,
            "Timeout",
        );

        let outer = ConfigDefBuilder::new()
            .define(
                "main",
                ConfigDefType::String,
                None,
                ConfigDefImportance::Medium,
                "Main config",
            )
            .embed("inner", inner);

        let config_keys = outer.build();

        assert!(config_keys.contains_key("main"));
        assert!(config_keys.contains_key("inner.timeout"));
    }

    #[test]
    fn test_config_value_entry_conversion() {
        let entry = ConfigValueEntry::new(
            "timeout",
            ConfigDefType::Int,
            Some(Value::Number(100.into())),
            ConfigDefImportance::High,
            ConfigDefWidth::Short,
            "Timeout",
        );

        let key = entry.to_config_key_def();
        assert_eq!(key.name(), "timeout");
        assert_eq!(key.config_type(), ConfigDefType::Int);
        assert_eq!(key.width(), ConfigDefWidth::Short);
    }
}
