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

//! Plugin recommenders for Kafka Connect configuration.
//!
//! This module provides recommenders for plugin configuration options.
//! Recommenders provide valid values and visibility controls for configuration
//! options in the Kafka Connect REST API.
//!
//! Corresponds to Java: org.apache.kafka.connect.runtime.isolation.PluginsRecommenders

use crate::config::connector_config::{
    CONNECTOR_CLASS_CONFIG, CONNECTOR_HEADER_CONVERTER_CLASS_CONFIG,
    CONNECTOR_KEY_CONVERTER_CLASS_CONFIG, CONNECTOR_VALUE_CONVERTER_CLASS_CONFIG,
};
use crate::isolation::plugin_desc::PluginDesc;
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// Trait for configuration recommenders.
///
/// Recommenders provide valid values and visibility controls for configuration options.
/// This corresponds to Java's `ConfigDef.Recommender` interface.
pub trait Recommender {
    /// Returns the list of valid values for a configuration option.
    ///
    /// # Arguments
    /// * `name` - The name of the configuration option
    /// * `parsed_config` - The parsed configuration values
    fn valid_values(&self, name: &str, parsed_config: &HashMap<String, Value>) -> Vec<Value>;

    /// Returns whether this configuration option should be visible.
    ///
    /// # Arguments
    /// * `name` - The name of the configuration option
    /// * `parsed_config` - The parsed configuration values
    fn visible(&self, name: &str, parsed_config: &HashMap<String, Value>) -> bool;
}

/// Plugins collection trait for retrieving plugin information.
///
/// This trait defines the interface that PluginsRecommenders needs to access
/// plugin information. It's implemented by the Plugins struct.
pub trait PluginsAccess {
    /// Returns all source connectors matching the given class or alias.
    fn source_connectors(&self, class_or_alias: &str) -> Vec<PluginDesc>;

    /// Returns all sink connectors matching the given class or alias.
    fn sink_connectors(&self, class_or_alias: &str) -> Vec<PluginDesc>;

    /// Returns all converters.
    fn converters(&self) -> Vec<PluginDesc>;

    /// Returns converters matching the given class or alias.
    fn converters_for_class(&self, class_or_alias: &str) -> Vec<PluginDesc>;

    /// Returns all header converters.
    fn header_converters(&self) -> Vec<PluginDesc>;

    /// Returns header converters matching the given class or alias.
    fn header_converters_for_class(&self, class_or_alias: &str) -> Vec<PluginDesc>;

    /// Returns transformations matching the given class or alias.
    fn transformations(&self, class_or_alias: &str) -> HashSet<PluginDesc>;

    /// Returns predicates matching the given class or alias.
    fn predicates(&self, class_or_alias: &str) -> HashSet<PluginDesc>;
}

/// Plugin recommenders manager.
///
/// This class provides recommenders for various plugin configuration options
/// in Kafka Connect. It corresponds to Java's `PluginsRecommenders`.
pub struct PluginsRecommenders {
    plugins: Option<Box<dyn PluginsAccess>>,
    converter_plugin_recommender: ConverterPluginRecommender,
    connector_plugin_version_recommender: ConnectorPluginVersionRecommender,
    header_converter_plugin_recommender: HeaderConverterPluginRecommender,
    key_converter_plugin_version_recommender: KeyConverterPluginVersionRecommender,
    value_converter_plugin_version_recommender: ValueConverterPluginVersionRecommender,
    header_converter_plugin_version_recommender: HeaderConverterPluginVersionRecommender,
}

impl PluginsRecommenders {
    /// Creates a new PluginsRecommenders without a plugins reference.
    pub fn new() -> Self {
        Self::with_plugins(None)
    }

    /// Creates a new PluginsRecommenders with a plugins reference.
    pub fn with_plugins(plugins: Option<Box<dyn PluginsAccess>>) -> Self {
        Self {
            plugins,
            converter_plugin_recommender: ConverterPluginRecommender,
            connector_plugin_version_recommender: ConnectorPluginVersionRecommender,
            header_converter_plugin_recommender: HeaderConverterPluginRecommender,
            key_converter_plugin_version_recommender: KeyConverterPluginVersionRecommender,
            value_converter_plugin_version_recommender: ValueConverterPluginVersionRecommender,
            header_converter_plugin_version_recommender: HeaderConverterPluginVersionRecommender,
        }
    }

    /// Returns the converter plugin recommender.
    pub fn converter_plugin_recommender(&self) -> &ConverterPluginRecommender {
        &self.converter_plugin_recommender
    }

    /// Returns the connector plugin version recommender.
    pub fn connector_plugin_version_recommender(&self) -> &ConnectorPluginVersionRecommender {
        &self.connector_plugin_version_recommender
    }

    /// Returns the header converter plugin recommender.
    pub fn header_converter_plugin_recommender(&self) -> &HeaderConverterPluginRecommender {
        &self.header_converter_plugin_recommender
    }

    /// Returns the key converter plugin version recommender.
    pub fn key_converter_plugin_version_recommender(
        &self,
    ) -> &KeyConverterPluginVersionRecommender {
        &self.key_converter_plugin_version_recommender
    }

    /// Returns the value converter plugin version recommender.
    pub fn value_converter_plugin_version_recommender(
        &self,
    ) -> &ValueConverterPluginVersionRecommender {
        &self.value_converter_plugin_version_recommender
    }

    /// Returns the header converter plugin version recommender.
    pub fn header_converter_plugin_version_recommender(
        &self,
    ) -> &HeaderConverterPluginVersionRecommender {
        &self.header_converter_plugin_version_recommender
    }

    /// Creates a transformation plugin recommender for the given config.
    pub fn transformation_plugin_recommender(
        &self,
        class_or_alias_config: &str,
    ) -> TransformationPluginRecommender<'_> {
        TransformationPluginRecommender {
            plugins: self.plugins.as_deref(),
            class_or_alias_config: class_or_alias_config.to_string(),
        }
    }

    /// Creates a predicate plugin recommender for the given config.
    pub fn predicate_plugin_recommender(
        &self,
        class_or_alias_config: &str,
    ) -> PredicatePluginRecommender<'_> {
        PredicatePluginRecommender {
            plugins: self.plugins.as_deref(),
            class_or_alias_config: class_or_alias_config.to_string(),
        }
    }

    /// Returns a reference to the plugins accessor if available.
    pub fn plugins_ref(&self) -> Option<&dyn PluginsAccess> {
        self.plugins.as_deref()
    }
}

impl Default for PluginsRecommenders {
    fn default() -> Self {
        Self::new()
    }
}

/// Recommender for connector plugin versions.
///
/// This recommender provides valid version values for connector plugins.
/// Corresponds to Java's `PluginsRecommenders.ConnectorPluginVersionRecommender`.
pub struct ConnectorPluginVersionRecommender;

impl Recommender for ConnectorPluginVersionRecommender {
    fn valid_values(&self, _name: &str, parsed_config: &HashMap<String, Value>) -> Vec<Value> {
        // Need Plugins reference to get actual versions
        // Return empty list if we can't access plugins or connector class
        let connector_class = parsed_config.get(CONNECTOR_CLASS_CONFIG);
        if connector_class.is_none() {
            return Vec::new();
        }
        // In full implementation, this would query plugins for available versions
        Vec::new()
    }

    fn visible(&self, _name: &str, parsed_config: &HashMap<String, Value>) -> bool {
        parsed_config.get(CONNECTOR_CLASS_CONFIG).is_some()
    }
}

/// Recommender for converter plugins.
///
/// This recommender provides valid converter class names.
/// Corresponds to Java's `PluginsRecommenders.ConverterPluginRecommender`.
pub struct ConverterPluginRecommender;

impl Recommender for ConverterPluginRecommender {
    fn valid_values(&self, _name: &str, _parsed_config: &HashMap<String, Value>) -> Vec<Value> {
        // In full implementation, this would return distinct converter class names
        // Example: ["org.apache.kafka.connect.storage.JsonConverter", ...]
        Vec::new()
    }

    fn visible(&self, _name: &str, _parsed_config: &HashMap<String, Value>) -> bool {
        true
    }
}

/// Recommender for header converter plugins.
///
/// This recommender provides valid header converter class names.
/// Corresponds to Java's `PluginsRecommenders.HeaderConverterPluginRecommender`.
pub struct HeaderConverterPluginRecommender;

impl Recommender for HeaderConverterPluginRecommender {
    fn valid_values(&self, _name: &str, _parsed_config: &HashMap<String, Value>) -> Vec<Value> {
        // In full implementation, this would return distinct header converter class names
        Vec::new()
    }

    fn visible(&self, _name: &str, _parsed_config: &HashMap<String, Value>) -> bool {
        true
    }
}

/// Recommender for key converter plugin versions.
///
/// This recommender provides valid version values for key converters.
/// Corresponds to Java's `PluginsRecommenders.KeyConverterPluginVersionRecommender`.
pub struct KeyConverterPluginVersionRecommender;

impl Recommender for KeyConverterPluginVersionRecommender {
    fn valid_values(&self, _name: &str, parsed_config: &HashMap<String, Value>) -> Vec<Value> {
        if parsed_config
            .get(CONNECTOR_KEY_CONVERTER_CLASS_CONFIG)
            .is_none()
        {
            return Vec::new();
        }
        // In full implementation, this would query plugins for available versions
        Vec::new()
    }

    fn visible(&self, _name: &str, parsed_config: &HashMap<String, Value>) -> bool {
        parsed_config
            .get(CONNECTOR_KEY_CONVERTER_CLASS_CONFIG)
            .is_some()
    }
}

/// Recommender for value converter plugin versions.
///
/// This recommender provides valid version values for value converters.
/// Corresponds to Java's `PluginsRecommenders.ValueConverterPluginVersionRecommender`.
pub struct ValueConverterPluginVersionRecommender;

impl Recommender for ValueConverterPluginVersionRecommender {
    fn valid_values(&self, _name: &str, parsed_config: &HashMap<String, Value>) -> Vec<Value> {
        if parsed_config
            .get(CONNECTOR_VALUE_CONVERTER_CLASS_CONFIG)
            .is_none()
        {
            return Vec::new();
        }
        // In full implementation, this would query plugins for available versions
        Vec::new()
    }

    fn visible(&self, _name: &str, parsed_config: &HashMap<String, Value>) -> bool {
        parsed_config
            .get(CONNECTOR_VALUE_CONVERTER_CLASS_CONFIG)
            .is_some()
    }
}

/// Recommender for header converter plugin versions.
///
/// This recommender provides valid version values for header converters.
/// Corresponds to Java's `PluginsRecommenders.HeaderConverterPluginVersionRecommender`.
pub struct HeaderConverterPluginVersionRecommender;

impl Recommender for HeaderConverterPluginVersionRecommender {
    fn valid_values(&self, _name: &str, parsed_config: &HashMap<String, Value>) -> Vec<Value> {
        if parsed_config
            .get(CONNECTOR_HEADER_CONVERTER_CLASS_CONFIG)
            .is_none()
        {
            return Vec::new();
        }
        // In full implementation, this would query plugins for available versions
        Vec::new()
    }

    fn visible(&self, _name: &str, parsed_config: &HashMap<String, Value>) -> bool {
        parsed_config
            .get(CONNECTOR_HEADER_CONVERTER_CLASS_CONFIG)
            .is_some()
    }
}

/// Recommender for transformation plugins.
///
/// This recommender provides valid version values for transformation plugins.
/// Corresponds to Java: `PluginsRecommenders.TransformationPluginRecommender`.
pub struct TransformationPluginRecommender<'a> {
    plugins: Option<&'a dyn PluginsAccess>,
    class_or_alias_config: String,
}

impl<'a> Recommender for TransformationPluginRecommender<'a> {
    fn valid_values(&self, _name: &str, parsed_config: &HashMap<String, Value>) -> Vec<Value> {
        if self.plugins.is_none() {
            return Vec::new();
        }
        if parsed_config.get(&self.class_or_alias_config).is_none() {
            return Vec::new();
        }
        // In full implementation, this would query plugins for available versions
        Vec::new()
    }

    fn visible(&self, _name: &str, _parsed_config: &HashMap<String, Value>) -> bool {
        true
    }
}

/// Recommender for predicate plugins.
///
/// This recommender provides valid version values for predicate plugins.
/// Corresponds to Java: `PluginsRecommenders.PredicatePluginRecommender`.
pub struct PredicatePluginRecommender<'a> {
    plugins: Option<&'a dyn PluginsAccess>,
    class_or_alias_config: String,
}

impl<'a> Recommender for PredicatePluginRecommender<'a> {
    fn valid_values(&self, _name: &str, parsed_config: &HashMap<String, Value>) -> Vec<Value> {
        if self.plugins.is_none() {
            return Vec::new();
        }
        if parsed_config.get(&self.class_or_alias_config).is_none() {
            return Vec::new();
        }
        // In full implementation, this would query plugins for available versions
        Vec::new()
    }

    fn visible(&self, _name: &str, _parsed_config: &HashMap<String, Value>) -> bool {
        true
    }
}

/// Abstract base for converter plugin version recommenders.
///
/// This provides common functionality for converter version recommenders.
pub struct ConverterPluginVersionRecommenderBase {
    converter_config: String,
}

impl ConverterPluginVersionRecommenderBase {
    /// Creates a new base recommender for the given converter config key.
    pub fn new(converter_config: &str) -> Self {
        Self {
            converter_config: converter_config.to_string(),
        }
    }

    /// Gets recommendations for converter versions.
    pub fn get_recommendations(
        &self,
        plugins: &dyn PluginsAccess,
        converter_class: &str,
        is_header_converter: bool,
    ) -> Vec<Value> {
        let descs = if is_header_converter {
            plugins.header_converters_for_class(converter_class)
        } else {
            plugins.converters_for_class(converter_class)
        };
        descs
            .iter()
            .map(|d| Value::String(d.version().to_string()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plugins_recommenders_new() {
        let recommenders = PluginsRecommenders::new();
        // Test that recommenders can be created without plugins
        assert!(recommenders
            .converter_plugin_recommender()
            .visible("test", &HashMap::new()));
    }

    #[test]
    fn test_converter_plugin_recommender_visible() {
        let recommender = ConverterPluginRecommender;
        let config = HashMap::new();
        assert!(recommender.visible("test", &config));
    }

    #[test]
    fn test_connector_plugin_version_recommender_visibility() {
        let recommender = ConnectorPluginVersionRecommender;

        // Without connector class - not visible
        let config = HashMap::new();
        assert!(!recommender.visible("test", &config));

        // With connector class - visible
        let mut config_with_class = HashMap::new();
        config_with_class.insert(
            CONNECTOR_CLASS_CONFIG.to_string(),
            Value::String("TestConnector".to_string()),
        );
        assert!(recommender.visible("test", &config_with_class));
    }

    #[test]
    fn test_key_converter_plugin_version_recommender_visibility() {
        let recommender = KeyConverterPluginVersionRecommender;

        let config = HashMap::new();
        assert!(!recommender.visible("test", &config));

        let mut config_with_converter = HashMap::new();
        config_with_converter.insert(
            CONNECTOR_KEY_CONVERTER_CLASS_CONFIG.to_string(),
            Value::String("JsonConverter".to_string()),
        );
        assert!(recommender.visible("test", &config_with_converter));
    }

    #[test]
    fn test_value_converter_plugin_version_recommender_visibility() {
        let recommender = ValueConverterPluginVersionRecommender;

        let config = HashMap::new();
        assert!(!recommender.visible("test", &config));

        let mut config_with_converter = HashMap::new();
        config_with_converter.insert(
            CONNECTOR_VALUE_CONVERTER_CLASS_CONFIG.to_string(),
            Value::String("JsonConverter".to_string()),
        );
        assert!(recommender.visible("test", &config_with_converter));
    }

    #[test]
    fn test_header_converter_plugin_version_recommender_visibility() {
        let recommender = HeaderConverterPluginVersionRecommender;

        let config = HashMap::new();
        assert!(!recommender.visible("test", &config));

        let mut config_with_converter = HashMap::new();
        config_with_converter.insert(
            CONNECTOR_HEADER_CONVERTER_CLASS_CONFIG.to_string(),
            Value::String("JsonConverter".to_string()),
        );
        assert!(recommender.visible("test", &config_with_converter));
    }

    #[test]
    fn test_transformation_plugin_recommender() {
        let recommenders = PluginsRecommenders::new();
        let transformer_recommender =
            recommenders.transformation_plugin_recommender("transforms.test.type");

        let config = HashMap::new();
        // Without plugins reference, returns empty values
        assert_eq!(
            transformer_recommender.valid_values("test", &config).len(),
            0
        );
        assert!(transformer_recommender.visible("test", &config));
    }

    #[test]
    fn test_predicate_plugin_recommender() {
        let recommenders = PluginsRecommenders::new();
        let predicate_recommender =
            recommenders.predicate_plugin_recommender("predicates.test.type");

        let config = HashMap::new();
        // Without plugins reference, returns empty values
        assert_eq!(predicate_recommender.valid_values("test", &config).len(), 0);
        assert!(predicate_recommender.visible("test", &config));
    }
}
