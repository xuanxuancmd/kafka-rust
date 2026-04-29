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

//! Plugin scan result for Kafka Connect plugin discovery.
//!
//! This corresponds to `org.apache.kafka.connect.runtime.isolation.PluginScanResult` in Java.
//! Contains the results of scanning for plugins of all types.

use crate::isolation::plugin_desc::PluginDesc;
use crate::isolation::plugin_type::PluginType;
use std::collections::BTreeSet;

/// Result of scanning for plugins.
///
/// This corresponds to `org.apache.kafka.connect.runtime.isolation.PluginScanResult` in Java.
/// Contains the results of scanning for plugins of all types, organized by plugin type.
///
/// In Java, each plugin type has its own `SortedSet<PluginDesc<T>>` where `T` is the specific
/// plugin interface type. In Rust, we use `BTreeSet<PluginDesc>` for all types since Rust's
/// type system doesn't have Java's class-based type parameterization.
#[derive(Debug, Clone)]
pub struct PluginScanResult {
    /// Sink connector plugins.
    /// Corresponds to `sinkConnectors` in Java.
    sink_connectors: BTreeSet<PluginDesc>,
    /// Source connector plugins.
    /// Corresponds to `sourceConnectors` in Java.
    source_connectors: BTreeSet<PluginDesc>,
    /// Converter plugins.
    /// Corresponds to `converters` in Java.
    converters: BTreeSet<PluginDesc>,
    /// Header converter plugins.
    /// Corresponds to `headerConverters` in Java.
    header_converters: BTreeSet<PluginDesc>,
    /// Transformation plugins.
    /// Corresponds to `transformations` in Java.
    transformations: BTreeSet<PluginDesc>,
    /// Predicate plugins.
    /// Corresponds to `predicates` in Java.
    predicates: BTreeSet<PluginDesc>,
    /// Config provider plugins.
    /// Corresponds to `configProviders` in Java.
    config_providers: BTreeSet<PluginDesc>,
    /// REST extension plugins.
    /// Corresponds to `restExtensions` in Java.
    rest_extensions: BTreeSet<PluginDesc>,
    /// Connector client config override policy plugins.
    /// Corresponds to `connectorClientConfigPolicies` in Java.
    connector_client_config_policies: BTreeSet<PluginDesc>,
}

impl PluginScanResult {
    /// Creates a new empty PluginScanResult.
    pub fn new() -> Self {
        PluginScanResult {
            sink_connectors: BTreeSet::new(),
            source_connectors: BTreeSet::new(),
            converters: BTreeSet::new(),
            header_converters: BTreeSet::new(),
            transformations: BTreeSet::new(),
            predicates: BTreeSet::new(),
            config_providers: BTreeSet::new(),
            rest_extensions: BTreeSet::new(),
            connector_client_config_policies: BTreeSet::new(),
        }
    }

    /// Creates a new PluginScanResult with the given plugins for each type.
    ///
    /// This corresponds to the main constructor in Java.
    pub fn with_plugins(
        sink_connectors: BTreeSet<PluginDesc>,
        source_connectors: BTreeSet<PluginDesc>,
        converters: BTreeSet<PluginDesc>,
        header_converters: BTreeSet<PluginDesc>,
        transformations: BTreeSet<PluginDesc>,
        predicates: BTreeSet<PluginDesc>,
        config_providers: BTreeSet<PluginDesc>,
        rest_extensions: BTreeSet<PluginDesc>,
        connector_client_config_policies: BTreeSet<PluginDesc>,
    ) -> Self {
        PluginScanResult {
            sink_connectors,
            source_connectors,
            converters,
            header_converters,
            transformations,
            predicates,
            config_providers,
            rest_extensions,
            connector_client_config_policies,
        }
    }

    /// Merges multiple PluginScanResults into one.
    ///
    /// This corresponds to the merge constructor in Java:
    /// `public PluginScanResult(List<PluginScanResult> results)`.
    ///
    /// # Arguments
    /// * `results` - A list of PluginScanResults to merge
    ///
    /// # Examples
    /// ```
    /// use connect_runtime::isolation::{PluginScanResult, PluginDesc, PluginType};
    /// use std::collections::BTreeSet;
    ///
    /// let result1 = PluginScanResult::new();
    /// let result2 = PluginScanResult::new();
    /// let merged = PluginScanResult::merge(vec![result1, result2]);
    /// ```
    pub fn merge(results: Vec<PluginScanResult>) -> Self {
        let mut merged = PluginScanResult::new();
        for result in results {
            merged.sink_connectors.extend(result.sink_connectors);
            merged.source_connectors.extend(result.source_connectors);
            merged.converters.extend(result.converters);
            merged.header_converters.extend(result.header_converters);
            merged.transformations.extend(result.transformations);
            merged.predicates.extend(result.predicates);
            merged.config_providers.extend(result.config_providers);
            merged.rest_extensions.extend(result.rest_extensions);
            merged
                .connector_client_config_policies
                .extend(result.connector_client_config_policies);
        }
        merged
    }

    /// Returns the sink connector plugins.
    /// Corresponds to `sinkConnectors()` in Java.
    pub fn sink_connectors(&self) -> &BTreeSet<PluginDesc> {
        &self.sink_connectors
    }

    /// Returns the source connector plugins.
    /// Corresponds to `sourceConnectors()` in Java.
    pub fn source_connectors(&self) -> &BTreeSet<PluginDesc> {
        &self.source_connectors
    }

    /// Returns the converter plugins.
    /// Corresponds to `converters()` in Java.
    pub fn converters(&self) -> &BTreeSet<PluginDesc> {
        &self.converters
    }

    /// Returns the header converter plugins.
    /// Corresponds to `headerConverters()` in Java.
    pub fn header_converters(&self) -> &BTreeSet<PluginDesc> {
        &self.header_converters
    }

    /// Returns the transformation plugins.
    /// Corresponds to `transformations()` in Java.
    pub fn transformations(&self) -> &BTreeSet<PluginDesc> {
        &self.transformations
    }

    /// Returns the predicate plugins.
    /// Corresponds to `predicates()` in Java.
    pub fn predicates(&self) -> &BTreeSet<PluginDesc> {
        &self.predicates
    }

    /// Returns the config provider plugins.
    /// Corresponds to `configProviders()` in Java.
    pub fn config_providers(&self) -> &BTreeSet<PluginDesc> {
        &self.config_providers
    }

    /// Returns the REST extension plugins.
    /// Corresponds to `restExtensions()` in Java.
    pub fn rest_extensions(&self) -> &BTreeSet<PluginDesc> {
        &self.rest_extensions
    }

    /// Returns the connector client config override policy plugins.
    /// Corresponds to `connectorClientConfigPolicies()` in Java.
    pub fn connector_client_config_policies(&self) -> &BTreeSet<PluginDesc> {
        &self.connector_client_config_policies
    }

    /// Adds a plugin to the appropriate set based on its type.
    pub fn add_plugin(&mut self, plugin: PluginDesc) {
        match plugin.type_() {
            PluginType::Sink => {
                self.sink_connectors.insert(plugin);
            }
            PluginType::Source => {
                self.source_connectors.insert(plugin);
            }
            PluginType::Converter => {
                self.converters.insert(plugin);
            }
            PluginType::HeaderConverter => {
                self.header_converters.insert(plugin);
            }
            PluginType::Transformation => {
                self.transformations.insert(plugin);
            }
            PluginType::Predicate => {
                self.predicates.insert(plugin);
            }
            PluginType::ConfigProvider => {
                self.config_providers.insert(plugin);
            }
            PluginType::RestExtension => {
                self.rest_extensions.insert(plugin);
            }
            PluginType::ConnectorClientConfigOverridePolicy => {
                self.connector_client_config_policies.insert(plugin);
            }
        }
    }

    /// Returns plugins of a specific type.
    pub fn plugins_by_type(&self, plugin_type: PluginType) -> &BTreeSet<PluginDesc> {
        match plugin_type {
            PluginType::Sink => &self.sink_connectors,
            PluginType::Source => &self.source_connectors,
            PluginType::Converter => &self.converters,
            PluginType::HeaderConverter => &self.header_converters,
            PluginType::Transformation => &self.transformations,
            PluginType::Predicate => &self.predicates,
            PluginType::ConfigProvider => &self.config_providers,
            PluginType::RestExtension => &self.rest_extensions,
            PluginType::ConnectorClientConfigOverridePolicy => {
                &self.connector_client_config_policies
            }
        }
    }

    /// Iterates over all plugins, calling the provided function for each.
    ///
    /// This corresponds to `forEach(Consumer<PluginDesc<?>> consumer)` in Java.
    ///
    /// # Examples
    /// ```
    /// use connect_runtime::isolation::PluginScanResult;
    ///
    /// let result = PluginScanResult::new();
    /// let mut count = 0;
    /// result.for_each(|_| count += 1);
    /// assert_eq!(count, 0);
    /// ```
    pub fn for_each<F>(&self, f: F)
    where
        F: FnMut(&PluginDesc),
    {
        let mut f = f;
        self.all_plugin_sets().iter().for_each(|plugins| {
            plugins.iter().for_each(|plugin| {
                f(plugin);
            });
        });
    }

    /// Returns true if this result contains no plugins.
    ///
    /// This corresponds to `isEmpty()` in Java.
    pub fn is_empty(&self) -> bool {
        self.sink_connectors.is_empty()
            && self.source_connectors.is_empty()
            && self.converters.is_empty()
            && self.header_converters.is_empty()
            && self.transformations.is_empty()
            && self.predicates.is_empty()
            && self.config_providers.is_empty()
            && self.rest_extensions.is_empty()
            && self.connector_client_config_policies.is_empty()
    }

    /// Returns the total number of plugins across all types.
    pub fn total_count(&self) -> usize {
        self.sink_connectors.len()
            + self.source_connectors.len()
            + self.converters.len()
            + self.header_converters.len()
            + self.transformations.len()
            + self.predicates.len()
            + self.config_providers.len()
            + self.rest_extensions.len()
            + self.connector_client_config_policies.len()
    }

    /// Returns the number of plugins for a specific type.
    pub fn count_by_type(&self, plugin_type: PluginType) -> usize {
        self.plugins_by_type(plugin_type).len()
    }

    /// Returns all plugin sets as a vector.
    fn all_plugin_sets(&self) -> Vec<&BTreeSet<PluginDesc>> {
        vec![
            &self.sink_connectors,
            &self.source_connectors,
            &self.converters,
            &self.header_converters,
            &self.transformations,
            &self.predicates,
            &self.config_providers,
            &self.rest_extensions,
            &self.connector_client_config_policies,
        ]
    }

    /// Returns an iterator over all plugins.
    pub fn iter_all(&self) -> impl Iterator<Item = &PluginDesc> {
        self.all_plugin_sets().into_iter().flatten()
    }
}

impl Default for PluginScanResult {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_plugin_desc(class_name: &str, plugin_type: PluginType) -> PluginDesc {
        PluginDesc::new(
            class_name.to_string(),
            "1.0.0".to_string(),
            plugin_type,
            "classpath".to_string(),
        )
    }

    #[test]
    fn test_new_empty() {
        let result = PluginScanResult::new();
        assert!(result.is_empty());
        assert_eq!(result.total_count(), 0);
    }

    #[test]
    fn test_add_plugin() {
        let mut result = PluginScanResult::new();

        let sink_plugin = create_plugin_desc("org.example.SinkConnector", PluginType::Sink);
        let source_plugin = create_plugin_desc("org.example.SourceConnector", PluginType::Source);

        result.add_plugin(sink_plugin);
        result.add_plugin(source_plugin);

        assert!(!result.is_empty());
        assert_eq!(result.total_count(), 2);
        assert_eq!(result.count_by_type(PluginType::Sink), 1);
        assert_eq!(result.count_by_type(PluginType::Source), 1);
    }

    #[test]
    fn test_plugins_by_type() {
        let mut result = PluginScanResult::new();

        result.add_plugin(create_plugin_desc("org.example.Sink1", PluginType::Sink));
        result.add_plugin(create_plugin_desc("org.example.Sink2", PluginType::Sink));
        result.add_plugin(create_plugin_desc(
            "org.example.Source1",
            PluginType::Source,
        ));

        let sinks = result.plugins_by_type(PluginType::Sink);
        assert_eq!(sinks.len(), 2);

        let sources = result.plugins_by_type(PluginType::Source);
        assert_eq!(sources.len(), 1);

        let converters = result.plugins_by_type(PluginType::Converter);
        assert_eq!(converters.len(), 0);
    }

    #[test]
    fn test_merge() {
        let mut result1 = PluginScanResult::new();
        result1.add_plugin(create_plugin_desc("org.example.Sink1", PluginType::Sink));

        let mut result2 = PluginScanResult::new();
        result2.add_plugin(create_plugin_desc("org.example.Sink2", PluginType::Sink));
        result2.add_plugin(create_plugin_desc(
            "org.example.Source1",
            PluginType::Source,
        ));

        let merged = PluginScanResult::merge(vec![result1, result2]);

        assert_eq!(merged.total_count(), 3);
        assert_eq!(merged.count_by_type(PluginType::Sink), 2);
        assert_eq!(merged.count_by_type(PluginType::Source), 1);
    }

    #[test]
    fn test_for_each() {
        let mut result = PluginScanResult::new();
        result.add_plugin(create_plugin_desc("org.example.Sink1", PluginType::Sink));
        result.add_plugin(create_plugin_desc(
            "org.example.Source1",
            PluginType::Source,
        ));

        let mut class_names = Vec::new();
        result.for_each(|plugin| {
            class_names.push(plugin.plugin_class().to_string());
        });

        assert_eq!(class_names.len(), 2);
        assert!(class_names.contains(&"org.example.Sink1".to_string()));
        assert!(class_names.contains(&"org.example.Source1".to_string()));
    }

    #[test]
    fn test_iter_all() {
        let mut result = PluginScanResult::new();
        result.add_plugin(create_plugin_desc("org.example.Sink1", PluginType::Sink));
        result.add_plugin(create_plugin_desc(
            "org.example.Source1",
            PluginType::Source,
        ));

        let count = result.iter_all().count();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_getters() {
        let mut result = PluginScanResult::new();

        result.add_plugin(create_plugin_desc("org.example.Sink", PluginType::Sink));
        result.add_plugin(create_plugin_desc("org.example.Source", PluginType::Source));
        result.add_plugin(create_plugin_desc(
            "org.example.Converter",
            PluginType::Converter,
        ));
        result.add_plugin(create_plugin_desc(
            "org.example.HeaderConverter",
            PluginType::HeaderConverter,
        ));
        result.add_plugin(create_plugin_desc(
            "org.example.Transformation",
            PluginType::Transformation,
        ));
        result.add_plugin(create_plugin_desc(
            "org.example.Predicate",
            PluginType::Predicate,
        ));
        result.add_plugin(create_plugin_desc(
            "org.example.ConfigProvider",
            PluginType::ConfigProvider,
        ));
        result.add_plugin(create_plugin_desc(
            "org.example.RestExtension",
            PluginType::RestExtension,
        ));
        result.add_plugin(create_plugin_desc(
            "org.example.Policy",
            PluginType::ConnectorClientConfigOverridePolicy,
        ));

        assert_eq!(result.sink_connectors().len(), 1);
        assert_eq!(result.source_connectors().len(), 1);
        assert_eq!(result.converters().len(), 1);
        assert_eq!(result.header_converters().len(), 1);
        assert_eq!(result.transformations().len(), 1);
        assert_eq!(result.predicates().len(), 1);
        assert_eq!(result.config_providers().len(), 1);
        assert_eq!(result.rest_extensions().len(), 1);
        assert_eq!(result.connector_client_config_policies().len(), 1);
    }

    #[test]
    fn test_with_plugins() {
        let mut sinks = BTreeSet::new();
        sinks.insert(create_plugin_desc("org.example.Sink", PluginType::Sink));

        let mut sources = BTreeSet::new();
        sources.insert(create_plugin_desc("org.example.Source", PluginType::Source));

        let result = PluginScanResult::with_plugins(
            sinks,
            sources,
            BTreeSet::new(),
            BTreeSet::new(),
            BTreeSet::new(),
            BTreeSet::new(),
            BTreeSet::new(),
            BTreeSet::new(),
            BTreeSet::new(),
        );

        assert_eq!(result.total_count(), 2);
    }

    #[test]
    fn test_default() {
        let result = PluginScanResult::default();
        assert!(result.is_empty());
    }
}
