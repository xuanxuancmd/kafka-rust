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

//! Service loader scanner for Kafka Connect plugin discovery.
//!
//! This corresponds to `org.apache.kafka.connect.runtime.isolation.ServiceLoaderScanner` in Java.
//!
//! This scanner uses a configuration-driven approach to simulate Java's ServiceLoader mechanism.
//! Instead of using Java's ServiceLoader SPI, it reads from plugin manifest files or
//! a plugin registry configuration.

use crate::isolation::plugin_desc::PluginDesc;
use crate::isolation::plugin_scan_result::PluginScanResult;
use crate::isolation::plugin_scanner::{PluginEntry, PluginScanner, ScannerHelper};
use crate::isolation::plugin_source::PluginSource;
use crate::isolation::plugin_type::PluginType;
use std::collections::BTreeSet;

/// Scanner that uses ServiceLoader-style discovery (configuration-driven).
///
/// This corresponds to `org.apache.kafka.connect.runtime.isolation.ServiceLoaderScanner` in Java.
/// In Rust, we use a configuration file (plugin manifest) instead of Java's ServiceLoader.
///
/// The scanner reads plugin definitions from:
/// 1. A plugin manifest file (JSON/YAML format)
/// 2. An in-memory plugin registry
///
/// ## Configuration Format Example
///
/// ```json
/// {
///   "plugins": [
///     {
///       "class": "org.apache.kafka.connect.file.FileSourceConnector",
///       "type": "source",
///       "version": "1.0.0"
///     }
///   ]
/// }
/// ```
pub struct ServiceLoaderScanner {
    /// Pre-configured plugin entries for simulation.
    plugins: Vec<PluginEntry>,
}

impl ServiceLoaderScanner {
    /// Creates a new ServiceLoaderScanner with empty plugin list.
    pub fn new() -> Self {
        ServiceLoaderScanner {
            plugins: Vec::new(),
        }
    }

    /// Creates a ServiceLoaderScanner with pre-configured plugins.
    ///
    /// This is useful for testing or when plugins are known at compile time.
    pub fn with_plugins(plugins: Vec<PluginEntry>) -> Self {
        ServiceLoaderScanner { plugins }
    }

    /// Adds a plugin entry to the scanner.
    pub fn add_plugin(&mut self, entry: PluginEntry) {
        self.plugins.push(entry);
    }

    /// Returns all registered plugin entries.
    pub fn plugins(&self) -> &[PluginEntry] {
        &self.plugins
    }

    /// Scans for plugins of a specific type using configuration-driven discovery.
    ///
    /// This corresponds to `getServiceLoaderPluginDesc` in Java.
    fn scan_plugins_by_type(
        &self,
        plugin_type: PluginType,
        source: &PluginSource,
    ) -> BTreeSet<PluginDesc> {
        ScannerHelper::get_service_loader_plugins(plugin_type, &self.plugins, source)
    }
}

impl Default for ServiceLoaderScanner {
    fn default() -> Self {
        Self::new()
    }
}

impl PluginScanner for ServiceLoaderScanner {
    fn scan_plugins(&self, source: &PluginSource) -> PluginScanResult {
        let mut result = PluginScanResult::new();

        // Scan for all plugin types
        for plugin_type in PluginType::all_types() {
            let plugins = self.scan_plugins_by_type(plugin_type, source);
            for desc in plugins {
                result.add_plugin(desc);
            }
        }

        result
    }

    fn scanner_name(&self) -> &str {
        "ServiceLoaderScanner"
    }
}

/// Extension for PluginType to iterate over all types.
impl PluginType {
    /// Returns all plugin types as an array.
    fn all_types() -> [PluginType; 9] {
        [
            PluginType::Source,
            PluginType::Sink,
            PluginType::Converter,
            PluginType::HeaderConverter,
            PluginType::Transformation,
            PluginType::Predicate,
            PluginType::ConfigProvider,
            PluginType::RestExtension,
            PluginType::ConnectorClientConfigOverridePolicy,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_scanner() {
        let scanner = ServiceLoaderScanner::new();
        assert!(scanner.plugins().is_empty());
        assert_eq!(scanner.scanner_name(), "ServiceLoaderScanner");
    }

    #[test]
    fn test_with_plugins() {
        let plugins = vec![PluginEntry::new(
            "org.example.SourceConnector".to_string(),
            PluginType::Source,
            Some("1.0".to_string()),
            None,
        )];
        let scanner = ServiceLoaderScanner::with_plugins(plugins);
        assert_eq!(scanner.plugins().len(), 1);
    }

    #[test]
    fn test_add_plugin() {
        let mut scanner = ServiceLoaderScanner::new();
        scanner.add_plugin(PluginEntry::new(
            "org.example.Connector".to_string(),
            PluginType::Sink,
            Some("2.0".to_string()),
            None,
        ));
        assert_eq!(scanner.plugins().len(), 1);
    }

    #[test]
    fn test_scan_plugins_empty() {
        let scanner = ServiceLoaderScanner::new();
        let source = PluginSource::classpath(vec![]);
        let result = scanner.scan_plugins(&source);
        assert!(result.is_empty());
    }

    #[test]
    fn test_scan_plugins_with_entries() {
        let plugins = vec![
            PluginEntry::new(
                "org.example.SourceConnector".to_string(),
                PluginType::Source,
                Some("1.0".to_string()),
                None,
            ),
            PluginEntry::new(
                "org.example.SinkConnector".to_string(),
                PluginType::Sink,
                Some("2.0".to_string()),
                None,
            ),
            PluginEntry::new(
                "org.example.JsonConverter".to_string(),
                PluginType::Converter,
                Some("1.5".to_string()),
                None,
            ),
        ];
        let scanner = ServiceLoaderScanner::with_plugins(plugins);
        let source = PluginSource::classpath(vec![]);
        let result = scanner.scan_plugins(&source);

        assert_eq!(result.total_count(), 3);
        assert_eq!(result.count_by_type(PluginType::Source), 1);
        assert_eq!(result.count_by_type(PluginType::Sink), 1);
        assert_eq!(result.count_by_type(PluginType::Converter), 1);
    }

    #[test]
    fn test_scan_plugins_isolated_source() {
        let plugins = vec![PluginEntry::new(
            "org.example.CustomSource".to_string(),
            PluginType::Source,
            Some("3.0".to_string()),
            None,
        )];
        let scanner = ServiceLoaderScanner::with_plugins(plugins);
        let source = PluginSource::single_jar(
            std::path::PathBuf::from("/plugins/custom.jar"),
            "file:///plugins/custom.jar".to_string(),
        );
        let result = scanner.scan_plugins(&source);

        assert_eq!(result.total_count(), 1);
        let sources = result.source_connectors();
        let first = sources.iter().next().unwrap();
        assert!(first.is_isolated()); // Not classpath
    }
}
