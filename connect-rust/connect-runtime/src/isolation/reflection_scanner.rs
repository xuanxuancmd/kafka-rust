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

//! Reflection scanner for Kafka Connect plugin discovery.
//!
//! This corresponds to `org.apache.kafka.connect.runtime.isolation.ReflectionScanner` in Java.
//!
//! This scanner simulates Java's reflection-based plugin discovery using a
//! configuration-driven approach. In Java, the ReflectionScanner scans JAR files
//! and class directories to find classes implementing plugin interfaces.
//! In Rust, we use a plugin manifest or registry to provide this information.

use crate::isolation::plugin_desc::PluginDesc;
use crate::isolation::plugin_scan_result::PluginScanResult;
use crate::isolation::plugin_scanner::{PluginEntry, PluginScanner, ScannerHelper};
use crate::isolation::plugin_source::PluginSource;
use crate::isolation::plugin_type::PluginType;
use std::collections::BTreeSet;

/// Scanner that uses reflection-style discovery (configuration-driven simulation).
///
/// This corresponds to `org.apache.kafka.connect.runtime.isolation.ReflectionScanner` in Java.
/// In Rust, we simulate reflection-based discovery through configuration.
///
/// ## Key Differences from Java
///
/// 1. **No actual reflection**: Java scans class bytes to find implementations.
///    Rust uses a manifest file to list available plugins.
///
/// 2. **Class hierarchy checking**: Java checks if a class implements a specific interface.
///    Rust uses the manifest's `type` field to indicate the plugin type.
///
/// 3. **Isolation filtering**: Java filters classes based on ClassLoader origin.
///    Rust uses the source location to determine isolation.
pub struct ReflectionScanner {
    /// Pre-configured plugin entries for simulation.
    plugins: Vec<PluginEntry>,
}

impl ReflectionScanner {
    /// Creates a new ReflectionScanner with empty plugin list.
    pub fn new() -> Self {
        ReflectionScanner {
            plugins: Vec::new(),
        }
    }

    /// Creates a ReflectionScanner with pre-configured plugins.
    pub fn with_plugins(plugins: Vec<PluginEntry>) -> Self {
        ReflectionScanner { plugins }
    }

    /// Adds a plugin entry to the scanner.
    pub fn add_plugin(&mut self, entry: PluginEntry) {
        self.plugins.push(entry);
    }

    /// Returns all registered plugin entries.
    pub fn plugins(&self) -> &[PluginEntry] {
        &self.plugins
    }

    /// Filters plugins that should be loaded in isolation.
    ///
    /// This corresponds to Java's filtering based on ClassLoader origin.
    /// Plugins from isolated sources (not classpath) should be returned.
    fn filter_isolated_plugins(
        &self,
        plugin_type: PluginType,
        source: &PluginSource,
    ) -> BTreeSet<PluginDesc> {
        let mut result = BTreeSet::new();

        for entry in &self.plugins {
            if entry.plugin_type == plugin_type {
                // In Java: filter based on ClassLoader
                // In Rust: all plugins from the source are included
                let desc = ScannerHelper::plugin_desc(
                    entry.class_name.clone(),
                    ScannerHelper::version_for(entry.version.as_deref()),
                    plugin_type,
                    source,
                );
                result.insert(desc);
            }
        }

        result
    }
}

impl Default for ReflectionScanner {
    fn default() -> Self {
        Self::new()
    }
}

impl PluginScanner for ReflectionScanner {
    fn scan_plugins(&self, source: &PluginSource) -> PluginScanResult {
        let mut result = PluginScanResult::new();

        // Scan for all plugin types
        for plugin_type in PluginType::all_types_reflection() {
            let plugins = self.filter_isolated_plugins(plugin_type, source);
            for desc in plugins {
                result.add_plugin(desc);
            }
        }

        result
    }

    fn scanner_name(&self) -> &str {
        "ReflectionScanner"
    }
}

/// Extension for PluginType with reflection-specific iteration.
impl PluginType {
    /// Returns all plugin types for reflection scanning.
    /// Note: In Java, ConfigProvider, RestExtension, and ConnectorClientConfigOverridePolicy
    /// are scanned using ServiceLoader even in reflection mode.
    fn all_types_reflection() -> [PluginType; 6] {
        [
            PluginType::Source,
            PluginType::Sink,
            PluginType::Converter,
            PluginType::HeaderConverter,
            PluginType::Transformation,
            PluginType::Predicate,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_scanner() {
        let scanner = ReflectionScanner::new();
        assert!(scanner.plugins().is_empty());
        assert_eq!(scanner.scanner_name(), "ReflectionScanner");
    }

    #[test]
    fn test_with_plugins() {
        let plugins = vec![PluginEntry::new(
            "org.example.SourceConnector".to_string(),
            PluginType::Source,
            Some("1.0".to_string()),
            None,
        )];
        let scanner = ReflectionScanner::with_plugins(plugins);
        assert_eq!(scanner.plugins().len(), 1);
    }

    #[test]
    fn test_add_plugin() {
        let mut scanner = ReflectionScanner::new();
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
        let scanner = ReflectionScanner::new();
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
            // Note: ConfigProvider is not included in reflection scan per Java behavior
            PluginEntry::new(
                "org.example.JsonConverter".to_string(),
                PluginType::Converter,
                Some("1.5".to_string()),
                None,
            ),
        ];
        let scanner = ReflectionScanner::with_plugins(plugins);
        let source = PluginSource::classpath(vec![]);
        let result = scanner.scan_plugins(&source);

        // Reflection scanner only scans Source, Sink, Converter, HeaderConverter,
        // Transformation, and Predicate (not ConfigProvider, RestExtension, etc.)
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
        let scanner = ReflectionScanner::with_plugins(plugins);
        let source = PluginSource::multi_jar(
            std::path::PathBuf::from("/plugins"),
            vec![
                "file:///plugins/plugin1.jar".to_string(),
                "file:///plugins/plugin2.jar".to_string(),
            ],
        );
        let result = scanner.scan_plugins(&source);

        assert_eq!(result.total_count(), 1);
        let sources = result.source_connectors();
        let first = sources.iter().next().unwrap();
        assert!(first.is_isolated());
    }

    #[test]
    fn test_all_types_reflection() {
        let types = PluginType::all_types_reflection();
        assert_eq!(types.len(), 6);
        // ConfigProvider, RestExtension, ConnectorClientConfigOverridePolicy not included
        assert!(!types.contains(&PluginType::ConfigProvider));
        assert!(!types.contains(&PluginType::RestExtension));
        assert!(!types.contains(&PluginType::ConnectorClientConfigOverridePolicy));
    }
}
