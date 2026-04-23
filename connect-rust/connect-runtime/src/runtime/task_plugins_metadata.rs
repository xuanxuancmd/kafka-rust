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

//! Task plugins metadata.
//!
//! Contains metadata about the plugins used by a task, including connector,
//! converters, transformations, and predicates.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.TaskPluginsMetadata` in Java.

use crate::isolation::plugin_type::PluginType;
use std::collections::HashSet;

/// Connector type enumeration.
///
/// Corresponds to Java: `org.apache.kafka.connect.runtime.rest.entities.ConnectorType`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectorType {
    /// Source connector.
    Source,
    /// Sink connector.
    Sink,
    /// Unknown connector type.
    Unknown,
}

impl ConnectorType {
    /// Determine connector type from class name.
    pub fn from_class_name(class_name: &str) -> Self {
        if class_name.contains("SourceConnector") {
            ConnectorType::Source
        } else if class_name.contains("SinkConnector") {
            ConnectorType::Sink
        } else {
            ConnectorType::Unknown
        }
    }
}

/// Aliased plugin information.
///
/// Contains information about a plugin with its alias and version.
/// Corresponds to Java: `TransformationStage.AliasedPluginInfo`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AliasedPluginInfo {
    /// The alias name for this plugin.
    alias: String,
    /// The plugin class name.
    plugin_class: String,
    /// The plugin version.
    version: String,
}

impl AliasedPluginInfo {
    /// Creates a new AliasedPluginInfo.
    pub fn new(alias: String, plugin_class: String, version: String) -> Self {
        AliasedPluginInfo {
            alias,
            plugin_class,
            version,
        }
    }

    /// Get the alias.
    pub fn alias(&self) -> &str {
        &self.alias
    }

    /// Get the plugin class.
    pub fn plugin_class(&self) -> &str {
        &self.plugin_class
    }

    /// Get the version.
    pub fn version(&self) -> &str {
        &self.version
    }
}

/// Stage info for transformation stage.
///
/// Contains information about a transformation stage and its predicate.
/// Corresponds to Java: `TransformationStage.StageInfo`
#[derive(Debug, Clone)]
pub struct StageInfo {
    /// Transformation plugin info.
    transform: AliasedPluginInfo,
    /// Predicate plugin info (optional).
    predicate: Option<AliasedPluginInfo>,
}

impl StageInfo {
    /// Creates a new StageInfo.
    pub fn new(transform: AliasedPluginInfo, predicate: Option<AliasedPluginInfo>) -> Self {
        StageInfo {
            transform,
            predicate,
        }
    }

    /// Get the transform info.
    pub fn transform(&self) -> &AliasedPluginInfo {
        &self.transform
    }

    /// Get the predicate info.
    pub fn predicate(&self) -> Option<&AliasedPluginInfo> {
        self.predicate.as_ref()
    }
}

/// Metadata about the plugins used by a task.
///
/// This contains information about:
/// - Connector class and version
/// - Task class and version
/// - Key/value/header converter classes and versions
/// - Transformations and predicates
///
/// Corresponds to `org.apache.kafka.connect.runtime.TaskPluginsMetadata` in Java.
#[derive(Debug, Clone)]
pub struct TaskPluginsMetadata {
    /// Connector class name.
    connector_class: String,
    /// Connector version.
    connector_version: String,
    /// Connector type (source or sink).
    connector_type: ConnectorType,
    /// Task class name.
    task_class: String,
    /// Task version.
    task_version: String,
    /// Key converter class name.
    key_converter_class: String,
    /// Key converter version.
    key_converter_version: String,
    /// Value converter class name.
    value_converter_class: String,
    /// Value converter version.
    value_converter_version: String,
    /// Header converter class name.
    header_converter_class: String,
    /// Header converter version.
    header_converter_version: String,
    /// Transformations used by this task.
    transformations: HashSet<AliasedPluginInfo>,
    /// Predicates used by this task.
    predicates: HashSet<AliasedPluginInfo>,
}

impl TaskPluginsMetadata {
    /// Creates a new TaskPluginsMetadata.
    ///
    /// # Arguments
    /// * `connector_class` - The connector class name
    /// * `connector_version` - The connector version
    /// * `task_class` - The task class name
    /// * `task_version` - The task version
    /// * `key_converter_class` - Key converter class name
    /// * `key_converter_version` - Key converter version
    /// * `value_converter_class` - Value converter class name
    /// * `value_converter_version` - Value converter version
    /// * `header_converter_class` - Header converter class name
    /// * `header_converter_version` - Header converter version
    /// * `transformation_stage_info` - List of transformation stage info
    pub fn new(
        connector_class: String,
        connector_version: String,
        task_class: String,
        task_version: String,
        key_converter_class: String,
        key_converter_version: String,
        value_converter_class: String,
        value_converter_version: String,
        header_converter_class: String,
        header_converter_version: String,
        transformation_stage_info: Vec<StageInfo>,
    ) -> Self {
        let connector_type = ConnectorType::from_class_name(&connector_class);

        // Collect transformations
        let transformations: HashSet<AliasedPluginInfo> = transformation_stage_info
            .iter()
            .map(|info| info.transform.clone())
            .collect();

        // Collect predicates (non-null only)
        let predicates: HashSet<AliasedPluginInfo> = transformation_stage_info
            .iter()
            .filter_map(|info| info.predicate.clone())
            .collect();

        TaskPluginsMetadata {
            connector_class,
            connector_version,
            connector_type,
            task_class,
            task_version,
            key_converter_class,
            key_converter_version,
            value_converter_class,
            value_converter_version,
            header_converter_class,
            header_converter_version,
            transformations,
            predicates,
        }
    }

    /// Creates a minimal TaskPluginsMetadata for testing.
    pub fn new_for_testing(connector_class: String) -> Self {
        let connector_type = ConnectorType::from_class_name(&connector_class);

        TaskPluginsMetadata {
            connector_class,
            connector_version: "unknown".to_string(),
            connector_type,
            task_class: "unknown".to_string(),
            task_version: "unknown".to_string(),
            key_converter_class: "unknown".to_string(),
            key_converter_version: "unknown".to_string(),
            value_converter_class: "unknown".to_string(),
            value_converter_version: "unknown".to_string(),
            header_converter_class: "unknown".to_string(),
            header_converter_version: "unknown".to_string(),
            transformations: HashSet::new(),
            predicates: HashSet::new(),
        }
    }

    /// Get the connector class name.
    pub fn connector_class(&self) -> &str {
        &self.connector_class
    }

    /// Get the connector version.
    pub fn connector_version(&self) -> &str {
        &self.connector_version
    }

    /// Get the connector type.
    pub fn connector_type(&self) -> ConnectorType {
        self.connector_type
    }

    /// Get the task class name.
    pub fn task_class(&self) -> &str {
        &self.task_class
    }

    /// Get the task version.
    pub fn task_version(&self) -> &str {
        &self.task_version
    }

    /// Get the key converter class name.
    pub fn key_converter_class(&self) -> &str {
        &self.key_converter_class
    }

    /// Get the key converter version.
    pub fn key_converter_version(&self) -> &str {
        &self.key_converter_version
    }

    /// Get the value converter class name.
    pub fn value_converter_class(&self) -> &str {
        &self.value_converter_class
    }

    /// Get the value converter version.
    pub fn value_converter_version(&self) -> &str {
        &self.value_converter_version
    }

    /// Get the header converter class name.
    pub fn header_converter_class(&self) -> &str {
        &self.header_converter_class
    }

    /// Get the header converter version.
    pub fn header_converter_version(&self) -> &str {
        &self.header_converter_version
    }

    /// Get the transformations.
    pub fn transformations(&self) -> &HashSet<AliasedPluginInfo> {
        &self.transformations
    }

    /// Get the predicates.
    pub fn predicates(&self) -> &HashSet<AliasedPluginInfo> {
        &self.predicates
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connector_type_from_class_name() {
        assert_eq!(
            ConnectorType::from_class_name("org.apache.kafka.connect.file.FileSourceConnector"),
            ConnectorType::Source
        );

        assert_eq!(
            ConnectorType::from_class_name("org.apache.kafka.connect.file.FileSinkConnector"),
            ConnectorType::Sink
        );

        assert_eq!(
            ConnectorType::from_class_name("org.apache.kafka.connect.file.FileConnector"),
            ConnectorType::Unknown
        );
    }

    #[test]
    fn test_aliased_plugin_info() {
        let info = AliasedPluginInfo::new(
            "InsertField".to_string(),
            "org.apache.kafka.connect.transforms.InsertField".to_string(),
            "1.0.0".to_string(),
        );

        assert_eq!(info.alias(), "InsertField");
        assert_eq!(
            info.plugin_class(),
            "org.apache.kafka.connect.transforms.InsertField"
        );
        assert_eq!(info.version(), "1.0.0");
    }

    #[test]
    fn test_stage_info() {
        let transform = AliasedPluginInfo::new(
            "InsertField".to_string(),
            "org.apache.kafka.connect.transforms.InsertField".to_string(),
            "1.0.0".to_string(),
        );

        let predicate = AliasedPluginInfo::new(
            "isvalid".to_string(),
            "org.apache.kafka.connect.transforms.predicates.RecordIsValid".to_string(),
            "1.0.0".to_string(),
        );

        let stage = StageInfo::new(transform.clone(), Some(predicate.clone()));

        assert_eq!(stage.transform(), &transform);
        assert_eq!(stage.predicate(), Some(&predicate));
    }

    #[test]
    fn test_stage_info_without_predicate() {
        let transform = AliasedPluginInfo::new(
            "InsertField".to_string(),
            "org.apache.kafka.connect.transforms.InsertField".to_string(),
            "1.0.0".to_string(),
        );

        let stage = StageInfo::new(transform.clone(), None);

        assert_eq!(stage.transform(), &transform);
        assert!(stage.predicate().is_none());
    }

    #[test]
    fn test_task_plugins_metadata_new() {
        let transform1 = AliasedPluginInfo::new(
            "InsertField".to_string(),
            "org.apache.kafka.connect.transforms.InsertField".to_string(),
            "1.0.0".to_string(),
        );

        let predicate1 = AliasedPluginInfo::new(
            "isvalid".to_string(),
            "org.apache.kafka.connect.transforms.predicates.RecordIsValid".to_string(),
            "1.0.0".to_string(),
        );

        let stages = vec![StageInfo::new(transform1.clone(), Some(predicate1.clone()))];

        let metadata = TaskPluginsMetadata::new(
            "org.apache.kafka.connect.file.FileSourceConnector".to_string(),
            "1.0.0".to_string(),
            "org.apache.kafka.connect.file.FileSourceTask".to_string(),
            "1.0.0".to_string(),
            "org.apache.kafka.connect.storage.StringConverter".to_string(),
            "1.0.0".to_string(),
            "org.apache.kafka.connect.storage.StringConverter".to_string(),
            "1.0.0".to_string(),
            "org.apache.kafka.connect.storage.SimpleHeaderConverter".to_string(),
            "1.0.0".to_string(),
            stages,
        );

        assert_eq!(
            metadata.connector_class(),
            "org.apache.kafka.connect.file.FileSourceConnector"
        );
        assert_eq!(metadata.connector_type(), ConnectorType::Source);
        assert_eq!(metadata.connector_version(), "1.0.0");
        assert_eq!(
            metadata.task_class(),
            "org.apache.kafka.connect.file.FileSourceTask"
        );
        assert_eq!(metadata.task_version(), "1.0.0");
        assert_eq!(
            metadata.key_converter_class(),
            "org.apache.kafka.connect.storage.StringConverter"
        );
        assert_eq!(
            metadata.value_converter_class(),
            "org.apache.kafka.connect.storage.StringConverter"
        );
        assert_eq!(
            metadata.header_converter_class(),
            "org.apache.kafka.connect.storage.SimpleHeaderConverter"
        );

        // Check transformations and predicates
        assert!(metadata.transformations().contains(&transform1));
        assert!(metadata.predicates().contains(&predicate1));
    }

    #[test]
    fn test_task_plugins_metadata_for_testing() {
        let metadata = TaskPluginsMetadata::new_for_testing(
            "org.apache.kafka.connect.file.FileSinkConnector".to_string(),
        );

        assert_eq!(metadata.connector_type(), ConnectorType::Sink);
        assert!(metadata.transformations().is_empty());
        assert!(metadata.predicates().is_empty());
    }

    #[test]
    fn test_task_plugins_metadata_multiple_transformations() {
        let transform1 = AliasedPluginInfo::new(
            "InsertField".to_string(),
            "org.apache.kafka.connect.transforms.InsertField".to_string(),
            "1.0.0".to_string(),
        );

        let transform2 = AliasedPluginInfo::new(
            "ReplaceField".to_string(),
            "org.apache.kafka.connect.transforms.ReplaceField".to_string(),
            "1.0.0".to_string(),
        );

        let stages = vec![
            StageInfo::new(transform1.clone(), None),
            StageInfo::new(transform2.clone(), None),
        ];

        let metadata = TaskPluginsMetadata::new(
            "TestConnector".to_string(),
            "1.0".to_string(),
            "TestTask".to_string(),
            "1.0".to_string(),
            "TestConverter".to_string(),
            "1.0".to_string(),
            "TestConverter".to_string(),
            "1.0".to_string(),
            "TestHeaderConverter".to_string(),
            "1.0".to_string(),
            stages,
        );

        assert_eq!(metadata.transformations().len(), 2);
        assert!(metadata.transformations().contains(&transform1));
        assert!(metadata.transformations().contains(&transform2));
    }
}
