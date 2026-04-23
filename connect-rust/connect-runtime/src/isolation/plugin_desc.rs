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

//! Plugin descriptor for Kafka Connect plugins.
//!
//! This module provides metadata about a plugin including its class name,
//! version, type, and location.

use crate::isolation::plugin_type::PluginType;
use std::cmp::Ordering;

/// Undefined version constant, matching Java's `PluginDesc.UNDEFINED_VERSION`.
pub const UNDEFINED_VERSION: &str = "undefined";

/// Plugin descriptor containing metadata about a Kafka Connect plugin.
///
/// This struct holds information about a plugin including its class name,
/// version, type, location, and optional documentation. It corresponds to
/// the Java `PluginDesc<T>` class.
#[derive(Debug, Clone)]
pub struct PluginDesc {
    /// The fully qualified class name of the plugin.
    plugin_class: String,
    /// The version of the plugin. Defaults to "undefined" if not specified.
    version: String,
    /// The location of the plugin (file path or "classpath").
    location: String,
    /// The type of the plugin (source, sink, converter, etc.).
    type_: PluginType,
    /// Optional documentation/description of the plugin.
    documentation: Option<String>,
}

impl PluginDesc {
    /// Creates a new PluginDesc with the given parameters.
    ///
    /// # Arguments
    /// * `plugin_class` - The fully qualified class name of the plugin
    /// * `version` - The version of the plugin (null becomes "null", empty becomes "undefined")
    /// * `type_` - The type of the plugin
    /// * `location` - The location of the plugin (file path or "classpath")
    ///
    /// # Examples
    /// ```
    /// use connect_runtime::isolation::{PluginDesc, PluginType};
    ///
    /// let desc = PluginDesc::new(
    ///     "org.apache.kafka.connect.file.FileSourceConnector".to_string(),
    ///     "1.0.0".to_string(),
    ///     PluginType::Source,
    ///     "/path/to/plugin.jar".to_string()
    /// );
    /// assert_eq!(desc.plugin_class(), "org.apache.kafka.connect.file.FileSourceConnector");
    /// ```
    pub fn new(plugin_class: String, version: String, type_: PluginType, location: String) -> Self {
        let version = if version.is_empty() {
            UNDEFINED_VERSION.to_string()
        } else {
            version
        };

        PluginDesc {
            plugin_class,
            version,
            location,
            type_,
            documentation: None,
        }
    }

    /// Creates a new PluginDesc with documentation.
    ///
    /// # Arguments
    /// * `plugin_class` - The fully qualified class name of the plugin
    /// * `version` - The version of the plugin
    /// * `type_` - The type of the plugin
    /// * `location` - The location of the plugin
    /// * `documentation` - Optional documentation/description
    pub fn with_documentation(
        plugin_class: String,
        version: String,
        type_: PluginType,
        location: String,
        documentation: Option<String>,
    ) -> Self {
        let mut desc = Self::new(plugin_class, version, type_, location);
        desc.documentation = documentation;
        desc
    }

    /// Returns the fully qualified class name of the plugin.
    ///
    /// This corresponds to Java's `className()` method with `@JsonProperty("class")`.
    pub fn plugin_class(&self) -> &str {
        &self.plugin_class
    }

    /// Returns the version of the plugin.
    ///
    /// This corresponds to Java's `version()` method.
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Returns the type of the plugin.
    ///
    /// This corresponds to Java's `type()` method.
    pub fn type_(&self) -> PluginType {
        self.type_
    }

    /// Returns the type name as a string.
    ///
    /// This corresponds to Java's `typeName()` method with `@JsonProperty("type")`.
    pub fn type_name(&self) -> String {
        self.type_.to_string()
    }

    /// Returns the location of the plugin.
    ///
    /// This corresponds to Java's `location()` method with `@JsonProperty("location")`.
    /// The location can be a file path or "classpath" for plugins loaded from the classpath.
    pub fn location(&self) -> &str {
        &self.location
    }

    /// Returns the documentation/description of the plugin.
    ///
    /// This is an optional field that provides additional information about the plugin.
    pub fn documentation(&self) -> Option<&str> {
        self.documentation.as_deref()
    }

    /// Sets the documentation for this plugin descriptor.
    pub fn set_documentation(&mut self, documentation: Option<String>) {
        self.documentation = documentation;
    }

    /// Returns true if this plugin is loaded from a plugin path (not classpath).
    ///
    /// This corresponds to Java's check for `loader instanceof PluginClassLoader`.
    pub fn is_isolated(&self) -> bool {
        self.location != "classpath"
    }

    /// Returns a simple class name (without package).
    ///
    /// This extracts just the class name from the fully qualified name.
    pub fn simple_name(&self) -> &str {
        self.plugin_class
            .rsplit('.')
            .next()
            .unwrap_or(&self.plugin_class)
    }
}

impl PartialEq for PluginDesc {
    fn eq(&self, other: &Self) -> bool {
        // Match Java's equals() which compares klass, version, and type
        self.plugin_class == other.plugin_class
            && self.version == other.version
            && self.type_ == other.type_
    }
}

impl Eq for PluginDesc {}

impl std::hash::Hash for PluginDesc {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Match Java's hashCode() which hashes klass, version, and type
        self.plugin_class.hash(state);
        self.version.hash(state);
        self.type_.hash(state);
    }
}

impl PartialOrd for PluginDesc {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PluginDesc {
    fn cmp(&self, other: &Self) -> Ordering {
        // Match Java's compareTo() ordering:
        // 1. name (class name)
        // 2. version (encoded version)
        // 3. isolated status (isolated plugins appear after classpath plugins)
        // 4. location
        // 5. type

        let name_cmp = self.plugin_class.cmp(&other.plugin_class);
        if name_cmp != Ordering::Equal {
            return name_cmp;
        }

        let version_cmp = compare_versions(&self.version, &other.version);
        if version_cmp != Ordering::Equal {
            return version_cmp;
        }

        // isolated plugins appear after classpath plugins when they have identical versions
        // Java: Boolean.compare(other.loader instanceof PluginClassLoader, loader instanceof PluginClassLoader)
        let isolated_cmp = other.is_isolated().cmp(&self.is_isolated());
        if isolated_cmp != Ordering::Equal {
            return isolated_cmp;
        }

        let location_cmp = self.location.cmp(&other.location);
        if location_cmp != Ordering::Equal {
            return location_cmp;
        }

        self.type_.cmp(&other.type_)
    }
}

impl std::fmt::Display for PluginDesc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PluginDesc{{class={}, version='{}', type={}, location='{}'}}",
            self.plugin_class, self.version, self.type_, self.location
        )
    }
}

/// Compare two version strings using Maven-style version comparison.
///
/// This mimics the behavior of `DefaultArtifactVersion.compareTo()` in Java.
/// Version strings are compared segment by segment, handling numeric and string parts.
/// Maven's rules:
/// - Qualifier versions (alpha, beta, etc.) are less than release versions
/// - Numeric segments are compared numerically
/// - String segments are compared lexicographically (case-insensitive)
fn compare_versions(v1: &str, v2: &str) -> Ordering {
    // Handle special cases
    if v1 == v2 {
        return Ordering::Equal;
    }
    if v1 == UNDEFINED_VERSION || v1 == "null" {
        return Ordering::Less;
    }
    if v2 == UNDEFINED_VERSION || v2 == "null" {
        return Ordering::Greater;
    }

    // Parse and compare version segments
    let segments1 = parse_version_segments(v1);
    let segments2 = parse_version_segments(v2);

    for (s1, s2) in segments1.iter().zip(segments2.iter()) {
        let cmp = compare_version_segment(s1, s2);
        if cmp != Ordering::Equal {
            return cmp;
        }
    }

    // Handle different length cases per Maven rules:
    // If one version is longer and ends with a string segment (qualifier),
    // it is considered LESS than the shorter version (release is greater than qualifier).
    // If one version is longer and ends with a numeric segment,
    // it is considered GREATER (e.g., 1.0.1 > 1.0).
    if segments1.len() != segments2.len() {
        let (longer, shorter) = if segments1.len() > segments2.len() {
            (&segments1, &segments2)
        } else {
            (&segments2, &segments1)
        };

        // Check if the longer version ends with a string segment (qualifier)
        if let Some(last) = longer.last() {
            if matches!(last, VersionSegment::String(_)) {
                // Qualifier versions are less than release versions
                // e.g., 1.0-alpha < 1.0, 1.0.1-beta < 1.0.1
                return if segments1.len() > segments2.len() {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
            }
        }

        // Longer version with trailing numeric segment is greater
        return segments1.len().cmp(&segments2.len());
    }

    Ordering::Equal
}

/// Parse version string into segments (numeric and string parts).
fn parse_version_segments(version: &str) -> Vec<VersionSegment> {
    let mut segments = Vec::new();
    let mut current = String::new();
    let mut is_numeric = true;

    for c in version.chars() {
        if c == '.' || c == '-' {
            if !current.is_empty() {
                segments.push(parse_segment(&current, is_numeric));
                current.clear();
            }
            is_numeric = true; // Reset for next segment
        } else if c.is_ascii_digit() {
            if !current.is_empty() && !is_numeric {
                // Transition from string to numeric
                segments.push(parse_segment(&current, is_numeric));
                current.clear();
            }
            current.push(c);
            is_numeric = true;
        } else {
            if !current.is_empty() && is_numeric && !current.chars().all(|d| d.is_ascii_digit()) {
                // This shouldn't happen normally, but handle it
                segments.push(parse_segment(&current, is_numeric));
                current.clear();
            }
            current.push(c);
            is_numeric = false;
        }
    }

    if !current.is_empty() {
        segments.push(parse_segment(&current, is_numeric));
    }

    segments
}

/// A version segment can be numeric or string.
#[derive(Debug, Clone, PartialEq)]
enum VersionSegment {
    Numeric(u64),
    String(String),
}

/// Parse a segment string into a VersionSegment.
fn parse_segment(s: &str, is_numeric: bool) -> VersionSegment {
    if is_numeric && s.chars().all(|c| c.is_ascii_digit()) {
        s.parse::<u64>()
            .map(VersionSegment::Numeric)
            .unwrap_or_else(|_| VersionSegment::String(s.to_string()))
    } else {
        VersionSegment::String(s.to_lowercase())
    }
}

/// Compare two version segments.
fn compare_version_segment(s1: &VersionSegment, s2: &VersionSegment) -> Ordering {
    match (s1, s2) {
        (VersionSegment::Numeric(n1), VersionSegment::Numeric(n2)) => n1.cmp(n2),
        (VersionSegment::Numeric(_), VersionSegment::String(_)) => {
            // Numeric segments come before string segments
            Ordering::Less
        }
        (VersionSegment::String(_), VersionSegment::Numeric(_)) => {
            // String segments come after numeric segments
            Ordering::Greater
        }
        (VersionSegment::String(s1), VersionSegment::String(s2)) => s1.cmp(s2),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_plugin_desc() {
        let desc = PluginDesc::new(
            "org.apache.kafka.connect.file.FileSourceConnector".to_string(),
            "1.0.0".to_string(),
            PluginType::Source,
            "/path/to/plugin.jar".to_string(),
        );

        assert_eq!(
            desc.plugin_class(),
            "org.apache.kafka.connect.file.FileSourceConnector"
        );
        assert_eq!(desc.version(), "1.0.0");
        assert_eq!(desc.type_(), PluginType::Source);
        assert_eq!(desc.location(), "/path/to/plugin.jar");
        assert_eq!(desc.type_name(), "source");
        assert!(desc.is_isolated());
    }

    #[test]
    fn test_empty_version_defaults_to_undefined() {
        let desc = PluginDesc::new(
            "TestClass".to_string(),
            "".to_string(),
            PluginType::Sink,
            "classpath".to_string(),
        );

        assert_eq!(desc.version(), UNDEFINED_VERSION);
    }

    #[test]
    fn test_classpath_plugin_is_not_isolated() {
        let desc = PluginDesc::new(
            "TestClass".to_string(),
            "1.0".to_string(),
            PluginType::Converter,
            "classpath".to_string(),
        );

        assert!(!desc.is_isolated());
    }

    #[test]
    fn test_simple_name() {
        let desc = PluginDesc::new(
            "org.apache.kafka.connect.file.FileSourceConnector".to_string(),
            "1.0".to_string(),
            PluginType::Source,
            "classpath".to_string(),
        );

        assert_eq!(desc.simple_name(), "FileSourceConnector");
    }

    #[test]
    fn test_with_documentation() {
        let desc = PluginDesc::with_documentation(
            "TestClass".to_string(),
            "1.0".to_string(),
            PluginType::Source,
            "classpath".to_string(),
            Some("A test connector".to_string()),
        );

        assert_eq!(desc.documentation(), Some("A test connector"));
    }

    #[test]
    fn test_equality() {
        let desc1 = PluginDesc::new(
            "TestClass".to_string(),
            "1.0".to_string(),
            PluginType::Source,
            "/path1".to_string(),
        );
        let desc2 = PluginDesc::new(
            "TestClass".to_string(),
            "1.0".to_string(),
            PluginType::Source,
            "/path2".to_string(), // Different location
        );
        let desc3 = PluginDesc::new(
            "TestClass".to_string(),
            "2.0".to_string(),
            PluginType::Source,
            "/path1".to_string(),
        );

        // Equal: same class, version, type (location not in equals)
        assert_eq!(desc1, desc2);
        // Not equal: different version
        assert_ne!(desc1, desc3);
    }

    #[test]
    fn test_ordering_by_class_name() {
        let desc1 = PluginDesc::new(
            "org.example.ConnectorA".to_string(),
            "1.0".to_string(),
            PluginType::Source,
            "classpath".to_string(),
        );
        let desc2 = PluginDesc::new(
            "org.example.ConnectorB".to_string(),
            "1.0".to_string(),
            PluginType::Source,
            "classpath".to_string(),
        );

        assert!(desc1 < desc2);
    }

    #[test]
    fn test_ordering_by_version() {
        let desc1 = PluginDesc::new(
            "Connector".to_string(),
            "1.0.0".to_string(),
            PluginType::Source,
            "classpath".to_string(),
        );
        let desc2 = PluginDesc::new(
            "Connector".to_string(),
            "2.0.0".to_string(),
            PluginType::Source,
            "classpath".to_string(),
        );

        assert!(desc1 < desc2);
    }

    #[test]
    fn test_ordering_isolated_before_classpath() {
        // Java's Boolean.compare(other.isolated, this.isolated) makes isolated appear BEFORE classpath
        // when versions are equal. This is the actual behavior of Kafka's PluginDesc.compareTo().
        let classpath_desc = PluginDesc::new(
            "Connector".to_string(),
            "1.0".to_string(),
            PluginType::Source,
            "classpath".to_string(),
        );
        let isolated_desc = PluginDesc::new(
            "Connector".to_string(),
            "1.0".to_string(),
            PluginType::Source,
            "/path/to/plugin.jar".to_string(),
        );

        // Per Java code behavior: isolated < classpath (isolated appears before/first)
        assert!(isolated_desc < classpath_desc);
        assert!(classpath_desc > isolated_desc);
    }

    #[test]
    fn test_version_comparison() {
        assert_eq!(compare_versions("1.0", "1.0"), Ordering::Equal);
        assert_eq!(compare_versions("1.0", "2.0"), Ordering::Less);
        assert_eq!(compare_versions("2.0", "1.0"), Ordering::Greater);
        assert_eq!(compare_versions("1.0.0", "1.0.1"), Ordering::Less);
        assert_eq!(compare_versions("1.0.1", "1.0.0"), Ordering::Greater);
        assert_eq!(compare_versions("1.0-alpha", "1.0"), Ordering::Less);
        assert_eq!(compare_versions("undefined", "1.0"), Ordering::Less);
        assert_eq!(compare_versions("1.0", "undefined"), Ordering::Greater);
    }

    #[test]
    fn test_display() {
        let desc = PluginDesc::new(
            "TestClass".to_string(),
            "1.0".to_string(),
            PluginType::Source,
            "classpath".to_string(),
        );

        let display = format!("{}", desc);
        assert!(display.contains("TestClass"));
        assert!(display.contains("1.0"));
        assert!(display.contains("source"));
        assert!(display.contains("classpath"));
    }
}
