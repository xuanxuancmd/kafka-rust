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

//! Versioned plugin loading exception.
//!
//! This exception is thrown when a plugin cannot be loaded due to version
//! mismatch or missing version requirements. It corresponds to Java's
//! `VersionedPluginLoadingException` which extends `ConfigException`.

use std::fmt;

/// Exception thrown when versioned plugin loading fails.
///
/// This corresponds to Java's `VersionedPluginLoadingException` which extends
/// `ConfigException`. It includes information about available versions when
/// a plugin is not found matching the required version range.
#[derive(Debug)]
pub struct VersionedPluginLoadingException {
    /// The error message describing the loading failure.
    message: String,
    /// Optional list of available versions for the plugin.
    /// This is populated when a version range is specified but no matching
    /// version is found.
    available_versions: Option<Vec<String>>,
}

impl VersionedPluginLoadingException {
    /// Creates a new exception with just a message.
    ///
    /// # Arguments
    /// * `message` - The error message describing the failure
    pub fn new(message: String) -> Self {
        Self {
            message,
            available_versions: None,
        }
    }

    /// Creates a new exception with a message and available versions.
    ///
    /// This constructor is used when a plugin cannot be found matching
    /// a specific version range. The available versions list helps users
    /// understand what versions are actually available.
    ///
    /// # Arguments
    /// * `message` - The error message describing the failure
    /// * `available_versions` - List of available plugin versions
    pub fn with_available_versions(message: String, available_versions: Vec<String>) -> Self {
        Self {
            message,
            available_versions: Some(available_versions),
        }
    }

    /// Returns the available versions for this plugin.
    ///
    /// This returns `None` if no version information was provided,
    /// or `Some(&[String])` with the list of available versions.
    pub fn available_versions(&self) -> Option<&[String]> {
        self.available_versions.as_deref()
    }

    /// Returns the error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for VersionedPluginLoadingException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for VersionedPluginLoadingException {}

/// Creates a plugin not found exception with version range information.
///
/// # Arguments
/// * `plugin_name` - The name of the plugin that was not found
/// * `version_range` - The version range that was requested
/// * `available_versions` - The list of available versions
pub fn plugin_not_found_with_version(
    plugin_name: &str,
    version_range: &str,
    available_versions: Vec<String>,
) -> VersionedPluginLoadingException {
    VersionedPluginLoadingException::with_available_versions(
        format!(
            "Plugin {} not found that matches the version range {}, available versions: {}",
            plugin_name,
            version_range,
            available_versions.join(", ")
        ),
        available_versions,
    )
}

/// Creates a soft version range error.
///
/// Soft version ranges are not supported for plugin loading.
/// Connect should automatically convert soft ranges to hard ranges.
pub fn soft_version_range_not_supported(version_range: &str) -> VersionedPluginLoadingException {
    VersionedPluginLoadingException::new(format!(
        "A soft version range is not supported for plugin loading, \
         this is an internal error as connect should automatically convert soft ranges to hard ranges. \
         Provided soft version: {}",
        version_range
    ))
}

/// Creates an exception for a plugin not in the loading mechanism.
pub fn plugin_not_in_loading_mechanism(plugin_name: &str) -> VersionedPluginLoadingException {
    VersionedPluginLoadingException::new(format!(
        "Plugin {} is not part of Connect's plugin loading mechanism (ClassPath or Plugin Path)",
        plugin_name
    ))
}

/// Creates an exception for multiple versions in classpath.
pub fn multiple_versions_in_classpath(plugin_name: &str) -> VersionedPluginLoadingException {
    VersionedPluginLoadingException::new(format!(
        "Plugin {} has multiple versions specified in class path, \
         only one version is allowed in class path for loading a plugin with version range",
        plugin_name
    ))
}

/// Creates an exception for version mismatch.
pub fn version_mismatch(
    plugin_name: &str,
    actual_version: &str,
    required_range: &str,
) -> VersionedPluginLoadingException {
    VersionedPluginLoadingException::with_available_versions(
        format!(
            "Plugin {} has version {} which does not match the required version range {}",
            plugin_name, actual_version, required_range
        ),
        vec![actual_version.to_string()],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_exception() {
        let ex = VersionedPluginLoadingException::new("Test error".to_string());
        assert_eq!(ex.message(), "Test error");
        assert!(ex.available_versions().is_none());
    }

    #[test]
    fn test_exception_with_versions() {
        let versions = vec!["1.0.0".to_string(), "2.0.0".to_string()];
        let ex = VersionedPluginLoadingException::with_available_versions(
            "Version not found".to_string(),
            versions.clone(),
        );
        assert_eq!(ex.message(), "Version not found");
        assert_eq!(ex.available_versions(), Some(&versions[..]));
    }

    #[test]
    fn test_plugin_not_found() {
        let ex = plugin_not_found_with_version(
            "TestPlugin",
            "[1.0,2.0]",
            vec!["0.9.0".to_string(), "2.1.0".to_string()],
        );
        assert!(ex.message().contains("TestPlugin"));
        assert!(ex.message().contains("[1.0,2.0]"));
        assert!(ex.available_versions().is_some());
    }

    #[test]
    fn test_display() {
        let ex = VersionedPluginLoadingException::new("Test message".to_string());
        assert_eq!(format!("{}", ex), "Test message");
    }
}
