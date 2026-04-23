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

//! Utility module for constructing test plugins for Connect.
//!
//! This module provides test plugin definitions that mirror the Java TestPlugins.java.
//! Plugins are defined as enum values for test isolation and failure injection scenarios.
//!
//! # Usage
//!
//! To add a plugin, create the source files in the resource tree, and define
//! a TestPlugin enum value with the corresponding TestPackage and class name.
//! You can then assemble a `plugin.path` using `plugin_path()` function.

use std::path::PathBuf;

/// Unit of compilation and distribution, containing zero or more plugin classes.
///
/// Each TestPackage corresponds to a resource directory containing plugin source files.
/// This mirrors the Java TestPackage enum from TestPlugins.java (lines 75-111).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TestPackage {
    /// Plugin with aliased static field for testing field resolution
    AliasedStaticField,
    /// Plugin that always throws exception during loading
    AlwaysThrowException,
    /// Plugin package with bad packaging issues (contains multiple invalid plugins)
    BadPackaging,
    /// Plugin package containing multiple plugins in a single jar
    MultiplePluginsInJar,
    /// Plugin that does not have ServiceLoader manifest (non-migrated plugin test)
    NonMigrated,
    /// Plugin that reads version from resource file (version string 1.0.0)
    ReadVersionFromResourceV1,
    /// Plugin that reads version from resource file (version string 2.0.0)
    ReadVersionFromResourceV2,
    /// Configurable plugin that samples method call information
    SamplingConfigurable,
    /// ConfigProvider plugin that samples method call information
    SamplingConfigProvider,
    /// Connector plugin that samples method call information
    SamplingConnector,
    /// Converter plugin that samples method call information
    SamplingConverter,
    /// HeaderConverter plugin that samples method call information
    SamplingHeaderConverter,
    /// Plugin that uses ServiceLoader to load internal classes
    ServiceLoader,
    /// Plugin that subclasses another plugin present on classpath
    SubclassOfClasspath,
    /// Converter that is part of classpath by default (ByteArrayConverter)
    ClasspathConverter,
}

impl TestPackage {
    /// Returns the resource directory name for this test package.
    ///
    /// This corresponds to the `resourceDir()` method in Java TestPackage (lines 104-106).
    pub fn resource_dir(&self) -> &'static str {
        match self {
            TestPackage::AliasedStaticField => "aliased-static-field",
            TestPackage::AlwaysThrowException => "always-throw-exception",
            TestPackage::BadPackaging => "bad-packaging",
            TestPackage::MultiplePluginsInJar => "multiple-plugins-in-jar",
            TestPackage::NonMigrated => "non-migrated",
            TestPackage::ReadVersionFromResourceV1 => "read-version-from-resource-v1",
            TestPackage::ReadVersionFromResourceV2 => "read-version-from-resource-v2",
            TestPackage::SamplingConfigurable => "sampling-configurable",
            TestPackage::SamplingConfigProvider => "sampling-config-provider",
            TestPackage::SamplingConnector => "sampling-connector",
            TestPackage::SamplingConverter => "sampling-converter",
            TestPackage::SamplingHeaderConverter => "sampling-header-converter",
            TestPackage::ServiceLoader => "service-loader",
            TestPackage::SubclassOfClasspath => "subclass-of-classpath",
            TestPackage::ClasspathConverter => "classpath-converter",
        }
    }

    /// Returns whether a given runtime class filename should be removed for this package.
    ///
    /// This corresponds to the `removeRuntimeClasses()` predicate in Java TestPackage (lines 108-110).
    /// Only `BadPackaging` package has special filtering for `NonExistentInterface` classes.
    pub fn should_remove_runtime_class(&self, class_filename: &str) -> bool {
        match self {
            TestPackage::BadPackaging => class_filename.contains("NonExistentInterface"),
            _ => false,
        }
    }
}

/// Individual test plugin definition with package, class name, and default inclusion flag.
///
/// Each TestPlugin corresponds to a specific plugin class that can be loaded for testing.
/// This mirrors the Java TestPlugin enum from TestPlugins.java (lines 113-287).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TestPlugin {
    /// A plugin which samples information about its initialization with aliased static field
    AliasedStaticField,
    /// A plugin which will always throw an exception during loading (not included by default)
    AlwaysThrowException,
    /// A plugin which is packaged with other incorrectly packaged plugins, but itself has no issues loading
    BadPackagingCoLocated,
    /// A plugin which is incorrectly packaged, which has a private default constructor
    BadPackagingDefaultConstructorPrivateConnector,
    /// A plugin which is incorrectly packaged, which throws an exception from default constructor (Connector)
    BadPackagingDefaultConstructorThrowsConnector,
    /// A plugin which is incorrectly packaged, which throws an exception from default constructor (Converter)
    BadPackagingDefaultConstructorThrowsConverter,
    /// A plugin which is incorrectly packaged, inner class (OuterClass$InnerClass)
    BadPackagingInnerClassConnector,
    /// A valid plugin in bad-packaging package that can be used to test other plugins (InnocuousSinkConnector)
    BadPackagingInnocuousConnector,
    /// A plugin which is incorrectly packaged, and is missing a superclass definition
    BadPackagingMissingSuperclass,
    /// A plugin which is incorrectly packaged, which has a private default constructor (Connector)
    BadPackagingNoDefaultConstructorConnector,
    /// A plugin which is incorrectly packaged, which has no default constructor (Converter)
    BadPackagingNoDefaultConstructorConverter,
    /// A plugin which is incorrectly packaged, which has no default constructor (OverridePolicy)
    BadPackagingNoDefaultConstructorOverridePolicy,
    /// A connector which is incorrectly packaged, and throws during static initialization
    BadPackagingStaticInitializerThrowsConnector,
    /// A RestExtension which is incorrectly packaged, and throws during static initialization
    BadPackagingStaticInitializerThrowsRestExtension,
    /// A plugin which is incorrectly packaged, which throws an exception from version() method
    BadPackagingVersionMethodThrowsConnector,
    /// A plugin which shares a jar file with ThingTwo
    MultiplePluginsInJarThingOne,
    /// A plugin which shares a jar file with ThingOne
    MultiplePluginsInJarThingTwo,
    /// A converter which does not have a corresponding ServiceLoader manifest
    NonMigratedConverter,
    /// A header converter which does not have a corresponding ServiceLoader manifest
    NonMigratedHeaderConverter,
    /// A plugin which implements multiple interfaces with partial ServiceLoader manifests
    NonMigratedMultiPlugin,
    /// A predicate which does not have a corresponding ServiceLoader manifest
    NonMigratedPredicate,
    /// A sink connector which does not have a corresponding ServiceLoader manifest
    NonMigratedSinkConnector,
    /// A source connector which does not have a corresponding ServiceLoader manifest
    NonMigratedSourceConnector,
    /// A transformation which does not have a corresponding ServiceLoader manifest
    NonMigratedTransformation,
    /// A plugin which reads a version string from a resource (version 1.0.0)
    ReadVersionFromResourceV1,
    /// A plugin which reads a version string from a resource (version 2.0.0, not included by default)
    ReadVersionFromResourceV2,
    /// A Configurable which samples information about its method calls
    SamplingConfigurable,
    /// A ConfigProvider which samples information about its method calls
    SamplingConfigProvider,
    /// A SinkConnector which samples information about its method calls
    SamplingConnector,
    /// A Converter which samples information about its method calls
    SamplingConverter,
    /// A HeaderConverter which samples information about its method calls
    SamplingHeaderConverter,
    /// A plugin which uses ServiceLoader to load internal classes
    ServiceLoader,
    /// A reflectively discovered plugin which subclasses a classpath plugin (Converter)
    SubclassOfClasspathConverter,
    /// A ServiceLoader discovered plugin which subclasses a classpath plugin (OverridePolicy)
    SubclassOfClasspathOverridePolicy,
    /// A plugin which is part of the classpath by default (ByteArrayConverter, not included by default)
    ClasspathConverter,
}

impl TestPlugin {
    /// Returns the TestPackage this plugin belongs to.
    ///
    /// This corresponds to the `testPackage()` method in Java TestPlugin (lines 276-278).
    pub fn test_package(&self) -> TestPackage {
        match self {
            TestPlugin::AliasedStaticField => TestPackage::AliasedStaticField,
            TestPlugin::AlwaysThrowException => TestPackage::AlwaysThrowException,
            TestPlugin::BadPackagingCoLocated => TestPackage::BadPackaging,
            TestPlugin::BadPackagingDefaultConstructorPrivateConnector => TestPackage::BadPackaging,
            TestPlugin::BadPackagingDefaultConstructorThrowsConnector => TestPackage::BadPackaging,
            TestPlugin::BadPackagingDefaultConstructorThrowsConverter => TestPackage::BadPackaging,
            TestPlugin::BadPackagingInnerClassConnector => TestPackage::BadPackaging,
            TestPlugin::BadPackagingInnocuousConnector => TestPackage::BadPackaging,
            TestPlugin::BadPackagingMissingSuperclass => TestPackage::BadPackaging,
            TestPlugin::BadPackagingNoDefaultConstructorConnector => TestPackage::BadPackaging,
            TestPlugin::BadPackagingNoDefaultConstructorConverter => TestPackage::BadPackaging,
            TestPlugin::BadPackagingNoDefaultConstructorOverridePolicy => TestPackage::BadPackaging,
            TestPlugin::BadPackagingStaticInitializerThrowsConnector => TestPackage::BadPackaging,
            TestPlugin::BadPackagingStaticInitializerThrowsRestExtension => {
                TestPackage::BadPackaging
            }
            TestPlugin::BadPackagingVersionMethodThrowsConnector => TestPackage::BadPackaging,
            TestPlugin::MultiplePluginsInJarThingOne => TestPackage::MultiplePluginsInJar,
            TestPlugin::MultiplePluginsInJarThingTwo => TestPackage::MultiplePluginsInJar,
            TestPlugin::NonMigratedConverter => TestPackage::NonMigrated,
            TestPlugin::NonMigratedHeaderConverter => TestPackage::NonMigrated,
            TestPlugin::NonMigratedMultiPlugin => TestPackage::NonMigrated,
            TestPlugin::NonMigratedPredicate => TestPackage::NonMigrated,
            TestPlugin::NonMigratedSinkConnector => TestPackage::NonMigrated,
            TestPlugin::NonMigratedSourceConnector => TestPackage::NonMigrated,
            TestPlugin::NonMigratedTransformation => TestPackage::NonMigrated,
            TestPlugin::ReadVersionFromResourceV1 => TestPackage::ReadVersionFromResourceV1,
            TestPlugin::ReadVersionFromResourceV2 => TestPackage::ReadVersionFromResourceV2,
            TestPlugin::SamplingConfigurable => TestPackage::SamplingConfigurable,
            TestPlugin::SamplingConfigProvider => TestPackage::SamplingConfigProvider,
            TestPlugin::SamplingConnector => TestPackage::SamplingConnector,
            TestPlugin::SamplingConverter => TestPackage::SamplingConverter,
            TestPlugin::SamplingHeaderConverter => TestPackage::SamplingHeaderConverter,
            TestPlugin::ServiceLoader => TestPackage::ServiceLoader,
            TestPlugin::SubclassOfClasspathConverter => TestPackage::SubclassOfClasspath,
            TestPlugin::SubclassOfClasspathOverridePolicy => TestPackage::SubclassOfClasspath,
            TestPlugin::ClasspathConverter => TestPackage::ClasspathConverter,
        }
    }

    /// Returns the fully qualified class name for this plugin.
    ///
    /// This corresponds to the `className()` method in Java TestPlugin (lines 280-282).
    pub fn class_name(&self) -> &'static str {
        match self {
            TestPlugin::AliasedStaticField => "test.plugins.AliasedStaticField",
            TestPlugin::AlwaysThrowException => "test.plugins.AlwaysThrowException",
            TestPlugin::BadPackagingCoLocated => "test.plugins.CoLocatedPlugin",
            TestPlugin::BadPackagingDefaultConstructorPrivateConnector => {
                "test.plugins.DefaultConstructorPrivateConnector"
            }
            TestPlugin::BadPackagingDefaultConstructorThrowsConnector => {
                "test.plugins.DefaultConstructorThrowsConnector"
            }
            TestPlugin::BadPackagingDefaultConstructorThrowsConverter => {
                "test.plugins.DefaultConstructorThrowsConverter"
            }
            TestPlugin::BadPackagingInnerClassConnector => "test.plugins.OuterClass$InnerClass",
            TestPlugin::BadPackagingInnocuousConnector => "test.plugins.InnocuousSinkConnector",
            TestPlugin::BadPackagingMissingSuperclass => "test.plugins.MissingSuperclassConverter",
            TestPlugin::BadPackagingNoDefaultConstructorConnector => {
                "test.plugins.NoDefaultConstructorConnector"
            }
            TestPlugin::BadPackagingNoDefaultConstructorConverter => {
                "test.plugins.NoDefaultConstructorConverter"
            }
            TestPlugin::BadPackagingNoDefaultConstructorOverridePolicy => {
                "test.plugins.NoDefaultConstructorOverridePolicy"
            }
            TestPlugin::BadPackagingStaticInitializerThrowsConnector => {
                "test.plugins.StaticInitializerThrowsConnector"
            }
            TestPlugin::BadPackagingStaticInitializerThrowsRestExtension => {
                "test.plugins.StaticInitializerThrowsRestExtension"
            }
            TestPlugin::BadPackagingVersionMethodThrowsConnector => {
                "test.plugins.VersionMethodThrowsConnector"
            }
            TestPlugin::MultiplePluginsInJarThingOne => "test.plugins.ThingOne",
            TestPlugin::MultiplePluginsInJarThingTwo => "test.plugins.ThingTwo",
            TestPlugin::NonMigratedConverter => "test.plugins.NonMigratedConverter",
            TestPlugin::NonMigratedHeaderConverter => "test.plugins.NonMigratedHeaderConverter",
            TestPlugin::NonMigratedMultiPlugin => "test.plugins.NonMigratedMultiPlugin",
            TestPlugin::NonMigratedPredicate => "test.plugins.NonMigratedPredicate",
            TestPlugin::NonMigratedSinkConnector => "test.plugins.NonMigratedSinkConnector",
            TestPlugin::NonMigratedSourceConnector => "test.plugins.NonMigratedSourceConnector",
            TestPlugin::NonMigratedTransformation => "test.plugins.NonMigratedTransformation",
            TestPlugin::ReadVersionFromResourceV1 => "test.plugins.ReadVersionFromResource",
            TestPlugin::ReadVersionFromResourceV2 => "test.plugins.ReadVersionFromResource",
            TestPlugin::SamplingConfigurable => "test.plugins.SamplingConfigurable",
            TestPlugin::SamplingConfigProvider => "test.plugins.SamplingConfigProvider",
            TestPlugin::SamplingConnector => "test.plugins.SamplingConnector",
            TestPlugin::SamplingConverter => "test.plugins.SamplingConverter",
            TestPlugin::SamplingHeaderConverter => "test.plugins.SamplingHeaderConverter",
            TestPlugin::ServiceLoader => "test.plugins.ServiceLoaderPlugin",
            TestPlugin::SubclassOfClasspathConverter => "test.plugins.SubclassOfClasspathConverter",
            TestPlugin::SubclassOfClasspathOverridePolicy => {
                "test.plugins.SubclassOfClasspathOverridePolicy"
            }
            TestPlugin::ClasspathConverter => {
                "org.apache.kafka.connect.converters.ByteArrayConverter"
            }
        }
    }

    /// Returns whether this plugin should be included in the default plugin path.
    ///
    /// This corresponds to the `includeByDefault()` method in Java TestPlugin (lines 284-286).
    /// Plugins with `false` are used for specific test scenarios and must be included explicitly.
    pub fn include_by_default(&self) -> bool {
        match self {
            TestPlugin::AliasedStaticField => true,
            TestPlugin::AlwaysThrowException => false,
            TestPlugin::BadPackagingCoLocated => true,
            TestPlugin::BadPackagingDefaultConstructorPrivateConnector => false,
            TestPlugin::BadPackagingDefaultConstructorThrowsConnector => false,
            TestPlugin::BadPackagingDefaultConstructorThrowsConverter => false,
            TestPlugin::BadPackagingInnerClassConnector => false,
            TestPlugin::BadPackagingInnocuousConnector => true,
            TestPlugin::BadPackagingMissingSuperclass => false,
            TestPlugin::BadPackagingNoDefaultConstructorConnector => false,
            TestPlugin::BadPackagingNoDefaultConstructorConverter => false,
            TestPlugin::BadPackagingNoDefaultConstructorOverridePolicy => false,
            TestPlugin::BadPackagingStaticInitializerThrowsConnector => false,
            TestPlugin::BadPackagingStaticInitializerThrowsRestExtension => false,
            TestPlugin::BadPackagingVersionMethodThrowsConnector => true,
            TestPlugin::MultiplePluginsInJarThingOne => true,
            TestPlugin::MultiplePluginsInJarThingTwo => true,
            TestPlugin::NonMigratedConverter => false,
            TestPlugin::NonMigratedHeaderConverter => false,
            TestPlugin::NonMigratedMultiPlugin => false,
            TestPlugin::NonMigratedPredicate => false,
            TestPlugin::NonMigratedSinkConnector => false,
            TestPlugin::NonMigratedSourceConnector => false,
            TestPlugin::NonMigratedTransformation => false,
            TestPlugin::ReadVersionFromResourceV1 => true,
            TestPlugin::ReadVersionFromResourceV2 => false,
            TestPlugin::SamplingConfigurable => true,
            TestPlugin::SamplingConfigProvider => true,
            TestPlugin::SamplingConnector => true,
            TestPlugin::SamplingConverter => true,
            TestPlugin::SamplingHeaderConverter => true,
            TestPlugin::ServiceLoader => true,
            TestPlugin::SubclassOfClasspathConverter => true,
            TestPlugin::SubclassOfClasspathOverridePolicy => true,
            TestPlugin::ClasspathConverter => false,
        }
    }
}

/// Plugin loading error classification.
///
/// This enum provides distinct error types for different plugin failure scenarios,
/// ensuring failures are distinguishable (not unified into a single error type).
/// This mirrors the failure injection capabilities of Java TestPlugins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PluginLoadingError {
    /// Plugin is invalid due to structural issues.
    ///
    /// Covers scenarios like:
    /// - Missing default constructor
    /// - Private default constructor
    /// - Inner class issues
    /// - Missing superclass definition
    /// - Non-existent interface reference
    InvalidPlugin {
        plugin_name: String,
        reason: InvalidPluginReason,
    },

    /// Plugin version operation failed.
    ///
    /// Covers scenarios like:
    /// - version() method throws exception
    /// - Version resource read failure
    /// - Version format mismatch
    VersionError {
        plugin_name: String,
        expected_version: Option<String>,
        actual_error: String,
    },

    /// Plugin loading process failed with exception.
    ///
    /// Covers scenarios like:
    /// - Static initializer throws exception
    /// - Default constructor throws exception
    /// - ServiceLoader discovery failure
    /// - Classpath resolution failure
    LoadingException {
        plugin_name: String,
        exception_type: LoadingExceptionType,
        message: String,
    },
}

/// Specific reason for InvalidPlugin error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidPluginReason {
    /// Plugin has no default constructor
    NoDefaultConstructor,
    /// Plugin has a private default constructor
    PrivateDefaultConstructor,
    /// Plugin is an inner class with access issues
    InnerClassIssue,
    /// Plugin references a missing superclass
    MissingSuperclass,
    /// Plugin references a non-existent interface
    NonExistentInterface,
    /// Plugin packaging is malformed
    BadPackaging,
}

/// Type of loading exception.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadingExceptionType {
    /// Exception thrown during static initialization
    StaticInitializer,
    /// Exception thrown during default constructor invocation
    DefaultConstructor,
    /// Exception thrown during ServiceLoader discovery
    ServiceLoaderDiscovery,
    /// Exception due to classpath resolution failure
    ClasspathResolution,
    /// Generic loading exception
    Generic,
}

impl std::fmt::Display for PluginLoadingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PluginLoadingError::InvalidPlugin {
                plugin_name,
                reason,
            } => {
                write!(f, "Invalid plugin '{}': {}", plugin_name, reason)
            }
            PluginLoadingError::VersionError {
                plugin_name,
                expected_version,
                actual_error,
            } => {
                if let Some(expected) = expected_version {
                    write!(
                        f,
                        "Version error for plugin '{}': expected '{}', but got error: {}",
                        plugin_name, expected, actual_error
                    )
                } else {
                    write!(
                        f,
                        "Version error for plugin '{}': {}",
                        plugin_name, actual_error
                    )
                }
            }
            PluginLoadingError::LoadingException {
                plugin_name,
                exception_type,
                message,
            } => {
                write!(
                    f,
                    "Loading exception for plugin '{}' ({}): {}",
                    plugin_name, exception_type, message
                )
            }
        }
    }
}

impl std::fmt::Display for InvalidPluginReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InvalidPluginReason::NoDefaultConstructor => write!(f, "no default constructor"),
            InvalidPluginReason::PrivateDefaultConstructor => {
                write!(f, "private default constructor")
            }
            InvalidPluginReason::InnerClassIssue => write!(f, "inner class issue"),
            InvalidPluginReason::MissingSuperclass => write!(f, "missing superclass"),
            InvalidPluginReason::NonExistentInterface => write!(f, "non-existent interface"),
            InvalidPluginReason::BadPackaging => write!(f, "bad packaging"),
        }
    }
}

impl std::fmt::Display for LoadingExceptionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoadingExceptionType::StaticInitializer => write!(f, "static initializer"),
            LoadingExceptionType::DefaultConstructor => write!(f, "default constructor"),
            LoadingExceptionType::ServiceLoaderDiscovery => write!(f, "ServiceLoader discovery"),
            LoadingExceptionType::ClasspathResolution => write!(f, "classpath resolution"),
            LoadingExceptionType::Generic => write!(f, "generic"),
        }
    }
}

impl std::error::Error for PluginLoadingError {}

/// Assemble a default plugin path containing all TestPlugin instances which are included by default.
///
/// This corresponds to the `pluginPath()` method in Java TestPlugins (lines 326-328).
/// Returns a set of unique plugin jar paths (represented as PathBuf in Rust).
///
/// # Returns
///
/// A vector of PathBuf representing plugin jar paths for plugins with `include_by_default() == true`.
///
/// # Example
///
/// ```
/// use connect_runtime::isolation::test_plugins::plugin_path;
/// let paths = plugin_path();
/// // paths contains jar paths for all default-included plugins
/// ```
pub fn plugin_path() -> Vec<PathBuf> {
    plugin_path_for_plugins(&default_plugins())
}

/// Assemble a plugin path containing specific TestPlugin instances.
///
/// This corresponds to the `pluginPath(TestPlugin... plugins)` method in Java TestPlugins (lines 340-348).
///
/// # Arguments
///
/// * `plugins` - Array of TestPlugin instances to include in the plugin path.
///
/// # Returns
///
/// A vector of unique PathBuf representing plugin jar paths for the specified plugins.
pub fn plugin_path_for_plugins(plugins: &[TestPlugin]) -> Vec<PathBuf> {
    let unique_packages: std::collections::HashSet<TestPackage> = plugins
        .iter()
        .filter(|p| !matches!(p, TestPlugin::ClasspathConverter)) // Filter out null-equivalent
        .map(|p| p.test_package())
        .collect();

    unique_packages
        .iter()
        .map(|pkg| PathBuf::from(format!("{}.jar", pkg.resource_dir())))
        .collect()
}

/// Get all plugin classes which are included on the default classpath.
///
/// This corresponds to the `pluginClasses()` method in Java TestPlugins (lines 359-361).
///
/// # Returns
///
/// A vector of class names for plugins with `include_by_default() == true`.
pub fn plugin_classes() -> Vec<String> {
    plugin_classes_for_plugins(&default_plugins())
}

/// Get all plugin classes for specified plugins.
///
/// This corresponds to the `pluginClasses(TestPlugin... plugins)` method in Java TestPlugins (lines 369-376).
///
/// # Arguments
///
/// * `plugins` - Array of TestPlugin instances to query.
///
/// # Returns
///
/// A vector of unique class names for the specified plugins.
pub fn plugin_classes_for_plugins(plugins: &[TestPlugin]) -> Vec<String> {
    plugins
        .iter()
        .filter(|p| !matches!(p, TestPlugin::ClasspathConverter)) // Filter out null-equivalent
        .map(|p| p.class_name().to_string())
        .collect()
}

/// Get all TestPlugin values that are included by default.
///
/// This corresponds to the `defaultPlugins()` private method in Java TestPlugins (lines 382-386).
fn default_plugins() -> Vec<TestPlugin> {
    TestPlugin::iter_default_included().collect()
}

/// Iterator over all TestPlugin values included by default.
impl TestPlugin {
    /// Returns an iterator over all TestPlugin values that have `include_by_default() == true`.
    pub fn iter_default_included() -> impl Iterator<Item = TestPlugin> {
        [
            TestPlugin::AliasedStaticField,
            TestPlugin::BadPackagingCoLocated,
            TestPlugin::BadPackagingInnocuousConnector,
            TestPlugin::BadPackagingVersionMethodThrowsConnector,
            TestPlugin::MultiplePluginsInJarThingOne,
            TestPlugin::MultiplePluginsInJarThingTwo,
            TestPlugin::ReadVersionFromResourceV1,
            TestPlugin::SamplingConfigurable,
            TestPlugin::SamplingConfigProvider,
            TestPlugin::SamplingConnector,
            TestPlugin::SamplingConverter,
            TestPlugin::SamplingHeaderConverter,
            TestPlugin::ServiceLoader,
            TestPlugin::SubclassOfClasspathConverter,
            TestPlugin::SubclassOfClasspathOverridePolicy,
        ]
        .into_iter()
    }

    /// Returns an iterator over all TestPlugin values.
    pub fn iter_all() -> impl Iterator<Item = TestPlugin> {
        [
            TestPlugin::AliasedStaticField,
            TestPlugin::AlwaysThrowException,
            TestPlugin::BadPackagingCoLocated,
            TestPlugin::BadPackagingDefaultConstructorPrivateConnector,
            TestPlugin::BadPackagingDefaultConstructorThrowsConnector,
            TestPlugin::BadPackagingDefaultConstructorThrowsConverter,
            TestPlugin::BadPackagingInnerClassConnector,
            TestPlugin::BadPackagingInnocuousConnector,
            TestPlugin::BadPackagingMissingSuperclass,
            TestPlugin::BadPackagingNoDefaultConstructorConnector,
            TestPlugin::BadPackagingNoDefaultConstructorConverter,
            TestPlugin::BadPackagingNoDefaultConstructorOverridePolicy,
            TestPlugin::BadPackagingStaticInitializerThrowsConnector,
            TestPlugin::BadPackagingStaticInitializerThrowsRestExtension,
            TestPlugin::BadPackagingVersionMethodThrowsConnector,
            TestPlugin::MultiplePluginsInJarThingOne,
            TestPlugin::MultiplePluginsInJarThingTwo,
            TestPlugin::NonMigratedConverter,
            TestPlugin::NonMigratedHeaderConverter,
            TestPlugin::NonMigratedMultiPlugin,
            TestPlugin::NonMigratedPredicate,
            TestPlugin::NonMigratedSinkConnector,
            TestPlugin::NonMigratedSourceConnector,
            TestPlugin::NonMigratedTransformation,
            TestPlugin::ReadVersionFromResourceV1,
            TestPlugin::ReadVersionFromResourceV2,
            TestPlugin::SamplingConfigurable,
            TestPlugin::SamplingConfigProvider,
            TestPlugin::SamplingConnector,
            TestPlugin::SamplingConverter,
            TestPlugin::SamplingHeaderConverter,
            TestPlugin::ServiceLoader,
            TestPlugin::SubclassOfClasspathConverter,
            TestPlugin::SubclassOfClasspathOverridePolicy,
            TestPlugin::ClasspathConverter,
        ]
        .into_iter()
    }
}

/// Iterator over all TestPackage values.
impl TestPackage {
    /// Returns an iterator over all TestPackage values.
    pub fn iter_all() -> impl Iterator<Item = TestPackage> {
        [
            TestPackage::AliasedStaticField,
            TestPackage::AlwaysThrowException,
            TestPackage::BadPackaging,
            TestPackage::MultiplePluginsInJar,
            TestPackage::NonMigrated,
            TestPackage::ReadVersionFromResourceV1,
            TestPackage::ReadVersionFromResourceV2,
            TestPackage::SamplingConfigurable,
            TestPackage::SamplingConfigProvider,
            TestPackage::SamplingConnector,
            TestPackage::SamplingConverter,
            TestPackage::SamplingHeaderConverter,
            TestPackage::ServiceLoader,
            TestPackage::SubclassOfClasspath,
            TestPackage::ClasspathConverter,
        ]
        .into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_package_resource_dir() {
        assert_eq!(
            TestPackage::AliasedStaticField.resource_dir(),
            "aliased-static-field"
        );
        assert_eq!(TestPackage::BadPackaging.resource_dir(), "bad-packaging");
        assert_eq!(
            TestPackage::SamplingConnector.resource_dir(),
            "sampling-connector"
        );
    }

    #[test]
    fn test_package_remove_runtime_classes() {
        assert!(TestPackage::BadPackaging.should_remove_runtime_class("NonExistentInterface.class"));
        assert!(!TestPackage::BadPackaging.should_remove_runtime_class("SomeOtherClass.class"));
        assert!(!TestPackage::AliasedStaticField
            .should_remove_runtime_class("NonExistentInterface.class"));
    }

    #[test]
    fn test_plugin_class_name() {
        assert_eq!(
            TestPlugin::AliasedStaticField.class_name(),
            "test.plugins.AliasedStaticField"
        );
        assert_eq!(
            TestPlugin::SamplingConnector.class_name(),
            "test.plugins.SamplingConnector"
        );
        assert_eq!(
            TestPlugin::ClasspathConverter.class_name(),
            "org.apache.kafka.connect.converters.ByteArrayConverter"
        );
    }

    #[test]
    fn test_plugin_include_by_default() {
        assert!(TestPlugin::AliasedStaticField.include_by_default());
        assert!(TestPlugin::SamplingConnector.include_by_default());
        assert!(!TestPlugin::AlwaysThrowException.include_by_default());
        assert!(!TestPlugin::NonMigratedConverter.include_by_default());
        assert!(!TestPlugin::ClasspathConverter.include_by_default());
    }

    #[test]
    fn test_plugin_test_package() {
        assert_eq!(
            TestPlugin::AliasedStaticField.test_package(),
            TestPackage::AliasedStaticField
        );
        assert_eq!(
            TestPlugin::BadPackagingCoLocated.test_package(),
            TestPackage::BadPackaging
        );
        assert_eq!(
            TestPlugin::MultiplePluginsInJarThingOne.test_package(),
            TestPackage::MultiplePluginsInJar
        );
    }

    #[test]
    fn test_plugin_path_returns_default_plugins() {
        let paths = plugin_path();
        assert!(!paths.is_empty());

        // Verify that bad-packaging.jar is included (since BadPackagingCoLocated and
        // BadPackagingInnocuousConnector are included by default)
        assert!(paths
            .iter()
            .any(|p| p.to_str().unwrap().contains("bad-packaging")));
    }

    #[test]
    fn test_plugin_classes_returns_default_classes() {
        let classes = plugin_classes();
        assert!(classes.contains(&"test.plugins.SamplingConnector".to_string()));
        assert!(classes.contains(&"test.plugins.AliasedStaticField".to_string()));
        assert!(!classes.contains(&"test.plugins.AlwaysThrowException".to_string()));
    }

    #[test]
    fn test_plugin_path_for_specific_plugins() {
        let paths = plugin_path_for_plugins(&[
            TestPlugin::AlwaysThrowException,
            TestPlugin::SamplingConnector,
        ]);

        // Should contain jars for both packages
        assert!(paths
            .iter()
            .any(|p| p.to_str().unwrap().contains("always-throw-exception")));
        assert!(paths
            .iter()
            .any(|p| p.to_str().unwrap().contains("sampling-connector")));
    }

    #[test]
    fn test_error_display() {
        let err = PluginLoadingError::InvalidPlugin {
            plugin_name: "TestPlugin".to_string(),
            reason: InvalidPluginReason::NoDefaultConstructor,
        };
        assert_eq!(
            err.to_string(),
            "Invalid plugin 'TestPlugin': no default constructor"
        );

        let err = PluginLoadingError::VersionError {
            plugin_name: "TestPlugin".to_string(),
            expected_version: Some("1.0.0".to_string()),
            actual_error: "method threw exception".to_string(),
        };
        assert!(err.to_string().contains("expected '1.0.0'"));

        let err = PluginLoadingError::LoadingException {
            plugin_name: "TestPlugin".to_string(),
            exception_type: LoadingExceptionType::StaticInitializer,
            message: "failed to initialize".to_string(),
        };
        assert!(err.to_string().contains("static initializer"));
    }

    #[test]
    fn test_all_plugins_count() {
        let count = TestPlugin::iter_all().count();
        assert_eq!(count, 35);
    }

    #[test]
    fn test_all_packages_count() {
        let count = TestPackage::iter_all().count();
        assert_eq!(count, 15);
    }
}
