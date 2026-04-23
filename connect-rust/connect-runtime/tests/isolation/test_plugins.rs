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

//! Test plugins tests for plugin isolation.
//!
//! ## Java Test Correspondence
//!
//! | Java Class | Rust Test Function |
//! |------------|-------------------|
//! | `TestPluginsTest` | `test_plugins_*` |
//! | `AbstractHerderTest` (plugin parts) | `test_herder_plugins_*` |
//!
//! ## Test Plugin Categories (from Java)
//!
//! 1. **sampling** series - Basic plugin verification
//! 2. **versioned** series - Plugin version semantic tests
//! 3. **service-loader** series - ServiceLoader mechanism tests
//! 4. **bad-packaging** series - Failure scenario tests
//! 5. **non-migrated** series - Non-migrated plugin discovery tests
//! 6. **read-version-from-resource** series - Resource-based version tests
//!
//! ## Evidence Directory
//!
//! Test outputs: `target/test-evidence/isolation/test_plugins/`

use connect_runtime::isolation::{TestPackage, TestPlugin};

/// Test that plugin enums are properly defined and accessible.
/// Verifies test infrastructure can import and use test plugin definitions.
#[test]
fn test_plugins_skeleton_discovery() {
    // Verify TestPackage enum is accessible and has expected variant
    let package = TestPackage::SamplingConnector;
    assert_eq!(package.resource_dir(), "sampling-connector");

    // Verify TestPlugin enum is accessible and has expected methods
    let plugin = TestPlugin::SamplingConnector;
    assert!(plugin.include_by_default());
}

/// Test plugin package enumeration completeness.
/// Corresponds to Java: TestPluginsTest.testTestPackageValues
/// Validates that all 15 TestPackage variants are defined.
#[test]
fn test_plugin_package_enum_exists() {
    // Verify TestPackage enum has all expected variants (15 total per Java spec)
    let all_packages: Vec<TestPackage> = TestPackage::iter_all().collect();
    assert_eq!(
        all_packages.len(),
        15,
        "TestPackage should have 15 variants matching Java"
    );

    // Verify key packages exist
    assert!(all_packages.contains(&TestPackage::SamplingConnector));
    assert!(all_packages.contains(&TestPackage::BadPackaging));
    assert!(all_packages.contains(&TestPackage::ServiceLoader));
    assert!(all_packages.contains(&TestPackage::NonMigrated));
}

/// Test plugin enumeration completeness.
/// Corresponds to Java: TestPluginsTest.testTestPluginValues
/// Validates that all 35 TestPlugin variants are defined.
#[test]
fn test_plugin_enum_completeness() {
    // Verify TestPlugin enum has all expected variants (35 total per Java spec)
    let all_plugins: Vec<TestPlugin> = TestPlugin::iter_all().collect();
    assert_eq!(
        all_plugins.len(),
        35,
        "TestPlugin should have 35 variants matching Java"
    );

    // Verify key plugins exist
    assert!(all_plugins.contains(&TestPlugin::SamplingConnector));
    assert!(all_plugins.contains(&TestPlugin::SamplingConverter));
    assert!(all_plugins.contains(&TestPlugin::ServiceLoader));
    assert!(all_plugins.contains(&TestPlugin::NonMigratedConverter));
}

/// Test sampling connector plugin properties.
/// Corresponds to Java: TestPluginsTest.testSamplingConnectorDiscovery
/// Priority: P0 (from migration contract)
#[test]
fn test_sampling_connector_discovery() {
    // Verify SamplingConnector properties match Java spec
    let plugin = TestPlugin::SamplingConnector;

    // Test package mapping
    assert_eq!(plugin.test_package(), TestPackage::SamplingConnector);

    // Class name matches Java definition
    assert_eq!(plugin.class_name(), "test.plugins.SamplingConnector");

    // Should be included by default (Java: includeByDefault() == true)
    assert!(
        plugin.include_by_default(),
        "SamplingConnector should be included by default"
    );
}

/// Test versioned plugin version semantics.
/// Corresponds to Java: TestPluginsTest.testVersionedPluginVersion
/// Priority: P0 (from migration contract)
#[test]
fn test_versioned_plugin_version() {
    // Verify ReadVersionFromResourceV1 properties
    let plugin_v1 = TestPlugin::ReadVersionFromResourceV1;
    assert_eq!(
        plugin_v1.test_package(),
        TestPackage::ReadVersionFromResourceV1
    );
    assert_eq!(
        plugin_v1.class_name(),
        "test.plugins.ReadVersionFromResource"
    );
    assert!(
        plugin_v1.include_by_default(),
        "V1 should be included by default"
    );

    // Verify ReadVersionFromResourceV2 properties (different inclusion)
    let plugin_v2 = TestPlugin::ReadVersionFromResourceV2;
    assert_eq!(
        plugin_v2.test_package(),
        TestPackage::ReadVersionFromResourceV2
    );
    assert_eq!(
        plugin_v2.class_name(),
        "test.plugins.ReadVersionFromResource"
    );
    assert!(
        !plugin_v2.include_by_default(),
        "V2 should NOT be included by default"
    );
}
