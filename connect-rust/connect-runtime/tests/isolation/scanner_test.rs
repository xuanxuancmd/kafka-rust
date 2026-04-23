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

//! Scanner tests for plugin isolation.
//!
//! ## Java Test Correspondence
//!
//! | Java Class | Rust Test Function |
//! |------------|-------------------|
//! | `PluginScannerTest` | `test_plugin_scanner_*` |
//! | `AbstractHerderTest` (scanner parts) | `test_herder_scanner_*` |
//!
//! ## Evidence Directory
//!
//! Test outputs: `target/test-evidence/isolation/scanner_test/`

use connect_runtime::isolation::{plugin_scan_result::PluginScanResult, PluginType};

/// Test that PluginScanResult infrastructure is properly defined and accessible.
/// Verifies test infrastructure can create and manipulate scan results.
#[test]
fn test_scanner_skeleton_discovery() {
    // Verify PluginScanResult can be created
    let result = PluginScanResult::new();

    // Verify basic properties on empty result
    assert!(result.is_empty(), "New PluginScanResult should be empty");
    assert_eq!(
        result.total_count(),
        0,
        "Empty result should have zero total count"
    );

    // Verify all type-specific getters return empty sets
    assert!(result.sink_connectors().is_empty());
    assert!(result.source_connectors().is_empty());
    assert!(result.converters().is_empty());
}

/// Test plugin type enum completeness.
/// Corresponds to Java: PluginScannerTest.testPluginTypeValues
#[test]
fn test_plugin_type_completeness() {
    // Verify all plugin types are defined
    let types = vec![
        PluginType::Source,
        PluginType::Sink,
        PluginType::Converter,
        PluginType::HeaderConverter,
        PluginType::Transformation,
        PluginType::Predicate,
        PluginType::ConfigProvider,
    ];

    // Each type should have a string representation
    for plugin_type in types {
        let type_str = format!("{:?}", plugin_type);
        assert!(
            !type_str.is_empty(),
            "PluginType should have a string representation"
        );
    }
}

/// Test empty scan result behavior.
/// Corresponds to Java: PluginScannerTest.testEmptyScanResult
/// Verifies that empty PluginScanResult correctly reports its state.
#[test]
fn test_empty_scan_result() {
    // Create empty result and verify all emptiness indicators
    let result = PluginScanResult::new();

    // Primary emptiness check
    assert!(
        result.is_empty(),
        "Empty result should report is_empty() == true"
    );

    // Total count verification
    assert_eq!(
        result.total_count(),
        0,
        "Empty result total_count should be 0"
    );

    // Per-type counts should all be zero
    assert_eq!(result.count_by_type(PluginType::Sink), 0);
    assert_eq!(result.count_by_type(PluginType::Source), 0);
    assert_eq!(result.count_by_type(PluginType::Converter), 0);
    assert_eq!(result.count_by_type(PluginType::HeaderConverter), 0);
    assert_eq!(result.count_by_type(PluginType::Transformation), 0);
    assert_eq!(result.count_by_type(PluginType::Predicate), 0);
    assert_eq!(result.count_by_type(PluginType::ConfigProvider), 0);

    // Iterator over all plugins should yield nothing
    assert_eq!(result.iter_all().count(), 0);

    // Default should also be empty
    let default_result = PluginScanResult::default();
    assert!(default_result.is_empty());
}
