// Versioned plugin tests
// Corresponds to Java: TestPlugins versioned series

use connect_runtime::isolation::test_plugins::{TestPackage, TestPlugin};

/// Test versioned transformation plugin definition
#[test]
fn test_versioned_transformation_plugin_defined() {
    let plugin = TestPlugin::ReadVersionFromResourceV1;
    assert_eq!(
        plugin.test_package(),
        TestPackage::ReadVersionFromResourceV1
    );
    assert_eq!(plugin.class_name(), "test.plugins.ReadVersionFromResource");
    assert!(plugin.include_by_default());
}

/// Test versioned plugin v2 not included by default
#[test]
fn test_versioned_plugin_v2_not_default() {
    let plugin = TestPlugin::ReadVersionFromResourceV2;
    assert_eq!(
        plugin.test_package(),
        TestPackage::ReadVersionFromResourceV2
    );
    assert!(!plugin.include_by_default());
}

/// Test versioned plugin version error handling
#[test]
fn test_version_error_classification() {
    use connect_runtime::isolation::test_plugins::{InvalidPluginReason, PluginLoadingError};

    let err = PluginLoadingError::VersionError {
        plugin_name: "test.plugins.VersionedPlugin".to_string(),
        expected_version: Some("1.0.0".to_string()),
        actual_error: "version method threw exception".to_string(),
    };

    assert!(err.to_string().contains("Version error"));
}
