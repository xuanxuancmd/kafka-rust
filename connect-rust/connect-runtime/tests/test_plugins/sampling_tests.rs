// Sampling plugin tests
// Corresponds to Java: TestPlugins sampling series

use connect_runtime::isolation::test_plugins::{TestPackage, TestPlugin};

/// Test sampling connector plugin definition
#[test]
fn test_sampling_connector_plugin_defined() {
    let plugin = TestPlugin::SamplingConnector;
    assert_eq!(plugin.test_package(), TestPackage::SamplingConnector);
    assert_eq!(plugin.class_name(), "test.plugins.SamplingConnector");
    assert!(plugin.include_by_default());
}

/// Test sampling converter plugin definition
#[test]
fn test_sampling_converter_plugin_defined() {
    let plugin = TestPlugin::SamplingConverter;
    assert_eq!(plugin.test_package(), TestPackage::SamplingConverter);
    assert_eq!(plugin.class_name(), "test.plugins.SamplingConverter");
    assert!(plugin.include_by_default());
}

/// Test sampling header converter plugin definition
#[test]
fn test_sampling_header_converter_plugin_defined() {
    let plugin = TestPlugin::SamplingHeaderConverter;
    assert_eq!(plugin.test_package(), TestPackage::SamplingHeaderConverter);
    assert_eq!(plugin.class_name(), "test.plugins.SamplingHeaderConverter");
    assert!(plugin.include_by_default());
}

/// Test sampling configurable plugin definition
#[test]
fn test_sampling_configurable_plugin_defined() {
    let plugin = TestPlugin::SamplingConfigurable;
    assert_eq!(plugin.test_package(), TestPackage::SamplingConfigurable);
    assert_eq!(plugin.class_name(), "test.plugins.SamplingConfigurable");
    assert!(plugin.include_by_default());
}

/// Test sampling config provider plugin definition
#[test]
fn test_sampling_config_provider_plugin_defined() {
    let plugin = TestPlugin::SamplingConfigProvider;
    assert_eq!(plugin.test_package(), TestPackage::SamplingConfigProvider);
    assert_eq!(plugin.class_name(), "test.plugins.SamplingConfigProvider");
    assert!(plugin.include_by_default());
}
