// Service loader plugin tests

use connect_runtime::isolation::test_plugins::{TestPackage, TestPlugin};

/// Test service loader plugin definition
#[test]
fn test_service_loader_plugin_defined() {
    let plugin = TestPlugin::ServiceLoader;
    assert_eq!(plugin.test_package(), TestPackage::ServiceLoader);
    assert_eq!(plugin.class_name(), "test.plugins.ServiceLoaderPlugin");
    assert!(plugin.include_by_default());
}
