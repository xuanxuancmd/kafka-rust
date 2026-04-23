// Bad packaging plugin tests
// Corresponds to Java: TestPlugins bad-packaging series

use connect_runtime::isolation::test_plugins::{
    InvalidPluginReason, LoadingExceptionType, PluginLoadingError, TestPackage, TestPlugin,
};

/// Test bad packaging co-located plugin (valid plugin in bad package)
#[test]
fn test_bad_packaging_co_located_defined() {
    let plugin = TestPlugin::BadPackagingCoLocated;
    assert_eq!(plugin.test_package(), TestPackage::BadPackaging);
    assert_eq!(plugin.class_name(), "test.plugins.CoLocatedPlugin");
    assert!(plugin.include_by_default());
}

/// Test bad packaging innocuous connector (valid plugin)
#[test]
fn test_bad_packaging_innocuous_defined() {
    let plugin = TestPlugin::BadPackagingInnocuousConnector;
    assert_eq!(plugin.test_package(), TestPackage::BadPackaging);
    assert_eq!(plugin.class_name(), "test.plugins.InnocuousSinkConnector");
    assert!(plugin.include_by_default());
}

/// Test invalid plugin error - no default constructor
#[test]
fn test_invalid_plugin_no_default_constructor() {
    let err = PluginLoadingError::InvalidPlugin {
        plugin_name: "NoDefaultConstructorConnector".to_string(),
        reason: InvalidPluginReason::NoDefaultConstructor,
    };

    assert_eq!(
        err.to_string(),
        "Invalid plugin 'NoDefaultConstructorConnector': no default constructor"
    );
}

/// Test invalid plugin error - private constructor
#[test]
fn test_invalid_plugin_private_constructor() {
    let err = PluginLoadingError::InvalidPlugin {
        plugin_name: "PrivateConstructorConnector".to_string(),
        reason: InvalidPluginReason::PrivateDefaultConstructor,
    };

    assert!(err.to_string().contains("private default constructor"));
}

/// Test loading exception - static initializer
#[test]
fn test_loading_exception_static_initializer() {
    let err = PluginLoadingError::LoadingException {
        plugin_name: "StaticInitializerThrowsConnector".to_string(),
        exception_type: LoadingExceptionType::StaticInitializer,
        message: "static block threw exception".to_string(),
    };

    assert!(err.to_string().contains("static initializer"));
}

/// Test loading exception - default constructor throws
#[test]
fn test_loading_exception_default_constructor() {
    let err = PluginLoadingError::LoadingException {
        plugin_name: "ConstructorThrowsConnector".to_string(),
        exception_type: LoadingExceptionType::DefaultConstructor,
        message: "constructor threw exception".to_string(),
    };

    assert!(err.to_string().contains("default constructor"));
}

/// Test all bad packaging plugins count
#[test]
fn test_bad_packaging_plugins_count() {
    let bad_plugins: Vec<TestPlugin> = TestPlugin::iter_all()
        .filter(|p| p.test_package() == TestPackage::BadPackaging)
        .collect();

    // Should have 13 bad-packaging plugins (as defined in TestPlugin::iter_all)
    assert_eq!(bad_plugins.len(), 13);
}
