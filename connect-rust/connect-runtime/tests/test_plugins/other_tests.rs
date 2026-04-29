// Other test plugin tests (aliased, always-throw, multiple-plugins, subclass)

use connect_runtime::isolation::test_plugins::{TestPackage, TestPlugin};

/// Test aliased static field plugin
#[test]
fn test_aliased_static_field_defined() {
    let plugin = TestPlugin::AliasedStaticField;
    assert_eq!(plugin.test_package(), TestPackage::AliasedStaticField);
    assert!(plugin.include_by_default());
}

/// Test always throw exception plugin (not included by default)
#[test]
fn test_always_throw_exception_defined() {
    let plugin = TestPlugin::AlwaysThrowException;
    assert_eq!(plugin.test_package(), TestPackage::AlwaysThrowException);
    assert!(!plugin.include_by_default());
}

/// Test multiple plugins in jar - ThingOne
#[test]
fn test_multiple_plugins_thing_one_defined() {
    let plugin = TestPlugin::MultiplePluginsInJarThingOne;
    assert_eq!(plugin.test_package(), TestPackage::MultiplePluginsInJar);
    assert_eq!(plugin.class_name(), "test.plugins.ThingOne");
    assert!(plugin.include_by_default());
}

/// Test multiple plugins in jar - ThingTwo
#[test]
fn test_multiple_plugins_thing_two_defined() {
    let plugin = TestPlugin::MultiplePluginsInJarThingTwo;
    assert_eq!(plugin.test_package(), TestPackage::MultiplePluginsInJar);
    assert_eq!(plugin.class_name(), "test.plugins.ThingTwo");
    assert!(plugin.include_by_default());
}

/// Test subclass of classpath converter
#[test]
fn test_subclass_of_classpath_converter_defined() {
    let plugin = TestPlugin::SubclassOfClasspathConverter;
    assert_eq!(plugin.test_package(), TestPackage::SubclassOfClasspath);
    assert!(plugin.include_by_default());
}

/// Test subclass of classpath override policy
#[test]
fn test_subclass_of_classpath_override_policy_defined() {
    let plugin = TestPlugin::SubclassOfClasspathOverridePolicy;
    assert_eq!(plugin.test_package(), TestPackage::SubclassOfClasspath);
    assert!(plugin.include_by_default());
}

/// Test classpath converter (ByteArrayConverter, not included by default)
#[test]
fn test_classpath_converter_defined() {
    let plugin = TestPlugin::ClasspathConverter;
    assert_eq!(plugin.test_package(), TestPackage::ClasspathConverter);
    assert_eq!(
        plugin.class_name(),
        "org.apache.kafka.connect.converters.ByteArrayConverter"
    );
    assert!(!plugin.include_by_default());
}
