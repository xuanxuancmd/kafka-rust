// Non-migrated plugin tests

use connect_runtime::isolation::test_plugins::{TestPackage, TestPlugin};

/// Test non-migrated converter (no ServiceLoader manifest)
#[test]
fn test_non_migrated_converter_defined() {
    let plugin = TestPlugin::NonMigratedConverter;
    assert_eq!(plugin.test_package(), TestPackage::NonMigrated);
    assert_eq!(plugin.class_name(), "test.plugins.NonMigratedConverter");
    assert!(!plugin.include_by_default());
}

/// Test non-migrated plugins count
#[test]
fn test_non_migrated_plugins_count() {
    let non_migrated: Vec<TestPlugin> = TestPlugin::iter_all()
        .filter(|p| p.test_package() == TestPackage::NonMigrated)
        .collect();

    assert_eq!(non_migrated.len(), 7);
}
