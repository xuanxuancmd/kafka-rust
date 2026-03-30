//! Tests for config module

use connect_api::config::{ConfigDef, ConfigValue};

#[test]
fn test_config_value_string() {
    let value = ConfigValue::String("test".to_string());
    match value {
        ConfigValue::String(s) => assert_eq!(s, "test"),
        _ => panic!("Expected String variant"),
    }
}

#[test]
fn test_config_value_int() {
    let value = ConfigValue::Int(42);
    match value {
        ConfigValue::Int(i) => assert_eq!(i, 42),
        _ => panic!("Expected Int variant"),
    }
}

#[test]
fn test_config_value_bool() {
    let value = ConfigValue::Boolean(true);
    match value {
        ConfigValue::Boolean(b) => assert!(b),
        _ => panic!("Expected Boolean variant"),
    }
}

#[test]
fn test_config_def_new() {
    let config_def = ConfigDef::new();
    // ConfigDef should be created successfully
    // In a full implementation, we'd test adding/retrieving configs
    let _ = config_def;
}
