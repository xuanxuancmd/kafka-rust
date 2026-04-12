//! Tests for config module in kafka-clients-trait

use kafka_clients_trait::config::{
    Config, ConfigDef, ConfigImportance, ConfigKey, ConfigType, ConfigValue,
};

#[test]
fn test_config_type_variants() {
    // Test all ConfigType variants exist and can be created
    let _ = ConfigType::Boolean;
    let _ = ConfigType::String;
    let _ = ConfigType::Int;
    let _ = ConfigType::Short;
    let _ = ConfigType::Long;
    let _ = ConfigType::Double;
    let _ = ConfigType::List;
    let _ = ConfigType::Class;
    let _ = ConfigType::Password;
}

#[test]
fn test_config_type_equality() {
    assert_eq!(ConfigType::Boolean, ConfigType::Boolean);
    assert_ne!(ConfigType::Boolean, ConfigType::String);
    assert_eq!(ConfigType::Int, ConfigType::Int);
    assert_ne!(ConfigType::Int, ConfigType::Long);
}

#[test]
fn test_config_importance_variants() {
    let _ = ConfigImportance::High;
    let _ = ConfigImportance::Medium;
    let _ = ConfigImportance::Low;
}

#[test]
fn test_config_importance_equality() {
    assert_eq!(ConfigImportance::High, ConfigImportance::High);
    assert_ne!(ConfigImportance::High, ConfigImportance::Medium);
}

#[test]
fn test_config_key_creation() {
    let config_key = ConfigKey::new(
        "test.key".to_string(),
        "Test configuration key".to_string(),
        ConfigType::String,
        Some("default".to_string()),
        ConfigImportance::High,
        true,
    );

    assert_eq!(config_key.name, "test.key");
    assert_eq!(config_key.doc, "Test configuration key");
    assert_eq!(config_key.type_, ConfigType::String);
    assert_eq!(config_key.default_value, Some("default".to_string()));
    assert_eq!(config_key.importance, ConfigImportance::High);
    assert!(config_key.required);
}

#[test]
fn test_config_key_builder() {
    let config_key = ConfigKey::builder("test.key".to_string())
        .doc("Test configuration key".to_string())
        .type_(ConfigType::Int)
        .default_value("42".to_string())
        .importance(ConfigImportance::Medium)
        .required(false)
        .build();

    assert_eq!(config_key.name, "test.key");
    assert_eq!(config_key.doc, "Test configuration key");
    assert_eq!(config_key.type_, ConfigType::Int);
    assert_eq!(config_key.default_value, Some("42".to_string()));
    assert_eq!(config_key.importance, ConfigImportance::Medium);
    assert!(!config_key.required);
}

#[test]
fn test_config_key_builder_defaults() {
    let config_key = ConfigKey::builder("test.key".to_string()).build();

    assert_eq!(config_key.name, "test.key");
    assert_eq!(config_key.doc, "");
    assert_eq!(config_key.type_, ConfigType::String);
    assert_eq!(config_key.default_value, None);
    assert_eq!(config_key.importance, ConfigImportance::Medium);
    assert!(!config_key.required);
}

#[test]
fn test_config_value_creation() {
    let config_value = ConfigValue::new(
        "test.value".to_string(),
        Some("actual".to_string()),
        Some("default".to_string()),
        "Test configuration value".to_string(),
        ConfigImportance::High,
        true,
    );

    assert_eq!(config_value.name, "test.value");
    assert_eq!(config_value.value, Some("actual".to_string()));
    assert_eq!(config_value.default_value, Some("default".to_string()));
    assert_eq!(config_value.documentation, "Test configuration value");
    assert_eq!(config_value.importance, ConfigImportance::High);
    assert!(config_value.required);
}

#[test]
fn test_config_value_error_messages() {
    let mut config_value = ConfigValue::new(
        "test.value".to_string(),
        Some("invalid".to_string()),
        None,
        "Test configuration value".to_string(),
        ConfigImportance::High,
        true,
    );

    assert!(!config_value.has_errors());
    assert_eq!(config_value.error_messages().len(), 0);

    config_value.add_error("Invalid value".to_string());
    config_value.add_error("Another error".to_string());

    assert!(config_value.has_errors());
    assert_eq!(config_value.error_messages().len(), 2);
    assert_eq!(config_value.error_messages()[0], "Invalid value");
    assert_eq!(config_value.error_messages()[1], "Another error");
}

#[test]
fn test_config_creation() {
    let config_values = vec![
        ConfigValue::new(
            "key1".to_string(),
            Some("value1".to_string()),
            None,
            "First key".to_string(),
            ConfigImportance::High,
            true,
        ),
        ConfigValue::new(
            "key2".to_string(),
            Some("value2".to_string()),
            None,
            "Second key".to_string(),
            ConfigImportance::Medium,
            false,
        ),
    ];

    let config = Config::new(config_values);

    assert_eq!(config.values().len(), 2);
    assert!(config.is_valid());
    assert_eq!(config.error_messages().len(), 0);
}

#[test]
fn test_config_empty() {
    let config = Config::empty();

    assert_eq!(config.values().len(), 0);
    assert!(config.is_valid());
    assert_eq!(config.error_messages().len(), 0);
}

#[test]
fn test_config_default() {
    let config = Config::default();

    assert_eq!(config.values().len(), 0);
    assert!(config.is_valid());
}

#[test]
fn test_config_add_error() {
    let mut config = Config::empty();

    assert!(config.is_valid());

    config.add_error("Configuration error".to_string());

    assert!(!config.is_valid());
    assert_eq!(config.error_messages().len(), 1);
    assert_eq!(config.error_messages()[0], "Configuration error");
}

#[test]
fn test_config_get() {
    let config_values = vec![
        ConfigValue::new(
            "key1".to_string(),
            Some("value1".to_string()),
            None,
            "First key".to_string(),
            ConfigImportance::High,
            true,
        ),
        ConfigValue::new(
            "key2".to_string(),
            Some("value2".to_string()),
            None,
            "Second key".to_string(),
            ConfigImportance::Medium,
            false,
        ),
    ];

    let config = Config::new(config_values);

    let found = config.get("key1");
    assert!(found.is_some());
    assert_eq!(found.unwrap().name, "key1");

    let not_found = config.get("key3");
    assert!(not_found.is_none());
}

#[test]
fn test_config_collect_errors() {
    let mut config_values = vec![
        ConfigValue::new(
            "key1".to_string(),
            Some("value1".to_string()),
            None,
            "First key".to_string(),
            ConfigImportance::High,
            true,
        ),
        ConfigValue::new(
            "key2".to_string(),
            Some("invalid".to_string()),
            None,
            "Second key".to_string(),
            ConfigImportance::Medium,
            false,
        ),
    ];

    // Add errors to the second config value
    config_values[1].add_error("Invalid value for key2".to_string());

    let mut config = Config::new(config_values);

    // Initially, config should be invalid because key2 has errors
    assert!(!config.is_valid());

    // Collect errors
    config.collect_errors();

    // Now error_messages should contain the error from key2
    assert_eq!(config.error_messages().len(), 1);
    assert_eq!(config.error_messages()[0], "Invalid value for key2");
}

#[test]
fn test_config_clone() {
    let config_values = vec![ConfigValue::new(
        "key1".to_string(),
        Some("value1".to_string()),
        None,
        "First key".to_string(),
        ConfigImportance::High,
        true,
    )];

    let config = Config::new(config_values);
    let cloned = config.clone();

    assert_eq!(cloned.values().len(), 1);
    assert!(cloned.is_valid());
}

#[test]
fn test_config_key_clone() {
    let config_key = ConfigKey::new(
        "test.key".to_string(),
        "Test configuration key".to_string(),
        ConfigType::String,
        Some("default".to_string()),
        ConfigImportance::High,
        true,
    );

    let cloned = config_key.clone();

    assert_eq!(cloned.name, "test.key");
    assert_eq!(cloned.doc, "Test configuration key");
    assert_eq!(cloned.type_, ConfigType::String);
}

#[test]
fn test_config_value_clone() {
    let mut config_value = ConfigValue::new(
        "test.value".to_string(),
        Some("actual".to_string()),
        Some("default".to_string()),
        "Test configuration value".to_string(),
        ConfigImportance::High,
        true,
    );

    config_value.add_error("Test error".to_string());

    let cloned = config_value.clone();

    assert_eq!(cloned.name, "test.value");
    assert_eq!(cloned.value, Some("actual".to_string()));
    assert_eq!(cloned.error_messages().len(), 1);
}

#[test]
fn test_config_def_existing_functionality() {
    let mut config_def = ConfigDef::new();

    // Test that existing ConfigDef functionality still works
    let config_value = ConfigValue::new(
        "test.key".to_string(),
        Some("test.value".to_string()),
        None,
        "Test key".to_string(),
        ConfigImportance::Medium,
        false,
    );

    config_def.add(config_value);

    let retrieved = config_def.get("test.key");
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().name, "test.key");
}

#[test]
fn test_config_def_validate() {
    let mut config_def = ConfigDef::new();

    // Add a required config without a value
    let required_config = ConfigValue::new(
        "required.key".to_string(),
        None, // No value
        None,
        "Required configuration".to_string(),
        ConfigImportance::High,
        true, // required
    );

    config_def.add(required_config);

    // Add an optional config without a value
    let optional_config = ConfigValue::new(
        "optional.key".to_string(),
        None, // No value
        None,
        "Optional configuration".to_string(),
        ConfigImportance::Low,
        false, // not required
    );

    config_def.add(optional_config);

    // Validate should fail because required.key is missing
    let result = config_def.validate();
    assert!(result.is_err());

    let errors = result.unwrap_err();
    assert_eq!(errors.len(), 1);
    assert!(errors[0].contains("required.key"));
}

#[test]
fn test_config_def_validate_success() {
    let mut config_def = ConfigDef::new();

    // Add a required config with a value
    let required_config = ConfigValue::new(
        "required.key".to_string(),
        Some("value".to_string()), // Has value
        None,
        "Required configuration".to_string(),
        ConfigImportance::High,
        true, // required
    );

    config_def.add(required_config);

    // Validate should succeed
    let result = config_def.validate();
    assert!(result.is_ok());
}
