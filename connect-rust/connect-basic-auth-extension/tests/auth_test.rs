//! Unit tests for basic auth extension

use connect_basic_auth_extension::{BasicAuthConfig, BasicAuthCredentials, BasicAuthExtension};

#[test]
fn test_basic_auth_config_new() {
    let config = BasicAuthConfig::new();
    assert!(config.username().is_none());
    assert!(config.password().is_none());
    assert!(!config.is_enabled());
}

#[test]
fn test_basic_auth_config_with_credentials() {
    let config = BasicAuthConfig::with_credentials("testuser".to_string(), "testpass".to_string());
    assert_eq!(config.username(), Some(&"testuser".to_string()));
    assert_eq!(config.password(), Some(&"testpass".to_string()));
    assert!(config.is_enabled());
}

#[test]
fn test_basic_auth_config_set_username() {
    let mut config = BasicAuthConfig::new();
    assert!(!config.is_enabled());
    config.set_username("user1".to_string());
    assert_eq!(config.username(), Some(&"user1".to_string()));
    assert!(!config.is_enabled()); // Still not enabled without password
}

#[test]
fn test_basic_auth_config_set_password() {
    let mut config = BasicAuthConfig::new();
    config.set_password("pass1".to_string());
    assert_eq!(config.password(), Some(&"pass1".to_string()));
    assert!(!config.is_enabled()); // Still not enabled without username
}

#[test]
fn test_basic_auth_config_is_enabled() {
    let mut config = BasicAuthConfig::new();
    assert!(!config.is_enabled());

    config.set_username("user".to_string());
    assert!(!config.is_enabled());

    config.set_password("pass".to_string());
    assert!(config.is_enabled());
}

#[test]
fn test_basic_auth_credentials_new() {
    let creds = BasicAuthCredentials::new("admin".to_string(), "secret".to_string());
    assert_eq!(creds.username(), "admin");
    assert_eq!(creds.password(), "secret");
}

#[test]
fn test_basic_auth_credentials_validate() {
    let creds = BasicAuthCredentials::new("admin".to_string(), "secret".to_string());
    assert!(creds.validate("admin", "secret"));
    assert!(!creds.validate("admin", "wrong"));
    assert!(!creds.validate("wrong", "secret"));
    assert!(!creds.validate("wrong", "wrong"));
}

#[test]
fn test_basic_auth_credentials_from_authorization_header() {
    // "admin:secret" in base64
    let header = "Basic YWRtaW46c2VjcmV0";
    let creds = BasicAuthCredentials::from_authorization_header(header).unwrap();
    assert_eq!(creds.username(), "admin");
    assert_eq!(creds.password(), "secret");
}

#[test]
fn test_basic_auth_credentials_from_authorization_header_invalid_format() {
    // Missing "Basic " prefix
    let result = BasicAuthCredentials::from_authorization_header("YWRtaW46c2VjcmV0");
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .contains("Invalid authorization header format"));
}

#[test]
fn test_basic_auth_credentials_from_authorization_header_invalid_base64() {
    let result = BasicAuthCredentials::from_authorization_header("Basic !!!invalid!!!");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Failed to decode base64"));
}

#[test]
fn test_basic_auth_credentials_from_authorization_header_no_colon() {
    // "adminpassword" in base64 (no colon separator)
    let header = "Basic YWRtaW5wYXNzd29yZA==";
    let result = BasicAuthCredentials::from_authorization_header(header);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Invalid credentials format"));
}

#[test]
fn test_basic_auth_extension_new() {
    let extension = BasicAuthExtension::new();
    assert!(extension.config().username().is_none());
    assert!(extension.config().password().is_none());
    assert!(extension.credentials().is_none());
    assert_eq!(extension.version(), env!("CARGO_PKG_VERSION"));
}

#[test]
fn test_basic_auth_extension_with_credentials() {
    let extension = BasicAuthExtension::with_credentials("admin".to_string(), "secret".to_string());
    assert!(extension.config().is_enabled());
    assert!(extension.credentials().is_some());
    let creds = extension.credentials().unwrap();
    assert_eq!(creds.username(), "admin");
    assert_eq!(creds.password(), "secret");
}

#[test]
fn test_basic_auth_extension_configure() {
    use std::any::Any;
    use std::collections::HashMap;

    let mut extension = BasicAuthExtension::new();
    assert!(!extension.config().is_enabled());

    let mut configs = HashMap::new();
    configs.insert(
        "basic.auth.username".to_string(),
        Box::new("user1".to_string()) as Box<dyn Any>,
    );
    configs.insert(
        "basic.auth.password".to_string(),
        Box::new("pass1".to_string()) as Box<dyn Any>,
    );

    extension.configure(configs);

    assert!(extension.config().is_enabled());
    assert_eq!(extension.config().username(), Some(&"user1".to_string()));
    assert_eq!(extension.config().password(), Some(&"pass1".to_string()));
    assert!(extension.credentials().is_some());
}

#[test]
fn test_basic_auth_extension_close() {
    let mut extension =
        BasicAuthExtension::with_credentials("admin".to_string(), "secret".to_string());
    assert!(extension.credentials().is_some());

    extension.close();

    assert!(extension.credentials().is_none());
    // Config should still be there
    assert!(extension.config().is_enabled());
}
