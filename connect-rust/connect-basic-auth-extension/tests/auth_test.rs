//! Unit tests for basic auth extension

use connect_basic_auth_extension::error::LoginException;
use connect_basic_auth_extension::{BasicAuthConfig, BasicAuthCredentials};

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
fn test_basic_auth_credentials_from_authorization_header() {
    // "admin:secret" in base64
    let header = "Basic YWRtaW46c2VjcmV0";
    let creds = BasicAuthCredentials::from_authorization_header(header).unwrap();
    assert_eq!(creds.username(), Some(&"admin".to_string()));
    assert_eq!(creds.password(), Some(&"secret".to_string()));
}

#[test]
fn test_basic_auth_credentials_validate() {
    // Create credentials from username and password
    let header = "Basic YWRtaW46c2VjcmV0";
    let creds = BasicAuthCredentials::from_authorization_header(header).unwrap();

    if let (Some(username), Some(password)) = (creds.username(), creds.password()) {
        assert_eq!(username, "admin");
        assert_eq!(password, "secret");
    } else {
        panic!("Expected credentials to be present");
    }
}

#[test]
fn test_basic_auth_credentials_from_authorization_header_invalid_format() {
    // Missing "Basic " prefix
    let result = BasicAuthCredentials::from_authorization_header("YWRtaW46c2VjcmV0");
    assert!(result.is_err());

    match result {
        Err(LoginException::InvalidAuthorizationHeader(msg)) => {
            assert!(msg.contains("Invalid authorization header format"));
        }
        _ => panic!("Expected InvalidAuthorizationHeader error"),
    }
}

#[test]
fn test_basic_auth_credentials_from_authorization_header_invalid_base64() {
    let result = BasicAuthCredentials::from_authorization_header("Basic !!!invalid!!!");
    assert!(result.is_err());

    match result {
        Err(LoginException::Base64Error(msg)) => {
            assert!(msg.contains("Failed to decode base64"));
        }
        _ => panic!("Expected Base64Error"),
    }
}

#[test]
fn test_basic_auth_credentials_from_authorization_header_no_colon() {
    // "adminpassword" in base64 (no colon separator)
    let header = "Basic YWRtaW5wYXNzd29yZA==";
    let result = BasicAuthCredentials::from_authorization_header(header);
    assert!(result.is_err());

    match result {
        Err(LoginException::InvalidAuthorizationHeader(msg)) => {
            assert!(msg.contains("Invalid credentials format"));
        }
        _ => panic!("Expected InvalidAuthorizationHeader error"),
    }
}

#[test]
fn test_basic_auth_credentials_empty_header() {
    let creds = BasicAuthCredentials::from_authorization_header("").unwrap();
    assert!(creds.username().is_none());
    assert!(creds.password().is_none());
}
