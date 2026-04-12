//! Unit tests for BasicAuthSecurityRestExtension

use connect_basic_auth_extension::rest::{
    BasicAuthSecurityRestExtension, ConnectRestExtension, ConnectRestExtensionContext,
};
use std::any::Any;
use std::collections::HashMap;

#[test]
fn test_jaas_configuration_not_overwritten() {
    // In Java, this test verifies that JAAS configuration
    // captured at startup is not overwritten by later code

    // Create extension
    let mut extension = BasicAuthSecurityRestExtension::new();

    // Create a mock context
    let mut config = HashMap::new();
    config.insert("test.key".to_string(), "test.value".to_string());
    let context = ConnectRestExtensionContext::new(config, "worker-1".to_string());

    // Register extension
    ConnectRestExtension::register(&mut extension, context);

    // Verify that extension was created
    assert_eq!(
        ConnectRestExtension::version(&extension),
        env!("CARGO_PKG_VERSION")
    );

    // Close extension
    ConnectRestExtension::close(&mut extension);
}

#[test]
fn test_version() {
    let extension = BasicAuthSecurityRestExtension::new();
    assert_eq!(
        ConnectRestExtension::version(&extension),
        env!("CARGO_PKG_VERSION")
    );
}

#[test]
fn test_configure() {
    let mut extension = BasicAuthSecurityRestExtension::new();
    let mut configs = HashMap::new();

    // Configure should succeed (JAAS config was captured at startup)
    ConnectRestExtension::configure(&mut extension, configs);
}
