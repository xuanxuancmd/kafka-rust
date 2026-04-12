//! Kafka Connect Basic Auth Extension
//!
//! This crate provides basic authentication extension for Kafka Connect REST API.

pub mod auth;
pub mod basic_auth_callback_handler;
pub mod basic_auth_credentials;
pub mod error;
pub mod jaas;
pub mod jaas_basic_auth_filter;
pub mod property_file_login_module;
pub mod request_matcher;
pub mod rest;

pub use auth::{BasicAuthConfig, BasicAuthStoredCredentials};
pub use basic_auth_callback_handler::BasicAuthCallBackHandler;
pub use basic_auth_credentials::BasicAuthCredentials;
pub use error::{AuthResult, FilterError, FilterResult, LoginException};
pub use jaas::{
    AppConfigurationEntry, Callback, CallbackHandler, ControlFlag, JaasConfiguration, LoginModule,
    NameCallback, PasswordCallback, Subject,
};
pub use jaas_basic_auth_filter::JaasBasicAuthFilter;
pub use property_file_login_module::PropertyFileLoginModule;
pub use request_matcher::RequestMatcher;
pub use rest::{BasicAuthSecurityRestExtension, ConnectRestExtension, ConnectRestExtensionContext};
