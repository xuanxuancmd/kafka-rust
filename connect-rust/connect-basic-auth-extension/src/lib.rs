//! Kafka Connect Basic Auth Extension
//!
//! This crate provides basic authentication extension for Kafka Connect REST API.

pub mod auth;
pub mod rest;

pub use auth::{BasicAuthConfig, BasicAuthCredentials, BasicAuthExtension};
pub use rest::{BasicAuthRestExtension, BasicAuthFilter, ConnectRestExtension, ConnectRestExtensionContext};
