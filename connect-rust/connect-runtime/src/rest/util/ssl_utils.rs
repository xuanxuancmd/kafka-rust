// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! SSL utilities for configuring SSL/TLS for REST server and client.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.rest.util.SSLUtils` in Java.
//!
//! This module provides helper methods for setting up SSL/TLS configuration
//! for HTTPS connections in Kafka Connect REST API.

use std::collections::HashMap;
use std::path::PathBuf;

/// SSL configuration for server or client.
///
/// Contains all SSL-related settings extracted from Kafka Connect configuration.
#[derive(Debug, Clone)]
pub struct SslConfig {
    /// KeyStore type (JKS, PKCS12, etc.)
    pub keystore_type: String,
    /// KeyStore file path.
    pub keystore_path: Option<PathBuf>,
    /// KeyStore password.
    pub keystore_password: Option<String>,
    /// Key password (for private key).
    pub key_password: Option<String>,
    /// TrustStore type.
    pub truststore_type: String,
    /// TrustStore file path.
    pub truststore_path: Option<PathBuf>,
    /// TrustStore password.
    pub truststore_password: Option<String>,
    /// SSL protocol (TLSv1.2, TLSv1.3, etc.)
    pub protocol: String,
    /// Enabled SSL protocols.
    pub enabled_protocols: Vec<String>,
    /// SSL provider.
    pub provider: Option<String>,
    /// Cipher suites.
    pub cipher_suites: Vec<String>,
    /// Key manager factory algorithm.
    pub key_manager_algorithm: String,
    /// Trust manager factory algorithm.
    pub trust_manager_algorithm: String,
    /// Secure random implementation.
    pub secure_random_impl: Option<String>,
    /// Endpoint identification algorithm (for client).
    pub endpoint_identification_algorithm: Option<String>,
    /// Client authentication mode (for server).
    pub client_auth: ClientAuthMode,
}

/// Client authentication mode for server SSL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientAuthMode {
    /// No client authentication required.
    None,
    /// Client authentication requested but not required.
    Requested,
    /// Client authentication required.
    Required,
}

impl Default for SslConfig {
    fn default() -> Self {
        SslConfig {
            keystore_type: "JKS".to_string(),
            keystore_path: None,
            keystore_password: None,
            key_password: None,
            truststore_type: "JKS".to_string(),
            truststore_path: None,
            truststore_password: None,
            protocol: "TLS".to_string(),
            enabled_protocols: vec!["TLSv1.2".to_string(), "TLSv1.3".to_string()],
            provider: None,
            cipher_suites: vec![],
            key_manager_algorithm: "SunX509".to_string(),
            trust_manager_algorithm: "SunX509".to_string(),
            secure_random_impl: None,
            endpoint_identification_algorithm: None,
            client_auth: ClientAuthMode::None,
        }
    }
}

impl SslConfig {
    /// Creates a new SslConfig with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates SslConfig from a configuration map with a prefix.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration map
    /// * `prefix` - Prefix for SSL configuration keys (e.g., "listeners.https.")
    pub fn from_config_with_prefix(config: &HashMap<String, String>, prefix: &str) -> Self {
        let ssl_config_values = Self::extract_prefix_config(config, prefix);
        Self::build_ssl_config(ssl_config_values)
    }

    /// Creates server-side SSL configuration.
    pub fn server_config(config: &HashMap<String, String>) -> Self {
        Self::from_config_with_prefix(config, "listeners.https.")
    }

    /// Creates client-side SSL configuration.
    pub fn client_config(config: &HashMap<String, String>) -> Self {
        Self::from_config_with_prefix(config, "listeners.https.")
    }

    /// Extracts configuration values with the given prefix.
    fn extract_prefix_config(
        config: &HashMap<String, String>,
        prefix: &str,
    ) -> HashMap<String, String> {
        config
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k[prefix.len()..].to_string(), v.clone()))
            .collect()
    }

    /// Builds SslConfig from extracted configuration values.
    fn build_ssl_config(ssl_values: HashMap<String, String>) -> Self {
        let mut ssl_config = SslConfig::new();

        // KeyStore configuration
        ssl_config.keystore_type = ssl_values
            .get("ssl.keystore.type")
            .cloned()
            .unwrap_or_else(|| "JKS".to_string());

        if let Some(path) = ssl_values.get("ssl.keystore.location") {
            ssl_config.keystore_path = Some(PathBuf::from(path));
        }

        if let Some(pwd) = ssl_values.get("ssl.keystore.password") {
            ssl_config.keystore_password = Some(pwd.clone());
        }

        if let Some(pwd) = ssl_values.get("ssl.key.password") {
            ssl_config.key_password = Some(pwd.clone());
        }

        // TrustStore configuration
        ssl_config.truststore_type = ssl_values
            .get("ssl.truststore.type")
            .cloned()
            .unwrap_or_else(|| "JKS".to_string());

        if let Some(path) = ssl_values.get("ssl.truststore.location") {
            ssl_config.truststore_path = Some(PathBuf::from(path));
        }

        if let Some(pwd) = ssl_values.get("ssl.truststore.password") {
            ssl_config.truststore_password = Some(pwd.clone());
        }

        // Protocol configuration
        ssl_config.protocol = ssl_values
            .get("ssl.protocol")
            .cloned()
            .unwrap_or_else(|| "TLS".to_string());

        if let Some(protocols) = ssl_values.get("ssl.enabled.protocols") {
            ssl_config.enabled_protocols = Self::parse_comma_list(protocols);
        }

        if let Some(provider) = ssl_values.get("ssl.provider") {
            ssl_config.provider = Some(provider.clone());
        }

        if let Some(ciphers) = ssl_values.get("ssl.cipher.suites") {
            ssl_config.cipher_suites = Self::parse_comma_list(ciphers);
        }

        // Algorithm configuration
        ssl_config.key_manager_algorithm = ssl_values
            .get("ssl.keymanager.algorithm")
            .cloned()
            .unwrap_or_else(|| "SunX509".to_string());

        ssl_config.trust_manager_algorithm = ssl_values
            .get("ssl.trustmanager.algorithm")
            .cloned()
            .unwrap_or_else(|| "SunX509".to_string());

        if let Some(random) = ssl_values.get("ssl.secure.random.implementation") {
            ssl_config.secure_random_impl = Some(random.clone());
        }

        // Endpoint identification (for client)
        if let Some(alg) = ssl_values.get("ssl.endpoint.identification.algorithm") {
            ssl_config.endpoint_identification_algorithm = Some(alg.clone());
        }

        // Client auth (for server)
        if let Some(auth) = ssl_values.get("ssl.client.auth") {
            ssl_config.client_auth = match auth.as_str() {
                "requested" => ClientAuthMode::Requested,
                "required" => ClientAuthMode::Required,
                _ => ClientAuthMode::None,
            };
        }

        ssl_config
    }

    /// Parses a comma-separated list.
    fn parse_comma_list(value: &str) -> Vec<String> {
        value
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    }

    /// Checks if SSL is enabled (has keystore configured).
    pub fn is_ssl_enabled(&self) -> bool {
        self.keystore_path.is_some() && self.keystore_password.is_some()
    }

    /// Checks if truststore is configured.
    pub fn has_truststore(&self) -> bool {
        self.truststore_path.is_some() && self.truststore_password.is_some()
    }
}

/// SSLUtils - helper class for setting up SSL for REST server and client.
///
/// This provides static utility methods for SSL configuration.
/// Corresponds to `org.apache.kafka.connect.runtime.rest.util.SSLUtils` in Java.
pub struct SSLUtils;

impl SSLUtils {
    /// Creates server-side SSL configuration from config map.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration map
    /// * `prefix` - Prefix for SSL config keys
    pub fn create_server_ssl_config(config: &HashMap<String, String>, prefix: &str) -> SslConfig {
        SslConfig::from_config_with_prefix(config, prefix)
    }

    /// Creates server-side SSL configuration with default prefix.
    pub fn create_server_ssl_config_default(config: &HashMap<String, String>) -> SslConfig {
        SslConfig::server_config(config)
    }

    /// Creates client-side SSL configuration.
    pub fn create_client_ssl_config(config: &HashMap<String, String>) -> SslConfig {
        SslConfig::client_config(config)
    }

    /// Gets a value from config map or returns default.
    pub fn get_or_default(config: &HashMap<String, String>, key: &str, default: &str) -> String {
        config
            .get(key)
            .cloned()
            .unwrap_or_else(|| default.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_ssl_config() {
        let config = SslConfig::new();
        assert_eq!(config.keystore_type, "JKS");
        assert_eq!(config.protocol, "TLS");
        assert_eq!(config.client_auth, ClientAuthMode::None);
        assert!(!config.is_ssl_enabled());
    }

    #[test]
    fn test_ssl_config_from_map() {
        let mut config = HashMap::new();
        config.insert(
            "listeners.https.ssl.keystore.location".to_string(),
            "/path/to/keystore.jks".to_string(),
        );
        config.insert(
            "listeners.https.ssl.keystore.password".to_string(),
            "secret".to_string(),
        );
        config.insert(
            "listeners.https.ssl.protocol".to_string(),
            "TLSv1.3".to_string(),
        );
        config.insert(
            "listeners.https.ssl.client.auth".to_string(),
            "required".to_string(),
        );

        let ssl_config = SSLUtils::create_server_ssl_config_default(&config);

        assert_eq!(
            ssl_config.keystore_path,
            Some(PathBuf::from("/path/to/keystore.jks"))
        );
        assert_eq!(ssl_config.keystore_password, Some("secret".to_string()));
        assert_eq!(ssl_config.protocol, "TLSv1.3");
        assert_eq!(ssl_config.client_auth, ClientAuthMode::Required);
        assert!(ssl_config.is_ssl_enabled());
    }

    #[test]
    fn test_parse_comma_list() {
        let list = SslConfig::parse_comma_list("TLSv1.2, TLSv1.3");
        assert_eq!(list, vec!["TLSv1.2", "TLSv1.3"]);

        let empty = SslConfig::parse_comma_list("");
        assert!(empty.is_empty());
    }

    #[test]
    fn test_client_auth_modes() {
        assert_eq!(ClientAuthMode::None, ClientAuthMode::None);
        assert_ne!(ClientAuthMode::None, ClientAuthMode::Required);
    }
}
