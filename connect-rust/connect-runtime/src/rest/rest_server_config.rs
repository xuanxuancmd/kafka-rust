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

//! RestServerConfig - Configuration for REST server.
//!
//! Defines the configuration surface for a RestServer instance, with support for both
//! internal-only and user-facing servers.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.rest.RestServerConfig` in Java.

use std::collections::HashMap;

/// Configuration constants for REST server.
///
/// These constants correspond to the public static final fields in Java's RestServerConfig.
pub mod config_keys {
    /// List of comma-separated URIs the REST API will listen on.
    pub const LISTENERS_CONFIG: &str = "listeners";

    /// If this is set, this is the hostname that will be given out to other workers.
    pub const REST_ADVERTISED_HOST_NAME_CONFIG: &str = "rest.advertised.host.name";

    /// If this is set, this is the port that will be given out to other workers.
    pub const REST_ADVERTISED_PORT_CONFIG: &str = "rest.advertised.port";

    /// Sets the advertised listener (HTTP or HTTPS).
    pub const REST_ADVERTISED_LISTENER_CONFIG: &str = "rest.advertised.listener";

    /// Value to set the Access-Control-Allow-Origin header.
    pub const ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG: &str = "access.control.allow.origin";

    /// Sets the methods supported for cross origin requests.
    pub const ACCESS_CONTROL_ALLOW_METHODS_CONFIG: &str = "access.control.allow.methods";

    /// List of comma-separated URIs the Admin REST API will listen on.
    pub const ADMIN_LISTENERS_CONFIG: &str = "admin.listeners";

    /// Prefix for HTTPS configs on admin listeners.
    pub const ADMIN_LISTENERS_HTTPS_CONFIGS_PREFIX: &str = "admin.listeners.https.";

    /// Comma-separated names of ConnectRestExtension classes.
    pub const REST_EXTENSION_CLASSES_CONFIG: &str = "rest.extension.classes";

    /// Rules for REST API HTTP response headers.
    pub const RESPONSE_HTTP_HEADERS_CONFIG: &str = "response.http.headers.config";

    /// SSL client authentication configuration.
    pub const SSL_CLIENT_AUTH_CONFIG: &str = "ssl.client.auth";
}

/// Default values for REST server configuration.
pub mod defaults {
    /// Default listeners value.
    pub const LISTENERS_DEFAULT: &[&str] = &["http://:8083"];

    /// Default value for access control allow origin.
    pub const ACCESS_CONTROL_ALLOW_ORIGIN_DEFAULT: &str = "";

    /// Default value for access control allow methods.
    pub const ACCESS_CONTROL_ALLOW_METHODS_DEFAULT: &str = "";

    /// Default value for response HTTP headers.
    pub const RESPONSE_HTTP_HEADERS_DEFAULT: &str = "";

    /// Default value for SSL client auth.
    pub const SSL_CLIENT_AUTH_DEFAULT: &str = "none";
}

/// Valid header actions for response header config.
pub const HEADER_ACTIONS: &[&str] = &["set", "add", "setDate", "addDate"];

/// Configuration for the REST server.
///
/// An internal-only server exposes endpoints for intra-cluster communication.
/// A user-facing server exposes all public REST API endpoints.
///
/// Corresponds to `RestServerConfig` abstract class in Java.
#[derive(Debug, Clone)]
pub struct RestServerConfig {
    /// List of URIs the REST API will listen on.
    listeners: Vec<String>,
    /// Raw listeners string from original config.
    raw_listeners: Option<String>,
    /// Advertised host name.
    advertised_host_name: Option<String>,
    /// Advertised port.
    advertised_port: Option<u16>,
    /// Advertised listener protocol.
    advertised_listener: Option<String>,
    /// Allowed origins for CORS.
    allowed_origins: String,
    /// Allowed methods for CORS.
    allowed_methods: String,
    /// Response HTTP headers config.
    response_headers: String,
    /// Admin listeners (for user-facing servers).
    admin_listeners: Option<Vec<String>>,
    /// REST extension classes.
    rest_extensions: Option<Vec<String>>,
    /// Rebalance timeout in milliseconds (for user-facing servers).
    rebalance_timeout_ms: Option<u64>,
    /// Whether topic tracking is enabled.
    topic_tracking_enabled: bool,
    /// Whether topic tracking reset is allowed.
    topic_tracking_reset_enabled: bool,
    /// Whether this is a public (user-facing) config.
    is_public: bool,
}

impl RestServerConfig {
    /// Creates an internal-only REST server configuration.
    ///
    /// An internal-only server exposes only endpoints for intra-cluster communication.
    ///
    /// # Arguments
    ///
    /// * `props` - Configuration properties map
    pub fn for_internal(props: HashMap<String, String>) -> Self {
        let listeners = Self::parse_listeners(&props);
        let raw_listeners = props.get(config_keys::LISTENERS_CONFIG).cloned();
        let advertised_host_name = props
            .get(config_keys::REST_ADVERTISED_HOST_NAME_CONFIG)
            .cloned();
        let advertised_port = props
            .get(config_keys::REST_ADVERTISED_PORT_CONFIG)
            .and_then(|s| s.parse::<u16>().ok());
        let advertised_listener = props
            .get(config_keys::REST_ADVERTISED_LISTENER_CONFIG)
            .cloned();
        let allowed_origins = props
            .get(config_keys::ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG)
            .cloned()
            .unwrap_or_else(|| defaults::ACCESS_CONTROL_ALLOW_ORIGIN_DEFAULT.to_string());
        let allowed_methods = props
            .get(config_keys::ACCESS_CONTROL_ALLOW_METHODS_CONFIG)
            .cloned()
            .unwrap_or_else(|| defaults::ACCESS_CONTROL_ALLOW_METHODS_DEFAULT.to_string());
        let response_headers = props
            .get(config_keys::RESPONSE_HTTP_HEADERS_CONFIG)
            .cloned()
            .unwrap_or_else(|| defaults::RESPONSE_HTTP_HEADERS_DEFAULT.to_string());

        RestServerConfig {
            listeners,
            raw_listeners,
            advertised_host_name,
            advertised_port,
            advertised_listener,
            allowed_origins,
            allowed_methods,
            response_headers,
            admin_listeners: Some(Vec::new()), // Disable admin resources
            rest_extensions: None,             // Disable REST extensions
            rebalance_timeout_ms: None,
            topic_tracking_enabled: false, // Unnecessary for internal-only
            topic_tracking_reset_enabled: false,
            is_public: false,
        }
    }

    /// Creates a user-facing (public) REST server configuration.
    ///
    /// A user-facing server exposes all public REST API endpoints and REST extensions.
    ///
    /// # Arguments
    ///
    /// * `rebalance_timeout_ms` - Rebalance timeout in milliseconds
    /// * `props` - Configuration properties map
    pub fn for_public(rebalance_timeout_ms: Option<u64>, props: HashMap<String, String>) -> Self {
        let listeners = Self::parse_listeners(&props);
        let raw_listeners = props.get(config_keys::LISTENERS_CONFIG).cloned();
        let advertised_host_name = props
            .get(config_keys::REST_ADVERTISED_HOST_NAME_CONFIG)
            .cloned();
        let advertised_port = props
            .get(config_keys::REST_ADVERTISED_PORT_CONFIG)
            .and_then(|s| s.parse::<u16>().ok());
        let advertised_listener = props
            .get(config_keys::REST_ADVERTISED_LISTENER_CONFIG)
            .cloned();
        let allowed_origins = props
            .get(config_keys::ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG)
            .cloned()
            .unwrap_or_else(|| defaults::ACCESS_CONTROL_ALLOW_ORIGIN_DEFAULT.to_string());
        let allowed_methods = props
            .get(config_keys::ACCESS_CONTROL_ALLOW_METHODS_CONFIG)
            .cloned()
            .unwrap_or_else(|| defaults::ACCESS_CONTROL_ALLOW_METHODS_DEFAULT.to_string());
        let response_headers = props
            .get(config_keys::RESPONSE_HTTP_HEADERS_CONFIG)
            .cloned()
            .unwrap_or_else(|| defaults::RESPONSE_HTTP_HEADERS_DEFAULT.to_string());
        let admin_listeners = Self::parse_admin_listeners(&props);
        let rest_extensions = Self::parse_rest_extensions(&props);
        let topic_tracking_enabled = props
            .get("topic.tracking.enable")
            .and_then(|s| s.parse::<bool>().ok())
            .unwrap_or(true);
        let topic_tracking_reset_enabled = props
            .get("topic.tracking.allow.reset")
            .and_then(|s| s.parse::<bool>().ok())
            .unwrap_or(true);

        RestServerConfig {
            listeners,
            raw_listeners,
            advertised_host_name,
            advertised_port,
            advertised_listener,
            allowed_origins,
            allowed_methods,
            response_headers,
            admin_listeners,
            rest_extensions,
            rebalance_timeout_ms,
            topic_tracking_enabled,
            topic_tracking_reset_enabled,
            is_public: true,
        }
    }

    /// Parses listeners from configuration.
    fn parse_listeners(props: &HashMap<String, String>) -> Vec<String> {
        props
            .get(config_keys::LISTENERS_CONFIG)
            .and_then(|s| {
                if s.is_empty() {
                    None
                } else {
                    Some(
                        s.split(',')
                            .map(|part| part.trim().to_string())
                            .filter(|part| !part.is_empty())
                            .collect(),
                    )
                }
            })
            .unwrap_or_else(|| {
                defaults::LISTENERS_DEFAULT
                    .iter()
                    .map(|s| s.to_string())
                    .collect()
            })
    }

    /// Parses admin listeners from configuration.
    fn parse_admin_listeners(props: &HashMap<String, String>) -> Option<Vec<String>> {
        props
            .get(config_keys::ADMIN_LISTENERS_CONFIG)
            .and_then(|s| {
                if s.trim().is_empty() {
                    None
                } else {
                    Some(
                        s.split(',')
                            .map(|part| part.trim().to_string())
                            .filter(|part| !part.is_empty())
                            .collect(),
                    )
                }
            })
    }

    /// Parses REST extension classes from configuration.
    fn parse_rest_extensions(props: &HashMap<String, String>) -> Option<Vec<String>> {
        props
            .get(config_keys::REST_EXTENSION_CLASSES_CONFIG)
            .and_then(|s| {
                if s.trim().is_empty() {
                    Some(Vec::new())
                } else {
                    Some(
                        s.split(',')
                            .map(|part| part.trim().to_string())
                            .filter(|part| !part.is_empty())
                            .collect(),
                    )
                }
            })
    }

    /// Returns the listeners for this server.
    pub fn listeners(&self) -> &[String] {
        &self.listeners
    }

    /// Returns the raw listeners string from original config.
    pub fn raw_listeners(&self) -> Option<&str> {
        self.raw_listeners.as_deref()
    }

    /// Returns the admin listeners, or empty if disabled.
    pub fn admin_listeners(&self) -> Option<&[String]> {
        self.admin_listeners.as_deref()
    }

    /// Returns the REST extension classes.
    pub fn rest_extensions(&self) -> Option<&[String]> {
        self.rest_extensions.as_deref()
    }

    /// Returns the allowed origins for CORS.
    pub fn allowed_origins(&self) -> &str {
        &self.allowed_origins
    }

    /// Returns the allowed methods for CORS.
    pub fn allowed_methods(&self) -> &str {
        &self.allowed_methods
    }

    /// Returns the response HTTP headers config.
    pub fn response_headers(&self) -> &str {
        &self.response_headers
    }

    /// Returns the advertised listener protocol.
    pub fn advertised_listener(&self) -> Option<&str> {
        self.advertised_listener.as_deref()
    }

    /// Returns the advertised host name.
    pub fn advertised_host_name(&self) -> Option<&str> {
        self.advertised_host_name.as_deref()
    }

    /// Returns the advertised port.
    pub fn advertised_port(&self) -> Option<u16> {
        self.advertised_port
    }

    /// Returns the rebalance timeout in milliseconds.
    pub fn rebalance_timeout_ms(&self) -> Option<u64> {
        self.rebalance_timeout_ms
    }

    /// Returns whether topic tracking is enabled.
    pub fn topic_tracking_enabled(&self) -> bool {
        self.topic_tracking_enabled
    }

    /// Returns whether topic tracking reset is allowed.
    pub fn topic_tracking_reset_enabled(&self) -> bool {
        self.topic_tracking_reset_enabled
    }

    /// Returns whether this is a public (user-facing) config.
    pub fn is_public(&self) -> bool {
        self.is_public
    }

    /// Validates the HTTP response header config format.
    ///
    /// Expected format: `[action] [header name]:[header value]`
    pub fn validate_http_response_header_config(config: &str) -> Result<(), String> {
        let config_tokens: Vec<&str> = config.trim().splitn(2, ' ').collect();
        if config_tokens.len() != 2 {
            return Err(format!(
                "Invalid format of header config '{}'. Expected: '[action] [header name]:[header value]'",
                config
            ));
        }

        // Validate action
        let method = config_tokens[0].trim();
        Self::validate_header_config_action(method)?;

        // Validate header name and header value pair
        let header = config_tokens[1];
        let header_tokens: Vec<&str> = header.trim().splitn(2, ':').collect();
        if header_tokens.len() != 2 {
            return Err(format!(
                "Invalid format of header name and header value pair '{}'. Expected: '[header name]:[header value]'",
                header
            ));
        }

        // Validate header name
        let header_name = header_tokens[0].trim();
        if header_name.is_empty() || header_name.contains(char::is_whitespace) {
            return Err(format!(
                "Invalid header name '{}'. The '[header name]' cannot contain whitespace",
                header_name
            ));
        }

        Ok(())
    }

    /// Validates the header config action.
    pub fn validate_header_config_action(action: &str) -> Result<(), String> {
        if !HEADER_ACTIONS
            .iter()
            .any(|a| a.eq_ignore_ascii_case(action))
        {
            return Err(format!(
                "Invalid header config action: '{}'. Expected one of {}",
                action,
                HEADER_ACTIONS.join(", ")
            ));
        }
        Ok(())
    }

    /// Validates the listeners configuration.
    pub fn validate_listeners(listeners: &[String]) -> Result<(), String> {
        if listeners.is_empty() {
            return Err("Invalid value for listeners, at least one URL is expected.".to_string());
        }

        for listener in listeners {
            if listener.trim().is_empty() {
                return Err("Empty URL found when parsing listeners list.".to_string());
            }
        }

        Ok(())
    }
}

impl Default for RestServerConfig {
    fn default() -> Self {
        RestServerConfig::for_internal(HashMap::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_for_internal() {
        let config = RestServerConfig::for_internal(HashMap::new());
        assert!(!config.is_public());
        assert_eq!(config.listeners().len(), 1);
        assert_eq!(config.listeners()[0], "http://:8083");
        assert!(config.admin_listeners().unwrap().is_empty());
        assert!(!config.topic_tracking_enabled());
    }

    #[test]
    fn test_for_public() {
        let mut props = HashMap::new();
        props.insert("listeners".to_string(), "http://localhost:8083".to_string());
        props.insert(
            "rest.advertised.host.name".to_string(),
            "myhost".to_string(),
        );
        props.insert("rest.advertised.port".to_string(), "9092".to_string());

        let config = RestServerConfig::for_public(Some(60_000), props);
        assert!(config.is_public());
        assert_eq!(config.listeners()[0], "http://localhost:8083");
        assert_eq!(config.advertised_host_name(), Some("myhost"));
        assert_eq!(config.advertised_port(), Some(9092));
        assert_eq!(config.rebalance_timeout_ms(), Some(60_000));
        assert!(config.topic_tracking_enabled());
    }

    #[test]
    fn test_parse_listeners() {
        let mut props = HashMap::new();
        props.insert(
            "listeners".to_string(),
            "http://host1:8083,https://host2:8443".to_string(),
        );

        let config = RestServerConfig::for_internal(props);
        assert_eq!(config.listeners().len(), 2);
        assert_eq!(config.listeners()[0], "http://host1:8083");
        assert_eq!(config.listeners()[1], "https://host2:8443");
    }

    #[test]
    fn test_validate_header_config_action() {
        assert!(RestServerConfig::validate_header_config_action("set").is_ok());
        assert!(RestServerConfig::validate_header_config_action("add").is_ok());
        assert!(RestServerConfig::validate_header_config_action("setDate").is_ok());
        assert!(RestServerConfig::validate_header_config_action("addDate").is_ok());
        assert!(RestServerConfig::validate_header_config_action("invalid").is_err());
    }

    #[test]
    fn test_validate_http_response_header_config() {
        assert!(
            RestServerConfig::validate_http_response_header_config("set X-Custom: value").is_ok()
        );
        assert!(RestServerConfig::validate_http_response_header_config(
            "add Content-Type: application/json"
        )
        .is_ok());
        assert!(RestServerConfig::validate_http_response_header_config("invalid").is_err());
        assert!(
            RestServerConfig::validate_http_response_header_config("set invalidheader").is_err()
        );
        assert!(RestServerConfig::validate_http_response_header_config(
            "set header with space: value"
        )
        .is_err());
    }

    #[test]
    fn test_validate_listeners() {
        assert!(
            RestServerConfig::validate_listeners(&["http://localhost:8083".to_string()]).is_ok()
        );
        assert!(RestServerConfig::validate_listeners(&[]).is_err());
        assert!(RestServerConfig::validate_listeners(&["".to_string()]).is_err());
    }

    #[test]
    fn test_default_config() {
        let config = RestServerConfig::default();
        assert!(!config.is_public());
        assert_eq!(config.listeners()[0], "http://:8083");
    }

    #[test]
    fn test_allowed_origins_methods() {
        let mut props = HashMap::new();
        props.insert("access.control.allow.origin".to_string(), "*".to_string());
        props.insert(
            "access.control.allow.methods".to_string(),
            "GET,POST,PUT".to_string(),
        );

        let config = RestServerConfig::for_public(None, props);
        assert_eq!(config.allowed_origins(), "*");
        assert_eq!(config.allowed_methods(), "GET,POST,PUT");
    }
}
