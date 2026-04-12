// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
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

//! JAAS Basic Auth Filter
//!
//! This module provides a JAX-RS ContainerRequestFilter implementation
//! that performs JAAS-based Basic Auth authentication.

use once_cell::sync::Lazy;
use std::sync::Arc;

use crate::basic_auth_callback_handler::BasicAuthCallBackHandler;
use crate::basic_auth_credentials::BasicAuthCredentials;
use crate::error::{FilterError, FilterResult, LoginException};
use crate::jaas::{
    AppConfigurationEntry, CallbackHandler, ControlFlag, JaasConfiguration, LoginModule, Subject,
};
use crate::property_file_login_module::PropertyFileLoginModule;
use crate::request_matcher::RequestMatcher;

/// Authorization header name
const AUTHORIZATION: &str = "Authorization";

/// Connect login module name
///
/// This is hardcoded name used in JAAS configuration
const CONNECT_LOGIN_MODULE: &str = "KafkaConnect";

/// Basic auth scheme
const BASIC_AUTH: &str = "BASIC";

/// Internal request matchers
///
/// These requests bypass authentication as they are made by Connect Worker internally
static INTERNAL_REQUEST_MATCHERS: Lazy<Vec<RequestMatcher>> = Lazy::new(|| {
    let mut matchers = Vec::new();

    // Add POST /connectors/{name}/tasks matcher
    if let Ok(matcher) = RequestMatcher::new("POST", r"/?connectors/([^/]+)/tasks/?") {
        matchers.push(matcher);
    }

    // Add PUT /connectors/{name}/fence matcher
    if let Ok(matcher) = RequestMatcher::new("PUT", r"/?connectors/[^/]+/fence/?") {
        matchers.push(matcher);
    }

    matchers
});

/// JAAS Basic Auth Filter
///
/// Equivalent to Java's JaasBasicAuthFilter class.
/// Implements ContainerRequestFilter to perform JAAS-based Basic Auth authentication.
pub struct JaasBasicAuthFilter {
    /// JAAS configuration
    configuration: Arc<JaasConfiguration>,
}

impl JaasBasicAuthFilter {
    /// Create a new JaasBasicAuthFilter
    ///
    /// # Arguments
    /// * `configuration` - JAAS configuration
    pub fn new(configuration: Arc<JaasConfiguration>) -> Self {
        JaasBasicAuthFilter { configuration }
    }

    /// Check if request is internal
    ///
    /// Internal requests bypass authentication
    ///
    /// # Arguments
    /// * `request_method` - HTTP method (GET, POST, PUT, etc.)
    /// * `request_path` - Request path
    ///
    /// # Returns
    /// * `true` if request is internal, `false` otherwise
    fn is_internal_request(&self, request_method: &str, request_path: &str) -> bool {
        INTERNAL_REQUEST_MATCHERS
            .iter()
            .any(|matcher| matcher.test(request_method, request_path))
    }

    /// Authenticate a request
    ///
    /// # Arguments
    /// * `authorization_header` - Authorization header value
    ///
    /// # Returns
    /// * `Ok(())` if authentication succeeds
    /// * `Err(FilterError::Unauthorized)` if authentication fails
    fn authenticate_request(&self, authorization_header: Option<&str>) -> FilterResult<()> {
        // Parse credentials from header
        let credentials =
            BasicAuthCredentials::from_authorization_header(authorization_header.unwrap_or(""))
                .map_err(|_| FilterError::Unauthorized)?;

        // Create callback handler
        let callback_handler = BasicAuthCallBackHandler::from_credentials(
            credentials.username().cloned(),
            credentials.password().cloned(),
        );

        // Create and initialize login module
        let mut login_module = PropertyFileLoginModule::new();
        let subject = Subject::new();
        let shared_state = std::collections::HashMap::new();

        // Get login module configuration
        let app_config = self
            .configuration
            .get_app_configuration(CONNECT_LOGIN_MODULE)
            .ok_or_else(|| {
                FilterError::InternalError(format!(
                    "Login module '{}' not found in JAAS configuration",
                    CONNECT_LOGIN_MODULE
                ))
            })?;

        // Convert options to HashMap<String, String>
        let mut options = std::collections::HashMap::new();
        for (key, value) in &app_config.options {
            options.insert(key.clone(), value.clone());
        }

        // Initialize login module
        login_module
            .initialize(&subject, &callback_handler, &shared_state, &options)
            .map_err(|e| FilterError::InternalError(e.to_string()))?;

        // Perform login
        let authenticated = login_module
            .login()
            .map_err(|e| FilterError::InternalError(e.to_string()))?;

        if !authenticated {
            return Err(FilterError::Unauthorized);
        }

        // Commit authentication
        if !login_module.commit() {
            return Err(FilterError::InternalError(
                "Failed to commit authentication".to_string(),
            ));
        }

        Ok(())
    }
}

impl Default for JaasBasicAuthFilter {
    fn default() -> Self {
        JaasBasicAuthFilter::new(Arc::new(JaasConfiguration::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_is_internal_request_post_tasks() {
        let filter = JaasBasicAuthFilter::default();
        assert!(filter.is_internal_request("POST", "/connectors/my-connector/tasks"));
        assert!(filter.is_internal_request("POST", "/connectors/my-connector/tasks/"));
        assert!(!filter.is_internal_request("GET", "/connectors/my-connector/tasks"));
        assert!(!filter.is_internal_request("POST", "/connectors/my-connector/other"));
    }

    #[test]
    fn test_is_internal_request_put_fence() {
        let filter = JaasBasicAuthFilter::default();
        assert!(filter.is_internal_request("PUT", "/connectors/my-connector/fence"));
        assert!(filter.is_internal_request("PUT", "/connectors/my-connector/fence/"));
        assert!(!filter.is_internal_request("GET", "/connectors/my-connector/fence"));
        assert!(!filter.is_internal_request("PUT", "/connectors/my-connector/other"));
    }

    #[test]
    fn test_is_internal_request_not_internal() {
        let filter = JaasBasicAuthFilter::default();
        assert!(!filter.is_internal_request("GET", "/connectors"));
        assert!(!filter.is_internal_request("POST", "/connectors"));
        assert!(!filter.is_internal_request("PUT", "/connectors"));
    }

    #[test]
    fn test_authenticate_request_success() {
        // Create a temporary credentials file
        let temp_file = create_temp_credentials_file(true);
        let file_path = temp_file.to_str().unwrap().to_string();

        // Create JAAS configuration
        let mut config = JaasConfiguration::new();
        let mut options = std::collections::HashMap::new();
        options.insert("file".to_string(), file_path);
        let app_config = AppConfigurationEntry::new(
            CONNECT_LOGIN_MODULE.to_string(),
            ControlFlag::Required,
            options,
        );
        config.add_app_configuration(CONNECT_LOGIN_MODULE.to_string(), app_config);

        // Create filter
        let filter = JaasBasicAuthFilter::new(Arc::new(config));

        // Create valid authorization header
        let auth_header = "Basic YWRtaW46c2VjcmV0"; // admin:password

        // Authenticate
        let result = filter.authenticate_request(Some(auth_header));
        assert!(result.is_ok());

        // Cleanup
        std::fs::remove_file(&temp_file).unwrap();
    }

    #[test]
    fn test_authenticate_request_wrong_password() {
        // Create a temporary credentials file
        let temp_file = create_temp_credentials_file(true);
        let file_path = temp_file.to_str().unwrap().to_string();

        let mut config = JaasConfiguration::new();
        let mut options = std::collections::HashMap::new();
        options.insert("file".to_string(), file_path);
        let app_config = AppConfigurationEntry::new(
            CONNECT_LOGIN_MODULE.to_string(),
            ControlFlag::Required,
            options,
        );
        config.add_app_configuration(CONNECT_LOGIN_MODULE.to_string(), app_config);

        let filter = JaasBasicAuthFilter::new(Arc::new(config));

        let auth_header = "Basic YWRtaW46c2VjcmV1"; // admin:wrongpassword

        let result = filter.authenticate_request(Some(auth_header));
        assert!(result.is_err());
        assert!(matches!(result, Err(FilterError::Unauthorized)));

        std::fs::remove_file(&temp_file).unwrap();
    }

    #[test]
    fn test_authenticate_request_no_header() {
        let filter = JaasBasicAuthFilter::default();
        let result = filter.authenticate_request(None);
        assert!(result.is_err());
        assert!(matches!(result, Err(FilterError::Unauthorized)));
    }

    #[test]
    fn test_authenticate_request_invalid_header() {
        let filter = JaasBasicAuthFilter::default();
        let result = filter.authenticate_request(Some("Invalid"));
        assert!(result.is_err());
        assert!(matches!(result, Err(FilterError::Unauthorized)));
    }

    // Helper function to create a temporary credentials file
    fn create_temp_credentials_file(include_users: bool) -> std::path::PathBuf {
        use std::io::Write;

        let mut temp_file = std::env::temp_dir();
        temp_file.push(format!("credentials_{}.properties", std::process::id()));

        if include_users {
            let mut file = std::fs::File::create(&temp_file).unwrap();
            writeln!(file, "admin=password").unwrap();
            writeln!(file, "user1=password1").unwrap();
        } else {
            std::fs::File::create(&temp_file).unwrap();
        }

        temp_file
    }
}
