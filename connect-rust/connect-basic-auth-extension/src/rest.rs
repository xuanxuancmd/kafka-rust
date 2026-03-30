// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! REST API extension for basic authentication

use std::any::Any;
use std::collections::HashMap;

/// Connect REST extension context
///
/// Provides context for REST extensions to interact with Connect runtime.
#[derive(Debug, Clone)]
pub struct ConnectRestExtensionContext {
    config: HashMap<String, String>,
    worker_id: String,
}

impl ConnectRestExtensionContext {
    /// Create a new ConnectRestExtensionContext
    pub fn new(config: HashMap<String, String>, worker_id: String) -> Self {
        ConnectRestExtensionContext { config, worker_id }
    }

    /// Get configuration
    pub fn config(&self) -> &HashMap<String, String> {
        &self.config
    }

    /// Get worker ID
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }
}

/// Connect REST extension trait
///
/// Allows connectors to extend the Connect REST API with custom endpoints.
pub trait ConnectRestExtension {
    /// Register this extension with the given context
    ///
    /// @param context ConnectRestExtensionContext that can be used to interact with Connect runtime
    fn register(&mut self, context: ConnectRestExtensionContext);

    /// Close this extension and release any resources
    fn close(&mut self);
}

/// Basic auth REST extension
///
/// Implements ConnectRestExtension with basic authentication specific functionality.
#[derive(Debug)]
pub struct BasicAuthRestExtension {
    config: Option<ConnectRestExtensionContext>,
    credentials: Option<super::auth::BasicAuthCredentials>,
    version: String,
}

impl BasicAuthRestExtension {
    /// Create a new BasicAuthRestExtension
    pub fn new() -> Self {
        BasicAuthRestExtension {
            config: None,
            credentials: None,
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    /// Create a new BasicAuthRestExtension with credentials
    pub fn with_credentials(username: String, password: String) -> Self {
        BasicAuthRestExtension {
            config: None,
            credentials: Some(super::auth::BasicAuthCredentials::new(username, password)),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    /// Configure the basic auth extension
    ///
    /// @param configs Configuration properties for the extension
    pub fn configure(&mut self, configs: HashMap<String, Box<dyn Any>>) {
        if let Some(username) = configs.get("basic.auth.username") {
            if let Some(username_str) = username.downcast_ref::<String>() {
                if let Some(password) = configs.get("basic.auth.password") {
                    if let Some(password_str) = password.downcast_ref::<String>() {
                        self.credentials = Some(super::auth::BasicAuthCredentials::new(
                            username_str.clone(),
                            password_str.clone(),
                        ));
                    }
                }
            }
        }
    }

    /// Get the version of this extension
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Get the credentials
    pub fn credentials(&self) -> Option<&super::auth::BasicAuthCredentials> {
        self.credentials.as_ref()
    }
}

impl Default for BasicAuthRestExtension {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectRestExtension for BasicAuthRestExtension {
    /// Register this extension with the given context
    ///
    /// @param context ConnectRestExtensionContext that can be used to interact with Connect runtime
    fn register(&mut self, context: ConnectRestExtensionContext) {
        self.config = Some(context);
        // In a real implementation, this would register a filter with the REST server
    }

    /// Close this extension and release any resources
    fn close(&mut self) {
        self.credentials = None;
        self.config = None;
    }
}

/// Basic auth filter
///
/// Filters HTTP requests and performs Basic Auth authentication.
#[derive(Debug)]
pub struct BasicAuthFilter {
    credentials: Option<super::auth::BasicAuthCredentials>,
    internal_paths: Vec<String>,
}

impl BasicAuthFilter {
    /// Create a new BasicAuthFilter
    pub fn new() -> Self {
        BasicAuthFilter {
            credentials: None,
            internal_paths: vec!["/connector-plugins".to_string(), "/".to_string()],
        }
    }

    /// Create a new BasicAuthFilter with credentials
    pub fn with_credentials(username: String, password: String) -> Self {
        BasicAuthFilter {
            credentials: Some(super::auth::BasicAuthCredentials::new(username, password)),
            internal_paths: vec!["/connector-plugins".to_string(), "/".to_string()],
        }
    }

    /// Set credentials for authentication
    pub fn set_credentials(&mut self, username: String, password: String) {
        self.credentials = Some(super::auth::BasicAuthCredentials::new(username, password));
    }

    /// Check if the given path is an internal path that bypasses authentication
    pub fn is_internal_path(&self, path: &str) -> bool {
        self.internal_paths
            .iter()
            .any(|internal| path.starts_with(internal))
    }

    /// Authenticate a request using Basic Auth header
    ///
    /// @param authorization_header The Authorization header value
    /// @return true if authentication succeeds, false otherwise
    pub fn authenticate(&self, authorization_header: Option<&str>) -> bool {
        // Internal paths bypass authentication
        if let Some(header) = authorization_header {
            if header.is_empty() {
                return false;
            }

            // Parse credentials from header
            match super::auth::BasicAuthCredentials::from_authorization_header(header) {
                Ok(provided_credentials) => {
                    // Validate against stored credentials
                    if let Some(stored_credentials) = &self.credentials {
                        stored_credentials.validate(
                            provided_credentials.username(),
                            provided_credentials.password(),
                        )
                    } else {
                        false
                    }
                }
                Err(_) => false,
            }
        } else {
            false
        }
    }

    /// Get the credentials
    pub fn credentials(&self) -> Option<&super::auth::BasicAuthCredentials> {
        self.credentials.as_ref()
    }
}

impl Default for BasicAuthFilter {
    fn default() -> Self {
        Self::new()
    }
}
