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

//! Basic authentication extension for Kafka Connect REST API

use std::any::Any;
use std::collections::HashMap;

/// Basic auth configuration
///
/// Stores the username and password for basic authentication.
#[derive(Debug, Clone)]
pub struct BasicAuthConfig {
    username: Option<String>,
    password: Option<String>,
}

impl BasicAuthConfig {
    /// Create a new BasicAuthConfig
    pub fn new() -> Self {
        BasicAuthConfig {
            username: None,
            password: None,
        }
    }

    /// Create a new BasicAuthConfig with username and password
    pub fn with_credentials(username: String, password: String) -> Self {
        BasicAuthConfig {
            username: Some(username),
            password: Some(password),
        }
    }

    /// Set the username
    pub fn set_username(&mut self, username: String) {
        self.username = Some(username);
    }

    /// Set the password
    pub fn set_password(&mut self, password: String) {
        self.password = Some(password);
    }

    /// Get the username for authentication
    pub fn username(&self) -> Option<&String> {
        self.username.as_ref()
    }

    /// Get the password for authentication
    pub fn password(&self) -> Option<&String> {
        self.password.as_ref()
    }

    /// Check if authentication is enabled
    pub fn is_enabled(&self) -> bool {
        self.username.is_some() && self.password.is_some()
    }
}

impl Default for BasicAuthConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Basic auth credentials
///
/// Stores and validates username and password credentials.
#[derive(Debug, Clone)]
pub struct BasicAuthCredentials {
    username: String,
    password: String,
}

impl BasicAuthCredentials {
    /// Create new BasicAuthCredentials
    pub fn new(username: String, password: String) -> Self {
        BasicAuthCredentials { username, password }
    }

    /// Parse credentials from Basic Auth header
    ///
    /// The header format is "Basic base64(username:password)"
    pub fn from_authorization_header(header: &str) -> Result<Self, String> {
        if !header.starts_with("Basic ") {
            return Err("Invalid authorization header format".to_string());
        }

        let encoded = &header[6..]; // Skip "Basic "
        let decoded =
            base64::decode(encoded).map_err(|e| format!("Failed to decode base64: {}", e))?;

        let credentials_str =
            String::from_utf8(decoded).map_err(|e| format!("Failed to decode UTF-8: {}", e))?;

        let parts: Vec<&str> = credentials_str.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err("Invalid credentials format".to_string());
        }

        Ok(BasicAuthCredentials {
            username: parts[0].to_string(),
            password: parts[1].to_string(),
        })
    }

    /// Validate the provided credentials
    pub fn validate(&self, username: &str, password: &str) -> bool {
        self.username == username && self.password == password
    }

    /// Get the stored username
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Get the stored password
    pub fn password(&self) -> &str {
        &self.password
    }
}

/// Basic auth extension
///
/// Provides basic authentication functionality for Connect REST API.
#[derive(Debug)]
pub struct BasicAuthExtension {
    config: BasicAuthConfig,
    credentials: Option<BasicAuthCredentials>,
    version: String,
}

impl BasicAuthExtension {
    /// Create a new BasicAuthExtension
    pub fn new() -> Self {
        BasicAuthExtension {
            config: BasicAuthConfig::new(),
            credentials: None,
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    /// Create a new BasicAuthExtension with credentials
    pub fn with_credentials(username: String, password: String) -> Self {
        let credentials = BasicAuthCredentials::new(username, password);
        BasicAuthExtension {
            config: BasicAuthConfig::with_credentials(
                credentials.username().to_string(),
                credentials.password().to_string(),
            ),
            credentials: Some(credentials),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    /// Get the basic auth config
    pub fn config(&self) -> &BasicAuthConfig {
        &self.config
    }

    /// Get the credentials
    pub fn credentials(&self) -> Option<&BasicAuthCredentials> {
        self.credentials.as_ref()
    }

    /// Configure the basic auth extension
    ///
    /// @param configs Configuration properties for the extension
    pub fn configure(&mut self, configs: HashMap<String, Box<dyn Any>>) {
        if let Some(username) = configs.get("basic.auth.username") {
            if let Some(username_str) = username.downcast_ref::<String>() {
                self.config.set_username(username_str.clone());
            }
        }

        if let Some(password) = configs.get("basic.auth.password") {
            if let Some(password_str) = password.downcast_ref::<String>() {
                self.config.set_password(password_str.clone());
            }
        }

        // Update credentials if both username and password are set
        if let (Some(username), Some(password)) = (self.config.username(), self.config.password()) {
            self.credentials = Some(BasicAuthCredentials::new(
                username.clone(),
                password.clone(),
            ));
        }
    }

    /// Get the version of this extension
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Close the extension and release any resources
    pub fn close(&mut self) {
        self.credentials = None;
    }
}

impl Default for BasicAuthExtension {
    fn default() -> Self {
        Self::new()
    }
}
