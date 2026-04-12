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

/// Basic auth stored credentials
///
/// Stores and validates username and password credentials for comparison.
#[derive(Debug, Clone)]
pub struct BasicAuthStoredCredentials {
    username: String,
    password: String,
}

impl BasicAuthStoredCredentials {
    /// Create new BasicAuthStoredCredentials
    pub fn new(username: String, password: String) -> Self {
        BasicAuthStoredCredentials { username, password }
    }

    /// Validate provided credentials
    pub fn validate(&self, username: &str, password: &str) -> bool {
        self.username == username && self.password == password
    }

    /// Get stored username
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Get stored password
    pub fn password(&self) -> &str {
        &self.password
    }
}
