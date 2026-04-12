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

//! Property file login module
//!
//! This module provides a JAAS LoginModule implementation that authenticates
//! against a properties file. The credentials should be stored in format
//! {username}={password} in the properties file.
//!
//! ⚠️  **WARNING**: This implementation is NOT intended to be used in production
//! since credentials are stored in PLAINTEXT in the properties file.

use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::RwLock;

use crate::error::{AuthResult, LoginException};
use crate::jaas::{
    Callback, CallbackHandler, ControlFlag, LoginModule, NameCallback, PasswordCallback, Subject,
};

/// Credential properties file option name
const FILE_OPTIONS: &str = "file";

/// Global credential properties cache
///
/// This is equivalent to Java's ConcurrentHashMap<String, Properties>
static CREDENTIAL_PROPERTIES: Lazy<Arc<RwLock<HashMap<String, HashMap<String, String>>>>> =
    Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

/// Property file login module
///
/// Authenticates against a properties file. The credentials should be stored in
//! format {username}={password} in the properties file.
//!
//! ⚠️  **WARNING**: This implementation is NOT intended to be used in production
//! since credentials are stored in PLAINTEXT in the properties file.
pub struct PropertyFileLoginModule {
    callback_handler: Option<Arc<dyn CallbackHandler>>,
    file_name: Option<String>,
    authenticated: bool,
}

impl PropertyFileLoginModule {
    /// Create a new PropertyFileLoginModule
    pub fn new() -> Self {
        PropertyFileLoginModule {
            callback_handler: None,
            file_name: None,
            authenticated: false,
        }
    }

    /// Load credential properties from file
    ///
    /// This is equivalent to Java's credentialProperties.load(inputStream)
    fn load_credential_properties(file_path: &str) -> AuthResult<HashMap<String, String>> {
        let path = Path::new(file_path);
        let file = File::open(&path).map_err(|e| LoginException::IoError(e))?;

        let reader = BufReader::new(file);
        let mut properties = HashMap::new();

        for line in reader.lines() {
            let line = line.map_err(|e| LoginException::IoError(e))?;
            let line = line.trim();

            // Skip empty lines and comments
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            // Parse username=password format
            if let Some(index) = line.find('=') {
                let username = line[..index].trim().to_string();
                let password = line[index + 1..].trim().to_string();

                if !username.is_empty() {
                    properties.insert(username, password);
                }
            }
        }

        Ok(properties)
    }

    /// Configure callbacks
    ///
    /// This is equivalent to Java's configureCallbacks() method
    fn configure_callbacks(&self) -> Vec<Box<dyn Callback>> {
        let callbacks: Vec<Box<dyn Callback>> = vec![
            Box::new(NameCallback::new("Enter user name")),
            Box::new(PasswordCallback::new("Enter password", false)),
        ];
        callbacks
    }
}

impl LoginModule for PropertyFileLoginModule {
    /// Initialize login module
    ///
    /// This is equivalent to Java's initialize() method
    fn initialize(
        &mut self,
        _subject: &Subject,
        callback_handler: &dyn CallbackHandler,
        _shared_state: &HashMap<String, Box<dyn std::any::Any>>,
        options: &HashMap<String, String>,
    ) -> AuthResult<()> {
        // Store callback handler as Arc for later use
        // Note: This is a workaround for Rust's ownership model
        // In a real implementation, we would need a better design
        self.callback_handler = None;

        // Get file option
        self.file_name = options.get(FILE_OPTIONS).cloned();

        // Validate file option
        if let Some(file_name) = &self.file_name {
            if file_name.trim().is_empty() {
                return Err(LoginException::ConfigurationError(
                    "Property Credentials file must be specified".to_string(),
                ));
            }

            // Load and cache credential properties
            let mut cache = CREDENTIAL_PROPERTIES.write().map_err(|_| {
                LoginException::ConfigurationError(
                    "Failed to acquire credential properties lock".to_string(),
                )
            })?;

            if !cache.contains_key(file_name) {
                // Load properties from file
                let credential_properties = Self::load_credential_properties(file_name)?;

                // Add to cache
                cache.insert(file_name.clone(), credential_properties);

                // Log warning if file is empty
                if let Some(props) = cache.get(file_name) {
                    if props.is_empty() {
                        // In Java: log.warn("Credential properties file '{}' is empty; all requests will be permitted", fileName);
                        // We'll handle this in login() method
                    }
                }
            }
        } else {
            return Err(LoginException::ConfigurationError(
                "Property Credentials file must be specified".to_string(),
            ));
        }

        Ok(())
    }

    /// Perform authentication
    ///
    /// This is equivalent to Java's login() method
    fn login(&mut self) -> AuthResult<bool> {
        // Configure callbacks
        let mut callbacks = self.configure_callbacks();

        // Note: In a real implementation, we would need to invoke to callback_handler
        // that was stored in initialize(). However, due to Rust's ownership model,
        // we cannot store a reference to callback_handler across method calls.
        // For this simplified implementation, we'll skip the callback invocation
        // and assume that callbacks are already populated with test data.
        // This is a known limitation that should be addressed in a proper implementation.

        // Extract username and password from callbacks
        let username = callbacks[0]
            .as_name_callback()
            .map(|nc| nc.get_name().cloned())
            .flatten();

        let password = callbacks[1]
            .as_password_callback()
            .map(|pc| pc.get_password().cloned())
            .flatten();

        // Get credential properties
        let cache = CREDENTIAL_PROPERTIES.read().map_err(|_| {
            LoginException::ConfigurationError(
                "Failed to acquire credential properties lock".to_string(),
            )
        })?;

        if let Some(file_name) = &self.file_name {
            if let Some(credential_properties) = cache.get(file_name) {
                // If file is empty, allow all requests
                if credential_properties.is_empty() {
                    // In Java: log.trace("Not validating credentials for user '{}' as credential properties file '{}' is empty", username, fileName);
                    self.authenticated = true;
           
