// Licensed Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the language governing permissions and
// limitations under the License.

//! JAAS (Java Authentication and Authorization Service) equivalent framework

use once_cell::sync::Lazy;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use crate::error::{AuthResult, LoginException};

/// JAAS LoginModule trait
///
/// Equivalent to javax.security.auth.spi.LoginModule
pub trait LoginModule: Send + Sync {
    /// Initialize the login module
    ///
    /// # Arguments
    /// * `subject` - Subject to be authenticated
    /// * `callback_handler` - Callback handler for user interaction
    /// * `shared_state` - Shared state between login modules
    /// * `options` - Configuration options for this module
    fn initialize(
        &mut self,
        subject: &Subject,
        callback_handler: &dyn CallbackHandler,
        shared_state: &HashMap<String, Box<dyn Any>>,
        options: &HashMap<String, String>,
    ) -> AuthResult<()>;

    /// Perform the authentication
    ///
    /// # Returns
    /// * `true` if authentication succeeded
    /// * `false` if authentication failed
    fn login(&mut self) -> AuthResult<bool>;

    /// Commit the authentication
    ///
    /// # Returns
    /// * `true` if commit succeeded
    fn commit(&self) -> bool;

    /// Abort the authentication
    ///
    /// # Returns
    /// * `true` if abort succeeded
    fn abort(&self) -> bool;

    /// Logout the subject
    ///
    /// # Returns
    /// * `true` if logout succeeded
    fn logout(&self) -> bool;
}

/// JAAS CallbackHandler trait
///
/// Equivalent to javax.security.auth.callback.CallbackHandler
pub trait CallbackHandler: Send + Sync {
    /// Handle the callbacks
    ///
    /// # Arguments
    /// * `callbacks` - Array of callbacks to handle
    fn handle(&self, callbacks: &mut [Box<dyn Callback>]) -> AuthResult<()>;
}

/// JAAS Callback trait
///
/// Base trait for all callback types
pub trait Callback: Send + Sync {
    /// Get as NameCallback
    fn as_name_callback(&mut self) -> Option<&mut NameCallback>;

    /// Get as PasswordCallback
    fn as_password_callback(&mut self) -> Option<&mut PasswordCallback>;
}

/// NameCallback
///
/// Equivalent to javax.security.auth.callback.NameCallback
pub struct NameCallback {
    name: Option<String>,
}

impl NameCallback {
    /// Create a new NameCallback
    pub fn new(prompt: &str) -> Self {
        NameCallback { name: None }
    }

    /// Set the name
    pub fn set_name(&mut self, name: String) {
        self.name = Some(name);
    }

    /// Get the name
    pub fn get_name(&self) -> Option<&String> {
        self.name.as_ref()
    }

    /// Get the prompt
    pub fn get_prompt(&self) -> &str {
        "Enter user name"
    }
}

impl Callback for NameCallback {
    fn as_name_callback(&mut self) -> Option<&mut NameCallback> {
        Some(self)
    }

    fn as_password_callback(&mut self) -> Option<&mut PasswordCallback> {
        None
    }
}

/// PasswordCallback
///
/// Equivalent to javax.security.auth.callback.PasswordCallback
pub struct PasswordCallback {
    password: Option<String>,
    echo_on: bool,
}

impl PasswordCallback {
    /// Create a new PasswordCallback
    pub fn new(prompt: &str, echo_on: bool) -> Self {
        PasswordCallback {
            password: None,
            echo_on,
        }
    }

    /// Set the password
    pub fn set_password(&mut self, password: String) {
        self.password = Some(password);
    }

    /// Get the password
    pub fn get_password(&self) -> Option<&String> {
        self.password.as_ref()
    }

    /// Get the prompt
    pub fn get_prompt(&self) -> &str {
        "Enter password"
    }

    /// Check if echo is enabled
    pub fn is_echo_on(&self) -> bool {
        self.echo_on
    }
}

impl Callback for PasswordCallback {
    fn as_name_callback(&mut self) -> Option<&mut NameCallback> {
        None
    }

    fn as_password_callback(&mut self) -> Option<&mut PasswordCallback> {
        Some(self)
    }
}

/// Subject
///
/// Equivalent to javax.security.auth.Subject
pub struct Subject {
    principals: Vec<String>,
}

impl Subject {
    /// Create a new Subject
    pub fn new() -> Self {
        Subject {
            principals: Vec::new(),
        }
    }

    /// Add a principal
    pub fn add_principal(&mut self, principal: String) {
        self.principals.push(principal);
    }

    /// Get all principals
    pub fn get_principals(&self) -> &[String] {
        &self.principals
    }
}

/// JAAS Configuration entry
///
/// Represents a single application configuration entry in JAAS config
#[derive(Debug, Clone)]
pub struct AppConfigurationEntry {
    /// Login module name
    pub login_module_name: String,
    /// Control flag (required, sufficient, etc.)
    pub control_flag: ControlFlag,
    /// Module options
    pub options: HashMap<String, String>,
}

impl AppConfigurationEntry {
    /// Create a new AppConfigurationEntry
    pub fn new(
        login_module_name: String,
        control_flag: ControlFlag,
        options: HashMap<String, String>,
    ) -> Self {
        AppConfigurationEntry {
            login_module_name,
            control_flag,
            options,
        }
    }
}

/// Control flag
///
/// Represents the login module control flag
#[derive(Debug, Clone, PartialEq)]
pub enum ControlFlag {
    /// Required - authentication must succeed
    Required,
    /// Sufficient - authentication is sufficient
    Sufficient,
    /// Optional - authentication is optional
    Optional,
    /// Requisite - authentication is required
    Requisite,
}

/// JAAS Configuration
///
/// Equivalent to javax.security.auth.login.Configuration
#[derive(Debug)]
pub struct JaasConfiguration {
    /// Application configurations
    app_configurations: Arc<RwLock<HashMap<String, AppConfigurationEntry>>>,
}

impl JaasConfiguration {
    /// Create a new JaasConfiguration
    pub fn new() -> Self {
        JaasConfiguration {
            app_configurations: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add an application configuration entry
    pub fn add_app_configuration(&self, app_name: String, entry: AppConfigurationEntry) {
        let mut configs = self.app_configurations.write().unwrap();
        configs.insert(app_name, entry);
    }

    /// Get an application configuration entry
    pub fn get_app_configuration(&self, app_name: &str) -> Option<AppConfigurationEntry> {
        let configs = self.app_configurations.read().unwrap();
        configs.get(app_name).cloned()
    }

    /// Get the global JAAS configuration
    ///
    /// This is equivalent to Configuration.getConfiguration()
    pub fn get_configuration() -> Result<Self, LoginException> {
        // In a real implementation, this would load from JVM system properties
        // or from a configuration file specified by java.security.auth.login.config
        Ok(Self::new())
    }
}

impl Default for JaasConfiguration {
    fn default() -> Self {
        Self::new()
    }
}
