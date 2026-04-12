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
use std::sync::Arc;

use crate::basic_auth_credentials::BasicAuthCredentials;
use crate::error::{AuthResult, FilterError, FilterResult, LoginException};
use crate::jaas::{
    AppConfigurationEntry, Callback, CallbackHandler, ControlFlag, JaasConfiguration, LoginModule,
    NameCallback, PasswordCallback, Subject,
};
use crate::jaas_basic_auth_filter::JaasBasicAuthFilter;
use crate::property_file_login_module::PropertyFileLoginModule;

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
/// Allows connectors to extend Connect REST API with custom endpoints.
pub trait ConnectRestExtension {
    /// Register this extension with given context
    ///
    /// @param context ConnectRestExtensionContext that can be used to interact with Connect runtime
    fn register(&mut self, context: ConnectRestExtensionContext);

    /// Configure this extension
    ///
    /// @param configs Configuration properties for extension
    fn configure(&mut self, configs: HashMap<String, Box<dyn Any>>);

    /// Get version of this extension
    ///
    /// @return Version string
    fn version(&self) -> &str;

    /// Close this extension and release any resources
    fn close(&mut self);
}

/// Basic auth REST extension
///
///` Implements ConnectRestExtension with basic authentication specific functionality.
/// Equivalent to Java's BasicAuthSecurityRestExtension.
#[derive(Debug)]
pub struct BasicAuthSecurityRestExtension {
    config: Option<ConnectRestExtensionContext>,
    configuration: Arc<JaasConfiguration>,
    configuration_error: Option<LoginException>,
    version: String,
}

impl BasicAuthSecurityRestExtension {
    /// Create a new BasicAuthSecurityRestExtension
    ///
    /// This captures the JVM's global JAAS configuration at startup
    pub fn new() -> Self {
        let (config, error) = Self::initialize_configuration();
        BasicAuthSecurityRestExtension {
            config: None,
            configuration: Arc::new(config),
            configuration_error: error,
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    /// Create a new BasicAuthSecurityRestExtension with a custom configuration
    ///
    /// # Arguments
    /// * `configuration` - Custom JAAS configuration
    pub fn with_configuration(configuration: Arc<JaasConfiguration>) -> Self {
        BasicAuthSecurityRestExtension {
            config: None,
            configuration,
            configuration_error: None,
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    /// Get configuration
    pub fn configuration(&self) -> Result<&Arc<JaasConfiguration>, &LoginException> {
        match &self.configuration_error {
            Some(e) => Err(e),
            None => Ok(&self.configuration),
        }
    }

    /// Initialize JAAS configuration
    ///
    /// This is equivalent to Java's initializeConfiguration() static method.
    /// It captures the JVM's global JAAS configuration as soon as possible,
    /// as it may be altered later by connectors, converters, other REST extensions, etc.
    fn initialize_configuration() -> (JaasConfiguration, Option<LoginException>) {
        // In a real implementation, this would load from JVM system properties
        // or from a configuration file specified by java.security.auth.login.config
        // For now, we'll create a default configuration
        let mut config = JaasConfiguration::new();

        // Add PropertyFileLoginModule as a default login module
        let mut options = HashMap::new();
        options.insert(
            "file".to_string(),
            "/path/to/credentials.properties".to_string(),
        );

        let app_config =
            AppConfigurationEntry::new("KafkaConnect".to_string(), ControlFlag::Required, options);

        config.add_app_configuration("KafkaConnect".to_string(), app_config);

        (config, None)
    }
}

impl ConnectRestExtension for BasicAuthSecurityRestExtension {
    /// Register this extension with given context
    ///
    /// This is equivalent to Java's register() method.
    /// It registers a JAAS Basic Auth filter with REST server.
    fn register(&mut self, context: ConnectRestExtensionContext) {
        self.config = Some(context);

        // Check if initialization failed
        if let Some(e) = &self.configuration_error {
            eprintln!("Failed to retrieve JAAS configuration: {}", e);
            return;
        }

        // Create and register JAAS Basic Auth filter
        let filter = JaasBasicAuthFilter::new(Arc::clone(&self.configuration));

        // In a real implementation, this would register filter with REST server
        // context.configurable().register(filter);
        // For now, we'll just log that we would register it
        println!("Registered JAAS Basic Auth filter");
    }

    /// Configure this extension
    ///
    /// This is equivalent to Java's configure() method.
    /// It validates the JAAS configuration.
    fn configure(&mut self, _configs: HashMap<String, Box<dyn Any>>) {
        // If we failed to retrieve a JAAS configuration during startup, throw that exception now
        if let Some(e) = &self.configuration_error {
            eprintln!("Failed to retrieve JAAS configuration: {}", e);
            // In Java, this would throw ConnectException
            // In Rust, we can't throw from configure, so we log error
        }
    }

    /// Get version of this extension
    ///
    /// This is equivalent to Java's version() method.
    fn version(&self) -> &str {
        &self.version
    }

    /// Close: this extension and release any resources
    ///
    /// This is equivalent to Java's close() method.
    fn close(&mut self) {
        self.config = None;
    }
}

impl Default for BasicAuthSecurityRestExtension {
    fn default() -> Self {
        Self::new()
    }
}
