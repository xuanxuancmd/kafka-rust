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

//! Basic auth callback handler
//!
//! This module provides a JAAS callback handler for Basic Auth authentication.

use crate::error::{AuthResult, LoginException};
use crate::jaas::{Callback, CallbackHandler, NameCallback, PasswordCallback};

/// Basic auth callback handler
///
/// Equivalent to Java's BasicAuthCallBackHandler
pub struct BasicAuthCallBackHandler {
    username: Option<String>,
    password: Option<String>,
}

impl BasicAuthCallBackHandler {
    /// Create a new BasicAuthCallBackHandler
    pub fn new(username: Option<String>, password: Option<String>) -> Self {
        BasicAuthCallBackHandler { username, password }
    }

    /// Create from BasicAuthCredentials
    pub fn from_credentials(username: Option<String>, password: Option<String>) -> Self {
        BasicAuthCallBackHandler { username, password }
    }
}

impl CallbackHandler for BasicAuthCallBackHandler {
    /// Handle callbacks
    ///
    /// This is equivalent to Java's handle() method
    fn handle(&self, callbacks: &mut [Box<dyn Callback>]) -> AuthResult<()> {
        let mut unsupported_callbacks = Vec::new();

        for callback in callbacks.iter_mut() {
            if let Some(name_callback) = callback.as_name_callback() {
                if let Some(username) = &self.username {
                    name_callback.set_name(username.clone());
                }
            } else if let Some(password_callback) = callback.as_password_callback() {
                if let Some(password) = &self.password {
                    password_callback.set_password(password.clone());
                }
            } else {
                unsupported_callbacks.push(callback);
            }
        }

        if !unsupported_callbacks.is_empty() {
            return Err(LoginException::UnsupportedCallback(format!(
                "Unsupported callbacks; request authentication will fail. \
                 This indicates that Connect worker was configured with a JAAS \
                 LoginModule that is incompatible with BasicAuthSecurityRestExtension, \
                 and will need to be corrected and restarted."
            )));
        }

        Ok(())
    }
}

impl Default for BasicAuthCallBackHandler {
    fn default() -> Self {
        BasicAuthCallBackHandler {
            username: None,
            password: None,
        }
    }
}
