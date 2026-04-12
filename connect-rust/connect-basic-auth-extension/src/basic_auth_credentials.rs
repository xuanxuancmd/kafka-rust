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

//! Basic auth credentials
//!
//! This module provides Basic Auth credentials parsing and validation.

use crate::error::{AuthResult, LoginException};
use base64::{engine::general_purpose::STANDARD, Engine as _};

/// Basic auth credentials
///
/// Parses and validates Basic Auth credentials from Authorization header.
pub struct BasicAuthCredentials {
    username: Option<String>,
    password: Option<String>,
}

impl BasicAuthCredentials {
    /// Create new BasicAuthCredentials from authorization header
    ///
    /// The header format is "Basic base64(username:password)"
    ///
    /// # Arguments
    /// * `authorization_header` - The Authorization header value
    ///
    /// # Returns
    /// * Parsed credentials
    ///
    /// # Errors
    /// * `LoginException::Base64Error` - Invalid Base64 encoding
    /// * `LoginException::Utf8Error` - Invalid UTF-8 encoding
    pub fn from_authorization_header(authorization_header: &str) -> AuthResult<Self> {
        // If header is null, return empty credentials
        if authorization_header.is_empty() {
            return Ok(BasicAuthCredentials {
                username: None,
                password: None,
            });
        }

        // Check for "Basic " prefix
        if !authorization_header.starts_with("Basic ") {
            return Ok(BasicAuthCredentials {
                username: None,
                password: None,
            });
        }

        // Decode Base64
        let encoded = &authorization_header[6..]; // Skip "Basic "
        let decoded = STANDARD
            .decode(encoded)
            .map_err(|e| LoginException::Base64Error(e.to_string()))?;

        // Convert to UTF-8
        let decoded_str =
            String::from_utf8(decoded).map_err(|e| LoginException::Utf8Error(e.to_string()))?;

        // Split by colon
        let colon_index = match decoded_str.find(':') {
            Some(index) => index,
            None => {
                return Ok(BasicAuthCredentials {
                    username: None,
                    password: None,
                });
            }
        };

        if colon_index == 0 {
            return Ok(BasicAuthCredentials {
                username: None,
                password: None,
            });
        }

        let username = decoded_str[..colon_index].to_string();
        let password = decoded_str[colon_index + 1..].to_string();

        Ok(BasicAuthCredentials {
            username: Some(username),
            password: Some(password),
        })
    }

    /// Get username
    pub fn username(&self) -> Option<&String> {
        self.username.as_ref()
    }

    /// Get password
    pub fn password(&self) -> Option<&String> {
        self.password.as_ref()
    }
}

impl Default for BasicAuthCredentials {
    fn default() -> Self {
        BasicAuthCredentials {
            username: None,
            password: None,
        }
    }
}
