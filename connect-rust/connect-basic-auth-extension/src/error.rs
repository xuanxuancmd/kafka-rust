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
// See the License for the language governing permissions and
// limitations under the License.

//! Error types for basic auth extension

use thiserror::Error;

/// Login exception
///
/// Thrown when authentication fails.
#[derive(Error, Debug)]
pub enum LoginException {
    /// Login failed with message
    #[error("Login failed: {0}")]
    Failed(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigurationError(String),

    /// Unsupported callback type
    #[error("Unsupported callback: {0}")]
    UnsupportedCallback(String),

    /// IO error
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// Base64 decoding error
    #[error("Base64 decoding error: {0}")]
    Base64Error(String),

    /// UTF-8 decoding error
    #[error("UTF-8 decoding error: {0}")]
    Utf8Error(String),

    /// Invalid authorization header format
    #[error("Invalid authorization header format: {0}")]
    InvalidAuthorizationHeader(String),
}

/// Filter error
///
/// Thrown when request filtering fails.
#[derive(Error, Debug)]
pub enum FilterError {
    /// Unauthorized access
    #[error("Unauthorized")]
    Unauthorized,

    /// Internal filter error
    #[error("Filter error: {0}")]
    InternalError(String),
}

/// Authentication error
///
/// General authentication error type.
pub type AuthResult<T> = Result<T, LoginException>;

/// Filter result
///
/// Result type for filter operations.
pub type FilterResult<T> = Result<T, FilterError>;
