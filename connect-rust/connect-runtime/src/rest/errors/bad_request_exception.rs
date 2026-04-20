// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
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

//! BadRequestException - exception for 400 Bad Request errors.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.rest.errors.BadRequestException` in Java.

use std::error::Error;

use super::{ConnectRestException, HttpStatus};

/// BadRequestException - exception for 400 Bad Request errors.
///
/// This exception is thrown when a REST request is malformed or invalid.
/// It always has HTTP status 400 (Bad Request).
///
/// Corresponds to `org.apache.kafka.connect.runtime.rest.errors.BadRequestException` in Java.
#[derive(Debug)]
pub struct BadRequestException {
    /// The inner ConnectRestException.
    inner: ConnectRestException,
}

impl BadRequestException {
    /// Creates a new BadRequestException with a message.
    ///
    /// # Arguments
    ///
    /// * `message` - Error message describing the bad request
    pub fn new(message: impl Into<String>) -> Self {
        BadRequestException {
            inner: ConnectRestException::from_status(HttpStatus::BadRequest, message),
        }
    }

    /// Creates a new BadRequestException with a message and a cause.
    ///
    /// # Arguments
    ///
    /// * `message` - Error message describing the bad request
    /// * `cause` - The underlying cause of the error
    pub fn with_cause(message: impl Into<String>, cause: Box<dyn Error + Send + Sync>) -> Self {
        BadRequestException {
            inner: ConnectRestException::from_status_with_cause(
                HttpStatus::BadRequest,
                message,
                cause,
            ),
        }
    }

    /// Returns the HTTP status code (always 400).
    pub fn status_code(&self) -> u16 {
        self.inner.status_code()
    }

    /// Returns the error code.
    pub fn error_code(&self) -> i32 {
        self.inner.error_code()
    }

    /// Returns the error message.
    pub fn message(&self) -> &str {
        self.inner.message()
    }

    /// Returns the cause, if any.
    pub fn cause(&self) -> Option<&(dyn Error + Send + Sync)> {
        self.inner.cause()
    }
}

impl std::fmt::Display for BadRequestException {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BadRequestException: {}", self.inner.message())
    }
}

impl Error for BadRequestException {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.inner.source()
    }
}

impl From<BadRequestException> for ConnectRestException {
    fn from(e: BadRequestException) -> Self {
        e.inner
    }
}

impl From<BadRequestException> for connect_api::errors::ConnectException {
    fn from(e: BadRequestException) -> Self {
        connect_api::errors::ConnectException::new(e.message())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let ex = BadRequestException::new("Invalid request body");
        assert_eq!(ex.status_code(), 400);
        assert_eq!(ex.error_code(), 400);
        assert_eq!(ex.message(), "Invalid request body");
    }

    #[test]
    fn test_display() {
        let ex = BadRequestException::new("Missing required field");
        let display = format!("{}", ex);
        assert!(display.contains("BadRequestException"));
        assert!(display.contains("Missing required field"));
    }
}
