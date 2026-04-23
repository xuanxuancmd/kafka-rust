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

//! Exception used when a privileged write fails.
//!
//! This exception is used when a write that requires special privileges
//! (obtained via `ConfigBackingStore::claim_write_privileges()`) fails.
//!
//! Corresponds to `org.apache.kafka.connect.storage.PrivilegedWriteException` in Java.

use std::error::Error;
use std::fmt;

/// Exception thrown when a write that requires special privileges fails.
///
/// This is used in the context of the ConfigBackingStore when a worker
/// has claimed write privileges but the write operation still fails.
#[derive(Debug)]
pub struct PrivilegedWriteException {
    /// The error message
    message: String,
    /// The underlying cause of the error
    cause: Option<Box<dyn Error + Send + Sync>>,
}

impl PrivilegedWriteException {
    /// Creates a new PrivilegedWriteException with a message and cause.
    ///
    /// # Arguments
    /// * `message` - The error message
    /// * `cause` - The underlying cause of the error
    pub fn new(message: impl Into<String>, cause: Option<Box<dyn Error + Send + Sync>>) -> Self {
        Self {
            message: message.into(),
            cause,
        }
    }

    /// Creates a new PrivilegedWriteException with just a message.
    ///
    /// # Arguments
    /// * `message` - The error message
    pub fn with_message(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            cause: None,
        }
    }

    /// Returns the error message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the underlying cause if one exists.
    pub fn cause(&self) -> Option<&(dyn Error + Send + Sync)> {
        self.cause.as_ref().map(|e| e.as_ref())
    }
}

impl fmt::Display for PrivilegedWriteException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PrivilegedWriteException: {}", self.message)?;
        if let Some(cause) = &self.cause {
            write!(f, " (caused by: {})", cause)?;
        }
        Ok(())
    }
}

impl Error for PrivilegedWriteException {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.cause
            .as_ref()
            .map(|e| e.as_ref() as &(dyn Error + 'static))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn test_new_with_cause() {
        let io_error = io::Error::new(io::ErrorKind::Other, "test error");
        let error =
            PrivilegedWriteException::new("Failed to write config", Some(Box::new(io_error)));
        assert_eq!(error.message(), "Failed to write config");
        assert!(error.cause().is_some());
    }

    #[test]
    fn test_with_message() {
        let error = PrivilegedWriteException::with_message("Failed to write config");
        assert_eq!(error.message(), "Failed to write config");
        assert!(error.cause().is_none());
    }

    #[test]
    fn test_display() {
        let error = PrivilegedWriteException::with_message("Failed to write config");
        let display = format!("{}", error);
        assert!(display.contains("PrivilegedWriteException"));
        assert!(display.contains("Failed to write config"));
    }

    #[test]
    fn test_display_with_cause() {
        let io_error = io::Error::new(io::ErrorKind::Other, "test error");
        let error =
            PrivilegedWriteException::new("Failed to write config", Some(Box::new(io_error)));
        let display = format!("{}", error);
        assert!(display.contains("caused by"));
    }

    #[test]
    fn test_error_trait() {
        let error = PrivilegedWriteException::with_message("test");
        let _: &dyn Error = &error;
    }
}
