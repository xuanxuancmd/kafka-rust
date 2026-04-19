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

use std::error::Error;
use std::fmt;

/// KafkaException is the base exception for all Kafka-related errors.
///
/// This corresponds to `org.apache.kafka.common.errors.KafkaException` in Java.
#[derive(Debug)]
pub struct KafkaException {
    message: String,
    cause: Option<Box<dyn Error + Send + Sync>>,
}

impl KafkaException {
    /// Creates a new KafkaException with the given message.
    pub fn new(message: impl Into<String>) -> Self {
        KafkaException {
            message: message.into(),
            cause: None,
        }
    }

    /// Creates a new KafkaException with the given message and cause.
    pub fn with_cause(message: impl Into<String>, cause: Box<dyn Error + Send + Sync>) -> Self {
        KafkaException {
            message: message.into(),
            cause: Some(cause),
        }
    }

    /// Returns the message of this exception.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the cause of this exception, if any.
    pub fn cause(&self) -> Option<&(dyn Error + Send + Sync)> {
        self.cause.as_ref().map(|c| c.as_ref())
    }
}

impl fmt::Display for KafkaException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "KafkaException: {}", self.message)?;
        if let Some(cause) = &self.cause {
            write!(f, "\nCaused by: {}", cause)?;
        }
        Ok(())
    }
}

impl Error for KafkaException {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.cause
            .as_ref()
            .map(|c| c.as_ref() as &(dyn Error + 'static))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let ex = KafkaException::new("test error");
        assert_eq!(ex.message(), "test error");
        assert!(ex.cause().is_none());
    }

    #[test]
    fn test_with_cause() {
        let cause = KafkaException::new("inner error");
        let ex = KafkaException::with_cause("outer error", Box::new(cause));
        assert_eq!(ex.message(), "outer error");
        assert!(ex.cause().is_some());
    }

    #[test]
    fn test_display() {
        let ex = KafkaException::new("test error");
        assert_eq!(format!("{}", ex), "KafkaException: test error");
    }
}
