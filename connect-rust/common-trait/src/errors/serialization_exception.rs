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

/// SerializationException is thrown when serialization fails.
///
/// This corresponds to `org.apache.kafka.common.errors.SerializationException` in Java.
#[derive(Debug)]
pub struct SerializationException {
    message: String,
}

impl SerializationException {
    /// Creates a new SerializationException with the given message.
    pub fn new(message: impl Into<String>) -> Self {
        SerializationException {
            message: message.into(),
        }
    }

    /// Returns the message of this exception.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for SerializationException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SerializationException: {}", self.message)
    }
}

impl Error for SerializationException {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let ex = SerializationException::new("test error");
        assert_eq!(ex.message(), "test error");
    }

    #[test]
    fn test_display() {
        let ex = SerializationException::new("test error");
        assert_eq!(format!("{}", ex), "SerializationException: test error");
    }
}
