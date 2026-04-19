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

use super::ConnectException;
use std::error::Error;
use std::fmt;

/// Indicates that a method has been invoked illegally or at an invalid time
/// by a connector or task.
///
/// This corresponds to `org.apache.kafka.connect.errors.IllegalWorkerStateException` in Java.
#[derive(Debug)]
pub struct IllegalWorkerStateException {
    inner: ConnectException,
}

impl IllegalWorkerStateException {
    /// Creates a new IllegalWorkerStateException with a message.
    pub fn new(message: impl Into<String>) -> Self {
        IllegalWorkerStateException {
            inner: ConnectException::new(message),
        }
    }

    /// Creates a new IllegalWorkerStateException with a message and a cause.
    pub fn with_cause(message: impl Into<String>, cause: Box<dyn Error + Send + Sync>) -> Self {
        IllegalWorkerStateException {
            inner: ConnectException::with_cause(message, cause),
        }
    }

    /// Creates a new IllegalWorkerStateException from a cause.
    pub fn from_cause(cause: Box<dyn Error + Send + Sync>) -> Self {
        IllegalWorkerStateException {
            inner: ConnectException::from_cause(cause),
        }
    }

    /// Returns the message of this exception.
    pub fn message(&self) -> &str {
        self.inner.message()
    }

    /// Returns the cause of this exception, if any.
    pub fn cause(&self) -> Option<&(dyn Error + Send + Sync)> {
        self.inner.cause()
    }
}

impl fmt::Display for IllegalWorkerStateException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IllegalWorkerStateException: {}", self.inner.message())
    }
}

impl Error for IllegalWorkerStateException {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.inner.source()
    }
}

impl From<ConnectException> for IllegalWorkerStateException {
    fn from(inner: ConnectException) -> Self {
        IllegalWorkerStateException { inner }
    }
}
