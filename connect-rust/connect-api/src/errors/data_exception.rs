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

/// Base class for all Kafka Connect data API exceptions.
///
/// This corresponds to `org.apache.kafka.connect.errors.DataException` in Java.
#[derive(Debug)]
pub struct DataException {
    inner: ConnectException,
}

impl DataException {
    /// Creates a new DataException with a message.
    pub fn new(message: impl Into<String>) -> Self {
        DataException {
            inner: ConnectException::new(message),
        }
    }

    /// Creates a new DataException with a message and a cause.
    pub fn with_cause(message: impl Into<String>, cause: Box<dyn Error + Send + Sync>) -> Self {
        DataException {
            inner: ConnectException::with_cause(message, cause),
        }
    }

    /// Creates a new DataException from a cause.
    pub fn from_cause(cause: Box<dyn Error + Send + Sync>) -> Self {
        DataException {
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

impl fmt::Display for DataException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DataException: {}", self.inner.message())
    }
}

impl Error for DataException {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.inner.source()
    }
}

impl From<ConnectException> for DataException {
    fn from(inner: ConnectException) -> Self {
        DataException { inner }
    }
}
