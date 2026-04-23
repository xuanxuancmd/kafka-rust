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

use super::DataException;
use std::error::Error;
use std::fmt;

/// Indicates an error while projecting a schema via SchemaProjector.
///
/// This corresponds to `org.apache.kafka.connect.errors.SchemaProjectorException` in Java.
#[derive(Debug)]
pub struct SchemaProjectorException {
    inner: DataException,
}

impl SchemaProjectorException {
    /// Creates a new SchemaProjectorException with a message.
    pub fn new(message: impl Into<String>) -> Self {
        SchemaProjectorException {
            inner: DataException::new(message),
        }
    }

    /// Creates a new SchemaProjectorException with a message and a cause.
    pub fn with_cause(message: impl Into<String>, cause: Box<dyn Error + Send + Sync>) -> Self {
        SchemaProjectorException {
            inner: DataException::with_cause(message, cause),
        }
    }

    /// Creates a new SchemaProjectorException from a cause.
    pub fn from_cause(cause: Box<dyn Error + Send + Sync>) -> Self {
        SchemaProjectorException {
            inner: DataException::from_cause(cause),
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

impl fmt::Display for SchemaProjectorException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SchemaProjectorException: {}", self.inner.message())
    }
}

impl Error for SchemaProjectorException {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.inner.source()
    }
}

impl From<DataException> for SchemaProjectorException {
    fn from(inner: DataException) -> Self {
        SchemaProjectorException { inner }
    }
}
