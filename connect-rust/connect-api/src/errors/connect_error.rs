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

/// ConnectError represents errors in Kafka Connect.
///
/// This is a convenience enum that wraps all exception types.
/// For strict 1:1 correspondence with Java, use the individual exception types.
#[derive(Debug)]
pub enum ConnectError {
    /// DataException for data-related errors.
    Data { message: String },
    /// RetriableException for retriable errors.
    Retriable { message: String },
    /// NotFoundException for not found errors.
    NotFound { message: String },
    /// IllegalWorkerStateException for illegal worker state.
    IllegalWorkerState { message: String },
    /// SchemaBuilderException for schema builder errors.
    SchemaBuilder { message: String },
    /// SchemaProjectorException for schema projector errors.
    SchemaProjector { message: String },
    /// AlreadyExistsException for already exists errors.
    AlreadyExists { message: String },
    /// SerializationException for serialization errors.
    SerializationError { message: String },
    /// DeserializationException for deserialization errors.
    DeserializationError { message: String },
    /// General ConnectException.
    General { message: String },
}

impl ConnectError {
    /// Creates a new DataException variant.
    pub fn data(message: impl Into<String>) -> Self {
        ConnectError::Data {
            message: message.into(),
        }
    }

    /// Creates a new RetriableException variant.
    pub fn retriable(message: impl Into<String>) -> Self {
        ConnectError::Retriable {
            message: message.into(),
        }
    }

    /// Creates a new NotFoundException variant.
    pub fn not_found(message: impl Into<String>) -> Self {
        ConnectError::NotFound {
            message: message.into(),
        }
    }

    /// Creates a new IllegalWorkerStateException variant.
    pub fn illegal_worker_state(message: impl Into<String>) -> Self {
        ConnectError::IllegalWorkerState {
            message: message.into(),
        }
    }

    /// Creates a new SchemaBuilderException variant.
    pub fn schema_builder(message: impl Into<String>) -> Self {
        ConnectError::SchemaBuilder {
            message: message.into(),
        }
    }

    /// Creates a new SchemaProjectorException variant.
    pub fn schema_projector(message: impl Into<String>) -> Self {
        ConnectError::SchemaProjector {
            message: message.into(),
        }
    }

    /// Creates a new AlreadyExistsException variant.
    pub fn already_exists(message: impl Into<String>) -> Self {
        ConnectError::AlreadyExists {
            message: message.into(),
        }
    }

    /// Creates a new general ConnectException variant.
    pub fn general(message: impl Into<String>) -> Self {
        ConnectError::General {
            message: message.into(),
        }
    }

    /// Returns the message of this error.
    pub fn message(&self) -> &str {
        match self {
            ConnectError::Data { message } => message,
            ConnectError::Retriable { message } => message,
            ConnectError::NotFound { message } => message,
            ConnectError::IllegalWorkerState { message } => message,
            ConnectError::SchemaBuilder { message } => message,
            ConnectError::SchemaProjector { message } => message,
            ConnectError::AlreadyExists { message } => message,
            ConnectError::SerializationError { message } => message,
            ConnectError::DeserializationError { message } => message,
            ConnectError::General { message } => message,
        }
    }

    /// Returns whether this error is retriable.
    pub fn is_retriable(&self) -> bool {
        matches!(self, ConnectError::Retriable { .. })
    }
}

impl fmt::Display for ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectError::Data { message } => write!(f, "DataException: {}", message),
            ConnectError::Retriable { message } => write!(f, "RetriableException: {}", message),
            ConnectError::NotFound { message } => write!(f, "NotFoundException: {}", message),
            ConnectError::IllegalWorkerState { message } => {
                write!(f, "IllegalWorkerStateException: {}", message)
            }
            ConnectError::SchemaBuilder { message } => {
                write!(f, "SchemaBuilderException: {}", message)
            }
            ConnectError::SchemaProjector { message } => {
                write!(f, "SchemaProjectorException: {}", message)
            }
            ConnectError::AlreadyExists { message } => {
                write!(f, "AlreadyExistsException: {}", message)
            }
            ConnectError::SerializationError { message } => {
                write!(f, "SerializationException: {}", message)
            }
            ConnectError::DeserializationError { message } => {
                write!(f, "DeserializationException: {}", message)
            }
            ConnectError::General { message } => write!(f, "ConnectException: {}", message),
        }
    }
}

impl Error for ConnectError {}
