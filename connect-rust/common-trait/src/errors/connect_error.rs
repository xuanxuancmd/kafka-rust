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

//! ConnectError represents errors in Kafka Connect.
//!
//! This corresponds to `org.apache.kafka.connect.errors.ConnectException` in Java.

use std::error::Error;
use std::fmt;

/// ConnectError represents errors in Kafka Connect.
///
/// This corresponds to various exceptions in `org.apache.kafka.connect.errors` in Java.
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
    /// General ConnectException.
    General { message: String },
}

impl ConnectError {
    /// Creates a new DataException.
    pub fn data(message: impl Into<String>) -> Self {
        ConnectError::Data {
            message: message.into(),
        }
    }

    /// Creates a new RetriableException.
    pub fn retriable(message: impl Into<String>) -> Self {
        ConnectError::Retriable {
            message: message.into(),
        }
    }

    /// Creates a new NotFoundException.
    pub fn not_found(message: impl Into<String>) -> Self {
        ConnectError::NotFound {
            message: message.into(),
        }
    }

    /// Creates a new IllegalWorkerStateException.
    pub fn illegal_worker_state(message: impl Into<String>) -> Self {
        ConnectError::IllegalWorkerState {
            message: message.into(),
        }
    }

    /// Creates a new general ConnectException.
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
            ConnectError::General { message } => write!(f, "ConnectException: {}", message),
        }
    }
}

impl Error for ConnectError {}
