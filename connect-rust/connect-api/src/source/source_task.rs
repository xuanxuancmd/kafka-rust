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

use crate::connector::Task;
use crate::errors::ConnectError;
use crate::source::SourceRecord;
use crate::source::SourceTaskContext;
use kafka_clients_mock::RecordMetadata;

/// The configuration key that determines how source tasks will define transaction boundaries
/// when exactly-once support is enabled.
pub const TRANSACTION_BOUNDARY_CONFIG: &str = "transaction.boundary";

/// TransactionBoundary enum for transaction boundaries.
///
/// Represents the permitted values for the TRANSACTION_BOUNDARY_CONFIG property.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionBoundary {
    /// A new transaction will be started and committed for every batch of records returned by poll().
    Poll,
    /// Transactions will be started and committed on a user-defined time interval.
    Interval,
    /// Transactions will be defined by the connector itself, via a TransactionContext.
    Connector,
}

impl TransactionBoundary {
    /// The default transaction boundary style.
    pub const DEFAULT: TransactionBoundary = TransactionBoundary::Poll;

    /// Parse a TransactionBoundary from the given string.
    pub fn from_property(property: &str) -> Result<TransactionBoundary, ConnectError> {
        if property.is_empty() {
            return Err(ConnectError::data(
                "Value for transaction boundary property may not be null",
            ));
        }
        let trimmed = property.trim().to_uppercase();
        match trimmed.as_str() {
            "POLL" => Ok(TransactionBoundary::Poll),
            "INTERVAL" => Ok(TransactionBoundary::Interval),
            "CONNECTOR" => Ok(TransactionBoundary::Connector),
            _ => Err(ConnectError::data(format!(
                "Invalid transaction boundary: {}",
                property
            ))),
        }
    }

    /// Returns the string representation of this TransactionBoundary.
    pub fn to_string(&self) -> String {
        match self {
            TransactionBoundary::Poll => "poll",
            TransactionBoundary::Interval => "interval",
            TransactionBoundary::Connector => "connector",
        }
        .to_string()
    }
}

impl std::fmt::Display for TransactionBoundary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

/// SourceTask trait for source tasks.
///
/// This corresponds to `org.apache.kafka.connect.source.SourceTask` in Java.
pub trait SourceTask: Task {
    /// Initializes this task with the given context.
    fn initialize(&mut self, context: impl SourceTaskContext);

    /// Polls for new records.
    fn poll(&mut self) -> Result<Vec<SourceRecord>, ConnectError>;

    /// Commits the current batch of records. Default implementation does nothing.
    fn commit(&mut self) -> Result<(), ConnectError> {
        Ok(())
    }

    /// Commits a single record. Default implementation does nothing.
    fn commit_record(
        &mut self,
        _record: &SourceRecord,
        _metadata: &RecordMetadata,
    ) -> Result<(), ConnectError> {
        Ok(())
    }
}
