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

//! RecordIsTombstone predicate for Kafka Connect.
//!
//! This corresponds to `org.apache.kafka.connect.transforms.predicates.RecordIsTombstone` in Java.
//!
//! A predicate which is true for records which are tombstones (i.e. have null value).

use connect_api::components::Versioned;
use connect_api::connector::ConnectRecord;
use connect_api::transforms::predicates::Predicate;
use serde_json::Value;
use std::collections::HashMap;

/// A predicate which is true for records which are tombstones (i.e. have null value).
///
/// This corresponds to `org.apache.kafka.connect.transforms.predicates.RecordIsTombstone` in Java.
///
/// # Example
///
/// ```ignore
/// use connect_transforms::transforms::predicates::RecordIsTombstone;
/// use connect_api::transforms::predicates::Predicate;
///
/// let predicate = RecordIsTombstone::new();
/// // For a record with null value, test() returns true
/// // For a record with non-null value, test() returns false
/// ```
pub struct RecordIsTombstone<R> {
    _record_type: std::marker::PhantomData<R>,
}

impl<R> RecordIsTombstone<R> {
    /// Creates a new RecordIsTombstone predicate.
    pub fn new() -> Self {
        RecordIsTombstone {
            _record_type: std::marker::PhantomData,
        }
    }
}

impl<R> Default for RecordIsTombstone<R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R> Versioned for RecordIsTombstone<R> {
    fn version() -> &'static str {
        "3.9.0"
    }
}

impl<R: ConnectRecord> Predicate<R> for RecordIsTombstone<R> {
    /// Configures this predicate.
    ///
    /// This predicate has no configuration options.
    fn configure(&mut self, _configs: HashMap<String, Value>) {
        // No configuration needed
    }

    /// Tests the given record.
    ///
    /// Returns true if the record is a tombstone (i.e. has null value).
    ///
    /// # Arguments
    ///
    /// * `record` - The record to test.
    ///
    /// # Returns
    ///
    /// `true` if the record's value is null (tombstone), `false` otherwise.
    fn test(&self, record: &R) -> bool {
        matches!(record.value(), Value::Null)
    }

    /// Closes this predicate.
    ///
    /// This predicate has no resources to release.
    fn close(&mut self) {
        // No resources to release
    }
}

impl<R> std::fmt::Display for RecordIsTombstone<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecordIsTombstone{{}}")
    }
}

