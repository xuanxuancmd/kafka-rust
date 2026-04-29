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

//! Connector offsets for managing source and sink connector offsets.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.rest.entities.ConnectorOffsets` and
//! `org.apache.kafka.connect.runtime.rest.entities.ConnectorOffset` in Java.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A single offset entry for a partition.
///
/// Represents the offset for a single partition of a connector.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectorOffset {
    /// The partition key (source partition or topic partition).
    partition: HashMap<String, serde_json::Value>,
    /// The offset value, or None if no offset is committed.
    offset: Option<HashMap<String, serde_json::Value>>,
}

impl ConnectorOffset {
    /// Creates a new connector offset.
    pub fn new(
        partition: HashMap<String, serde_json::Value>,
        offset: Option<HashMap<String, serde_json::Value>>,
    ) -> Self {
        ConnectorOffset { partition, offset }
    }

    /// Returns the partition.
    pub fn partition(&self) -> &HashMap<String, serde_json::Value> {
        &self.partition
    }

    /// Returns the offset, or None if no offset is committed.
    pub fn offset(&self) -> &Option<HashMap<String, serde_json::Value>> {
        &self.offset
    }

    /// Returns true if this offset represents an uncommitted (null) offset.
    pub fn is_uncommitted(&self) -> bool {
        self.offset.is_none()
    }
}

/// Collection of offsets for a connector.
///
/// Contains all offset entries for all partitions of a connector.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectorOffsets {
    /// The list of offset entries.
    offsets: Vec<ConnectorOffset>,
}

impl ConnectorOffsets {
    /// Creates an empty connector offsets collection.
    pub fn empty() -> Self {
        ConnectorOffsets {
            offsets: Vec::new(),
        }
    }

    /// Creates a new connector offsets collection from a list of offsets.
    pub fn new(offsets: Vec<ConnectorOffset>) -> Self {
        ConnectorOffsets { offsets }
    }

    /// Returns the offset entries.
    pub fn offsets(&self) -> &Vec<ConnectorOffset> {
        &self.offsets
    }

    /// Returns true if there are no offsets.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Returns the number of offset entries.
    pub fn len(&self) -> usize {
        self.offsets.len()
    }
}
