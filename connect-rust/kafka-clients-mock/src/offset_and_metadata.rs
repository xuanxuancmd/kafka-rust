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

use common_trait::TopicPartition;

/// OffsetAndMetadata represents an offset and metadata pair.
///
/// This corresponds to `org.apache.kafka.clients.consumer.OffsetAndMetadata` in Java.
#[derive(Debug, Clone)]
pub struct OffsetAndMetadata {
    offset: i64,
    metadata: String,
    leader_epoch: Option<i32>,
}

impl OffsetAndMetadata {
    /// Creates a new OffsetAndMetadata with the given offset.
    pub fn new(offset: i64) -> Self {
        OffsetAndMetadata {
            offset,
            metadata: String::new(),
            leader_epoch: None,
        }
    }

    /// Creates a new OffsetAndMetadata with the given offset and metadata.
    pub fn with_metadata(offset: i64, metadata: impl Into<String>) -> Self {
        OffsetAndMetadata {
            offset,
            metadata: metadata.into(),
            leader_epoch: None,
        }
    }

    /// Creates a new OffsetAndMetadata with the given offset, metadata, and leader epoch.
    pub fn with_leader_epoch(offset: i64, metadata: impl Into<String>, leader_epoch: i32) -> Self {
        OffsetAndMetadata {
            offset,
            metadata: metadata.into(),
            leader_epoch: Some(leader_epoch),
        }
    }

    /// Returns the offset.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Returns the metadata.
    pub fn metadata(&self) -> &str {
        &self.metadata
    }

    /// Returns the leader epoch.
    pub fn leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let om = OffsetAndMetadata::new(100);
        assert_eq!(om.offset(), 100);
        assert_eq!(om.metadata(), "");
        assert!(om.leader_epoch().is_none());
    }

    #[test]
    fn test_with_metadata() {
        let om = OffsetAndMetadata::with_metadata(100, "test");
        assert_eq!(om.offset(), 100);
        assert_eq!(om.metadata(), "test");
    }
}
