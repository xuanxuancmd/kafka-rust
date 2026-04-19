/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::collections::HashMap;
use std::fmt;
use std::io::Cursor;

use common_trait::protocol::{Field, Schema, SchemaError, Struct, INT16, INT32, INT64, STRING};
use common_trait::TopicPartition;
use std::sync::Arc;

use super::mirror_client::OffsetAndMetadata;

/// Checkpoint records emitted by MirrorCheckpointConnector.
/// Corresponds to Java: org.apache.kafka.connect.mirror.Checkpoint
#[derive(Debug, Clone, PartialEq)]
pub struct Checkpoint {
    consumer_group_id: String,
    topic_partition: TopicPartition,
    upstream_offset: i64,
    downstream_offset: i64,
    metadata: String,
}

// Schema constants
pub const TOPIC_KEY: &str = "topic";
pub const PARTITION_KEY: &str = "partition";
pub const CONSUMER_GROUP_ID_KEY: &str = "group";
pub const UPSTREAM_OFFSET_KEY: &str = "upstreamOffset";
pub const DOWNSTREAM_OFFSET_KEY: &str = "offset";
pub const METADATA_KEY: &str = "metadata";
pub const VERSION_KEY: &str = "version";
pub const VERSION: i16 = 0;

impl Checkpoint {
    pub fn new(
        consumer_group_id: impl Into<String>,
        topic_partition: TopicPartition,
        upstream_offset: i64,
        downstream_offset: i64,
        metadata: impl Into<String>,
    ) -> Self {
        Self {
            consumer_group_id: consumer_group_id.into(),
            topic_partition,
            upstream_offset,
            downstream_offset,
            metadata: metadata.into(),
        }
    }

    pub fn consumer_group_id(&self) -> &str {
        &self.consumer_group_id
    }

    pub fn topic_partition(&self) -> &TopicPartition {
        &self.topic_partition
    }

    pub fn upstream_offset(&self) -> i64 {
        self.upstream_offset
    }

    pub fn downstream_offset(&self) -> i64 {
        self.downstream_offset
    }

    pub fn metadata(&self) -> &str {
        &self.metadata
    }

    /// Returns downstream offset and metadata pair.
    /// Corresponds to Java: Checkpoint.offsetAndMetadata()
    pub fn offset_and_metadata(&self) -> OffsetAndMetadata {
        OffsetAndMetadata::new(self.downstream_offset, self.metadata.clone())
    }

    /// Returns Connect partition map for this checkpoint.
    /// Corresponds to Java: Checkpoint.connectPartition()
    pub fn connect_partition(&self) -> HashMap<String, String> {
        let mut partition = HashMap::new();
        partition.insert(
            CONSUMER_GROUP_ID_KEY.to_string(),
            self.consumer_group_id.clone(),
        );
        partition.insert(
            TOPIC_KEY.to_string(),
            self.topic_partition.topic().to_string(),
        );
        partition.insert(
            PARTITION_KEY.to_string(),
            self.topic_partition.partition().to_string(),
        );
        partition
    }

    /// Returns group id from Connect partition map.
    /// Corresponds to Java: Checkpoint.unwrapGroup(Map<String, ?> connectPartition)
    pub fn unwrap_group(connect_partition: &HashMap<String, String>) -> String {
        connect_partition
            .get(CONSUMER_GROUP_ID_KEY)
            .cloned()
            .unwrap_or_default()
    }

    /// Serialize key to byte array
    pub fn record_key(&self) -> Result<Vec<u8>, SchemaError> {
        let key_schema = Checkpoint::key_schema();
        let mut struct_ref = Struct::new(&key_schema);
        struct_ref.set_string(CONSUMER_GROUP_ID_KEY, &self.consumer_group_id)?;
        struct_ref.set_string(TOPIC_KEY, self.topic_partition.topic())?;
        struct_ref.set_int(PARTITION_KEY, self.topic_partition.partition())?;

        let mut buffer = Vec::new();
        key_schema.write(&mut buffer, &struct_ref)?;
        Ok(buffer)
    }

    /// Serialize value to byte array
    pub fn record_value(&self) -> Result<Vec<u8>, SchemaError> {
        let header_schema = Checkpoint::header_schema();
        let value_schema = Checkpoint::value_schema();

        // Write header
        let mut header_struct = Struct::new(&header_schema);
        header_struct.set_short(VERSION_KEY, VERSION)?;

        // Write value
        let mut value_struct = Struct::new(&value_schema);
        value_struct.set_long(UPSTREAM_OFFSET_KEY, self.upstream_offset)?;
        value_struct.set_long(DOWNSTREAM_OFFSET_KEY, self.downstream_offset)?;
        value_struct.set_string(METADATA_KEY, &self.metadata)?;

        let mut buffer = Vec::new();
        header_schema.write(&mut buffer, &header_struct)?;
        value_schema.write(&mut buffer, &value_struct)?;
        Ok(buffer)
    }

    /// Deserialize from byte arrays
    pub fn deserialize_record(key: &[u8], value: &[u8]) -> Result<Self, SchemaError> {
        let header_schema = Checkpoint::header_schema();
        let key_schema = Checkpoint::key_schema();

        // Read header
        let mut value_cursor = Cursor::new(value);
        let header_struct = header_schema.read(&mut value_cursor)?;
        let version = header_struct.get_short(VERSION_KEY)?;

        // Read value
        let value_schema = Checkpoint::value_schema_for_version(version);
        let value_struct = value_schema.read(&mut value_cursor)?;
        let upstream_offset = value_struct.get_long(UPSTREAM_OFFSET_KEY)?;
        let downstream_offset = value_struct.get_long(DOWNSTREAM_OFFSET_KEY)?;
        let metadata = value_struct.get_string(METADATA_KEY)?.to_string();

        // Read key
        let mut key_cursor = Cursor::new(key);
        let key_struct = key_schema.read(&mut key_cursor)?;
        let group = key_struct.get_string(CONSUMER_GROUP_ID_KEY)?.to_string();
        let topic = key_struct.get_string(TOPIC_KEY)?.to_string();
        let partition = key_struct.get_int(PARTITION_KEY)?;

        Ok(Self::new(
            group,
            TopicPartition::new(topic, partition),
            upstream_offset,
            downstream_offset,
            metadata,
        ))
    }

    fn key_schema() -> Schema {
        Schema::new(vec![
            Field::new(CONSUMER_GROUP_ID_KEY, Arc::new(STRING)),
            Field::new(TOPIC_KEY, Arc::new(STRING)),
            Field::new(PARTITION_KEY, Arc::new(INT32)),
        ])
        .expect("Invalid key schema")
    }

    fn header_schema() -> Schema {
        Schema::new(vec![Field::new(VERSION_KEY, Arc::new(INT16))]).expect("Invalid header schema")
    }

    fn value_schema() -> Schema {
        Schema::new(vec![
            Field::new(UPSTREAM_OFFSET_KEY, Arc::new(INT64)),
            Field::new(DOWNSTREAM_OFFSET_KEY, Arc::new(INT64)),
            Field::new(METADATA_KEY, Arc::new(STRING)),
        ])
        .expect("Invalid value schema")
    }

    fn value_schema_for_version(version: i16) -> Schema {
        assert_eq!(version, 0, "Unsupported version: {}", version);
        Self::value_schema()
    }
}

impl fmt::Display for Checkpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Checkpoint{{consumerGroupId={}, topicPartition={}, upstreamOffset={}, downstreamOffset={}, metadata={}}}",
            self.consumer_group_id, self.topic_partition, self.upstream_offset, self.downstream_offset, self.metadata
        )
    }
}
