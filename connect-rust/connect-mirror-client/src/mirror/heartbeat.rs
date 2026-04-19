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

use common_trait::protocol::{Field, Schema, SchemaError, Struct, INT16, INT64, STRING};
use std::sync::Arc;

/// Heartbeat records emitted by MirrorHeartbeatConnector.
/// Corresponds to Java: org.apache.kafka.connect.mirror.Heartbeat
#[derive(Debug, Clone, PartialEq)]
pub struct Heartbeat {
    source_cluster_alias: String,
    target_cluster_alias: String,
    timestamp: i64,
}

// Schema constants
pub const SOURCE_CLUSTER_ALIAS_KEY: &str = "sourceClusterAlias";
pub const TARGET_CLUSTER_ALIAS_KEY: &str = "targetClusterAlias";
pub const TIMESTAMP_KEY: &str = "timestamp";
pub const VERSION_KEY: &str = "version";
pub const VERSION: i16 = 0;

impl Heartbeat {
    pub fn new(
        source_cluster_alias: impl Into<String>,
        target_cluster_alias: impl Into<String>,
        timestamp: i64,
    ) -> Self {
        Self {
            source_cluster_alias: source_cluster_alias.into(),
            target_cluster_alias: target_cluster_alias.into(),
            timestamp,
        }
    }

    pub fn source_cluster_alias(&self) -> &str {
        &self.source_cluster_alias
    }

    pub fn target_cluster_alias(&self) -> &str {
        &self.target_cluster_alias
    }

    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Returns Connect partition map for this heartbeat.
    /// Corresponds to Java: Heartbeat.connectPartition()
    pub fn connect_partition(&self) -> HashMap<String, String> {
        let mut partition = HashMap::new();
        partition.insert(
            SOURCE_CLUSTER_ALIAS_KEY.to_string(),
            self.source_cluster_alias.clone(),
        );
        partition.insert(
            TARGET_CLUSTER_ALIAS_KEY.to_string(),
            self.target_cluster_alias.clone(),
        );
        partition
    }

    /// Serialize key to byte array
    pub fn record_key(&self) -> Result<Vec<u8>, SchemaError> {
        let key_schema = Heartbeat::key_schema();
        let mut struct_ref = Struct::new(&key_schema);
        struct_ref.set_string(SOURCE_CLUSTER_ALIAS_KEY, &self.source_cluster_alias)?;
        struct_ref.set_string(TARGET_CLUSTER_ALIAS_KEY, &self.target_cluster_alias)?;

        let mut buffer = Vec::new();
        key_schema.write(&mut buffer, &struct_ref)?;
        Ok(buffer)
    }

    /// Serialize value to byte array
    pub fn record_value(&self) -> Result<Vec<u8>, SchemaError> {
        let header_schema = Heartbeat::header_schema();
        let value_schema = Heartbeat::value_schema();

        // Write header
        let mut header_struct = Struct::new(&header_schema);
        header_struct.set_short(VERSION_KEY, VERSION)?;

        // Write value
        let mut value_struct = Struct::new(&value_schema);
        value_struct.set_long(TIMESTAMP_KEY, self.timestamp)?;

        let mut buffer = Vec::new();
        header_schema.write(&mut buffer, &header_struct)?;
        value_schema.write(&mut buffer, &value_struct)?;
        Ok(buffer)
    }

    /// Deserialize from byte arrays
    pub fn deserialize_record(key: &[u8], value: &[u8]) -> Result<Self, SchemaError> {
        let header_schema = Heartbeat::header_schema();
        let key_schema = Heartbeat::key_schema();

        // Read header
        let mut value_cursor = Cursor::new(value);
        let header_struct = header_schema.read(&mut value_cursor)?;
        let version = header_struct.get_short(VERSION_KEY)?;

        // Read value
        let value_schema = Heartbeat::value_schema_for_version(version);
        let value_struct = value_schema.read(&mut value_cursor)?;
        let timestamp = value_struct.get_long(TIMESTAMP_KEY)?;

        // Read key
        let mut key_cursor = Cursor::new(key);
        let key_struct = key_schema.read(&mut key_cursor)?;
        let source_cluster_alias = key_struct.get_string(SOURCE_CLUSTER_ALIAS_KEY)?.to_string();
        let target_cluster_alias = key_struct.get_string(TARGET_CLUSTER_ALIAS_KEY)?.to_string();

        Ok(Self::new(
            source_cluster_alias,
            target_cluster_alias,
            timestamp,
        ))
    }

    fn key_schema() -> Schema {
        Schema::new(vec![
            Field::new(SOURCE_CLUSTER_ALIAS_KEY, Arc::new(STRING)),
            Field::new(TARGET_CLUSTER_ALIAS_KEY, Arc::new(STRING)),
        ])
        .expect("Invalid key schema")
    }

    fn header_schema() -> Schema {
        Schema::new(vec![Field::new(VERSION_KEY, Arc::new(INT16))]).expect("Invalid header schema")
    }

    fn value_schema() -> Schema {
        Schema::new(vec![Field::new(TIMESTAMP_KEY, Arc::new(INT64))]).expect("Invalid value schema")
    }

    fn value_schema_for_version(version: i16) -> Schema {
        assert_eq!(version, 0, "Unsupported version: {}", version);
        Self::value_schema()
    }
}

impl fmt::Display for Heartbeat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Heartbeat{{sourceClusterAlias={}, targetClusterAlias={}, timestamp={}}}",
            self.source_cluster_alias, self.target_cluster_alias, self.timestamp
        )
    }
}
