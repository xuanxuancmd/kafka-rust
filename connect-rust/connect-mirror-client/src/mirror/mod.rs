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

//! Mirror client module - corresponds to Java: org.apache.kafka.connect.mirror

pub mod checkpoint;
pub mod heartbeat;
pub mod mirror_client;
pub mod mirror_client_config;
pub mod offset_sync;
pub mod remote_cluster_utils;
pub mod replication_policy;
pub mod source_and_target;

pub use checkpoint::{
    Checkpoint, CONSUMER_GROUP_ID_KEY, DOWNSTREAM_OFFSET_KEY, METADATA_KEY, PARTITION_KEY,
    TOPIC_KEY, UPSTREAM_OFFSET_KEY,
};
pub use heartbeat::{Heartbeat, SOURCE_CLUSTER_ALIAS_KEY, TARGET_CLUSTER_ALIAS_KEY, TIMESTAMP_KEY};
pub use mirror_client::{
    AdminClient, Consumer, ConsumerRecord, MirrorClient, MockAdminClient, MockConsumer,
    OffsetAndMetadata,
};
pub use mirror_client_config::{
    ForwardingAdmin, MirrorClientConfig, ADMIN_CLIENT_PREFIX, CONSUMER_CLIENT_PREFIX,
    FORWARDING_ADMIN_CLASS, FORWARDING_ADMIN_CLASS_DEFAULT, INTERNAL_TOPIC_SEPARATOR_ENABLED,
    INTERNAL_TOPIC_SEPARATOR_ENABLED_DEFAULT, PRODUCER_CLIENT_PREFIX, REPLICATION_POLICY_CLASS,
    REPLICATION_POLICY_CLASS_DEFAULT, REPLICATION_POLICY_SEPARATOR,
    REPLICATION_POLICY_SEPARATOR_DEFAULT,
};
pub use offset_sync::{
    OffsetSync, DOWNSTREAM_OFFSET_KEY as OFFSET_SYNC_DOWNSTREAM_OFFSET_KEY,
    UPSTREAM_OFFSET_KEY as OFFSET_SYNC_UPSTREAM_OFFSET_KEY,
};
pub use remote_cluster_utils::{
    checkpoint_topics, heartbeat_topics, replication_hops, translate_offsets, upstream_clusters,
};
pub use replication_policy::{
    DefaultReplicationPolicy, IdentityReplicationPolicy, ReplicationPolicy,
    INTERNAL_TOPIC_SEPARATOR_ENABLED_CONFIG, SEPARATOR_CONFIG, SOURCE_CLUSTER_ALIAS_CONFIG,
};
pub use source_and_target::SourceAndTarget;
