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

//! Tests for Checkpoint module
//! Corresponds to Java: org.apache.kafka.connect.mirror.Checkpoint

use common_trait::TopicPartition;
use connect_mirror_client::{Checkpoint, CONSUMER_GROUP_ID_KEY, PARTITION_KEY, TOPIC_KEY};

#[test]
fn test_offset_and_metadata() {
    let checkpoint = Checkpoint::new(
        "group-a",
        TopicPartition::new("topic-a", 2),
        10,
        20,
        "meta-a",
    );

    let offset = checkpoint.offset_and_metadata();
    assert_eq!(offset.offset(), 20);
    assert_eq!(offset.metadata(), "meta-a");
}

#[test]
fn test_connect_partition_and_unwrap_group() {
    let checkpoint = Checkpoint::new(
        "group-a",
        TopicPartition::new("topic-a", 2),
        10,
        20,
        "meta-a",
    );

    let partition = checkpoint.connect_partition();
    assert_eq!(
        partition.get(CONSUMER_GROUP_ID_KEY),
        Some(&"group-a".to_string())
    );
    assert_eq!(partition.get(TOPIC_KEY), Some(&"topic-a".to_string()));
    assert_eq!(partition.get(PARTITION_KEY), Some(&"2".to_string()));
    assert_eq!(Checkpoint::unwrap_group(&partition), "group-a");
}
