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

//! Tests for Heartbeat module
//! Corresponds to Java: org.apache.kafka.connect.mirror.Heartbeat

use connect_mirror_client::{Heartbeat, SOURCE_CLUSTER_ALIAS_KEY, TARGET_CLUSTER_ALIAS_KEY};

#[test]
fn test_connect_partition() {
    let heartbeat = Heartbeat::new("source-a", "target-a", 123);
    let partition = heartbeat.connect_partition();

    assert_eq!(
        partition.get(SOURCE_CLUSTER_ALIAS_KEY),
        Some(&"source-a".to_string())
    );
    assert_eq!(
        partition.get(TARGET_CLUSTER_ALIAS_KEY),
        Some(&"target-a".to_string())
    );
}
