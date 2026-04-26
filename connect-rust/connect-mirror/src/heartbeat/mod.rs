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

//! Heartbeat module for MirrorMaker 2 heartbeat connector.
//!
//! This module provides the MirrorHeartbeatConnector, MirrorHeartbeatConfig,
//! and MirrorHeartbeatTask for emitting heartbeat records to monitor
//! replication health between clusters.
//!
//! The MirrorHeartbeatConnector:
//! - Emits heartbeat records to the target cluster
//! - Creates a single task for heartbeat emission
//! - Monitors connectivity and latency between source and target clusters
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror heartbeat classes

pub mod mirror_heartbeat_config;
pub mod mirror_heartbeat_connector;
pub mod mirror_heartbeat_task;

pub use mirror_heartbeat_config::*;
pub use mirror_heartbeat_connector::*;
pub use mirror_heartbeat_task::*;
