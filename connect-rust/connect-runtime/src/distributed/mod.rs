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

//! Distributed connect runtime module for Kafka Connect.
//!
//! This module contains components for distributed worker coordination,
//! including assignment strategies, protocols, and rebalancing logic.
//!
//! **API correspondence (Java 1:1)**:
//! - `DistributedHerder` → `org.apache.kafka.connect.runtime.distributed.DistributedHerder`
//! - `DistributedConfig` → `org.apache.kafka.connect.runtime.distributed.DistributedConfig`
//! - `ExtendedAssignment` → `org.apache.kafka.connect.runtime.distributed.ExtendedAssignment`

pub mod connect_assignor;
pub mod connect_protocol;
pub mod crypto;
pub mod distributed_config;
pub mod eager_assignor;
pub mod exceptions;
pub mod extended_assignment;
pub mod extended_worker_state;
pub mod incremental_cooperative_assignor;
pub mod incremental_cooperative_connect_protocol;
pub mod worker_group_member;
pub mod worker_rebalance_listener;

// Note: worker_coordinator will be implemented in kafka-clients-mock
// pub mod worker_coordinator;

// Re-export DistributedHerder from herder module for Java 1:1 API correspondence.
// Java: org.apache.kafka.connect.runtime.distributed.DistributedHerder
// Rust: connect_runtime::distributed::DistributedHerder (via this re-export)
pub use crate::herder::DistributedHerder;
