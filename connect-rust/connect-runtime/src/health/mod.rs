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

//! Health module for Kafka Connect runtime.
//!
//! This module provides implementations of health-related interfaces
//! that correspond to `org.apache.kafka.connect.runtime.health` in Java.
//!
//! # Components
//!
//! - **ConnectClusterDetailsImpl**: Implementation of ConnectClusterDetails
//!   providing Kafka cluster ID information
//! - **ConnectClusterStateImpl**: Implementation of ConnectClusterState
//!   providing connector metadata and cluster state access

mod connect_cluster_details_impl;
mod connect_cluster_state_impl;

pub use connect_cluster_details_impl::*;
pub use connect_cluster_state_impl::*;