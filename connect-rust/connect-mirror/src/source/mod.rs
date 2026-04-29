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

//! Source module for MirrorMaker source connector.
//!
//! This module provides configuration and filtering classes for the
//! MirrorSourceConnector, including MirrorSourceConfig, TopicFilter,
//! and ConfigPropertyFilter.
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror sources

pub mod config_property_filter;
pub mod mirror_source_config;
pub mod mirror_source_connector;
pub mod mirror_source_task;
pub mod topic_filter;

pub use config_property_filter::*;
pub use mirror_source_config::*;
pub use mirror_source_connector::*;
pub use mirror_source_task::*;
pub use topic_filter::*;
