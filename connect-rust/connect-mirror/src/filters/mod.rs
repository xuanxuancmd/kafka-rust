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

//! Filter traits and implementations for MirrorMaker.
//!
//! This module provides unified filter implementations for MirrorMaker:
//! - [`TopicFilter`] - Determines which topics should be replicated
//! - [`GroupFilter`] - Determines which consumer groups should be replicated
//! - [`ConfigPropertyFilter`] - Determines which config properties should be replicated
//!
//! Corresponds to Java package: org.apache.kafka.connect.mirror

pub mod config_property_filter;
pub mod group_filter;
pub mod topic_filter;

pub use config_property_filter::*;
pub use group_filter::*;
pub use topic_filter::*;