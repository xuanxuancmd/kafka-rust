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

//! Plugin isolation module for Kafka Connect runtime.
//!
//! This module provides plugin isolation capabilities including:
//! - Plugin discovery and scanning
//! - Plugin class loading and isolation
//! - Plugin type management

pub mod class_loader_factory;
pub mod delegating_class_loader;
pub mod plugin_class_loader;
pub mod plugin_class_loader_factory;
pub mod plugin_desc;
pub mod plugin_discovery_mode;
pub mod plugin_scan_result;
pub mod plugin_scanner;
pub mod plugin_source;
pub mod plugin_type;
pub mod plugin_utils;
pub mod plugins;
pub mod plugins_recommenders;
pub mod reflection_scanner;
pub mod service_loader_scanner;
pub mod test_plugins;
pub mod versioned_plugin_loading_exception;

// Re-export key types for convenience
pub use plugin_desc::{PluginDesc, UNDEFINED_VERSION};
pub use plugin_type::PluginType;
