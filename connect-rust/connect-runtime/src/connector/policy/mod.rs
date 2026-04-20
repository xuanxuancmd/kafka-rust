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

//! Connector client config override policy module.
//!
//! This module provides policies for controlling which client configurations
//! can be overridden by connectors.
//!
//! # Components
//!
//! - **AbstractConnectorClientConfigOverridePolicy**: Base class for all policies
//! - **AllConnectorClientConfigOverridePolicy**: Allows all configurations
//! - **AllowlistConnectorClientConfigOverridePolicy**: Only allows whitelisted configs
//! - **NoneConnectorClientConfigOverridePolicy**: Disallows all configurations
//! - **PrincipalConnectorClientConfigOverridePolicy**: Allows SASL configs (deprecated)
//!
//! Corresponds to `org.apache.kafka.connect.connector.policy` in Java.

mod abstract_connector_client_config_override_policy;
mod all_connector_client_config_override_policy;
mod allowlist_connector_client_config_override_policy;
mod none_connector_client_config_override_policy;
mod principal_connector_client_config_override_policy;

pub use abstract_connector_client_config_override_policy::*;
pub use all_connector_client_config_override_policy::*;
pub use allowlist_connector_client_config_override_policy::*;
pub use none_connector_client_config_override_policy::*;
pub use principal_connector_client_config_override_policy::*;
