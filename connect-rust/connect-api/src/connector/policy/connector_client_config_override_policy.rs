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

use crate::connector::policy::ConnectorClientConfigRequest;
use crate::errors::ConnectError;

/// ConnectorClientConfigOverridePolicy trait for connector client config override policy.
///
/// This corresponds to `org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy` in Java.
pub trait ConnectorClientConfigOverridePolicy {
    /// Validates the connector client config request.
    fn validate(&self, request: &ConnectorClientConfigRequest) -> Result<(), ConnectError>;

    /// Returns the overridden client configs.
    fn override_configs(
        &self,
        request: &ConnectorClientConfigRequest,
    ) -> std::collections::HashMap<String, String>;
}
