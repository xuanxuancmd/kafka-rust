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

use common_trait::TopicPartition;

/// ConnectorClientConfigRequest represents a request for connector client config.
///
/// This corresponds to `org.apache.kafka.connect.connector.policy.ConnectorClientConfigRequest` in Java.
#[derive(Debug, Clone)]
pub struct ConnectorClientConfigRequest {
    connector_name: String,
    connector_type: ConnectorType,
    connector_config: std::collections::HashMap<String, String>,
    client_configs: std::collections::HashMap<String, String>,
    topic_partitions: Vec<TopicPartition>,
}

/// ConnectorType represents the type of connector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorType {
    Source,
    Sink,
}

impl ConnectorClientConfigRequest {
    /// Creates a new ConnectorClientConfigRequest.
    pub fn new(
        connector_name: impl Into<String>,
        connector_type: ConnectorType,
        connector_config: std::collections::HashMap<String, String>,
        client_configs: std::collections::HashMap<String, String>,
        topic_partitions: Vec<TopicPartition>,
    ) -> Self {
        ConnectorClientConfigRequest {
            connector_name: connector_name.into(),
            connector_type,
            connector_config,
            client_configs,
            topic_partitions,
        }
    }

    pub fn connector_name(&self) -> &str {
        &self.connector_name
    }

    pub fn connector_type(&self) -> ConnectorType {
        self.connector_type
    }

    pub fn connector_config(&self) -> &std::collections::HashMap<String, String> {
        &self.connector_config
    }

    pub fn client_configs(&self) -> &std::collections::HashMap<String, String> {
        &self.client_configs
    }

    pub fn topic_partitions(&self) -> &[TopicPartition] {
        &self.topic_partitions
    }
}
