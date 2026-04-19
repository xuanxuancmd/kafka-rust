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

use crate::connector::Connector;
use crate::errors::ConnectError;
use common_trait::TopicPartition;
use std::collections::HashMap;

/// Configuration key for the list of input topics for this connector.
pub const TOPICS_CONFIG: &str = "topics";

/// SinkConnector trait for sink connectors.
///
/// This corresponds to `org.apache.kafka.connect.sink.SinkConnector` in Java.
pub trait SinkConnector: Connector {
    /// Returns the task configs for the given topic partitions.
    fn task_configs_for_topics(&self, topics: &[TopicPartition]) -> Vec<HashMap<String, String>>;

    /// Invoked when users request to manually alter/reset the offsets for this connector.
    /// Connectors that manage offsets externally can propagate offset changes in this method.
    /// Returns true if this method has been overridden; false by default.
    fn alter_offsets(
        &self,
        connector_config: HashMap<String, String>,
        offsets: HashMap<TopicPartition, Option<i64>>,
    ) -> Result<bool, ConnectError> {
        Ok(false)
    }
}
