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

//! Implementation of ConnectClusterDetails.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.health.ConnectClusterDetailsImpl` in Java.

/// Implementation of ConnectClusterDetails that provides immutable Connect cluster information.
///
/// This is a simple implementation that holds the Kafka cluster ID.
/// In Java, this is implemented as a record: `record ConnectClusterDetailsImpl(String kafkaClusterId)`.
pub struct ConnectClusterDetailsImpl {
    kafka_cluster_id: String,
}

impl ConnectClusterDetailsImpl {
    /// Creates a new ConnectClusterDetailsImpl with the given Kafka cluster ID.
    ///
    /// # Arguments
    ///
    /// * `kafka_cluster_id` - The ID of the Kafka cluster backing this Connect cluster
    pub fn new(kafka_cluster_id: impl Into<String>) -> Self {
        ConnectClusterDetailsImpl {
            kafka_cluster_id: kafka_cluster_id.into(),
        }
    }

    /// Get the cluster ID of the Kafka cluster backing this Connect cluster.
    ///
    /// # Returns
    ///
    /// The Kafka cluster ID string
    pub fn kafka_cluster_id(&self) -> &str {
        &self.kafka_cluster_id
    }
}

impl Clone for ConnectClusterDetailsImpl {
    fn clone(&self) -> Self {
        ConnectClusterDetailsImpl {
            kafka_cluster_id: self.kafka_cluster_id.clone(),
        }
    }
}

impl std::fmt::Debug for ConnectClusterDetailsImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectClusterDetailsImpl")
            .field("kafka_cluster_id", &self.kafka_cluster_id)
            .finish()
    }
}

impl PartialEq for ConnectClusterDetailsImpl {
    fn eq(&self, other: &Self) -> bool {
        self.kafka_cluster_id == other.kafka_cluster_id
    }
}

impl Eq for ConnectClusterDetailsImpl {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let details = ConnectClusterDetailsImpl::new("test-cluster-id");
        assert_eq!(details.kafka_cluster_id(), "test-cluster-id");
    }

    #[test]
    fn test_clone() {
        let details = ConnectClusterDetailsImpl::new("cluster-123");
        let cloned = details.clone();
        assert_eq!(details, cloned);
    }

    #[test]
    fn test_debug() {
        let details = ConnectClusterDetailsImpl::new("abc123");
        let debug_str = format!("{:?}", details);
        assert!(debug_str.contains("ConnectClusterDetailsImpl"));
        assert!(debug_str.contains("abc123"));
    }
}
