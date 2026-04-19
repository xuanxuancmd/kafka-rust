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

use common_trait::metrics::PluginMetrics;
use common_trait::TopicPartition;
use kafka_clients_mock::OffsetAndMetadata;

/// SinkTaskContext trait for sink task context.
///
/// This corresponds to `org.apache.kafka.connect.sink.SinkTaskContext` in Java.
pub trait SinkTaskContext {
    /// Returns the offset for the given topic partition.
    fn offset(&self, topic_partition: &TopicPartition) -> Option<&OffsetAndMetadata>;

    /// Requests a timeout.
    fn request_timeout(&mut self, timeout_ms: i64);

    /// Sets the timeout for the next batch.
    fn timeout(&self) -> i64;

    /// Returns the plugin metrics.
    fn plugin_metrics(&self) -> Option<&dyn PluginMetrics>;
}
