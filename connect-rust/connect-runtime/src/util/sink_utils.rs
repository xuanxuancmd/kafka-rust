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

//! Sink connector utility functions.
//!
//! This corresponds to `org.apache.kafka.connect.util.SinkUtils` in Java.

use common_trait::TopicPartition;
use std::collections::HashMap;

/// Key for Kafka topic in sink connector offsets.
pub const KAFKA_TOPIC_KEY: &str = "kafka_topic";

/// Key for Kafka partition in sink connector offsets.
pub const KAFKA_PARTITION_KEY: &str = "kafka_partition";

/// Key for Kafka offset in sink connector offsets.
pub const KAFKA_OFFSET_KEY: &str = "kafka_offset";

/// Sink connector utility functions.
///
/// This corresponds to `org.apache.kafka.connect.util.SinkUtils` in Java.
pub struct SinkUtils;

impl SinkUtils {
    /// Returns the consumer group ID for a sink connector.
    ///
    /// The format is: `connect-{connector_name}`
    pub fn consumer_group_id(connector: &str) -> String {
        format!("connect-{}", connector)
    }

    /// Converts consumer group offsets to connector offsets format.
    ///
    /// # Arguments
    /// * `consumer_group_offsets` - Map of TopicPartition to OffsetAndMetadata
    ///
    /// # Returns
    /// A list of connector offsets in the format expected by the REST API
    pub fn consumer_group_offsets_to_connector_offsets(
        consumer_group_offsets: HashMap<TopicPartition, i64>,
    ) -> Vec<(HashMap<String, String>, HashMap<String, i64>)> {
        consumer_group_offsets
            .into_iter()
            .map(|(tp, offset)| {
                let partition = HashMap::from([
                    (KAFKA_TOPIC_KEY.to_string(), tp.topic().to_string()),
                    (KAFKA_PARTITION_KEY.to_string(), tp.partition().to_string()),
                ]);
                let offset_map = HashMap::from([(KAFKA_OFFSET_KEY.to_string(), offset)]);
                (partition, offset_map)
            })
            .collect()
    }

    /// Parses sink connector offsets from the REST API format.
    ///
    /// Validates that partitions contain `kafka_topic` and `kafka_partition` keys,
    /// and that offsets contain `kafka_offset` key (or are null for offset reset).
    ///
    /// # Arguments
    /// * `partition_offsets` - Map of partition maps to offset maps
    ///
    /// # Returns
    /// A map of TopicPartition to offset values (or None for reset)
    ///
    /// # Errors
    /// Returns an error message if the format is invalid
    pub fn parse_sink_connector_offsets(
        partition_offsets: impl IntoIterator<
            Item = (HashMap<String, String>, Option<HashMap<String, String>>),
        >,
    ) -> Result<HashMap<TopicPartition, Option<i64>>, String> {
        let mut result = HashMap::new();

        for (partition_map, offset_map) in partition_offsets {
            // Validate partition
            if partition_map.is_empty() {
                return Err(
                    "The partition for a sink connector offset cannot be null or missing"
                        .to_string(),
                );
            }

            let topic = partition_map.get(KAFKA_TOPIC_KEY);
            let partition = partition_map.get(KAFKA_PARTITION_KEY);

            if topic.is_none() || partition.is_none() {
                return Err(format!(
                    "The partition for a sink connector offset must contain the keys '{}' and '{}'",
                    KAFKA_TOPIC_KEY, KAFKA_PARTITION_KEY
                ));
            }

            let topic_str = topic.unwrap();
            if topic_str.is_empty() {
                return Err(
                    "Kafka topic names must be valid strings and may not be null".to_string(),
                );
            }

            let partition_num: i32 = match partition.unwrap().parse() {
                Ok(n) => n,
                Err(_) => return Err(format!(
                    "Failed to parse the following Kafka partition value in the provided offsets: '{}'. Partition values for sink connectors need to be integers.",
                    partition.unwrap()
                )),
            };

            let tp = TopicPartition::new(topic_str.clone(), partition_num);

            // Handle offset
            match offset_map {
                None => {
                    // Represents an offset reset
                    result.insert(tp, None);
                }
                Some(offsets) => {
                    if !offsets.contains_key(KAFKA_OFFSET_KEY) {
                        return Err(format!(
                            "The offset for a sink connector should either be null or contain the key '{}'",
                            KAFKA_OFFSET_KEY
                        ));
                    }

                    let offset_str = offsets.get(KAFKA_OFFSET_KEY).unwrap();
                    let offset: i64 = match offset_str.parse() {
                        Ok(n) => n,
                        Err(_) => return Err(format!(
                            "Failed to parse the following Kafka offset value in the provided offsets: '{}'. Offset values for sink connectors need to be integers.",
                            offset_str
                        )),
                    };

                    result.insert(tp, Some(offset));
                }
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;

    #[test]
    fn test_consumer_group_id() {
        assert_eq!(
            SinkUtils::consumer_group_id("my-connector"),
            "connect-my-connector"
        );
        assert_eq!(SinkUtils::consumer_group_id("test"), "connect-test");
    }

    #[test]
    fn test_consumer_group_offsets_to_connector_offsets() {
        let mut offsets = HashMap::new();
        offsets.insert(TopicPartition::new("topic1", 0), 100);
        offsets.insert(TopicPartition::new("topic1", 1), 200);

        let result = SinkUtils::consumer_group_offsets_to_connector_offsets(offsets);
        assert_eq!(result.len(), 2);

        for (partition, offset) in &result {
            assert!(partition.contains_key(KAFKA_TOPIC_KEY));
            assert!(partition.contains_key(KAFKA_PARTITION_KEY));
            assert!(offset.contains_key(KAFKA_OFFSET_KEY));
        }
    }

    #[test]
    fn test_parse_sink_connector_offsets_valid() {
        let partition1 = HashMap::from([
            (KAFKA_TOPIC_KEY.to_string(), "topic1".to_string()),
            (KAFKA_PARTITION_KEY.to_string(), "0".to_string()),
        ]);
        let offset1 = Some(HashMap::from([(
            KAFKA_OFFSET_KEY.to_string(),
            "100".to_string(),
        )]));

        let partition2 = HashMap::from([
            (KAFKA_TOPIC_KEY.to_string(), "topic1".to_string()),
            (KAFKA_PARTITION_KEY.to_string(), "1".to_string()),
        ]);
        let offset2 = None; // Reset

        let input = vec![(partition1, offset1), (partition2, offset2)];

        let result = SinkUtils::parse_sink_connector_offsets(input).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(
            result.get(&TopicPartition::new("topic1", 0)),
            Some(&Some(100))
        );
        assert_eq!(result.get(&TopicPartition::new("topic1", 1)), Some(&None));
    }

    #[test]
    fn test_parse_sink_connector_offsets_missing_keys() {
        let partition = HashMap::from([(KAFKA_TOPIC_KEY.to_string(), "topic1".to_string())]);
        let offset = Some(HashMap::from([(
            KAFKA_OFFSET_KEY.to_string(),
            "100".to_string(),
        )]));

        let input = vec![(partition, offset)];

        let result = SinkUtils::parse_sink_connector_offsets(input);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains(KAFKA_PARTITION_KEY));
    }

    #[test]
    fn test_parse_sink_connector_offsets_missing_offset_key() {
        let partition = HashMap::from([
            (KAFKA_TOPIC_KEY.to_string(), "topic1".to_string()),
            (KAFKA_PARTITION_KEY.to_string(), "0".to_string()),
        ]);
        let offset = Some(HashMap::from([(
            "wrong_key".to_string(),
            "100".to_string(),
        )]));

        let input = vec![(partition, offset)];

        let result = SinkUtils::parse_sink_connector_offsets(input);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains(KAFKA_OFFSET_KEY));
    }
}
