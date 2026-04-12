/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::errors::BadRequestException;
use crate::runtime::rest::entities::{ConnectorOffset, ConnectorOffsets};
use kafka_clients_trait::consumer::{OffsetAndMetadata, TopicPartition};
use std::collections::HashMap;

/// Utility methods for sink connectors
pub struct SinkUtils;

impl SinkUtils {
    /// Kafka topic key
    pub const KAFKA_TOPIC_KEY: &str = "kafka_topic";
    /// Kafka partition key
    pub const KAFKA_PARTITION_KEY: &str = "kafka_partition";
    /// Kafka offset key
    pub const KAFKA_OFFSET_KEY: &str = "kafka_offset";

    /// Get consumer group ID for a connector
    pub fn consumer_group_id(connector: &str) -> String {
        format!("connect-{}", connector)
    }

    /// Convert consumer group offsets to connector offsets
    pub fn consumer_group_offsets_to_connector_offsets(
        consumer_group_offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> ConnectorOffsets {
        let mut connector_offsets = Vec::new();

        for (topic_partition, offset_and_metadata) in consumer_group_offsets {
            let mut partition = HashMap::new();
            partition.insert(
                Self::KAFKA_TOPIC_KEY.to_string(),
                Box::new(topic_partition.topic().to_string()),
            );
            partition.insert(
                Self::KAFKA_PARTITION_KEY.to_string(),
                Box::new(topic_partition.partition()),
            );

            let mut offset_map = HashMap::new();
            offset_map.insert(
                Self::KAFKA_OFFSET_KEY.to_string(),
                Box::new(offset_and_metadata.offset()),
            );

            connector_offsets.push(ConnectorOffset::new(partition, offset_map));
        }

        ConnectorOffsets::new(connector_offsets)
    }

    /// Parse sink connector offsets from REST API format
    pub fn parse_sink_connector_offsets(
        partition_offsets: &HashMap<
            HashMap<String, Box<dyn std::any::Any>>,
            HashMap<String, Box<dyn std::any::Any>>,
        >,
    ) -> Result<HashMap<TopicPartition, Option<i64>>, BadRequestException> {
        let mut parsed_offset_map = HashMap::new();

        for (partition_map, offset_map) in partition_offsets {
            if partition_map.is_empty() {
                return Err(BadRequestException::new(
                    "The partition for a sink connector offset cannot be null or missing"
                        .to_string(),
                ));
            }

            if !partition_map.contains_key(Self::KAFKA_TOPIC_KEY)
                || !partition_map.contains_key(Self::KAFKA_PARTITION_KEY)
            {
                return Err(BadRequestException::new(format!(
                    "The partition for a sink connector offset must contain the keys '{}' and '{}'",
                    Self::KAFKA_TOPIC_KEY,
                    Self::KAFKA_PARTITION_KEY
                )));
            }

            let topic = partition_map
                .get(Self::KAFKA_TOPIC_KEY)
                .map(|v| v.to_string())
                .ok_or_else(|| {
                    BadRequestException::new(
                        "Kafka topic names must be valid strings and may not be null".to_string(),
                    )
                })?;

            let partition = partition_map
                .get(Self::KAFKA_PARTITION_KEY)
                .and_then(|v| {
                    v.to_string()
                        .parse::<i32>()
                        .map_err(|_| {
                            BadRequestException::new(format!(
                                "Failed to parse the following Kafka partition value in the provided offsets: '{}'. Partition values for sink connectors need to be integers.",
                                v.to_string()
                            ))
                        })
                })
                .transpose()?
                .ok_or_else(|| {
                    BadRequestException::new("Kafka partitions must be valid numbers and may not be null".to_string())
                })?;

            let topic_partition = TopicPartition::new(topic, partition);

            let offset = if offset_map.is_empty() {
                // Represents an offset reset
                None
            } else {
                if !offset_map.contains_key(Self::KAFKA_OFFSET_KEY) {
                    return Err(BadRequestException::new(format!(
                        "The offset for a sink connector should either be null or contain the key '{}'",
                        Self::KAFKA_OFFSET_KEY
                    )));
                }

                let offset_value = offset_map
                    .get(Self::KAFKA_OFFSET_KEY)
                    .map(|v| v.to_string())
                    .ok_or_else(|| {
                        BadRequestException::new("Offset values must be valid strings".to_string())
                    })?;

                Some(offset_value.parse::<i64>().map_err(|_| {
                    BadRequestException::new(format!(
                        "Failed to parse the following Kafka offset value in the provided offsets: '{}'. Offset values for sink connectors need to be integers.",
                        offset_value
                    ))
                })?)
            };

            parsed_offset_map.insert(topic_partition, offset);
        }

        Ok(parsed_offset_map)
    }
}
