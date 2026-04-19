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

use std::fmt;
use std::hash::{Hash, Hasher};

/// TopicPartition represents a topic and partition pair.
///
/// This corresponds to `org.apache.kafka.common.TopicPartition` in Java.
#[derive(Debug, Clone, Eq)]
pub struct TopicPartition {
    topic: String,
    partition: i32,
}

impl TopicPartition {
    /// Creates a new TopicPartition with the given topic and partition.
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        TopicPartition {
            topic: topic.into(),
            partition,
        }
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the partition number.
    pub fn partition(&self) -> i32 {
        self.partition
    }
}

impl fmt::Display for TopicPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.topic, self.partition)
    }
}

impl PartialEq for TopicPartition {
    fn eq(&self, other: &Self) -> bool {
        self.topic == other.topic && self.partition == other.partition
    }
}

impl Hash for TopicPartition {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.topic.hash(state);
        self.partition.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let tp = TopicPartition::new("test-topic", 0);
        assert_eq!(tp.topic(), "test-topic");
        assert_eq!(tp.partition(), 0);
    }

    #[test]
    fn test_display() {
        let tp = TopicPartition::new("test-topic", 5);
        assert_eq!(format!("{}", tp), "test-topic:5");
    }

    #[test]
    fn test_eq() {
        let tp1 = TopicPartition::new("test-topic", 0);
        let tp2 = TopicPartition::new("test-topic", 0);
        let tp3 = TopicPartition::new("test-topic", 1);
        assert_eq!(tp1, tp2);
        assert_ne!(tp1, tp3);
    }

    #[test]
    fn test_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        let tp = TopicPartition::new("test-topic", 0);
        set.insert(tp.clone());
        assert!(set.contains(&tp));
    }
}
