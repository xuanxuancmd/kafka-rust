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

/// TimestampType represents the type of timestamp in a Kafka record.
///
/// This corresponds to `org.apache.kafka.common.record.TimestampType` in Java.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampType {
    NoTimestampType,
    CreateTime,
    LogAppendTime,
}

impl TimestampType {
    /// Returns the name of this timestamp type.
    pub fn name(&self) -> &'static str {
        match self {
            TimestampType::NoTimestampType => "NoTimestampType",
            TimestampType::CreateTime => "CreateTime",
            TimestampType::LogAppendTime => "LogAppendTime",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_name() {
        assert_eq!(TimestampType::NoTimestampType.name(), "NoTimestampType");
        assert_eq!(TimestampType::CreateTime.name(), "CreateTime");
        assert_eq!(TimestampType::LogAppendTime.name(), "LogAppendTime");
    }
}
