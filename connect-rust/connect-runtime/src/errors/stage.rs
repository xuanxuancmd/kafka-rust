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

//! Stage enum represents different logical stages within the overall pipeline
//! of an operation where errors can occur.
//!
//! This corresponds to `org.apache.kafka.connect.runtime.errors.Stage` in Java.

use std::fmt;

/// The stage in the processing pipeline where an error occurred.
///
/// This is used to determine what exceptions are tolerable for each stage
/// when `errors.tolerance=all` is configured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Stage {
    /// Transformation stage - applying transformations to records
    TRANSFORMATION,
    /// Key converter stage - converting key data
    KEY_CONVERTER,
    /// Value converter stage - converting value data  
    VALUE_CONVERTER,
    /// Header converter stage - converting header data
    HEADER_CONVERTER,
    /// Kafka produce stage - producing records to Kafka
    KAFKA_PRODUCE,
    /// Kafka consume stage - consuming records from Kafka
    KAFKA_CONSUME,
}

impl Stage {
    /// Returns a human-readable name for this stage.
    pub fn name(&self) -> &'static str {
        match self {
            Stage::TRANSFORMATION => "TRANSFORMATION",
            Stage::KEY_CONVERTER => "KEY_CONVERTER",
            Stage::VALUE_CONVERTER => "VALUE_CONVERTER",
            Stage::HEADER_CONVERTER => "HEADER_CONVERTER",
            Stage::KAFKA_PRODUCE => "KAFKA_PRODUCE",
            Stage::KAFKA_CONSUME => "KAFKA_CONSUME",
        }
    }
}

impl fmt::Display for Stage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stage_name() {
        assert_eq!(Stage::TRANSFORMATION.name(), "TRANSFORMATION");
        assert_eq!(Stage::KEY_CONVERTER.name(), "KEY_CONVERTER");
        assert_eq!(Stage::VALUE_CONVERTER.name(), "VALUE_CONVERTER");
        assert_eq!(Stage::HEADER_CONVERTER.name(), "HEADER_CONVERTER");
        assert_eq!(Stage::KAFKA_PRODUCE.name(), "KAFKA_PRODUCE");
        assert_eq!(Stage::KAFKA_CONSUME.name(), "KAFKA_CONSUME");
    }

    #[test]
    fn test_stage_display() {
        assert_eq!(format!("{}", Stage::TRANSFORMATION), "TRANSFORMATION");
        assert_eq!(format!("{}", Stage::KEY_CONVERTER), "KEY_CONVERTER");
    }

    #[test]
    fn test_stage_equality() {
        assert_eq!(Stage::TRANSFORMATION, Stage::TRANSFORMATION);
        assert_ne!(Stage::TRANSFORMATION, Stage::KEY_CONVERTER);
    }

    #[test]
    fn test_stage_clone() {
        let stage = Stage::VALUE_CONVERTER;
        let cloned = stage.clone();
        assert_eq!(stage, cloned);
    }
}
