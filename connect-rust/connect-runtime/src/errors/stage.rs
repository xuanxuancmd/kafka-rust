//! Stage module
//!
//! Provides the Stage enum for logical stages in a Connect pipeline.

/// A logical stage in a Connect pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Stage {
    /// When calling SourceTask::poll()
    TaskPoll,

    /// When calling SinkTask::put()
    TaskPut,

    /// When running any transformation operation on a record
    Transformation,

    /// When using the key converter to serialize/deserialize keys in ConnectRecords
    KeyConverter,

    /// When using the value converter to serialize/deserialize values in ConnectRecords
    ValueConverter,

    /// When using the header converter to serialize/deserialize headers in ConnectRecords
    HeaderConverter,

    /// When producing to a Kafka topic
    KafkaProduce,

    /// When consuming from a Kafka topic
    KafkaConsume,
}

impl Stage {
    /// Get the stage name as a string
    pub fn name(&self) -> &'static str {
        match self {
            Stage::TaskPoll => "TASK_POLL",
            Stage::TaskPut => "TASK_PUT",
            Stage::Transformation => "TRANSFORMATION",
            Stage::KeyConverter => "KEY_CONVERTER",
            Stage::ValueConverter => "VALUE_CONVERTER",
            Stage::HeaderConverter => "HEADER_CONVERTER",
            Stage::KafkaProduce => "KAFKA_PRODUCE",
            Stage::KafkaConsume => "KAFKA_CONSUME",
        }
    }
}

impl std::fmt::Display for Stage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
