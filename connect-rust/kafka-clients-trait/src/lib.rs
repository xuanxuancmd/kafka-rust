//! Kafka Clients Trait Definitions
//!
//! This crate defines core trait interfaces for Kafka clients (Producer, Consumer, AdminClient).

pub mod admin;
pub mod config;
pub mod connect;
pub mod consumer;
pub mod producer;
pub mod serialization;

pub use admin::{ForwardingAdmin, KafkaAdmin, KafkaAdminSync};
pub use config::{
    Config, ConfigDef, ConfigImportance, ConfigKey, ConfigKeyBuilder, ConfigType, ConfigValue,
};
pub use connect::{
    Callback, Consumer, ConsumerRecord, Future, KafkaBasedLog, Predicate, ProducerCallback,
    RecordMetadata, Supplier, Time, TimestampType, TopicAdmin, TopicMetadata,
};
pub use consumer::{KafkaConsumer, KafkaConsumerSync};
pub use producer::KafkaProducer;
