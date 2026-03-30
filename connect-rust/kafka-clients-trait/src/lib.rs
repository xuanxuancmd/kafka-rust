//! Kafka Clients Trait Definitions
//!
//! This crate defines the core trait interfaces for Kafka clients (Producer, Consumer, AdminClient).

pub mod producer;
pub mod consumer;
pub mod admin;
pub mod config;
pub mod serialization;

pub use producer::KafkaProducer;
pub use consumer::KafkaConsumer;
pub use admin::KafkaAdmin;
pub use config::ConfigDef;
pub use serialization::{Serializer, Deserializer};
