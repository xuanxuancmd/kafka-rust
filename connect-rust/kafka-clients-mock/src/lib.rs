//! Kafka Clients Mock Implementation
//!
//! This crate provides in-memory mock implementations of Kafka clients for testing.

pub mod producer;
pub mod consumer;
pub mod admin;
pub mod config;
pub mod serialization;

pub use producer::MockProducer;
pub use consumer::MockConsumer;
pub use admin::MockAdminClient;
pub use config::MockConfigDef;
pub use serialization::{MockSerializer, MockDeserializer};
