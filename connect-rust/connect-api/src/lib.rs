//! Kafka Connect API
//!
//! This crate defines the core interfaces and data models for Kafka Connect.

pub mod config;
pub mod connector;
pub mod data;
pub mod error;

pub use config::{Config, ConfigDef, ConfigValue};
pub use connector::{
    Closeable, Configurable, Connector, ConnectorContext, Predicate, RecordMetadata, SinkConnector,
    SinkTask, SinkTaskContext, SourceConnector, SourceTask, SourceTaskContext, Task,
    TopicPartition, Transformation, Versioned,
};
pub use data::{
    ConcreteHeader, ConcreteHeaders, ConnectRecord, ConnectSchema, Field, Header, Headers, Schema,
    SchemaAndValue, SchemaBuilder, SchemaType, SinkRecord, SourceRecord, Struct, Type,
};
pub use error::{ConnectError, ConnectException, DataException, RetriableError};
