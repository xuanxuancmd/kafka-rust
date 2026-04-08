//! Unit tests for simple transformations

use connect_api::data::ConcreteHeaders;
use connect_api::{Closeable, Configurable, ConnectRecord, Transformation};
use connect_transforms::simple::{DropHeaders, Filter, InsertHeader, TimestampRouter, ValueToKey};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

// Mock record for testing
#[derive(Debug, Clone)]
struct MockRecord {
    topic: String,
}

impl ConnectRecord<MockRecord> for MockRecord {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn kafka_partition(&self) -> Option<i32> {
        Some(0)
    }

    fn key_schema(&self) -> Option<Arc<dyn connect_api::Schema>> {
        None
    }

    fn key(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        None
    }

    fn value_schema(&self) -> Option<Arc<dyn connect_api::Schema>> {
        None
    }

    fn value(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        None
    }

    fn timestamp(&self) -> Option<i64> {
        None
    }

    fn headers(&self) -> &dyn connect_api::Headers {
        use std::sync::OnceLock;
        static EMPTY_HEADERS: OnceLock<ConcreteHeaders> = OnceLock::new();
        EMPTY_HEADERS.get_or_init(|| ConcreteHeaders::new())
    }
}

#[test]
fn test_value_to_key_new() {
    let transformer = ValueToKey::<MockRecord>::new();
    // ValueToKey should be created successfully
    let _ = transformer;
}

#[test]
fn test_value_to_key_configure() {
    let mut transformer = ValueToKey::<MockRecord>::new();
    let configs: HashMap<String, Box<dyn std::any::Any>> = HashMap::new();
    transformer.configure(configs);
    // Configuration should not panic
}

#[test]
fn test_value_to_key_close() {
    let mut transformer = ValueToKey::<MockRecord>::new();
    assert!(transformer.close().is_ok());
}

#[test]
fn test_value_to_key_apply() {
    let mut transformer = ValueToKey::<MockRecord>::new();
    let record = MockRecord {
        topic: "test".to_string(),
    };
    let result = transformer.apply(record);
    assert!(result.is_ok());
    assert!(result.unwrap().is_some());
}

#[test]
fn test_value_to_key_config() {
    let transformer = ValueToKey::<MockRecord>::new();
    let config = transformer.config();
    // ConfigDef should be created without panicking
    let _ = config;
}

#[test]
fn test_timestamp_router_new() {
    let transformer = TimestampRouter::<MockRecord>::new();
    // TimestampRouter should be created successfully
    let _ = transformer;
}

#[test]
fn test_timestamp_router_configure() {
    let mut transformer = TimestampRouter::<MockRecord>::new();
    let configs: HashMap<String, Box<dyn std::any::Any>> = HashMap::new();
    transformer.configure(configs);
}

#[test]
fn test_timestamp_router_close() {
    let mut transformer = TimestampRouter::<MockRecord>::new();
    assert!(transformer.close().is_ok());
}

#[test]
fn test_timestamp_router_apply() {
    let mut transformer = TimestampRouter::<MockRecord>::new();
    let record = MockRecord {
        topic: "test".to_string(),
    };
    let result = transformer.apply(record);
    assert!(result.is_ok());
    assert!(result.unwrap().is_some());
}

#[test]
fn test_timestamp_router_config() {
    let transformer = TimestampRouter::<MockRecord>::new();
    let config = transformer.config();
    let _ = config;
}

#[test]
fn test_insert_header_new() {
    let transformer = InsertHeader::<MockRecord>::new();
    // InsertHeader should be created successfully
    let _ = transformer;
}

#[test]
fn test_insert_header_configure() {
    let mut transformer = InsertHeader::<MockRecord>::new();
    let configs: HashMap<String, Box<dyn std::any::Any>> = HashMap::new();
    transformer.configure(configs);
}

#[test]
fn test_insert_header_close() {
    let mut transformer = InsertHeader::<MockRecord>::new();
    assert!(transformer.close().is_ok());
}

#[test]
fn test_insert_header_apply() {
    let mut transformer = InsertHeader::<MockRecord>::new();
    let record = MockRecord {
        topic: "test".to_string(),
    };
    let result = transformer.apply(record);
    assert!(result.is_ok());
    assert!(result.unwrap().is_some());
}

#[test]
fn test_insert_header_config() {
    let transformer = InsertHeader::<MockRecord>::new();
    let config = transformer.config();
    let _ = config;
}

#[test]
fn test_filter_new() {
    let transformer = Filter::<MockRecord>::new();
    // Filter should be created successfully
    let _ = transformer;
}

#[test]
fn test_filter_configure() {
    let mut transformer = Filter::<MockRecord>::new();
    let configs: HashMap<String, Box<dyn std::any::Any>> = HashMap::new();
    transformer.configure(configs);
}

#[test]
fn test_filter_close() {
    let mut transformer = Filter::<MockRecord>::new();
    assert!(transformer.close().is_ok());
}

#[test]
fn test_filter_apply() {
    let mut transformer = Filter::<MockRecord>::new();
    let record = MockRecord {
        topic: "test".to_string(),
    };
    let result = transformer.apply(record);
    assert!(result.is_ok());
    // Filter should always return None
    assert!(result.unwrap().is_none());
}

#[test]
fn test_filter_config() {
    let transformer = Filter::<MockRecord>::new();
    let config = transformer.config();
    let _ = config;
}

#[test]
fn test_drop_headers_new() {
    let transformer = DropHeaders::<MockRecord>::new();
    // DropHeaders should be created successfully
    let _ = transformer;
}

#[test]
fn test_drop_headers_configure() {
    let mut transformer = DropHeaders::<MockRecord>::new();
    let configs: HashMap<String, Box<dyn std::any::Any>> = HashMap::new();
    transformer.configure(configs);
}

#[test]
fn test_drop_headers_close() {
    let mut transformer = DropHeaders::<MockRecord>::new();
    assert!(transformer.close().is_ok());
}

#[test]
fn test_drop_headers_apply() {
    let mut transformer = DropHeaders::<MockRecord>::new();
    let record = MockRecord {
        topic: "test".to_string(),
    };
    let result = transformer.apply(record);
    assert!(result.is_ok());
    assert!(result.unwrap().is_some());
}

#[test]
fn test_drop_headers_config() {
    let transformer = DropHeaders::<MockRecord>::new();
    let config = transformer.config();
    let _ = config;
}
