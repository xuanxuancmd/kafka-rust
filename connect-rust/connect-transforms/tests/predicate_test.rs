//! Unit tests for predicates

use connect_api::connector::{Closeable, Configurable};
use connect_api::{ConnectRecord, Predicate};
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
        use std::sync::LazyLock;
        static EMPTY_HEADERS: LazyLock<connect_api::ConcreteHeaders> =
            LazyLock::new(|| connect_api::ConcreteHeaders::new());
        &*EMPTY_HEADERS
    }
}

// Mock predicate for testing
#[derive(Debug, Clone)]
struct MockPredicate {
    always_true: bool,
}

impl MockPredicate {
    fn new(always_true: bool) -> Self {
        Self { always_true }
    }
}

impl Configurable for MockPredicate {
    fn configure(&mut self, _configs: HashMap<String, Box<dyn std::any::Any>>) {
        // No configuration needed
    }
}

impl Closeable for MockPredicate {
    fn close(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}

impl Predicate<MockRecord> for MockPredicate {
    fn config(&self) -> connect_api::ConfigDef {
        connect_api::ConfigDef::new()
    }

    fn test(&self, _record: MockRecord) -> bool {
        self.always_true
    }
}

#[test]
fn test_predicate_trait_reexport() {
    // Test that Predicate trait is properly re-exported
    let predicate = MockPredicate::new(true);
    assert!(predicate.test(MockRecord {
        topic: "test".to_string(),
    }));

    let predicate = MockPredicate::new(false);
    assert!(!predicate.test(MockRecord {
        topic: "test".to_string(),
    }));
}

#[test]
fn test_predicate_configure() {
    let mut predicate = MockPredicate::new(true);
    let configs: HashMap<String, Box<dyn std::any::Any>> = HashMap::new();
    predicate.configure(configs);
    // Should not panic
}

#[test]
fn test_predicate_close() {
    let mut predicate = MockPredicate::new(true);
    assert!(predicate.close().is_ok());
}

#[test]
fn test_predicate_config() {
    let predicate = MockPredicate::new(true);
    let config = predicate.config();
    // ConfigDef should be created without panicking
    let _ = config;
}

#[test]
fn test_mock_predicate_always_true() {
    let predicate = MockPredicate::new(true);
    for i in 0..10 {
        assert!(predicate.test(MockRecord {
            topic: format!("topic-{}", i),
        }));
    }
}

#[test]
fn test_mock_predicate_always_false() {
    let predicate = MockPredicate::new(false);
    for i in 0..10 {
        assert!(!predicate.test(MockRecord {
            topic: format!("topic-{}", i),
        }));
    }
}

#[test]
fn test_predicate_trait_bounds() {
    // Test that Predicate trait has correct bounds
    // This is a compile-time test to ensure trait bounds are correct
    fn accepts_predicate<P: Predicate<MockRecord>>(_predicate: &P) {}

    let predicate = MockPredicate::new(true);
    accepts_predicate(&predicate);
}
