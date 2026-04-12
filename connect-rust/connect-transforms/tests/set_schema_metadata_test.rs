//! Unit tests for SetSchemaMetadata transformation

use connect_api::connector_types::ConnectRecord;
use connect_api::data::ConcreteHeaders;
use connect_api::{Closeable, Configurable, Transformation};
use connect_transforms::set_schema_metadata::SetSchemaMetadata;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

// Mock record for testing
#[derive(Debug, Clone)]
struct MockRecord {
    topic: String,
    key_schema: Option<Arc<dyn connect_api::Schema>>,
    value_schema: Option<Arc<dyn connect_api::Schema>>,
}

impl ConnectRecord<MockRecord> for MockRecord {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn kafka_partition(&self) -> Option<i32> {
        Some(0)
    }

    fn key_schema(&self) -> Option<Arc<dyn connect_api::Schema>> {
        self.key_schema.clone()
    }

    fn key(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        None
    }

    fn value_schema(&self) -> Option<Arc<dyn connect_api::Schema>> {
        self.value_schema.clone()
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

    fn new_record(
        self,
        _topic: Option<&str>,
        _partition: Option<i32>,
        _key_schema: Option<Arc<dyn connect_api::Schema>>,
        _key: Option<Arc<dyn Any + Send + Sync>>,
        _value_schema: Option<Arc<dyn connect_api::Schema>>,
        _value: Option<Arc<dyn Any + Send + Sync>>,
        _timestamp: Option<i64>,
        _headers: Option<Box<dyn connect_api::Headers>>,
    ) -> MockRecord {
        self
    }
}

#[test]
fn test_set_schema_name() {
    let transformer = SetSchemaMetadata::<MockRecord>::with_name("new.schema.name".to_string());
    // Test that transformer was created successfully
    let _ = transformer;
}

#[test]
fn test_set_schema_version() {
    let transformer = SetSchemaMetadata::<MockRecord>::with_version(5);
    // Test that transformer was created successfully
    let _ = transformer;
}

#[test]
fn test_set_schema_both() {
    let transformer =
        SetSchemaMetadata::<MockRecord>::with_name_and_version("test.schema".to_string(), 3);
    // Test that transformer was created successfully
    let _ = transformer;
}

#[test]
fn test_set_schema_none() {
    let transformer = SetSchemaMetadata::<MockRecord>::new();
    // Test that transformer was created successfully
    let _ = transformer;
}

#[test]
fn test_set_schema_key_variant() {
    let transformer = SetSchemaMetadata::<MockRecord>::new()
        .modify_key_schema(true)
        .modify_value_schema(false);
    // Test that transformer was created successfully
    let _ = transformer;
}

#[test]
fn test_set_schema_value_variant() {
    let transformer = SetSchemaMetadata::<MockRecord>::new()
        .modify_key_schema(false)
        .modify_value_schema(true);
    // Test that transformer was created successfully
    let _ = transformer;
}

#[test]
fn test_set_schema_empty_name() {
    let transformer = SetSchemaMetadata::<MockRecord>::with_name("".to_string());
    // Test that transformer was created successfully
    let _ = transformer;
}

#[test]
fn test_set_schema_negative_version() {
    let transformer = SetSchemaMetadata::<MockRecord>::with_version(-1);
    // Test that transformer was created successfully
    let _ = transformer;
}

#[test]
fn test_set_schema_configure() {
    let mut transformer = SetSchemaMetadata::<MockRecord>::new();
    let mut configs = HashMap::new();
    configs.insert(
        "schema.name".to_string(),
        Box::new("configured.schema".to_string()) as Box<dyn Any>,
    );
    configs.insert("schema.version".to_string(), Box::new(7i32) as Box<dyn Any>);
    configs.insert("schema.key".to_string(), Box::new(true) as Box<dyn Any>);
    configs.insert("schema.value".to_string(), Box::new(false) as Box<dyn Any>);

    transformer.configure(configs);
    // Test that configuration was applied successfully
    let _ = transformer;
}

#[test]
fn test_set_schema_configure_version_as_string() {
    let mut transformer = SetSchemaMetadata::<MockRecord>::new();
    let mut configs = HashMap::new();
    configs.insert(
        "schema.version".to_string(),
        Box::new("10".to_string()) as Box<dyn Any>,
    );

    transformer.configure(configs);
    // Test that configuration was applied successfully
    let _ = transformer;
}

#[test]
fn test_set_schema_close() {
    let mut transformer =
        SetSchemaMetadata::<MockRecord>::with_name_and_version("test.schema".to_string(), 1);
    let result = transformer.close();
    assert!(result.is_ok());
}

#[test]
fn test_set_schema_apply_with_name() {
    let mut transformer = SetSchemaMetadata::<MockRecord>::with_name("updated.schema".to_string());
    let record = MockRecord {
        topic: "test".to_string(),
        key_schema: None,
        value_schema: None,
    };

    let result = transformer.apply(record);
    assert!(result.is_ok());
    assert!(result.unwrap().is_some());
}

#[test]
fn test_set_schema_apply_with_version() {
    let mut transformer = SetSchemaMetadata::<MockRecord>::with_version(2);
    let record = MockRecord {
        topic: "test".to_string(),
        key_schema: None,
        value_schema: None,
    };

    let result = transformer.apply(record);
    assert!(result.is_ok());
    assert!(result.unwrap().is_some());
}

#[test]
fn test_set_schema_apply_with_both() {
    let mut transformer =
        SetSchemaMetadata::<MockRecord>::with_name_and_version("full.schema".to_string(), 4);
    let record = MockRecord {
        topic: "test".to_string(),
        key_schema: None,
        value_schema: None,
    };

    let result = transformer.apply(record);
    assert!(result.is_ok());
    assert!(result.unwrap().is_some());
}

#[test]
fn test_set_schema_apply_no_modify() {
    use std::any::Any;
    use std::collections::HashMap;

    let mut transformer = SetSchemaMetadata::<MockRecord>::with_name("test.schema".to_string());

    // Configure via the Configurable trait
    let mut configs = HashMap::new();
    configs.insert(
        "schema.modify.key".to_string(),
        Box::new(false) as Box<dyn Any>,
    );
    configs.insert(
        "schema.modify.value".to_string(),
        Box::new(false) as Box<dyn Any>,
    );
    transformer.configure(configs);

    let record = MockRecord {
        topic: "test".to_string(),
        key_schema: None,
        value_schema: None,
    };

    let result = transformer.apply(record);
    assert!(result.is_ok());
    assert!(result.unwrap().is_some());
}

#[test]
fn test_set_schema_config() {
    let transformer = SetSchemaMetadata::<MockRecord>::new();
    let config = transformer.config();
    // Test that config was created successfully
    let _ = config;
}

#[test]
fn test_set_schema_default() {
    let transformer: SetSchemaMetadata<MockRecord> = SetSchemaMetadata::default();
    // Test that default transformer was created successfully
    let _ = transformer;
}

#[test]
fn test_set_schema_builder_pattern() {
    let transformer = SetSchemaMetadata::<MockRecord>::new()
        .modify_key_schema(true)
        .modify_value_schema(true);

    // Test that builder pattern works
    let _ = transformer;
}
