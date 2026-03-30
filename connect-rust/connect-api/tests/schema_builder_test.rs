//! Tests for SchemaBuilder

use connect_api::data::{SchemaBuilder, SchemaType};
use connect_api::Schema;

#[test]
fn test_int8_builder() {
    let schema = SchemaBuilder::int8().build().unwrap();
    assert_eq!(schema.type_(), SchemaType::Int8);
    assert!(!schema.is_optional());
    assert!(schema.default_value().is_none());
    assert!(schema.name().is_none());
    assert!(schema.version().is_none());
    assert!(schema.doc().is_none());
    assert!(schema.parameters().map_or(true, |p| p.is_empty()));
}

#[test]
fn test_int16_builder() {
    let schema = SchemaBuilder::int16().build().unwrap();
    assert_eq!(schema.type_(), SchemaType::Int16);
    assert!(!schema.is_optional());
    assert!(schema.default_value().is_none());
}

#[test]
fn test_int32_builder() {
    let schema = SchemaBuilder::int32().build().unwrap();
    assert_eq!(schema.type_(), SchemaType::Int32);
    assert!(!schema.is_optional());
    assert!(schema.default_value().is_none());
}

#[test]
fn test_int64_builder() {
    let schema = SchemaBuilder::int64().build().unwrap();
    assert_eq!(schema.type_(), SchemaType::Int64);
    assert!(!schema.is_optional());
    assert!(schema.default_value().is_none());
}

#[test]
fn test_float32_builder() {
    let schema = SchemaBuilder::float32().build().unwrap();
    assert_eq!(schema.type_(), SchemaType::Float32);
    assert!(!schema.is_optional());
    assert!(schema.default_value().is_none());
}

#[test]
fn test_float64_builder() {
    let schema = SchemaBuilder::float64().build().unwrap();
    assert_eq!(schema.type_(), SchemaType::Float64);
    assert!(!schema.is_optional());
    assert!(schema.default_value().is_none());
}

#[test]
fn test_bool_builder() {
    let schema = SchemaBuilder::boolean().build().unwrap();
    assert_eq!(schema.type_(), SchemaType::Boolean);
    assert!(!schema.is_optional());
    assert!(schema.default_value().is_none());
}

#[test]
fn test_string_builder() {
    let schema = SchemaBuilder::string().build().unwrap();
    assert_eq!(schema.type_(), SchemaType::String);
    assert!(!schema.is_optional());
    assert!(schema.default_value().is_none());
}

#[test]
fn test_bytes_builder() {
    let schema = SchemaBuilder::bytes().build().unwrap();
    assert_eq!(schema.type_(), SchemaType::Bytes);
    assert!(!schema.is_optional());
    assert!(schema.default_value().is_none());
}

#[test]
fn test_optional() {
    let schema = SchemaBuilder::int8().optional().build().unwrap();
    assert_eq!(schema.type_(), SchemaType::Int8);
    assert!(schema.is_optional());
}

#[test]
fn test_name() {
    let schema = SchemaBuilder::int8()
        .name("test_schema".to_string())
        .build()
        .unwrap();
    assert_eq!(schema.type_(), SchemaType::Int8);
    assert_eq!(schema.name(), Some("test_schema"));
}

#[test]
fn test_version() {
    let schema = SchemaBuilder::int8().version(2).build().unwrap();
    assert_eq!(schema.type_(), SchemaType::Int8);
    assert_eq!(schema.version(), Some(2));
}

#[test]
fn test_doc() {
    let schema = SchemaBuilder::int8()
        .doc("test documentation".to_string())
        .build()
        .unwrap();
    assert_eq!(schema.type_(), SchemaType::Int8);
    assert_eq!(schema.doc(), Some("test documentation"));
}
