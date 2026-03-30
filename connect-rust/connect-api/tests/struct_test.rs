//! Tests for Struct

use connect_api::data::{SchemaBuilder, Struct};
use connect_api::Schema;

#[test]
fn test_empty_struct() {
    let schema = SchemaBuilder::struct_().build().unwrap();
    let _struct = Struct::new(schema).unwrap();
    assert_eq!(
        _struct.schema().type_(),
        connect_api::data::SchemaType::Struct
    );
}

#[test]
fn test_struct_schema() {
    let schema = SchemaBuilder::struct_().build().unwrap();
    let struct_instance = Struct::new(schema).unwrap();

    // Verify we can get the schema back
    assert_eq!(
        struct_instance.schema().type_(),
        connect_api::data::SchemaType::Struct
    );
}

#[test]
fn test_struct_get_nonexistent_field() {
    let schema = SchemaBuilder::struct_().build().unwrap();
    let struct_instance = Struct::new(schema).unwrap();

    // Getting a non-existent field should return None
    assert!(struct_instance.get("nonexistent").is_none());
}
