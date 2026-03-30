//! Tests for Field

use connect_api::data::{Field, SchemaBuilder, SchemaType};

#[test]
fn test_equality() {
    let schema = SchemaBuilder::int8().build().unwrap();
    let field1 = Field::new(0, "name".to_string(), schema.clone());
    let field2 = Field::new(0, "name".to_string(), schema.clone());
    let different_name = Field::new(0, "name2".to_string(), schema.clone());
    let different_index = Field::new(1, "name".to_string(), schema);

    assert_eq!(field1, field2);
    assert_ne!(field1, different_name);
    assert_ne!(field1, different_index);
}
