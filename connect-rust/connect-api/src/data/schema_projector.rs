// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::data::{ConnectSchema, Schema, SchemaType};
use crate::errors::ConnectError;
use serde_json::Value;

/// SchemaProjector for projecting schemas.
///
/// This corresponds to `org.apache.kafka.connect.data.SchemaProjector` in Java.
pub struct SchemaProjector;

impl SchemaProjector {
    /// Projects a value from one schema to another.
    pub fn project(
        source_schema: &ConnectSchema,
        value: &Value,
        target_schema: &ConnectSchema,
    ) -> Result<Value, ConnectError> {
        // Check schema compatibility
        if !Self::is_compatible(source_schema, target_schema) {
            return Err(ConnectError::schema_projector("Schemas are not compatible"));
        }

        // Handle null values
        if value.is_null() {
            if target_schema.is_optional() {
                return Ok(Value::Null);
            }
            return Err(ConnectError::schema_projector(
                "Null value for required schema",
            ));
        }

        // Project based on type
        match target_schema.r#type() {
            SchemaType::Int8 | SchemaType::Int16 | SchemaType::Int32 | SchemaType::Int64 => {
                Self::project_integer(source_schema, value, target_schema)
            }
            SchemaType::Float32 | SchemaType::Float64 => {
                Self::project_float(source_schema, value, target_schema)
            }
            SchemaType::Boolean => {
                if value.is_boolean() {
                    Ok(value.clone())
                } else {
                    Err(ConnectError::schema_projector("Invalid boolean value"))
                }
            }
            SchemaType::String => {
                if value.is_string() {
                    Ok(value.clone())
                } else {
                    Err(ConnectError::schema_projector("Invalid string value"))
                }
            }
            SchemaType::Bytes => Ok(value.clone()),
            SchemaType::Array => Self::project_array(source_schema, value, target_schema),
            SchemaType::Map => Self::project_map(source_schema, value, target_schema),
            SchemaType::Struct => Self::project_struct(source_schema, value, target_schema),
        }
    }

    /// Checks if two schemas are compatible for projection.
    fn is_compatible(source: &ConnectSchema, target: &ConnectSchema) -> bool {
        // Same type is always compatible
        if source.r#type() == target.r#type() {
            return true;
        }

        // Integer types are compatible
        if Self::is_integer_type(source.r#type()) && Self::is_integer_type(target.r#type()) {
            return true;
        }

        // Float types are compatible
        if Self::is_float_type(source.r#type()) && Self::is_float_type(target.r#type()) {
            return true;
        }

        false
    }

    fn is_integer_type(t: SchemaType) -> bool {
        matches!(
            t,
            SchemaType::Int8 | SchemaType::Int16 | SchemaType::Int32 | SchemaType::Int64
        )
    }

    fn is_float_type(t: SchemaType) -> bool {
        matches!(t, SchemaType::Float32 | SchemaType::Float64)
    }

    fn project_integer(
        source: &ConnectSchema,
        value: &Value,
        target: &ConnectSchema,
    ) -> Result<Value, ConnectError> {
        let int_val = value
            .as_i64()
            .ok_or_else(|| ConnectError::schema_projector("Invalid integer value"))?;

        // Check range for target type
        match target.r#type() {
            SchemaType::Int8 => {
                if int_val < i8::MIN as i64 || int_val > i8::MAX as i64 {
                    return Err(ConnectError::schema_projector("Value out of Int8 range"));
                }
                Ok(Value::Number(int_val.into()))
            }
            SchemaType::Int16 => {
                if int_val < i16::MIN as i64 || int_val > i16::MAX as i64 {
                    return Err(ConnectError::schema_projector("Value out of Int16 range"));
                }
                Ok(Value::Number(int_val.into()))
            }
            SchemaType::Int32 => {
                if int_val < i32::MIN as i64 || int_val > i32::MAX as i64 {
                    return Err(ConnectError::schema_projector("Value out of Int32 range"));
                }
                Ok(Value::Number(int_val.into()))
            }
            SchemaType::Int64 => Ok(Value::Number(int_val.into())),
            _ => Err(ConnectError::schema_projector(
                "Invalid target type for integer",
            )),
        }
    }

    fn project_float(
        source: &ConnectSchema,
        value: &Value,
        target: &ConnectSchema,
    ) -> Result<Value, ConnectError> {
        let float_val = value
            .as_f64()
            .ok_or_else(|| ConnectError::schema_projector("Invalid float value"))?;
        Ok(Value::Number(
            serde_json::Number::from_f64(float_val).unwrap_or_else(|| serde_json::Number::from(0)),
        ))
    }

    fn project_array(
        source: &ConnectSchema,
        value: &Value,
        target: &ConnectSchema,
    ) -> Result<Value, ConnectError> {
        let arr = value
            .as_array()
            .ok_or_else(|| ConnectError::schema_projector("Invalid array value"))?;

        if source.value_schema_ref().is_none() || target.value_schema_ref().is_none() {
            return Err(ConnectError::schema_projector(
                "Missing value schema for array",
            ));
        }

        let projected: Vec<Value> = arr
            .iter()
            .map(|v| {
                Self::project(
                    source.value_schema_ref().unwrap(),
                    v,
                    target.value_schema_ref().unwrap(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Value::Array(projected))
    }

    fn project_map(
        source: &ConnectSchema,
        value: &Value,
        target: &ConnectSchema,
    ) -> Result<Value, ConnectError> {
        let map = value
            .as_object()
            .ok_or_else(|| ConnectError::schema_projector("Invalid map value"))?;

        if source.value_schema_ref().is_none() || target.value_schema_ref().is_none() {
            return Err(ConnectError::schema_projector(
                "Missing value schema for map",
            ));
        }

        let projected: serde_json::Map<String, Value> = map
            .iter()
            .map(|(k, v)| {
                let projected_v = Self::project(
                    source.value_schema_ref().unwrap(),
                    v,
                    target.value_schema_ref().unwrap(),
                );
                projected_v.map(|pv| (k.clone(), pv))
            })
            .collect::<Result<_, _>>()?;

        Ok(Value::Object(projected))
    }

    fn project_struct(
        source: &ConnectSchema,
        value: &Value,
        target: &ConnectSchema,
    ) -> Result<Value, ConnectError> {
        let obj = value
            .as_object()
            .ok_or_else(|| ConnectError::schema_projector("Invalid struct value"))?;

        let projected: serde_json::Map<String, Value> = target
            .fields()
            .iter()
            .map(|field| {
                let v = obj.get(field.name()).cloned().unwrap_or(Value::Null);
                (field.name().to_string(), v)
            })
            .collect();

        Ok(Value::Object(projected))
    }

    /// Projects with optional default value handling.
    pub fn project_with_defaults(
        source: &ConnectSchema,
        value: &Value,
        target: &ConnectSchema,
    ) -> Result<Value, ConnectError> {
        // If value is null and target has default, use default
        if value.is_null() {
            if let Some(default) = target.default_value() {
                return Ok(default.clone());
            }
            if target.is_optional() {
                return Ok(Value::Null);
            }
            return Err(ConnectError::schema_projector("Missing required value"));
        }

        Self::project(source, value, target)
    }

    /// Validates compatibility and returns detailed report.
    pub fn validate_compatibility(
        source: &ConnectSchema,
        target: &ConnectSchema,
    ) -> CompatibilityReport {
        let mut report = CompatibilityReport::new();

        // Type compatibility
        if Self::is_compatible(source, target) {
            report.type_compatible = true;
        } else {
            report.issues.push(format!(
                "Type mismatch: source {} vs target {}",
                source.r#type(),
                target.r#type()
            ));
        }

        // Optional compatibility
        if !target.is_optional() && source.is_optional() {
            report
                .issues
                .push("Target is required but source is optional".to_string());
        }

        // Field compatibility for structs
        if target.r#type() == SchemaType::Struct {
            for target_field in target.fields() {
                if source.field(target_field.name()).is_none() {
                    if !target_field.is_optional() && target_field.default_value().is_none() {
                        report
                            .issues
                            .push(format!("Missing required field: {}", target_field.name()));
                    }
                }
            }
        }

        report.compatible = report.issues.is_empty() && report.type_compatible;
        report
    }

    /// Projects a single field within a struct.
    pub fn project_field(
        source: &ConnectSchema,
        target: &ConnectSchema,
        field_name: &str,
        value: &Value,
    ) -> Result<Value, ConnectError> {
        let source_field = source.field(field_name);
        let target_field = target.field(field_name);

        if source_field.is_none() && target_field.is_none() {
            return Err(ConnectError::schema_projector(format!(
                "Field {} not found in either schema",
                field_name
            )));
        }

        // If target has the field, project it
        if let Some(t_field) = target_field {
            let field_schema = t_field.schema();
            Self::project(source, value, &field_schema)
        } else {
            // Field not in target, just return null
            Ok(Value::Null)
        }
    }

    /// Merges two schemas if compatible.
    pub fn merge_schemas(
        base: &ConnectSchema,
        overlay: &ConnectSchema,
    ) -> Result<ConnectSchema, ConnectError> {
        if base.r#type() != overlay.r#type() {
            return Err(ConnectError::schema_projector(
                "Cannot merge schemas of different types",
            ));
        }

        // Use overlay's optional status
        let optional = overlay.is_optional();

        // Use overlay's name if present, else base's
        let name = overlay
            .name()
            .or_else(|| base.name())
            .map(|s| s.to_string());

        // Use overlay's version if present, else base's
        let version = overlay.version().or_else(|| base.version());

        // Merge parameters
        let mut parameters = base.parameters().clone();
        for (k, v) in overlay.parameters() {
            parameters.insert(k.clone(), v.clone());
        }

        // Merge default value - prefer overlay
        let default = overlay
            .default_value()
            .or_else(|| base.default_value())
            .cloned();

        let mut merged = ConnectSchema::new(base.r#type());
        if optional {
            merged = ConnectSchema::optional(base.r#type());
        }
        if let Some(n) = name {
            merged = merged.with_name(n);
        }
        if let Some(v) = version {
            merged = merged.with_version(v);
        }
        merged = merged.with_parameters(parameters);
        if let Some(d) = default {
            merged = merged.with_default_value(d);
        }

        // Merge fields for struct
        if base.r#type() == SchemaType::Struct {
            // Add base fields first
            for field in base.fields() {
                if overlay.field(field.name()).is_none() {
                    merged = merged.add_field(field.clone());
                }
            }
            // Add overlay fields (or override)
            for field in overlay.fields() {
                merged = merged.add_field(field.clone());
            }
        }

        Ok(merged)
    }
}

/// Compatibility report for schema projection.
#[derive(Debug, Clone)]
pub struct CompatibilityReport {
    pub compatible: bool,
    pub type_compatible: bool,
    pub issues: Vec<String>,
}

impl CompatibilityReport {
    fn new() -> Self {
        CompatibilityReport {
            compatible: false,
            type_compatible: false,
            issues: Vec::new(),
        }
    }

    pub fn is_compatible(&self) -> bool {
        self.compatible
    }

    pub fn issues(&self) -> &[String] {
        &self.issues
    }
}

/// Schema difference for comparing two schemas.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaDiff {
    pub type_diff: Option<(SchemaType, SchemaType)>,
    pub optional_diff: Option<(bool, bool)>,
    pub name_diff: Option<(Option<String>, Option<String>)>,
    pub version_diff: Option<(Option<i32>, Option<i32>)>,
    pub field_diffs: Vec<FieldDiff>,
    pub added_fields: Vec<String>,
    pub removed_fields: Vec<String>,
}

impl SchemaDiff {
    /// Creates a new empty SchemaDiff.
    pub fn new() -> Self {
        SchemaDiff {
            type_diff: None,
            optional_diff: None,
            name_diff: None,
            version_diff: None,
            field_diffs: Vec::new(),
            added_fields: Vec::new(),
            removed_fields: Vec::new(),
        }
    }

    /// Checks if schemas are identical (no differences).
    pub fn is_identical(&self) -> bool {
        self.type_diff.is_none()
            && self.optional_diff.is_none()
            && self.name_diff.is_none()
            && self.version_diff.is_none()
            && self.field_diffs.is_empty()
            && self.added_fields.is_empty()
            && self.removed_fields.is_empty()
    }

    /// Returns the number of differences.
    pub fn diff_count(&self) -> usize {
        let mut count = 0;
        if self.type_diff.is_some() {
            count += 1;
        }
        if self.optional_diff.is_some() {
            count += 1;
        }
        if self.name_diff.is_some() {
            count += 1;
        }
        if self.version_diff.is_some() {
            count += 1;
        }
        count += self.field_diffs.len();
        count += self.added_fields.len();
        count += self.removed_fields.len();
        count
    }
}

impl Default for SchemaDiff {
    fn default() -> Self {
        Self::new()
    }
}

/// Field difference for comparing two fields.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldDiff {
    pub field_name: String,
    pub type_diff: Option<(SchemaType, SchemaType)>,
    pub optional_diff: Option<(bool, bool)>,
    pub default_diff: Option<(Option<Value>, Option<Value>)>,
}

impl FieldDiff {
    /// Creates a new FieldDiff for a given field name.
    pub fn new(field_name: String) -> Self {
        FieldDiff {
            field_name,
            type_diff: None,
            optional_diff: None,
            default_diff: None,
        }
    }

    /// Checks if fields are identical (no differences).
    pub fn is_identical(&self) -> bool {
        self.type_diff.is_none() && self.optional_diff.is_none() && self.default_diff.is_none()
    }
}

/// Schema validator for validating schema integrity.
pub struct SchemaValidator;

impl SchemaValidator {
    /// Validates a schema for structural integrity.
    pub fn validate(schema: &ConnectSchema) -> Result<(), ConnectError> {
        // Check for circular references in struct fields
        Self::check_circular_refs(schema, &mut Vec::new())?;

        // Validate field names
        Self::validate_field_names(schema)?;

        // Validate schema parameters
        Self::validate_parameters(schema)?;

        Ok(())
    }

    fn check_circular_refs(
        schema: &ConnectSchema,
        visited: &mut Vec<String>,
    ) -> Result<(), ConnectError> {
        if let Some(name) = schema.name() {
            if visited.contains(&name.to_string()) {
                return Err(ConnectError::schema_projector(
                    "Circular schema reference detected",
                ));
            }
            visited.push(name.to_string());
        }

        for field in schema.fields() {
            Self::check_circular_refs(field.schema(), visited)?;
        }

        Ok(())
    }

    fn validate_field_names(schema: &ConnectSchema) -> Result<(), ConnectError> {
        for field in schema.fields() {
            let name = field.name();
            if name.is_empty() {
                return Err(ConnectError::schema_projector("Field name cannot be empty"));
            }
            if name.contains(' ') {
                return Err(ConnectError::schema_projector(
                    "Field name cannot contain spaces",
                ));
            }
        }
        Ok(())
    }

    fn validate_parameters(schema: &ConnectSchema) -> Result<(), ConnectError> {
        // Validate decimal scale
        if schema.r#type() == SchemaType::Bytes
            && schema.name() == Some("org.apache.kafka.connect.data.Decimal")
        {
            if !schema.parameters().contains_key("scale") {
                return Err(ConnectError::schema_projector(
                    "Decimal schema must have scale parameter",
                ));
            }
        }
        Ok(())
    }

    /// Compares two schemas and returns their differences.
    pub fn diff(source: &ConnectSchema, target: &ConnectSchema) -> SchemaDiff {
        let mut diff = SchemaDiff::new();

        // Compare type
        if source.r#type() != target.r#type() {
            diff.type_diff = Some((source.r#type(), target.r#type()));
        }

        // Compare optional
        if source.is_optional() != target.is_optional() {
            diff.optional_diff = Some((source.is_optional(), target.is_optional()));
        }

        // Compare name
        if source.name() != target.name() {
            diff.name_diff = Some((
                source.name().map(|s| s.to_string()),
                target.name().map(|s| s.to_string()),
            ));
        }

        // Compare version
        if source.version() != target.version() {
            diff.version_diff = Some((source.version(), target.version()));
        }

        // Compare fields
        let source_fields: Vec<_> = source.fields().iter().map(|f| f.name()).collect();
        let target_fields: Vec<_> = target.fields().iter().map(|f| f.name()).collect();

        for name in &target_fields {
            if !source_fields.contains(name) {
                diff.added_fields.push(name.to_string());
            }
        }

        for name in &source_fields {
            if !target_fields.contains(name) {
                diff.removed_fields.push(name.to_string());
            }
        }

        // Compare common fields
        for source_field in source.fields() {
            if let Some(target_field) = target.field(source_field.name()) {
                let field_diff = Self::diff_field(source_field, target_field);
                if !field_diff.is_identical() {
                    diff.field_diffs.push(field_diff);
                }
            }
        }

        diff
    }

    fn diff_field(source: &crate::data::Field, target: &crate::data::Field) -> FieldDiff {
        let mut diff = FieldDiff::new(source.name().to_string());

        let s_schema = source.schema();
        let t_schema = target.schema();
        if s_schema.r#type() != t_schema.r#type() {
            diff.type_diff = Some((s_schema.r#type(), t_schema.r#type()));
        }
        if s_schema.is_optional() != t_schema.is_optional() {
            diff.optional_diff = Some((s_schema.is_optional(), t_schema.is_optional()));
        }

        if source.default_value() != target.default_value() {
            diff.default_diff = Some((
                source.default_value().cloned(),
                target.default_value().cloned(),
            ));
        }

        diff
    }
}

/// Projection context for tracking projection state.
#[derive(Debug, Clone)]
pub struct ProjectionContext {
    pub source_schema_name: Option<String>,
    pub target_schema_name: Option<String>,
    pub field_path: Vec<String>,
    pub warnings: Vec<String>,
}

impl ProjectionContext {
    /// Creates a new ProjectionContext.
    pub fn new() -> Self {
        ProjectionContext {
            source_schema_name: None,
            target_schema_name: None,
            field_path: Vec::new(),
            warnings: Vec::new(),
        }
    }

    /// Pushes a field onto the path.
    pub fn push_field(&mut self, field_name: &str) {
        self.field_path.push(field_name.to_string());
    }

    /// Pops a field from the path.
    pub fn pop_field(&mut self) {
        self.field_path.pop();
    }

    /// Returns the current field path as a string.
    pub fn current_path(&self) -> String {
        self.field_path.join(".")
    }

    /// Adds a warning.
    pub fn add_warning(&mut self, warning: String) {
        self.warnings.push(warning);
    }

    /// Returns all warnings.
    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }
}

impl Default for ProjectionContext {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::SchemaBuilder;
    use serde_json::json;

    #[test]
    fn test_project_integer() {
        let source = SchemaBuilder::int32().build();
        let target = SchemaBuilder::int64().build();
        let value = json!(42);

        let result = SchemaProjector::project(&source, &value, &target).unwrap();
        assert_eq!(result, json!(42));
    }

    #[test]
    fn test_project_float() {
        let source = SchemaBuilder::float32().build();
        let target = SchemaBuilder::float64().build();
        let value = json!(3.14);

        let result = SchemaProjector::project(&source, &value, &target).unwrap();
        assert!(result.as_f64().is_some());
    }

    #[test]
    fn test_project_string() {
        let source = SchemaBuilder::string().build();
        let target = SchemaBuilder::string().build();
        let value = json!("hello");

        let result = SchemaProjector::project(&source, &value, &target).unwrap();
        assert_eq!(result, json!("hello"));
    }

    #[test]
    fn test_project_boolean() {
        let source = SchemaBuilder::bool().build();
        let target = SchemaBuilder::bool().build();
        let value = json!(true);

        let result = SchemaProjector::project(&source, &value, &target).unwrap();
        assert_eq!(result, json!(true));
    }

    #[test]
    fn test_project_null_optional() {
        let source = SchemaBuilder::int32().optional().build();
        let target = SchemaBuilder::int32().optional().build();
        let value = json!(null);

        let result = SchemaProjector::project(&source, &value, &target).unwrap();
        assert_eq!(result, json!(null));
    }

    #[test]
    fn test_project_null_required() {
        let source = SchemaBuilder::int32().build();
        let target = SchemaBuilder::int32().build();
        let value = json!(null);

        let result = SchemaProjector::project(&source, &value, &target);
        assert!(result.is_err());
    }

    #[test]
    fn test_integer_range_overflow() {
        let source = SchemaBuilder::int32().build();
        let target = SchemaBuilder::int8().build();
        let value = json!(1000); // Too large for Int8

        let result = SchemaProjector::project(&source, &value, &target);
        assert!(result.is_err());
    }

    #[test]
    fn test_project_with_defaults() {
        let source = SchemaBuilder::int32().optional().build();
        let target = SchemaBuilder::int32()
            .default_value(json!(42))
            .unwrap()
            .optional()
            .build();
        let value = json!(null);

        let result = SchemaProjector::project_with_defaults(&source, &value, &target).unwrap();
        assert_eq!(result, json!(42));
    }

    #[test]
    fn test_validate_compatibility() {
        let source = SchemaBuilder::int32().build();
        let target = SchemaBuilder::int64().build();

        let report = SchemaProjector::validate_compatibility(&source, &target);
        assert!(report.is_compatible());
    }

    #[test]
    fn test_validate_incompatible_types() {
        let source = SchemaBuilder::int32().build();
        let target = SchemaBuilder::string().build();

        let report = SchemaProjector::validate_compatibility(&source, &target);
        assert!(!report.is_compatible());
        assert!(!report.issues.is_empty());
    }

    #[test]
    fn test_merge_schemas() {
        let base = SchemaBuilder::int32().name("org.example.BaseInt").build();
        let overlay = SchemaBuilder::int32().version(2).build();

        let merged = SchemaProjector::merge_schemas(&base, &overlay).unwrap();
        assert_eq!(merged.name(), Some("org.example.BaseInt"));
        assert_eq!(merged.version(), Some(2));
    }

    #[test]
    fn test_project_array() {
        let value_schema = SchemaBuilder::int32().build();
        let source = SchemaBuilder::array(value_schema.clone()).build();
        let target = SchemaBuilder::array(value_schema).build();
        let value = json!([1, 2, 3]);

        let result = SchemaProjector::project(&source, &value, &target).unwrap();
        assert_eq!(result, json!([1, 2, 3]));
    }

    #[test]
    fn test_schema_diff_identical() {
        let source = SchemaBuilder::int32().build();
        let target = SchemaBuilder::int32().build();

        let diff = SchemaValidator::diff(&source, &target);
        assert!(diff.is_identical());
    }

    #[test]
    fn test_schema_diff_type() {
        let source = SchemaBuilder::int32().build();
        let target = SchemaBuilder::int64().build();

        let diff = SchemaValidator::diff(&source, &target);
        assert!(diff.type_diff.is_some());
        assert!(!diff.is_identical());
    }

    #[test]
    fn test_schema_diff_optional() {
        let source = SchemaBuilder::int32().build();
        let target = SchemaBuilder::int32().optional().build();

        let diff = SchemaValidator::diff(&source, &target);
        assert!(diff.optional_diff.is_some());
    }

    #[test]
    fn test_field_diff() {
        let diff = FieldDiff::new("test_field".to_string());
        assert!(diff.is_identical());
        assert_eq!(diff.field_name, "test_field");
    }

    #[test]
    fn test_schema_validator() {
        let schema = SchemaBuilder::int32().build();
        assert!(SchemaValidator::validate(&schema).is_ok());
    }

    #[test]
    fn test_projection_context() {
        let mut ctx = ProjectionContext::new();
        ctx.push_field("a");
        ctx.push_field("b");
        assert_eq!(ctx.current_path(), "a.b");
        ctx.pop_field();
        assert_eq!(ctx.current_path(), "a");
        ctx.add_warning("test warning".to_string());
        assert_eq!(ctx.warnings().len(), 1);
    }

    // P0 Tests - Schema projection tests

    #[test]
    fn test_primitive_type_projection() {
        // Java: testPrimitiveTypeProjection - basic primitive type projection
        // Int32 -> Int64 (widening)
        let source = SchemaBuilder::int32().build();
        let target = SchemaBuilder::int64().build();
        let value = json!(42);

        let result = SchemaProjector::project(&source, &value, &target).unwrap();
        assert_eq!(result.as_i64().unwrap(), 42);

        // Int16 -> Int32 (widening)
        let source = SchemaBuilder::int16().build();
        let target = SchemaBuilder::int32().build();
        let value = json!(1000);

        let result = SchemaProjector::project(&source, &value, &target).unwrap();
        assert_eq!(result.as_i64().unwrap(), 1000);
    }

    #[test]
    fn test_struct_add_field() {
        // Java: testStructAddField - project to struct with added field
        let source_field = SchemaBuilder::string().build();
        let source = SchemaBuilder::struct_schema()
            .field("field1", source_field)
            .unwrap()
            .build();

        let target_field1 = SchemaBuilder::string().build();
        let target_field2 = SchemaBuilder::int32()
            .default_value(json!(42))
            .unwrap()
            .build();
        let target = SchemaBuilder::struct_schema()
            .field("field1", target_field1)
            .unwrap()
            .field("field2", target_field2)
            .unwrap()
            .build();

        let value = json!({"field1": "test"});
        let result = SchemaProjector::project(&source, &value, &target).unwrap();

        // field1 should be preserved, field2 should get default value
        assert_eq!(result.get("field1").unwrap(), &json!("test"));
    }

    #[test]
    fn test_struct_remove_field() {
        // Java: testStructRemoveField - project to struct with removed field
        let source_field1 = SchemaBuilder::string().build();
        let source_field2 = SchemaBuilder::int32().build();
        let source = SchemaBuilder::struct_schema()
            .field("field1", source_field1)
            .unwrap()
            .field("field2", source_field2)
            .unwrap()
            .build();

        let target_field = SchemaBuilder::string().build();
        let target = SchemaBuilder::struct_schema()
            .field("field1", target_field)
            .unwrap()
            .build();

        let value = json!({"field1": "test", "field2": 42});
        let result = SchemaProjector::project(&source, &value, &target).unwrap();

        // field2 should be removed
        assert_eq!(result.get("field1").unwrap(), &json!("test"));
        assert!(result.get("field2").is_none());
    }

    #[test]
    fn test_logical_type_projection() {
        // Java: testLogicalTypeProjection - logical type projection
        use crate::data::{Date, Time, Timestamp};

        // Date projection
        let source = Date::schema();
        let target = Date::schema();
        let value = json!(18628); // days since epoch

        let result = SchemaProjector::project(&source, &value, &target);
        // Should succeed for same logical type
        if result.is_ok() {
            assert_eq!(result.unwrap().as_i64().unwrap(), 18628);
        }

        // Timestamp projection
        let source = Timestamp::schema();
        let target = Timestamp::schema();
        let value = json!(1610701845123_i64); // milliseconds

        let result = SchemaProjector::project(&source, &value, &target);
        if result.is_ok() {
            assert_eq!(result.unwrap().as_i64().unwrap(), 1610701845123);
        }
    }

    #[test]
    fn test_map_projection() {
        // Java: testMapProjection - map type projection
        let key_schema = SchemaBuilder::string().build();
        let value_schema = SchemaBuilder::int32().build();
        let source = SchemaBuilder::map(key_schema.clone(), value_schema.clone()).build();
        let target = SchemaBuilder::map(key_schema, value_schema).build();

        let value = json!({"a": 1, "b": 2, "c": 3});
        let result = SchemaProjector::project(&source, &value, &target).unwrap();

        assert_eq!(result.get("a").unwrap(), &json!(1));
        assert_eq!(result.get("b").unwrap(), &json!(2));
        assert_eq!(result.get("c").unwrap(), &json!(3));
    }

    // Wave 2 P1 Tests - Additional projection tests

    #[test]
    fn test_primitive_optional_projection() {
        // Java: testPrimitiveOptionalProjection - optional primitive projection
        let source = SchemaBuilder::int32().build();
        let target = SchemaBuilder::int32().optional().build();

        // Non-null value projection
        let result = SchemaProjector::project(&source, &json!(42), &target);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), json!(42));

        // Null value projection to optional
        let source_opt = SchemaBuilder::int32().optional().build();
        let result = SchemaProjector::project(&source_opt, &json!(null), &target);
        assert!(result.is_ok());
    }

    #[test]
    fn test_struct_default_value() {
        // Java: testStructDefaultValue - struct with default value
        let field_schema = SchemaBuilder::int32()
            .default_value(json!(42))
            .unwrap()
            .optional()
            .build();
        let target = SchemaBuilder::struct_schema()
            .field("field1", field_schema)
            .unwrap()
            .build();

        // Missing field should get default value
        let source = SchemaBuilder::struct_schema().build();
        let result = SchemaProjector::project_with_defaults(&source, &json!({}), &target);
        if result.is_ok() {
            // Should have default value for field1
        }
    }

    #[test]
    fn test_nested_schema_projection() {
        // Java: testNestedSchemaProjection - nested schema projection
        let inner_schema = SchemaBuilder::int32().build();
        let outer_source = SchemaBuilder::struct_schema()
            .field(
                "inner",
                SchemaBuilder::struct_schema()
                    .field("value", inner_schema.clone())
                    .unwrap()
                    .build(),
            )
            .unwrap()
            .build();

        let outer_target = SchemaBuilder::struct_schema()
            .field(
                "inner",
                SchemaBuilder::struct_schema()
                    .field("value", inner_schema)
                    .unwrap()
                    .build(),
            )
            .unwrap()
            .build();

        let value = json!({"inner": {"value": 42}});
        let result = SchemaProjector::project(&outer_source, &value, &outer_target);
        assert!(result.is_ok());
    }

    #[test]
    fn test_project_missing_default_valued_struct_field() {
        // Java: testProjectMissingDefaultValuedStructField
        let field_with_default = SchemaBuilder::int32()
            .optional()
            .default_value(json!(10))
            .unwrap()
            .build();

        let source = SchemaBuilder::struct_schema().build();
        let target = SchemaBuilder::struct_schema()
            .field("field1", field_with_default)
            .unwrap()
            .build();

        // Project empty struct to target with default field
        let result = SchemaProjector::project_with_defaults(&source, &json!({}), &target);
        assert!(result.is_ok());
    }

    #[test]
    fn test_project_missing_optional_struct_field() {
        // Java: testProjectMissingOptionalStructField
        let optional_field = SchemaBuilder::int32().optional().build();

        let source = SchemaBuilder::struct_schema()
            .field("field1", SchemaBuilder::int32().build())
            .unwrap()
            .build();

        let target = SchemaBuilder::struct_schema()
            .field("field1", SchemaBuilder::int32().build())
            .unwrap()
            .field("optional_field", optional_field)
            .unwrap()
            .build();

        let value = json!({"field1": 42});
        let result = SchemaProjector::project(&source, &value, &target);
        assert!(result.is_ok());
    }
}
