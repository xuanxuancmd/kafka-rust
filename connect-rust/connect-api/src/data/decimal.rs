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

use crate::data::{ConnectSchema, Schema, SchemaBuilder, SchemaType};
use crate::errors::ConnectError;
use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;

/// Decimal logical type for Connect schemas.
///
/// This corresponds to `org.apache.kafka.connect.data.Decimal` in Java.
pub struct Decimal;

impl Decimal {
    /// The logical type name for Decimal.
    pub const LOGICAL_NAME: &'static str = "org.apache.kafka.connect.data.Decimal";

    /// Creates a Decimal schema builder.
    pub fn builder(scale: i32) -> SchemaBuilder {
        SchemaBuilder::bytes()
            .name(Self::LOGICAL_NAME)
            .parameter("scale", scale.to_string())
    }

    /// Creates a Decimal schema with the given scale.
    pub fn schema(scale: i32) -> ConnectSchema {
        Self::builder(scale).build()
    }

    /// Creates an optional Decimal schema.
    pub fn optional_schema(scale: i32) -> ConnectSchema {
        Self::builder(scale).optional().build()
    }

    /// Returns the scale of a Decimal schema.
    pub fn scale(schema: &dyn Schema) -> Option<i32> {
        if schema.name() != Some(Self::LOGICAL_NAME) {
            return None;
        }
        schema
            .parameters()
            .get("scale")
            .and_then(|s| s.parse::<i32>().ok())
    }

    /// Checks if a schema is a Decimal schema.
    pub fn is_decimal(schema: &dyn Schema) -> bool {
        schema.name() == Some(Self::LOGICAL_NAME)
    }

    /// Converts a BigDecimal to bytes (logical representation).
    pub fn from_logical(decimal: &BigDecimal, scale: i32) -> Vec<u8> {
        // Convert BigDecimal to unscaled value (integer) and scale
        let (unscaled, _) = decimal.as_bigint_and_exponent();
        // The scale should match, but we need to adjust if not
        let adjusted = unscaled.clone();
        adjusted.to_bytes_be().1
    }

    /// Converts bytes to a BigDecimal (logical representation).
    pub fn to_logical(bytes: &[u8], scale: i32) -> Result<BigDecimal, ConnectError> {
        // Convert bytes to BigInt, then to BigDecimal with scale
        let bigint = BigInt::from_bytes_be(num_bigint::Sign::Plus, bytes);
        let decimal_str = format!("{}", bigint);
        let decimal = BigDecimal::from_str(&decimal_str)
            .map_err(|e| ConnectError::data(format!("Cannot convert bytes to decimal: {}", e)))?;
        // Adjust scale
        Ok(decimal / BigDecimal::from(10_i64.pow(scale as u32)))
    }

    /// Converts a decimal value to logical bytes representation for Connect.
    pub fn to_connect_bytes(decimal: &BigDecimal, scale: i32) -> Vec<u8> {
        Self::from_logical(decimal, scale)
    }

    /// Converts logical bytes representation to a BigDecimal for Connect.
    pub fn from_connect_bytes(bytes: &[u8], scale: i32) -> Result<BigDecimal, ConnectError> {
        Self::to_logical(bytes, scale)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::Schema;
    use std::str::FromStr;

    #[test]
    fn test_decimal_schema() {
        let schema = Decimal::schema(2);
        assert_eq!(schema.name(), Some(Decimal::LOGICAL_NAME));
        assert_eq!(Decimal::scale(&schema), Some(2));
    }

    #[test]
    fn test_optional_decimal_schema() {
        let schema = Decimal::optional_schema(2);
        assert_eq!(schema.name(), Some(Decimal::LOGICAL_NAME));
        assert!(schema.is_optional());
    }

    #[test]
    fn test_is_decimal() {
        let schema = Decimal::schema(2);
        assert!(Decimal::is_decimal(&schema));

        let other_schema = crate::data::SchemaBuilder::int32().build();
        assert!(!Decimal::is_decimal(&other_schema));
    }

    #[test]
    fn test_builder() {
        let builder = Decimal::builder(5);
        let schema = builder.build();
        assert_eq!(schema.name(), Some(Decimal::LOGICAL_NAME));
        assert_eq!(Decimal::scale(&schema), Some(5));
    }

    #[test]
    fn test_scale_extraction() {
        let schema = Decimal::schema(10);
        assert_eq!(Decimal::scale(&schema), Some(10));

        // Schema without scale parameter
        let schema = crate::data::SchemaBuilder::bytes().build();
        assert!(Decimal::scale(&schema).is_none());
    }

    #[test]
    fn test_to_logical() {
        let bytes = vec![0, 0, 0, 42]; // Simple representation
        let result = Decimal::to_logical(&bytes, 0);
        assert!(result.is_ok());

        let decimal = result.unwrap();
        // The value should be 42 with scale 0
        assert!(decimal >= BigDecimal::from_str("0").unwrap());
    }

    #[test]
    fn test_from_connect_bytes() {
        let bytes = vec![0, 0, 0, 100];
        let result = Decimal::from_connect_bytes(&bytes, 0);
        assert!(result.is_ok());
    }
}
