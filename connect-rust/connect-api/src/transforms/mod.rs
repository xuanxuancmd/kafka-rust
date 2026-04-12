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

//! Transforms module for Kafka Connect record transformations.
//! This module provides transformation traits for modifying records.

pub mod predicates;

pub use predicates::*;

/// Single message transformation for Kafka Connect record types.
/// Transformations can be used to modify records as they pass through the pipeline.
pub trait Transformation<R>: Send + Sync
where
    R: Send + Sync,
{
    /// Apply transformation to record and return another record object or None to drop.
    ///
    /// # Arguments
    ///
    /// * `record` - The record to transform
    ///
    /// # Returns
    ///
    /// Some(new_record) if transformation should proceed, None if record should be dropped
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn std::error::Error + Send + Sync>>;

    /// Get the configuration definition for this transformation.
    fn config(&self) -> &crate::connector_impl::ConfigDef;
}

/// Configurable transformation that can be configured with properties.
pub trait ConfigurableTransformation<R>: Transformation<R>
where
    R: Send + Sync,
{
    /// Configure this transformation with properties.
    ///
    /// # Arguments
    ///
    /// * `configs` - Configuration properties
    fn configure(
        &mut self,
        configs: std::collections::HashMap<String, String>,
    ) -> Result<(), crate::error::ConnectException>;
}

/// A transformation that wraps another transformation and applies it conditionally.
pub struct ConditionalTransformation<R> {
    predicate: Box<dyn Predicate<R>>,
    transformation: Box<dyn Transformation<R>>,
}

impl<R> ConditionalTransformation<R>
where
    R: ConnectRecord<R> + Send + Sync + 'static,
{
    /// Create a new ConditionalTransformation.
    pub fn new(
        predicate: Box<dyn Predicate<R>>,
        transformation: Box<dyn Transformation<R>>,
    ) -> Self {
        Self {
            predicate,
            transformation,
        }
    }
}

impl<R> Transformation<R> for ConditionalTransformation<R>
where
    R: ConnectRecord<R> + Send + Sync + 'static,
{
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn std::error::Error + Send + Sync>> {
        if self.predicate.test(&record) {
            self.transformation.apply(record)
        } else {
            Ok(Some(record))
        }
    }

    fn config(&self) -> &crate::connector_impl::ConfigDef {
        self.transformation.config()
    }
}

/// A simple pass-through transformation that returns the record unchanged.
pub struct PassThroughTransformation;

impl<R> Transformation<R> for PassThroughTransformation
where
    R: Send + Sync,
{
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Some(record))
    }

    fn config(&self) -> &crate::connector_impl::ConfigDef {
        static CONFIG: std::sync::OnceLock<crate::connector_impl::ConfigDef> =
            std::sync::OnceLock::new();
        CONFIG.get_or_init(|| crate::connector_impl::ConfigDef::new())
    }
}

// Re-export ConnectRecord for use in predicates
use crate::connector_types::ConnectRecord;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pass_through_transformation() {
        let mut transformation = PassThroughTransformation;
        let result = transformation.apply("test record".to_string());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some("test record".to_string()));
    }
}
