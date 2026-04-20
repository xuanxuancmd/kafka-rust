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

//! Transformation chain and stage for Kafka Connect.
//!
//! This module provides the transformation chain that applies a series of
//! Single Message Transformations (SMTs) to records in sequence.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.TransformationChain` and
//! `org.apache.kafka.connect.runtime.TransformationStage` in Java.

use std::collections::HashMap;
use std::sync::Arc;

use connect_api::connector::ConnectRecord;
use connect_api::errors::ConnectError;
use connect_api::source::SourceRecord;
use connect_api::transforms::{Predicate, Transformation};
use serde_json::Value;

use crate::errors::{ProcessingContext, RetryWithToleranceOperator, Stage};

use super::abstract_worker_source_task::SourceTaskTransformationChain;
use super::task_plugins_metadata::StageInfo;

/// TransformationStage represents a single step in the transformation chain.
///
/// Each stage contains:
/// - A transformation that modifies the record
/// - An optional predicate that determines whether the transformation should be applied
/// - A negate flag to invert the predicate result
///
/// Corresponds to `org.apache.kafka.connect.runtime.TransformationStage` in Java.
pub struct TransformationStage<R> {
    /// The transformation to apply.
    transformation: Box<dyn Transformation<R> + Send + Sync>,
    /// The predicate to evaluate (optional).
    predicate: Option<Box<dyn Predicate<R> + Send + Sync>>,
    /// Whether to negate the predicate result.
    negate: bool,
    /// The stage info for metadata.
    stage_info: Option<StageInfo>,
}

impl<R> TransformationStage<R> {
    /// Creates a new TransformationStage with a transformation and optional predicate.
    ///
    /// # Arguments
    /// * `transformation` - The transformation to apply
    /// * `predicate` - Optional predicate for conditional application
    /// * `negate` - Whether to negate the predicate result
    /// * `stage_info` - Optional stage info for metadata
    pub fn new(
        transformation: Box<dyn Transformation<R> + Send + Sync>,
        predicate: Option<Box<dyn Predicate<R> + Send + Sync>>,
        negate: bool,
        stage_info: Option<StageInfo>,
    ) -> Self {
        TransformationStage {
            transformation,
            predicate,
            negate,
            stage_info,
        }
    }

    /// Creates a TransformationStage without a predicate (always applies).
    pub fn without_predicate(transformation: Box<dyn Transformation<R> + Send + Sync>) -> Self {
        TransformationStage::new(transformation, None, false, None)
    }

    /// Creates a TransformationStage with a predicate and optional negation.
    pub fn with_predicate(
        transformation: Box<dyn Transformation<R> + Send + Sync>,
        predicate: Box<dyn Predicate<R> + Send + Sync>,
        negate: bool,
    ) -> Self {
        TransformationStage::new(transformation, Some(predicate), negate, None)
    }

    /// Returns whether this stage has a predicate configured.
    pub fn has_predicate(&self) -> bool {
        self.predicate.is_some()
    }

    /// Returns whether negate is set for this stage.
    pub fn is_negated(&self) -> bool {
        self.negate
    }

    /// Returns the stage info if available.
    pub fn stage_info(&self) -> Option<&StageInfo> {
        self.stage_info.as_ref()
    }

    /// Evaluates the predicate for the given record.
    ///
    /// Returns true if the transformation should be applied.
    /// - If no predicate: always returns true
    /// - If predicate: returns predicate.test(record) XOR negate
    pub fn should_apply(&self, record: &R) -> bool {
        match &self.predicate {
            None => true,
            Some(predicate) => {
                let test_result = predicate.test(record);
                if self.negate {
                    !test_result
                } else {
                    test_result
                }
            }
        }
    }

    /// Applies the transformation to the record if the predicate allows.
    ///
    /// # Arguments
    /// * `record` - The record to transform
    /// * `context` - The processing context for error handling
    /// * `retry_operator` - The retry operator for error handling
    ///
    /// # Returns
    /// - `Ok(Some(record))` - The transformed record
    /// - `Ok(None)` - The record should be filtered/dropped
    /// - `Err(ConnectError)` - Transformation failed
    pub fn apply(
        &self,
        record: R,
        context: &mut ProcessingContext,
        _retry_operator: &RetryWithToleranceOperator,
    ) -> Result<Option<R>, ConnectError> {
        // Check if predicate allows transformation
        if !self.should_apply(&record) {
            // Predicate evaluates to false, skip this transformation
            return Ok(Some(record));
        }

        // Apply the transformation
        context.set_stage(Stage::TRANSFORMATION);

        let transformation_class = self
            .stage_info
            .as_ref()
            .map(|info| info.transform().plugin_class())
            .unwrap_or("unknown");

        context.set_executing_class(transformation_class.to_string());

        // Execute transformation directly
        // Note: Retry logic would need Clone bound on R, which we avoid here for simplicity
        // Full implementation would use RetryWithToleranceOperator with Clone records
        let result = self.transformation.transform(record);

        match result {
            Ok(Some(transformed)) => Ok(Some(transformed)),
            Ok(None) => {
                // Record was intentionally filtered
                Ok(None)
            }
            Err(e) => {
                // Transformation failed
                context.mark_failed();
                Err(e)
            }
        }
    }

    /// Closes this transformation stage.
    pub fn close(&mut self) {
        self.transformation.close();
        if let Some(predicate) = &mut self.predicate {
            predicate.close();
        }
    }
}

/// TransformationChain manages a list of TransformationStage objects.
///
/// The chain applies transformations in sequence to records.
/// If any transformation returns None (filter), the chain stops
/// and returns None for that record.
///
/// Corresponds to `org.apache.kafka.connect.runtime.TransformationChain` in Java.
pub struct TransformationChain<R> {
    /// The list of transformation stages.
    stages: Vec<TransformationStage<R>>,
    /// The retry operator for error handling.
    retry_operator: Arc<RetryWithToleranceOperator>,
}

impl<R> TransformationChain<R> {
    /// Creates a new empty TransformationChain.
    pub fn new(retry_operator: Arc<RetryWithToleranceOperator>) -> Self {
        TransformationChain {
            stages: Vec::new(),
            retry_operator,
        }
    }

    /// Creates a TransformationChain from a list of stages.
    pub fn with_stages(
        stages: Vec<TransformationStage<R>>,
        retry_operator: Arc<RetryWithToleranceOperator>,
    ) -> Self {
        TransformationChain {
            stages,
            retry_operator,
        }
    }

    /// Adds a transformation stage to the chain.
    pub fn add_stage(&mut self, stage: TransformationStage<R>) {
        self.stages.push(stage);
    }

    /// Returns the number of stages in the chain.
    pub fn stage_count(&self) -> usize {
        self.stages.len()
    }

    /// Returns true if the chain is empty.
    pub fn is_empty(&self) -> bool {
        self.stages.is_empty()
    }

    /// Returns the stage info for all stages.
    pub fn stage_infos(&self) -> Vec<StageInfo> {
        self.stages
            .iter()
            .filter_map(|stage| stage.stage_info.clone())
            .collect()
    }

    /// Applies all transformations in the chain to a record.
    ///
    /// # Arguments
    /// * `context` - The processing context for error handling
    /// * `record` - The record to transform
    ///
    /// # Returns
    /// - `Some(record)` - The transformed record after all stages
    /// - `None` - The record was filtered by a transformation or failed
    pub fn apply(&self, context: &mut ProcessingContext, record: R) -> Option<R> {
        let mut current_record = record;

        for stage in &self.stages {
            // Reset context for each stage
            context.reset();
            context.set_stage(Stage::TRANSFORMATION);

            let result = stage.apply(current_record, context, &*self.retry_operator);

            match result {
                Ok(Some(transformed)) => {
                    current_record = transformed;
                }
                Ok(None) => {
                    // Record was filtered, stop the chain
                    log::debug!("Record filtered by transformation stage");
                    return None;
                }
                Err(e) => {
                    // Transformation failed
                    log::error!("Transformation failed: {}", e.message());
                    context.mark_failed();
                    return None;
                }
            }
        }

        Some(current_record)
    }

    /// Closes all transformation stages in the chain.
    pub fn close(&mut self) {
        for stage in &mut self.stages {
            stage.close();
        }
        self.stages.clear();
    }
}

/// Implementation of SourceTaskTransformationChain for SourceRecord.
impl SourceTaskTransformationChain for TransformationChain<SourceRecord> {
    fn apply(
        &self,
        context: &mut ProcessingContext,
        record: &SourceRecord,
    ) -> Option<SourceRecord> {
        TransformationChain::apply(self, context, record.clone())
    }
}

/// Builder for creating TransformationChain instances.
///
/// This builder helps construct a transformation chain from configuration.
pub struct TransformationChainBuilder<R> {
    stages: Vec<TransformationStage<R>>,
    retry_operator: Option<Arc<RetryWithToleranceOperator>>,
}

impl<R> TransformationChainBuilder<R> {
    /// Creates a new builder.
    pub fn new() -> Self {
        TransformationChainBuilder {
            stages: Vec::new(),
            retry_operator: None,
        }
    }

    /// Sets the retry operator.
    pub fn with_retry_operator(mut self, operator: Arc<RetryWithToleranceOperator>) -> Self {
        self.retry_operator = Some(operator);
        self
    }

    /// Adds a transformation stage.
    pub fn add_stage(mut self, stage: TransformationStage<R>) -> Self {
        self.stages.push(stage);
        self
    }

    /// Adds a transformation stage in-place.
    pub fn add_stage_mut(&mut self, stage: TransformationStage<R>) {
        self.stages.push(stage);
    }

    /// Builds the TransformationChain.
    ///
    /// Returns an error if retry_operator is not set.
    pub fn build(self) -> Result<TransformationChain<R>, ConnectError> {
        let retry_operator = self.retry_operator.ok_or_else(|| {
            ConnectError::general("RetryWithToleranceOperator is required for TransformationChain")
        })?;

        Ok(TransformationChain::with_stages(
            self.stages,
            retry_operator,
        ))
    }
}

impl<R> Default for TransformationChainBuilder<R> {
    fn default() -> Self {
        Self::new()
    }
}

/// A simple predicate that always returns true.
/// Used as a default predicate when none is specified.
pub struct AlwaysTruePredicate;

impl<R> Predicate<R> for AlwaysTruePredicate {
    fn configure(&mut self, _configs: HashMap<String, Value>) {}

    fn test(&self, _record: &R) -> bool {
        true
    }

    fn close(&mut self) {}
}

/// A simple predicate that always returns false.
pub struct AlwaysFalsePredicate;

impl<R> Predicate<R> for AlwaysFalsePredicate {
    fn configure(&mut self, _configs: HashMap<String, Value>) {}

    fn test(&self, _record: &R) -> bool {
        false
    }

    fn close(&mut self) {}
}

/// A predicate that checks if a topic name matches a pattern.
/// This is a common predicate used in Kafka Connect.
pub struct TopicNameMatchesPredicate {
    pattern: String,
}

impl TopicNameMatchesPredicate {
    /// Creates a new TopicNameMatchesPredicate.
    pub fn new(pattern: String) -> Self {
        TopicNameMatchesPredicate { pattern }
    }
}

impl Predicate<SourceRecord> for TopicNameMatchesPredicate {
    fn configure(&mut self, configs: HashMap<String, Value>) {
        if let Some(pattern) = configs.get("pattern") {
            if let Some(pattern_str) = pattern.as_str() {
                self.pattern = pattern_str.to_string();
            }
        }
    }

    fn test(&self, record: &SourceRecord) -> bool {
        record.topic().contains(&self.pattern)
    }

    fn close(&mut self) {}
}

/// A no-op transformation that passes records through unchanged.
/// Used for testing and as a placeholder.
pub struct NoOpTransformation;

impl<R: Clone> Transformation<R> for NoOpTransformation {
    fn configure(&mut self, _configs: HashMap<String, Value>) {}

    fn transform(&self, record: R) -> Result<Option<R>, ConnectError> {
        Ok(Some(record))
    }

    fn close(&mut self) {}
}

/// A filter transformation that drops all records.
/// Used for testing predicate behavior.
pub struct FilterAllTransformation;

impl<R> Transformation<R> for FilterAllTransformation {
    fn configure(&mut self, _configs: HashMap<String, Value>) {}

    fn transform(&self, _record: R) -> Result<Option<R>, ConnectError> {
        Ok(None)
    }

    fn close(&mut self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::{ErrorHandlingMetrics, ToleranceType};
    use connect_api::connector::ConnectRecord;
    use connect_api::header::ConnectHeaders;

    fn create_test_record(topic: &str) -> SourceRecord {
        SourceRecord::new(
            HashMap::new(),
            HashMap::new(),
            topic,
            None,
            None,
            Value::String("test-value".to_string()),
        )
    }

    fn create_retry_operator() -> Arc<RetryWithToleranceOperator> {
        Arc::new(RetryWithToleranceOperator::new(
            0,
            0,
            ToleranceType::ALL,
            Arc::new(ErrorHandlingMetrics::new()),
        ))
    }

    #[test]
    fn test_transformation_stage_without_predicate() {
        let stage = TransformationStage::without_predicate(Box::new(NoOpTransformation));
        assert!(!stage.has_predicate());
        assert!(!stage.is_negated());

        let record = create_test_record("test-topic");
        assert!(stage.should_apply(&record));
    }

    #[test]
    fn test_transformation_stage_with_predicate() {
        let stage = TransformationStage::with_predicate(
            Box::new(NoOpTransformation),
            Box::new(TopicNameMatchesPredicate::new("test".to_string())),
            false,
        );
        assert!(stage.has_predicate());
        assert!(!stage.is_negated());

        let matching_record = create_test_record("test-topic");
        assert!(stage.should_apply(&matching_record));

        let non_matching_record = create_test_record("other-topic");
        assert!(!stage.should_apply(&non_matching_record));
    }

    #[test]
    fn test_transformation_stage_with_negated_predicate() {
        let stage = TransformationStage::with_predicate(
            Box::new(NoOpTransformation),
            Box::new(TopicNameMatchesPredicate::new("test".to_string())),
            true,
        );
        assert!(stage.has_predicate());
        assert!(stage.is_negated());

        let matching_record = create_test_record("test-topic");
        // Negated, so matching record should NOT apply
        assert!(!stage.should_apply(&matching_record));

        let non_matching_record = create_test_record("other-topic");
        // Negated, so non-matching record SHOULD apply
        assert!(stage.should_apply(&non_matching_record));
    }

    #[test]
    fn test_always_true_predicate() {
        let predicate = AlwaysTruePredicate;
        let record = create_test_record("any-topic");
        assert!(predicate.test(&record));
    }

    #[test]
    fn test_always_false_predicate() {
        let predicate = AlwaysFalsePredicate;
        let record = create_test_record("any-topic");
        assert!(!predicate.test(&record));
    }

    #[test]
    fn test_topic_name_matches_predicate() {
        let predicate = TopicNameMatchesPredicate::new("important".to_string());

        let matching_record = create_test_record("important-data");
        assert!(predicate.test(&matching_record));

        let non_matching_record = create_test_record("regular-data");
        assert!(!predicate.test(&non_matching_record));
    }

    #[test]
    fn test_noop_transformation() {
        let transformation = NoOpTransformation;
        let record = create_test_record("test-topic");

        let result = transformation.transform(record.clone()).unwrap();
        assert!(result.is_some());
        let transformed = result.unwrap();
        assert_eq!(transformed.topic(), record.topic());
    }

    #[test]
    fn test_filter_all_transformation() {
        let transformation = FilterAllTransformation;
        let record = create_test_record("test-topic");

        let result = transformation.transform(record).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_transformation_chain_empty() {
        let retry_operator = create_retry_operator();
        let chain = TransformationChain::<SourceRecord>::new(retry_operator);

        assert!(chain.is_empty());
        assert_eq!(chain.stage_count(), 0);

        let mut context = ProcessingContext::new();
        let record = create_test_record("test-topic");
        let result = chain.apply(&mut context, record);
        assert!(result.is_some());
    }

    #[test]
    fn test_transformation_chain_single_stage() {
        let retry_operator = create_retry_operator();
        let mut chain = TransformationChain::<SourceRecord>::new(retry_operator);

        chain.add_stage(TransformationStage::without_predicate(Box::new(
            NoOpTransformation,
        )));

        assert!(!chain.is_empty());
        assert_eq!(chain.stage_count(), 1);

        let mut context = ProcessingContext::new();
        let record = create_test_record("test-topic");
        let result = chain.apply(&mut context, record);
        assert!(result.is_some());
    }

    #[test]
    fn test_transformation_chain_filter() {
        let retry_operator = create_retry_operator();
        let mut chain = TransformationChain::<SourceRecord>::new(retry_operator);

        // Add a filter transformation
        chain.add_stage(TransformationStage::without_predicate(Box::new(
            FilterAllTransformation,
        )));

        let mut context = ProcessingContext::new();
        let record = create_test_record("test-topic");
        let result = chain.apply(&mut context, record);
        assert!(result.is_none());
    }

    #[test]
    fn test_transformation_chain_multiple_stages() {
        let retry_operator = create_retry_operator();
        let mut chain = TransformationChain::<SourceRecord>::new(retry_operator);

        chain.add_stage(TransformationStage::without_predicate(Box::new(
            NoOpTransformation,
        )));
        chain.add_stage(TransformationStage::without_predicate(Box::new(
            NoOpTransformation,
        )));
        chain.add_stage(TransformationStage::without_predicate(Box::new(
            NoOpTransformation,
        )));

        assert_eq!(chain.stage_count(), 3);

        let mut context = ProcessingContext::new();
        let record = create_test_record("test-topic");
        let result = chain.apply(&mut context, record);
        assert!(result.is_some());
    }

    #[test]
    fn test_transformation_chain_with_predicate_filter() {
        let retry_operator = create_retry_operator();
        let mut chain = TransformationChain::<SourceRecord>::new(retry_operator);

        // Add transformation that only applies to matching topics
        chain.add_stage(TransformationStage::with_predicate(
            Box::new(FilterAllTransformation),
            Box::new(TopicNameMatchesPredicate::new("filter".to_string())),
            false,
        ));

        // Record matching predicate should be filtered
        let mut context = ProcessingContext::new();
        let matching_record = create_test_record("filter-topic");
        let result = chain.apply(&mut context, matching_record);
        assert!(result.is_none());

        // Record not matching predicate should pass through
        context.reset();
        let non_matching_record = create_test_record("pass-topic");
        let result = chain.apply(&mut context, non_matching_record);
        assert!(result.is_some());
    }

    #[test]
    fn test_transformation_chain_with_negated_predicate() {
        let retry_operator = create_retry_operator();
        let mut chain = TransformationChain::<SourceRecord>::new(retry_operator);

        // Add transformation that filters records NOT matching "keep"
        chain.add_stage(TransformationStage::with_predicate(
            Box::new(FilterAllTransformation),
            Box::new(TopicNameMatchesPredicate::new("keep".to_string())),
            true, // Negated: apply filter to topics NOT matching "keep"
        ));

        // Record matching "keep" should pass (negated predicate is false, so filter not applied)
        let mut context = ProcessingContext::new();
        let keep_record = create_test_record("keep-topic");
        let result = chain.apply(&mut context, keep_record);
        assert!(result.is_some());

        // Record NOT matching "keep" should be filtered (negated predicate is true, filter applied)
        context.reset();
        let other_record = create_test_record("other-topic");
        let result = chain.apply(&mut context, other_record);
        assert!(result.is_none());
    }

    #[test]
    fn test_transformation_chain_builder() {
        let retry_operator = create_retry_operator();

        let chain = TransformationChainBuilder::<SourceRecord>::new()
            .with_retry_operator(retry_operator)
            .add_stage(TransformationStage::without_predicate(Box::new(
                NoOpTransformation,
            )))
            .add_stage(TransformationStage::without_predicate(Box::new(
                NoOpTransformation,
            )))
            .build()
            .unwrap();

        assert_eq!(chain.stage_count(), 2);
    }

    #[test]
    fn test_transformation_chain_builder_missing_retry_operator() {
        let result = TransformationChainBuilder::<SourceRecord>::new()
            .add_stage(TransformationStage::without_predicate(Box::new(
                NoOpTransformation,
            )))
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_transformation_chain_close() {
        let retry_operator = create_retry_operator();
        let mut chain = TransformationChain::<SourceRecord>::new(retry_operator);

        chain.add_stage(TransformationStage::without_predicate(Box::new(
            NoOpTransformation,
        )));
        chain.add_stage(TransformationStage::without_predicate(Box::new(
            NoOpTransformation,
        )));

        assert_eq!(chain.stage_count(), 2);

        chain.close();

        assert!(chain.is_empty());
        assert_eq!(chain.stage_count(), 0);
    }

    #[test]
    fn test_transformation_stage_apply_no_predicate() {
        let retry_operator = create_retry_operator();
        let stage = TransformationStage::without_predicate(Box::new(NoOpTransformation));

        let mut context = ProcessingContext::new();
        let record = create_test_record("test-topic");

        let result = stage.apply(record, &mut context, &*retry_operator).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_transformation_stage_apply_predicate_true() {
        let retry_operator = create_retry_operator();
        let stage = TransformationStage::with_predicate(
            Box::new(NoOpTransformation),
            Box::new(AlwaysTruePredicate),
            false,
        );

        let mut context = ProcessingContext::new();
        let record = create_test_record("test-topic");

        // Predicate returns true, transformation should be applied
        let result = stage.apply(record, &mut context, &*retry_operator).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_transformation_stage_apply_predicate_false() {
        let retry_operator = create_retry_operator();
        let stage = TransformationStage::with_predicate(
            Box::new(FilterAllTransformation),
            Box::new(AlwaysFalsePredicate),
            false,
        );

        let mut context = ProcessingContext::new();
        let record = create_test_record("test-topic");

        // Predicate returns false, transformation should NOT be applied
        // Record should pass through unchanged
        let result = stage.apply(record, &mut context, &*retry_operator).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_source_task_transformation_chain_impl() {
        let retry_operator = create_retry_operator();
        let mut chain = TransformationChain::<SourceRecord>::new(retry_operator);

        chain.add_stage(TransformationStage::without_predicate(Box::new(
            NoOpTransformation,
        )));

        // Test through trait interface
        let transformation_chain: &dyn SourceTaskTransformationChain = &chain;

        let mut context = ProcessingContext::new();
        let record = create_test_record("test-topic");
        let result = transformation_chain.apply(&mut context, &record);
        assert!(result.is_some());
    }

    #[test]
    fn test_predicate_configure() {
        let mut predicate = TopicNameMatchesPredicate::new("default".to_string());

        let configs: HashMap<String, Value> =
            HashMap::from([("pattern".to_string(), Value::String("custom".to_string()))]);

        predicate.configure(configs);

        let matching_record = create_test_record("custom-topic");
        assert!(predicate.test(&matching_record));

        let non_matching_record = create_test_record("default-topic");
        assert!(!predicate.test(&non_matching_record));
    }

    #[test]
    fn test_transformation_chain_stops_on_filter() {
        let retry_operator = create_retry_operator();
        let mut chain = TransformationChain::<SourceRecord>::new(retry_operator);

        // Add NoOp, then FilterAll, then NoOp
        // The third NoOp should never be reached because FilterAll returns None
        chain.add_stage(TransformationStage::without_predicate(Box::new(
            NoOpTransformation,
        )));
        chain.add_stage(TransformationStage::without_predicate(Box::new(
            FilterAllTransformation,
        )));
        chain.add_stage(TransformationStage::without_predicate(Box::new(
            NoOpTransformation,
        )));

        let mut context = ProcessingContext::new();
        let record = create_test_record("test-topic");
        let result = chain.apply(&mut context, record);
        assert!(result.is_none());
    }
}
