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

//! Predicate module for conditional transformations.
//! This module provides the Predicate trait for conditional.

use crate::connector_impl::{Closeable, ConfigDef, Configurable};
use crate::connector_types::ConnectRecord;
use std::collections::HashMap;
use std::error::Error;

/// Predicate for conditional transformations.
/// Predicates are used to determine whether a transformation should be applied to a record.
///
/// This trait extends Configurable and Closeable, matching the Java API:
/// ```java
/// public interface Predicate<R extends ConnectRecord<R>> extends Configurable, AutoCloseable {
///     ConfigDef config();
///     boolean test(R record);
///     void close();
/// }
/// ```
pub trait Predicate<R>: Configurable + Closeable + Send + Sync
where
    R: ConnectRecord<R>,
{
    /// Get the configuration definition for this predicate.
    ///
    /// # Returns
    ///
    /// The configuration definition
    fn config(&self) -> ConfigDef;

    /// Test if the record matches the predicate.
    ///
    /// # Arguments
    ///
    /// * `record` - The record to test
    ///
    /// # Returns
    ///
    /// true if the record matches the predicate, false otherwise
    fn test(&self, record: &R) -> bool;
}

/// Simple predicate that always returns true.
#[derive(Debug, Clone, Default)]
pub struct AlwaysTruePredicate;

impl<R> Predicate<R> for AlwaysTruePredicate
where
    R: ConnectRecord<R>,
{
    fn config(&self) -> ConfigDef {
        ConfigDef::new()
    }

    fn test(&self, _record: &R) -> bool {
        true
    }
}

impl Configurable for AlwaysTruePredicate {
    fn configure(&mut self, _configs: HashMap<String, Box<dyn std::any::Any>>) {
        // No configuration needed
    }
}

impl Closeable for AlwaysTruePredicate {
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        // No resources to release
        Ok(())
    }
}

/// Simple predicate that always returns false.
#[derive(Debug, Clone, Default)]
pub struct AlwaysFalsePredicate;

impl<R> Predicate<R> for AlwaysFalsePredicate
where
    R: ConnectRecord<R>,
{
    fn config(&self) -> ConfigDef {
        ConfigDef::new()
    }

    fn test(&self, _record: &R) -> bool {
        false
    }
}

impl Configurable for AlwaysFalsePredicate {
    fn configure(&mut self, _configs: HashMap<String, Box<dyn std::any::Any>>) {
        // No configuration needed
    }
}

impl Closeable for AlwaysFalsePredicate {
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        // No resources to release
        Ok(())
    }
}

/// Predicate that checks if the record's topic matches a pattern.
#[derive(Debug, Clone)]
pub struct TopicPredicate {
    topic: String,
}

impl TopicPredicate {
    /// Create a new TopicPredicate.
    pub fn new(topic: String) -> Self {
        Self { topic }
    }
}

impl<R> Predicate<R> for TopicPredicate
where
    R: ConnectRecord<R>,
{
    fn config(&self) -> ConfigDef {
        ConfigDef::new()
    }

    fn test(&self, record: &R) -> bool {
        record.topic() == self.topic
    }
}

impl Configurable for TopicPredicate {
    fn configure(&mut self, _configs: HashMap<String, Box<dyn std::any::Any>>) {
        // No configuration needed
    }
}

impl Closeable for TopicPredicate {
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        // No resources to release
        Ok(())
    }
}

/// Predicate that checks if the record has a specific header key.
#[derive(Debug, Clone)]
pub struct HeaderExistsPredicate {
    header_key: String,
}

impl HeaderExistsPredicate {
    /// Create a new HeaderExistsPredicate.
    pub fn new(header_key: String) -> Self {
        Self { header_key }
    }
}

impl<R> Predicate<R> for HeaderExistsPredicate
where
    R: ConnectRecord<R>,
{
    fn config(&self) -> ConfigDef {
        ConfigDef::new()
    }

    fn test(&self, record: &R) -> bool {
        let headers = record.headers();
        !headers.all_with_name(&self.header_key).is_empty()
    }
}

impl Configurable for HeaderExistsPredicate {
    fn configure(&mut self, _configs: HashMap<String, Box<dyn std::any::Any>>) {
        // No configuration needed
    }
}

impl Closeable for HeaderExistsPredicate {
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        // No resources to release
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector_impl::ConfigDef;
    use crate::data::{ConcreteHeaders, Headers};
    use std::sync::Arc;

    struct TestRecord {
        topic_value: String,
        headers: Box<dyn Headers>,
    }

    impl ConnectRecord<TestRecord> for TestRecord {
        fn topic(&self) -> &str {
            &self.topic_value
        }

        fn kafka_partition(&self) -> Option<i32> {
            None
        }

        fn key_schema(&self) -> Option<Arc<dyn crate::data::Schema>> {
            None
        }

        fn key(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
            None
        }

        fn value_schema(&self) -> Option<Arc<dyn crate::data::Schema>> {
            None
        }

        fn value(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
            None
        }

        fn timestamp(&self) -> Option<i64> {
            None
        }

        fn headers(&self) -> &dyn Headers {
            self.headers.as_ref()
        }

        fn new_record(
            self,
            _topic: Option<&str>,
            _partition: Option<i32>,
            _key_schema: Option<Arc<dyn crate::data::Schema>>,
            _key: Option<Arc<dyn std::any::Any + Send + Sync>>,
            _value_schema: Option<Arc<dyn crate::data::Schema>>,
            _value: Option<Arc<dyn std::any::Any + Send + Sync>>,
            _timestamp: Option<i64>,
            _headers: Option<Box<dyn Headers>>,
        ) -> TestRecord {
            self
        }
    }

    #[test]
    fn test_always_true_predicate() {
        let record = TestRecord {
            topic_value: "test".to_string(),
            headers: Box::new(ConcreteHeaders::new()),
        };
        let predicate = AlwaysTruePredicate;
        assert!(predicate.test(&record));
    }

    #[test]
    fn test_always_false_predicate() {
        let record = TestRecord {
            topic_value: "test".to_string(),
            headers: Box::new(ConcreteHeaders::new()),
        };
        let predicate = AlwaysFalsePredicate;
        assert!(!predicate.test(&record));
    }

    #[test]
    fn test_topic_predicate() {
        let record = TestRecord {
            topic_value: "test-topic".to_string(),
            headers: Box::new(ConcreteHeaders::new()),
        };
        let predicate = TopicPredicate::new("test-topic".to_string());
        assert!(predicate.test(&record));

        let predicate2 = TopicPredicate::new("other-topic".to_string());
        assert!(!predicate2.test(&record));
    }

    #[test]
    fn test_header() {
        let mut headers = ConcreteHeaders::new();
        #[test]
        fn test_header() {
            let mut headers = ConcreteHeaders::new();
            headers.add(Box::new(crate::connector_impl::SimpleHeader::new(
                "test-header".to_string(),
                vec![1, 2, 3],
            )));

            let record = TestRecord {
                topic_value: "test".to_string(),
                headers: Box::new(headers),
            };
            let predicate = HeaderExistsPredicate::new("test-header".to_string());
            assert!(predicate.test(&record));

            let predicate2 = HeaderExistsPredicate::new("missing-header".to_string());
            assert!(!predicate2.test(&record));
        }
    }

    #[test]
    fn test_predicate_config() {
        let predicate = AlwaysTruePredicate;
        let _config: ConfigDef = <AlwaysTruePredicate as Predicate<TestRecord>>::config(&predicate);
        // ConfigDef should be created successfully

        let predicate = AlwaysFalsePredicate;
        let _config: ConfigDef =
            <AlwaysFalsePredicate as Predicate<TestRecord>>::config(&predicate);

        let predicate = TopicPredicate::new("test".to_string());
        let _config: ConfigDef = <TopicPredicate as Predicate<TestRecord>>::config(&predicate);

        let predicate = HeaderExistsPredicate::new("test".to_string());
        let _config: ConfigDef =
            <HeaderExistsPredicate as Predicate<TestRecord>>::config(&predicate);
    }

    #[test]
    fn test_predicate_close() {
        let mut predicate = AlwaysTruePredicate;
        assert!(predicate.close().is_ok());

        let mut predicate = AlwaysFalsePredicate;
        assert!(predicate.close().is_ok());

        let mut predicate = TopicPredicate::new("test".to_string());
        assert!(predicate.close().is_ok());

        let mut predicate = HeaderExistsPredicate::new("test".to_string());
        assert!(predicate.close().is_ok());
    }

    #[test]
    fn test_predicate_configure() {
        use crate::connector_impl::Configurable;
        use std::any::Any;

        let mut predicate = AlwaysTruePredicate;
        let mut configs: HashMap<String, Box<dyn Any>> = HashMap::new();
        configs.insert("key".to_string(), Box::new("value".to_string()));
        predicate.configure(configs);

        let mut predicate = AlwaysFalsePredicate;
        let mut configs: HashMap<String, Box<dyn Any>> = HashMap::new();
        configs.insert("key".to_string(), Box::new("value".to_string()));
        predicate.configure(configs);

        let mut predicate = TopicPredicate::new("test".to_string());
        let mut configs: HashMap<String, Box<dyn Any>> = HashMap::new();
        configs.insert("key".to_string(), Box::new("value".to_string()));
        predicate.configure(configs);

        let mut predicate = HeaderExistsPredicate::new("test".to_string());
        let mut configs: HashMap<String, Box<dyn Any>> = HashMap::new();
        configs.insert("key".to_string(), Box::new("value".to_string()));
        predicate.configure(configs);
    }
}
