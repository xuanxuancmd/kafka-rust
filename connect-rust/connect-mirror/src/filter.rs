//! Topic and group filters
//!
//! This module provides filter traits for topics and consumer groups.

/// Topic filter
///
/// Determines which topics should be replicated.
pub trait TopicFilter: Send + Sync {
    /// Check if a topic should be replicated
    fn should_replicate_topic(&self, topic: String) -> bool;
}

/// Group filter
///
/// Determines which consumer groups should be replicated.
pub trait GroupFilter: Send + Sync {
    /// Check if a consumer group should be replicated
    fn should_replicate_group(&self, group: String) -> bool;
}

/// Default topic filter that includes all topics
pub struct DefaultTopicFilter;

impl TopicFilter for DefaultTopicFilter {
    fn should_replicate_topic(&self, _topic: String) -> bool {
        true
    }
}

/// Default group filter that includes all groups
pub struct DefaultGroupFilter;

impl GroupFilter for DefaultGroupFilter {
    fn should_replicate_group(&self, _group: String) -> bool {
        true
    }
}
