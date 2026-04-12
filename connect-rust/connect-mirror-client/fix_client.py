import sys

# Read the file
with open('src/client.rs', 'r', encoding='utf-8') as f:
    content = f.read()

# Find where tests module starts
tests_start = content.find('#[cfg(test)]\nmod tests {')

# Keep everything before tests
main_code = content[:tests_start]

# Write clean tests module
tests_module = '''#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::IdentityReplicationPolicy;
    use std::collections::HashMap;

    #[test]
    fn test_is_heartbeat_topic() {
        let client = TestMirrorClient::with_topics(vec![]);
        
        // Heartbeats topic is "heartbeats" (no suffix)
        assert!(client.is_heartbeat_topic("heartbeats"));
        // Topics ending with ".heartbeats" are also considered heartbeat topics
        assert!(client.is_heartbeat_topic("source1.heartbeats"));
        assert!(client.is_heartbeat_topic("source2.source1.heartbeats"));
        assert!(!client.is_heartbeat_topic("heartbeats!"));
        assert!(!client.is_heartbeat_topic("!heartbeats"));
        assert!(!client.is_heartbeat_topic("source1heartbeats"));
        assert!(!client.is_heartbeat_topic("source1-heartbeats"));
    }

    #[test]
    fn test_is_checkpoint_topic() {
        let client = TestMirrorClient::with_topics(vec![]);
        
        assert!(client.is_checkpoint_topic("source1.checkpoints.internal"));
        assert!(!client.is_checkpoint_topic("checkpoints.internal"));
        assert!(!client.is_checkpoint_topic("checkpoints-internal"));
        assert!(!client.is_checkpoint_topic("checkpoints.internal!"));
        assert!(!client.is_checkpoint_topic("!checkpoints.internal"));
        assert!(!client.is_checkpoint_topic("source1checkpointsinternal"));
    }

    #[test]
    fn test_count_hops_for_topic() {
        let client = TestMirrorClient::with_topics(vec![]);
        
        // With empty source (no prefix matching separator), returns error
        assert!(client.count_hops_for_topic("topic", "source").is_err());
        assert!(client.count_hops_for_topic("source", "source").is_err());
        assert!(client
            .count_hops_for_topic("sourcetopic", "source")
            .is_err());

        // When source matches, returns hops
        let result = client.count_hops_for_topic("source1.topic", "source1");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);

        let result = client.count_hops_for_topic("source2.source1.topic", "source2");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);

        let result = client.count_hops_for_topic("source2.source1.topic", "source1");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2);

        let result = client.count_hops_for_topic("source3.source2.source1.topic", "source1");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 3);

        let result = client.count_hops_for_topic("source3.source2.source1.topic", "source4");
        assert!(result.is_err());
    }

    #[test]
    fn test_heartbeat_topics() {
        let client = TestMirrorClient::with_topics(vec![
            "topic1".to_string(),
            "topic2".to_string(),
            "heartbeats".to_string(),
            "source1.heartbeats".to_string(),
            "source2.source1.heartbeats".to_string(),
            "source3.heartbeats".to_string(),
        ]);

        let heartbeat_topics = client.heartbeat_topics().unwrap();
        let topics_set: std::collections::HashSet<_> = heartbeat_topics.iter().cloned().collect();

        // Heartbeats topic is "heartbeats" (no suffix)
        assert!(topics_set.contains("heartbeats"));
        assert!(topics_set.contains("source1.heartbeats"));
        assert!(topics_set.contains("source2.source1.heartbeats"));
        assert!(topics_set.contains("source3.heartbeats"));
    }

    #[test]
    fn test_checkpoints_topics() {
        let client = TestMirrorClient::with_topics(vec![
            "topic1".to_string(),
            "topic2".to_string(),
            "checkpoints.internal".to_string(),
            "source.1.checkpoints.internal".to_string(),
            "source2.source1.checkpoints.internal".to_string(),
            "source3.checkpoints.internal".to_string(),
        ]);

        let checkpoint_topics = client.checkpoint_topics().unwrap();
        let topics_set: std::collections::HashSet<_> = checkpoint_topics.iter().cloned().collect();

        assert!(topics_set.contains("source1.checkpoints.internal"));
        assert!(topics_set.contains("source2.source1.checkpoints.internal"));
        assert!(topics_set.contains("source3.checkpoints.internal"));
    }

    #[test]
    fn test_replication_hops() {
        let client = TestMirrorClient::with_topics(vec![
            "topic1".to_string(),
            "topic2".to_string(),
            "heartbeats".to_string(),
            "source1.heartbeats".to_string(),
            "source1.source2.heartbeats".to_string(),
            "source3.heartbeats".to_string(),
        ]);

        assert_eq!(client.replication_hops("source1".to_string()).unwrap(), 1);
        assert_eq!(client.replication_hops("source2".to_string()).unwrap(), 2);
        assert_eq!(client.replication_hops("source3".to_string()).unwrap(), 1);
        assert!(client.replication_hops("source4".to_string()).is_err());
    }

    #[test]
    fn test_upstream_clusters() {
        let client = TestMirrorClient::with_topics(vec![
            "topic1".to_string(),
            "topic2".to_string(),
            "heartbeats".to_string(),
            "source1.heartbeats".to_string(),
            "source1.source2.heartbeats".to_string(),
            "source3.source4.source5.heartbeats".to_string(),
        ]);

        let sources = client.upstream_clusters().unwrap();
        let sources_set: std::collections::HashSet<_> = sources.iter().cloned().collect();

        assert!(sources_set.contains("source1"));
        assert!(sources_set.contains("source2"));
        assert!(sources_set.contains("source3"));
        assert!(sources_set.contains("source4"));
        assert!(sources_set.contains("source5"));
        assert!(!sources_set.contains("sourceX"));
        assert!(!sources_set.contains(""));
    }

    #[test]
    fn test_identity_replication_upstream_clusters() {
        let mut policy = IdentityReplicationPolicy::new();
        let mut config = HashMap::new();
        config.insert("source.cluster.alias".to_string(), "source".to_string());
        policy.configure(&config);

        let client = TestMirrorClient::with_policy_and_topics(
            Box::new(policy),
            vec![
                "topic1".to_string(),
                "topic2".to_string(),
                "heartbeats".to_string(),
                "source1.heartbeats".to_string(),
                "source1.source2.heartbeats".to_string(),
                "source3.source4.source5.heartbeats".to_string(),
            ],
        );

        let sources = client.upstream_clusters().unwrap();
        let sources_set: std::collections::HashSet<_> = sources.iter().cloned().collect();

        assert!(sources_set.contains("source1"));
        assert!(sources_set.contains.contains("source2"));
        assert!(sources_set.contains("source3"));
        assert!(sources_set.contains("source4"));
        assert!(sources_set.contains("source5"));
        assert!(!sources_set.contains(""));
        assert_eq!(sources_set.len(), 5);
    }

    #[test]
    fn test_remote_topics() {
        let client = TestMirrorClient::with_topics(vec![
            "topic1".to_string(),
            "topic2".to_string(),
            "topic3".to_string(),
            "source1.topic4".to_string(),
            "source1.source2.topic5".to_string(),
            "source3.source4.source5.topic6".to_string(),
        ]);

        let remote_topics = client.remote_topics().unwrap();
        let topics_set: std::collections::HashSet<_> = remote_topics.iter().cloned().collect();

        assert!(!topics_set.contains("topic1"));
        assert!(!topics_set.contains("topic2"));
        assert!(!topics_set.contains("topic3"));
        assert!(topics_set.conta
