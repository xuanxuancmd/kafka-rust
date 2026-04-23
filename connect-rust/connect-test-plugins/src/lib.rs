//! Connect Test Plugins

pub mod mock_connector;
pub mod mock_sink_connector;
pub mod mock_sink_task;
pub mod mock_source_connector;
pub mod mock_source_task;
pub mod schema_source_connector;
pub mod schema_source_task;
pub mod verifiable_sink_connector;
pub mod verifiable_sink_task;
pub mod verifiable_source_connector;
pub mod verifiable_source_task;

pub use mock_connector::MockConnector;
pub use mock_sink_connector::MockSinkConnector;
pub use mock_sink_task::MockSinkTask;
pub use mock_source_connector::MockSourceConnector;
pub use mock_source_task::MockSourceTask;
pub use schema_source_connector::SchemaSourceConnector;
pub use schema_source_task::SchemaSourceTask;
pub use verifiable_sink_connector::VerifiableSinkConnector;
pub use verifiable_sink_task::VerifiableSinkTask;
pub use verifiable_source_connector::VerifiableSourceConnector;
pub use verifiable_source_task::VerifiableSourceTask;

#[cfg(test)]
mod tests {
    use super::*;
    use connect_api::components::Versioned;
    use connect_api::connector::Connector;
    use connect_api::connector::Task;
    use connect_api::source::SourceTask;
    use std::collections::HashMap;

    #[test]
    fn test_mock_connector_version() {
        assert_eq!(MockConnector::version(), "0.1.0");
    }

    #[test]
    fn test_mock_source_connector_can_be_created() {
        let connector = MockSourceConnector::new();
        assert_eq!(MockSourceConnector::version(), "0.1.0");
        assert_eq!(connector.task_class(), "MockSourceTask");
    }

    #[test]
    fn test_mock_sink_connector_can_be_created() {
        let connector = MockSinkConnector::new();
        assert_eq!(MockSinkConnector::version(), "0.1.0");
        assert_eq!(connector.task_class(), "MockSinkTask");
    }

    #[test]
    fn test_verifiable_source_connector_can_be_created() {
        let connector = VerifiableSourceConnector::new();
        assert_eq!(VerifiableSourceConnector::version(), "0.1.0");
        assert_eq!(connector.task_class(), "VerifiableSourceTask");
    }

    #[test]
    fn test_verifiable_sink_connector_can_be_created() {
        let connector = VerifiableSinkConnector::new();
        assert_eq!(VerifiableSinkConnector::version(), "0.1.0");
        assert_eq!(connector.task_class(), "VerifiableSinkTask");
    }

    #[test]
    fn test_schema_source_connector_can_be_created() {
        let connector = SchemaSourceConnector::new();
        assert_eq!(SchemaSourceConnector::version(), "0.1.0");
        assert_eq!(connector.task_class(), "SchemaSourceTask");
    }

    #[test]
    fn test_mock_source_task_can_be_created() {
        let task = MockSourceTask::new();
        assert_eq!(task.version(), "0.1.0");
    }

    #[test]
    fn test_mock_sink_task_can_be_created() {
        let task = MockSinkTask::new();
        assert_eq!(task.version(), "0.1.0");
    }

    #[test]
    fn test_verifiable_source_task_can_be_created() {
        let task = VerifiableSourceTask::new();
        assert_eq!(task.version(), "0.1.0");
    }

    #[test]
    fn test_verifiable_sink_task_can_be_created() {
        let task = VerifiableSinkTask::new();
        assert_eq!(task.version(), "0.1.0");
    }

    #[test]
    fn test_schema_source_task_can_be_created() {
        let task = SchemaSourceTask::new();
        assert_eq!(task.version(), "0.1.0");
    }

    #[test]
    fn test_mock_source_connector_task_configs() {
        let mut connector = MockSourceConnector::new();
        connector.start(HashMap::new());
        let configs = connector.task_configs(3).unwrap();
        assert_eq!(configs.len(), 3);
    }

    #[test]
    fn test_verifiable_source_task_poll() {
        let mut task = VerifiableSourceTask::new();
        let mut props = HashMap::new();
        props.insert("topic".to_string(), "test-topic".to_string());
        task.start(props);
        let records = task.poll().unwrap();
        assert!(!records.is_empty());
    }
}
