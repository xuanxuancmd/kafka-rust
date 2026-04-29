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

//! Configuration constants for Kafka clients.
//!
//! This module defines configuration key constants used by AdminClient, Consumer, and Producer.
//! Corresponds to Java `org.apache.kafka.clients.CommonClientConfigs`, `ConsumerConfig`,
//! `ProducerConfig`, and `AdminClientConfig`.

/// Common client configuration constants.
///
/// These configurations are shared by all Kafka clients: producer, consumer, admin, and connect.
/// Corresponds to Java `org.apache.kafka.clients.CommonClientConfigs`.
pub struct CommonClientConfigs;

impl CommonClientConfigs {
    /// `bootstrap.servers` - A list of host/port pairs used to establish the initial connection to the Kafka cluster.
    pub const BOOTSTRAP_SERVERS_CONFIG: &'static str = "bootstrap.servers";

    /// `client.dns.lookup` - Controls how the client uses DNS lookups.
    pub const CLIENT_DNS_LOOKUP_CONFIG: &'static str = "client.dns.lookup";

    /// `metadata.max.age.ms` - The period of time in milliseconds after which we force a refresh of metadata.
    pub const METADATA_MAX_AGE_CONFIG: &'static str = "metadata.max.age.ms";

    /// `send.buffer.bytes` - The size of the TCP send buffer (SO_SNDBUF) to use when sending data.
    pub const SEND_BUFFER_CONFIG: &'static str = "send.buffer.bytes";

    /// `receive.buffer.bytes` - The size of the TCP receive buffer (SO_RCVBUF) to use when reading data.
    pub const RECEIVE_BUFFER_CONFIG: &'static str = "receive.buffer.bytes";

    /// `client.id` - An id string to pass to the server when making requests.
    pub const CLIENT_ID_CONFIG: &'static str = "client.id";

    /// `client.rack` - A rack identifier for this client.
    pub const CLIENT_RACK_CONFIG: &'static str = "client.rack";

    /// Default value for `client.rack` - empty string.
    pub const DEFAULT_CLIENT_RACK: &'static str = "";

    /// `reconnect.backoff.ms` - The base amount of time to wait before attempting to reconnect.
    pub const RECONNECT_BACKOFF_MS_CONFIG: &'static str = "reconnect.backoff.ms";

    /// `reconnect.backoff.max.ms` - The maximum amount of time to wait when reconnecting.
    pub const RECONNECT_BACKOFF_MAX_MS_CONFIG: &'static str = "reconnect.backoff.max.ms";

    /// `retries` - Setting a value greater than zero will cause the client to resend any request that fails.
    pub const RETRIES_CONFIG: &'static str = "retries";

    /// `retry.backoff.ms` - The amount of time to wait before attempting to retry a failed request.
    pub const RETRY_BACKOFF_MS_CONFIG: &'static str = "retry.backoff.ms";

    /// Default value for `retry.backoff.ms` - 100ms.
    pub const DEFAULT_RETRY_BACKOFF_MS: i64 = 100;

    /// `retry.backoff.max.ms` - The maximum amount of time to wait when retrying a request.
    pub const RETRY_BACKOFF_MAX_MS_CONFIG: &'static str = "retry.backoff.max.ms";

    /// Default value for `retry.backoff.max.ms` - 1000ms.
    pub const DEFAULT_RETRY_BACKOFF_MAX_MS: i64 = 1000;

    /// `enable.metrics.push` - Whether to enable pushing of client metrics to the cluster.
    pub const ENABLE_METRICS_PUSH_CONFIG: &'static str = "enable.metrics.push";

    /// `metrics.sample.window.ms` - The window of time a metrics sample is computed over.
    pub const METRICS_SAMPLE_WINDOW_MS_CONFIG: &'static str = "metrics.sample.window.ms";

    /// `metrics.num.samples` - The number of samples maintained to compute metrics.
    pub const METRICS_NUM_SAMPLES_CONFIG: &'static str = "metrics.num.samples";

    /// `metrics.recording.level` - The highest recording level for metrics.
    pub const METRICS_RECORDING_LEVEL_CONFIG: &'static str = "metrics.recording.level";

    /// `metric.reporters` - A list of classes to use as metrics reporters.
    pub const METRIC_REPORTER_CLASSES_CONFIG: &'static str = "metric.reporters";

    /// `security.protocol` - Protocol used to communicate with brokers.
    pub const SECURITY_PROTOCOL_CONFIG: &'static str = "security.protocol";

    /// Default value for `security.protocol` - PLAINTEXT.
    pub const DEFAULT_SECURITY_PROTOCOL: &'static str = "PLAINTEXT";

    /// `socket.connection.setup.timeout.ms` - The amount of time the client will wait for the socket connection to be established.
    pub const SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG: &'static str =
        "socket.connection.setup.timeout.ms";

    /// Default value for `socket.connection.setup.timeout.ms` - 10000ms (10 seconds).
    pub const DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS: i64 = 10_000;

    /// `socket.connection.setup.timeout.max.ms` - The maximum amount of time the client will wait for the socket connection.
    pub const SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG: &'static str =
        "socket.connection.setup.timeout.max.ms";

    /// Default value for `socket.connection.setup.timeout.max.ms` - 30000ms (30 seconds).
    pub const DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS: i64 = 30_000;

    /// `connections.max.idle.ms` - Close idle connections after the number of milliseconds specified.
    pub const CONNECTIONS_MAX_IDLE_MS_CONFIG: &'static str = "connections.max.idle.ms";

    /// `request.timeout.ms` - The maximum amount of time the client will wait for the response of a request.
    pub const REQUEST_TIMEOUT_MS_CONFIG: &'static str = "request.timeout.ms";

    /// `default.api.timeout.ms` - Specifies the default timeout for client APIs.
    pub const DEFAULT_API_TIMEOUT_MS_CONFIG: &'static str = "default.api.timeout.ms";

    /// `group.id` - A unique string that identifies the consumer group.
    pub const GROUP_ID_CONFIG: &'static str = "group.id";

    /// `group.instance.id` - A unique identifier of the consumer instance provided by the end user.
    pub const GROUP_INSTANCE_ID_CONFIG: &'static str = "group.instance.id";

    /// `max.poll.interval.ms` - The maximum delay between invocations of poll() when using consumer group management.
    pub const MAX_POLL_INTERVAL_MS_CONFIG: &'static str = "max.poll.interval.ms";

    /// `rebalance.timeout.ms` - The maximum allowed time for each worker to join the group once a rebalance has begun.
    pub const REBALANCE_TIMEOUT_MS_CONFIG: &'static str = "rebalance.timeout.ms";

    /// `session.timeout.ms` - The timeout used to detect client failures when using Kafka's group management.
    pub const SESSION_TIMEOUT_MS_CONFIG: &'static str = "session.timeout.ms";

    /// `heartbeat.interval.ms` - The expected time between heartbeats to the consumer coordinator.
    pub const HEARTBEAT_INTERVAL_MS_CONFIG: &'static str = "heartbeat.interval.ms";

    /// `metadata.recovery.strategy` - Controls how the client recovers when none of the brokers known to it is available.
    pub const METADATA_RECOVERY_STRATEGY_CONFIG: &'static str = "metadata.recovery.strategy";

    /// Default value for `metadata.recovery.strategy` - rebootstrap.
    pub const DEFAULT_METADATA_RECOVERY_STRATEGY: &'static str = "rebootstrap";

    /// `metadata.recovery.rebootstrap.trigger.ms` - Trigger rebootstrap after this interval.
    pub const METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG: &'static str =
        "metadata.recovery.rebootstrap.trigger.ms";

    /// Default value for `metadata.recovery.rebootstrap.trigger.ms` - 300000ms (5 minutes).
    pub const DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS: i64 = 300_000;

    /// Lower bound for send buffer - -1 means use OS default.
    pub const SEND_BUFFER_LOWER_BOUND: i32 = -1;

    /// Lower bound for receive buffer - -1 means use OS default.
    pub const RECEIVE_BUFFER_LOWER_BOUND: i32 = -1;
}

/// Consumer client configuration constants.
///
/// These configurations are specific to Kafka Consumer clients.
/// Corresponds to Java `org.apache.kafka.clients.consumer.ConsumerConfig`.
pub struct ConsumerConfig;

impl ConsumerConfig {
    /// `bootstrap.servers` - Inherited from CommonClientConfigs.
    pub const BOOTSTRAP_SERVERS_CONFIG: &'static str =
        CommonClientConfigs::BOOTSTRAP_SERVERS_CONFIG;

    /// `group.id` - Inherited from CommonClientConfigs.
    pub const GROUP_ID_CONFIG: &'static str = CommonClientConfigs::GROUP_ID_CONFIG;

    /// `client.id` - Inherited from CommonClientConfigs.
    pub const CLIENT_ID_CONFIG: &'static str = CommonClientConfigs::CLIENT_ID_CONFIG;

    /// `key.deserializer` - Deserializer class for key.
    pub const KEY_DESERIALIZER_CLASS_CONFIG: &'static str = "key.deserializer";

    /// `value.deserializer` - Deserializer class for value.
    pub const VALUE_DESERIALIZER_CLASS_CONFIG: &'static str = "value.deserializer";

    /// `max.poll.records` - The maximum number of records returned in a single call to poll().
    pub const MAX_POLL_RECORDS_CONFIG: &'static str = "max.poll.records";

    /// Default value for `max.poll.records` - 500.
    pub const DEFAULT_MAX_POLL_RECORDS: i32 = 500;

    /// `max.poll.interval.ms` - Inherited from CommonClientConfigs.
    pub const MAX_POLL_INTERVAL_MS_CONFIG: &'static str =
        CommonClientConfigs::MAX_POLL_INTERVAL_MS_CONFIG;

    /// `session.timeout.ms` - Inherited from CommonClientConfigs.
    pub const SESSION_TIMEOUT_MS_CONFIG: &'static str =
        CommonClientConfigs::SESSION_TIMEOUT_MS_CONFIG;

    /// `heartbeat.interval.ms` - Inherited from CommonClientConfigs.
    pub const HEARTBEAT_INTERVAL_MS_CONFIG: &'static str =
        CommonClientConfigs::HEARTBEAT_INTERVAL_MS_CONFIG;

    /// `enable.auto.commit` - If true the consumer's offset will be periodically committed in the background.
    pub const ENABLE_AUTO_COMMIT_CONFIG: &'static str = "enable.auto.commit";

    /// `auto.commit.interval.ms` - The frequency in milliseconds that the consumer offsets are auto-committed.
    pub const AUTO_COMMIT_INTERVAL_MS_CONFIG: &'static str = "auto.commit.interval.ms";

    /// `partition.assignment.strategy` - A list of class names of supported partition assignment strategies.
    pub const PARTITION_ASSIGNMENT_STRATEGY_CONFIG: &'static str = "partition.assignment.strategy";

    /// `auto.offset.reset` - What to do when there is no initial offset in Kafka.
    pub const AUTO_OFFSET_RESET_CONFIG: &'static str = "auto.offset.reset";

    /// `fetch.min.bytes` - The minimum amount of data the server should return for a fetch request.
    pub const FETCH_MIN_BYTES_CONFIG: &'static str = "fetch.min.bytes";

    /// Default value for `fetch.min.bytes` - 1 byte.
    pub const DEFAULT_FETCH_MIN_BYTES: i32 = 1;

    /// `fetch.max.bytes` - The maximum amount of data the server should return for a fetch request.
    pub const FETCH_MAX_BYTES_CONFIG: &'static str = "fetch.max.bytes";

    /// Default value for `fetch.max.bytes` - 50MB.
    pub const DEFAULT_FETCH_MAX_BYTES: i32 = 50 * 1024 * 1024;

    /// `fetch.max.wait.ms` - The maximum amount of time the server will block before answering the fetch request.
    pub const FETCH_MAX_WAIT_MS_CONFIG: &'static str = "fetch.max.wait.ms";

    /// Default value for `fetch.max.wait.ms` - 500ms.
    pub const DEFAULT_FETCH_MAX_WAIT_MS: i32 = 500;

    /// `max.partition.fetch.bytes` - The maximum amount of data per-partition the server will return.
    pub const MAX_PARTITION_FETCH_BYTES_CONFIG: &'static str = "max.partition.fetch.bytes";

    /// Default value for `max.partition.fetch.bytes` - 1MB.
    pub const DEFAULT_MAX_PARTITION_FETCH_BYTES: i32 = 1024 * 1024;

    /// `check.crcs` - Automatically check the CRC32 of the records consumed.
    pub const CHECK_CRCS_CONFIG: &'static str = "check.crcs";

    /// `interceptor.classes` - A list of classes to use as interceptors.
    pub const INTERCEPTOR_CLASSES_CONFIG: &'static str = "interceptor.classes";

    /// `exclude.internal.topics` - Whether internal topics matching a subscribed pattern should be excluded.
    pub const EXCLUDE_INTERNAL_TOPICS_CONFIG: &'static str = "exclude.internal.topics";

    /// Default value for `exclude.internal.topics` - true.
    pub const DEFAULT_EXCLUDE_INTERNAL_TOPICS: bool = true;

    /// `isolation.level` - Controls how to read messages written transactionally.
    pub const ISOLATION_LEVEL_CONFIG: &'static str = "isolation.level";

    /// Default value for `isolation.level` - read_uncommitted.
    pub const DEFAULT_ISOLATION_LEVEL: &'static str = "read_uncommitted";

    /// `allow.auto.create.topics` - Allow automatic topic creation on the broker when subscribing.
    pub const ALLOW_AUTO_CREATE_TOPICS_CONFIG: &'static str = "allow.auto.create.topics";

    /// Default value for `allow.auto.create.topics` - true.
    pub const DEFAULT_ALLOW_AUTO_CREATE_TOPICS: bool = true;

    /// `group.protocol` - The group protocol that the consumer uses.
    pub const GROUP_PROTOCOL_CONFIG: &'static str = "group.protocol";

    /// Default value for `group.protocol` - classic.
    pub const DEFAULT_GROUP_PROTOCOL: &'static str = "classic";

    /// `group.remote.assignor` - The name of the server-side assignor to use.
    pub const GROUP_REMOTE_ASSIGNOR_CONFIG: &'static str = "group.remote.assignor";

    /// `client.rack` - Inherited from CommonClientConfigs.
    pub const CLIENT_RACK_CONFIG: &'static str = CommonClientConfigs::CLIENT_RACK_CONFIG;
}

/// Producer client configuration constants.
///
/// These configurations are specific to Kafka Producer clients.
/// Corresponds to Java `org.apache.kafka.clients.producer.ProducerConfig`.
pub struct ProducerConfig;

impl ProducerConfig {
    /// `bootstrap.servers` - Inherited from CommonClientConfigs.
    pub const BOOTSTRAP_SERVERS_CONFIG: &'static str =
        CommonClientConfigs::BOOTSTRAP_SERVERS_CONFIG;

    /// `client.id` - Inherited from CommonClientConfigs.
    pub const CLIENT_ID_CONFIG: &'static str = CommonClientConfigs::CLIENT_ID_CONFIG;

    /// `key.serializer` - Serializer class for key.
    pub const KEY_SERIALIZER_CLASS_CONFIG: &'static str = "key.serializer";

    /// `value.serializer` - Serializer class for value.
    pub const VALUE_SERIALIZER_CLASS_CONFIG: &'static str = "value.serializer";

    /// `acks` - The number of acknowledgments the producer requires the leader to have received.
    pub const ACKS_CONFIG: &'static str = "acks";

    /// `batch.size` - The producer will attempt to batch records together into fewer requests.
    pub const BATCH_SIZE_CONFIG: &'static str = "batch.size";

    /// `linger.ms` - The producer groups together any records that arrive in between request transmissions.
    pub const LINGER_MS_CONFIG: &'static str = "linger.ms";

    /// `buffer.memory` - The total bytes of memory the producer can use to buffer records.
    pub const BUFFER_MEMORY_CONFIG: &'static str = "buffer.memory";

    /// `max.block.ms` - The configuration controls how long the KafkaProducer's methods will block.
    pub const MAX_BLOCK_MS_CONFIG: &'static str = "max.block.ms";

    /// `max.request.size` - The maximum size of a request in bytes.
    pub const MAX_REQUEST_SIZE_CONFIG: &'static str = "max.request.size";

    /// `compression.type` - The compression type for all data generated by the producer.
    pub const COMPRESSION_TYPE_CONFIG: &'static str = "compression.type";

    /// `retries` - Inherited from CommonClientConfigs.
    pub const RETRIES_CONFIG: &'static str = CommonClientConfigs::RETRIES_CONFIG;

    /// `retry.backoff.ms` - Inherited from CommonClientConfigs.
    pub const RETRY_BACKOFF_MS_CONFIG: &'static str = CommonClientConfigs::RETRY_BACKOFF_MS_CONFIG;

    /// `retry.backoff.max.ms` - Inherited from CommonClientConfigs.
    pub const RETRY_BACKOFF_MAX_MS_CONFIG: &'static str =
        CommonClientConfigs::RETRY_BACKOFF_MAX_MS_CONFIG;

    /// `delivery.timeout.ms` - An upper bound on the time to report success or failure after send().
    pub const DELIVERY_TIMEOUT_MS_CONFIG: &'static str = "delivery.timeout.ms";

    /// `request.timeout.ms` - Inherited from CommonClientConfigs.
    pub const REQUEST_TIMEOUT_MS_CONFIG: &'static str =
        CommonClientConfigs::REQUEST_TIMEOUT_MS_CONFIG;

    /// `metadata.max.age.ms` - Inherited from CommonClientConfigs.
    pub const METADATA_MAX_AGE_CONFIG: &'static str = CommonClientConfigs::METADATA_MAX_AGE_CONFIG;

    /// `metadata.max.idle.ms` - Controls how long the producer will cache metadata for a topic that's idle.
    pub const METADATA_MAX_IDLE_CONFIG: &'static str = "metadata.max.idle.ms";

    /// `max.in.flight.requests.per.connection` - The maximum number of unacknowledged requests per connection.
    pub const MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION: &'static str =
        "max.in.flight.requests.per.connection";

    /// `enable.idempotence` - When set to 'true', the producer will ensure exactly one copy of each message.
    pub const ENABLE_IDEMPOTENCE_CONFIG: &'static str = "enable.idempotence";

    /// `transactional.id` - The TransactionalId to use for transactional delivery.
    pub const TRANSACTIONAL_ID_CONFIG: &'static str = "transactional.id";

    /// `transaction.timeout.ms` - The maximum amount of time a transaction will remain open.
    pub const TRANSACTION_TIMEOUT_CONFIG: &'static str = "transaction.timeout.ms";

    /// `interceptor.classes` - A list of classes to use as interceptors.
    pub const INTERCEPTOR_CLASSES_CONFIG: &'static str = "interceptor.classes";

    /// `partitioner.class` - Determines which partition to send a record to.
    pub const PARTITIONER_CLASS_CONFIG: &'static str = "partitioner.class";

    /// `partitioner.adaptive.partitioning.enable` - When set to 'true', the producer adapts to broker performance.
    pub const PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE_CONFIG: &'static str =
        "partitioner.adaptive.partitioning.enable";

    /// `partitioner.availability.timeout.ms` - Treat partition as unavailable after this timeout.
    pub const PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG: &'static str =
        "partitioner.availability.timeout.ms";

    /// `partitioner.ignore.keys` - When set to 'true' the producer won't use record keys to choose a partition.
    pub const PARTITIONER_IGNORE_KEYS_CONFIG: &'static str = "partitioner.ignore.keys";

    /// `send.buffer.bytes` - Inherited from CommonClientConfigs.
    pub const SEND_BUFFER_CONFIG: &'static str = CommonClientConfigs::SEND_BUFFER_CONFIG;

    /// `receive.buffer.bytes` - Inherited from CommonClientConfigs.
    pub const RECEIVE_BUFFER_CONFIG: &'static str = CommonClientConfigs::RECEIVE_BUFFER_CONFIG;

    /// `connections.max.idle.ms` - Inherited from CommonClientConfigs.
    pub const CONNECTIONS_MAX_IDLE_MS_CONFIG: &'static str =
        CommonClientConfigs::CONNECTIONS_MAX_IDLE_MS_CONFIG;
}

/// Admin client configuration constants.
///
/// These configurations are specific to Kafka AdminClient.
/// Corresponds to Java `org.apache.kafka.clients.admin.AdminClientConfig`.
pub struct AdminClientConfig;

impl AdminClientConfig {
    /// `bootstrap.servers` - Inherited from CommonClientConfigs.
    pub const BOOTSTRAP_SERVERS_CONFIG: &'static str =
        CommonClientConfigs::BOOTSTRAP_SERVERS_CONFIG;

    /// `bootstrap.controllers` - A list of host/port pairs for KRaft controller quorum.
    pub const BOOTSTRAP_CONTROLLERS_CONFIG: &'static str = "bootstrap.controllers";

    /// `client.id` - Inherited from CommonClientConfigs.
    pub const CLIENT_ID_CONFIG: &'static str = CommonClientConfigs::CLIENT_ID_CONFIG;

    /// `request.timeout.ms` - Inherited from CommonClientConfigs.
    pub const REQUEST_TIMEOUT_MS_CONFIG: &'static str =
        CommonClientConfigs::REQUEST_TIMEOUT_MS_CONFIG;

    /// `default.api.timeout.ms` - Inherited from CommonClientConfigs.
    pub const DEFAULT_API_TIMEOUT_MS_CONFIG: &'static str =
        CommonClientConfigs::DEFAULT_API_TIMEOUT_MS_CONFIG;

    /// `retries` - Inherited from CommonClientConfigs.
    pub const RETRIES_CONFIG: &'static str = CommonClientConfigs::RETRIES_CONFIG;

    /// `retry.backoff.ms` - Inherited from CommonClientConfigs.
    pub const RETRY_BACKOFF_MS_CONFIG: &'static str = CommonClientConfigs::RETRY_BACKOFF_MS_CONFIG;

    /// `retry.backoff.max.ms` - Inherited from CommonClientConfigs.
    pub const RETRY_BACKOFF_MAX_MS_CONFIG: &'static str =
        CommonClientConfigs::RETRY_BACKOFF_MAX_MS_CONFIG;

    /// `metadata.max.age.ms` - Inherited from CommonClientConfigs.
    pub const METADATA_MAX_AGE_CONFIG: &'static str = CommonClientConfigs::METADATA_MAX_AGE_CONFIG;

    /// `send.buffer.bytes` - Inherited from CommonClientConfigs.
    pub const SEND_BUFFER_CONFIG: &'static str = CommonClientConfigs::SEND_BUFFER_CONFIG;

    /// `receive.buffer.bytes` - Inherited from CommonClientConfigs.
    pub const RECEIVE_BUFFER_CONFIG: &'static str = CommonClientConfigs::RECEIVE_BUFFER_CONFIG;

    /// `security.protocol` - Inherited from CommonClientConfigs.
    pub const SECURITY_PROTOCOL_CONFIG: &'static str =
        CommonClientConfigs::SECURITY_PROTOCOL_CONFIG;

    /// Default value for `security.protocol` - PLAINTEXT.
    pub const DEFAULT_SECURITY_PROTOCOL: &'static str =
        CommonClientConfigs::DEFAULT_SECURITY_PROTOCOL;

    /// `metric.reporters` - Inherited from CommonClientConfigs.
    pub const METRIC_REPORTER_CLASSES_CONFIG: &'static str =
        CommonClientConfigs::METRIC_REPORTER_CLASSES_CONFIG;

    /// `metrics.num.samples` - Inherited from CommonClientConfigs.
    pub const METRICS_NUM_SAMPLES_CONFIG: &'static str =
        CommonClientConfigs::METRICS_NUM_SAMPLES_CONFIG;

    /// `metrics.sample.window.ms` - Inherited from CommonClientConfigs.
    pub const METRICS_SAMPLE_WINDOW_MS_CONFIG: &'static str =
        CommonClientConfigs::METRICS_SAMPLE_WINDOW_MS_CONFIG;

    /// `metrics.recording.level` - Inherited from CommonClientConfigs.
    pub const METRICS_RECORDING_LEVEL_CONFIG: &'static str =
        CommonClientConfigs::METRICS_RECORDING_LEVEL_CONFIG;

    /// `connections.max.idle.ms` - Inherited from CommonClientConfigs.
    pub const CONNECTIONS_MAX_IDLE_MS_CONFIG: &'static str =
        CommonClientConfigs::CONNECTIONS_MAX_IDLE_MS_CONFIG;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_common_client_configs_constants() {
        assert_eq!(
            CommonClientConfigs::BOOTSTRAP_SERVERS_CONFIG,
            "bootstrap.servers"
        );
        assert_eq!(CommonClientConfigs::CLIENT_ID_CONFIG, "client.id");
        assert_eq!(CommonClientConfigs::GROUP_ID_CONFIG, "group.id");
        assert_eq!(
            CommonClientConfigs::SECURITY_PROTOCOL_CONFIG,
            "security.protocol"
        );
        assert_eq!(CommonClientConfigs::DEFAULT_SECURITY_PROTOCOL, "PLAINTEXT");
        assert_eq!(CommonClientConfigs::DEFAULT_RETRY_BACKOFF_MS, 100);
        assert_eq!(CommonClientConfigs::DEFAULT_RETRY_BACKOFF_MAX_MS, 1000);
    }

    #[test]
    fn test_consumer_config_constants() {
        assert_eq!(
            ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG,
            "bootstrap.servers"
        );
        assert_eq!(
            ConsumerConfig::KEY_DESERIALIZER_CLASS_CONFIG,
            "key.deserializer"
        );
        assert_eq!(
            ConsumerConfig::VALUE_DESERIALIZER_CLASS_CONFIG,
            "value.deserializer"
        );
        assert_eq!(ConsumerConfig::GROUP_ID_CONFIG, "group.id");
        assert_eq!(ConsumerConfig::DEFAULT_MAX_POLL_RECORDS, 500);
        assert_eq!(ConsumerConfig::DEFAULT_FETCH_MIN_BYTES, 1);
        assert_eq!(ConsumerConfig::DEFAULT_ISOLATION_LEVEL, "read_uncommitted");
    }

    #[test]
    fn test_producer_config_constants() {
        assert_eq!(
            ProducerConfig::BOOTSTRAP_SERVERS_CONFIG,
            "bootstrap.servers"
        );
        assert_eq!(
            ProducerConfig::KEY_SERIALIZER_CLASS_CONFIG,
            "key.serializer"
        );
        assert_eq!(
            ProducerConfig::VALUE_SERIALIZER_CLASS_CONFIG,
            "value.serializer"
        );
        assert_eq!(ProducerConfig::ACKS_CONFIG, "acks");
        assert_eq!(ProducerConfig::BATCH_SIZE_CONFIG, "batch.size");
        assert_eq!(
            ProducerConfig::ENABLE_IDEMPOTENCE_CONFIG,
            "enable.idempotence"
        );
    }

    #[test]
    fn test_admin_client_config_constants() {
        assert_eq!(
            AdminClientConfig::BOOTSTRAP_SERVERS_CONFIG,
            "bootstrap.servers"
        );
        assert_eq!(
            AdminClientConfig::BOOTSTRAP_CONTROLLERS_CONFIG,
            "bootstrap.controllers"
        );
        assert_eq!(AdminClientConfig::CLIENT_ID_CONFIG, "client.id");
        assert_eq!(
            AdminClientConfig::REQUEST_TIMEOUT_MS_CONFIG,
            "request.timeout.ms"
        );
    }

    #[test]
    fn test_inherited_constants_consistency() {
        // Verify that inherited constants match the source
        assert_eq!(
            ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG,
            CommonClientConfigs::BOOTSTRAP_SERVERS_CONFIG
        );
        assert_eq!(
            ProducerConfig::BOOTSTRAP_SERVERS_CONFIG,
            CommonClientConfigs::BOOTSTRAP_SERVERS_CONFIG
        );
        assert_eq!(
            AdminClientConfig::BOOTSTRAP_SERVERS_CONFIG,
            CommonClientConfigs::BOOTSTRAP_SERVERS_CONFIG
        );
        assert_eq!(
            ConsumerConfig::CLIENT_ID_CONFIG,
            CommonClientConfigs::CLIENT_ID_CONFIG
        );
        assert_eq!(
            ProducerConfig::CLIENT_ID_CONFIG,
            CommonClientConfigs::CLIENT_ID_CONFIG
        );
        assert_eq!(
            AdminClientConfig::CLIENT_ID_CONFIG,
            CommonClientConfigs::CLIENT_ID_CONFIG
        );
    }
}
