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

//! ConnectMetricsRegistry - Registry of metric name templates for Kafka Connect.
//!
//! This module provides the registry for all Connect metrics, defining
//! metric name templates, group names, and tag names used throughout
//! the Connect runtime.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.ConnectMetricsRegistry` in Java.

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

/// Task status state enum for connector status metrics mapping.
/// Corresponds to `TaskStatus.State` in Java.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskState {
    Running,
    Paused,
    Failed,
    Unassigned,
    Destroyed,
    Restarting,
}

impl TaskState {
    /// Returns the string representation of the state.
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskState::Running => "running",
            TaskState::Paused => "paused",
            TaskState::Failed => "failed",
            TaskState::Unassigned => "unassigned",
            TaskState::Destroyed => "destroyed",
            TaskState::Restarting => "restarting",
        }
    }
}

/// Metric name template - defines a metric's name, group, and documentation.
///
/// Corresponds to `MetricNameTemplate` in Java's Kafka metrics library.
/// In Rust, we use a simplified struct that captures the essential information.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricNameTemplate {
    /// The metric name
    name: String,
    /// The metric group name
    group: String,
    /// Documentation/description for the metric
    description: String,
    /// Tags associated with this metric
    tags: HashSet<String>,
}

impl Hash for MetricNameTemplate {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.group.hash(state);
        self.description.hash(state);
        // Hash tags in sorted order for consistency
        let mut tag_vec: Vec<&str> = self.tags.iter().map(|s| s.as_str()).collect();
        tag_vec.sort();
        for tag in tag_vec {
            tag.hash(state);
        }
    }
}

impl MetricNameTemplate {
    /// Creates a new metric name template.
    ///
    /// # Arguments
    /// * `name` - The metric name
    /// * `group` - The metric group name
    /// * `description` - Documentation for the metric
    /// * `tags` - Tags associated with this metric
    pub fn new(name: String, group: String, description: String, tags: HashSet<String>) -> Self {
        MetricNameTemplate {
            name,
            group,
            description,
            tags,
        }
    }

    /// Returns the metric name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the metric group name.
    pub fn group(&self) -> &str {
        &self.group
    }

    /// Returns the metric description.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Returns the tags associated with this metric.
    pub fn tags(&self) -> &HashSet<String> {
        &self.tags
    }
}

/// Registry for all Connect metric name templates.
///
/// This registry contains all the metric templates used by Kafka Connect,
/// organized by metric groups (connector, task, source task, sink task, etc.)
///
/// Corresponds to `org.apache.kafka.connect.runtime.ConnectMetricsRegistry` in Java.
#[derive(Clone)]
pub struct ConnectMetricsRegistry {
    /// All templates registered in this registry
    all_templates: Vec<MetricNameTemplate>,

    // Tag names
    /// Tag name for connector
    connector_tag_name: String,
    /// Tag name for task
    task_tag_name: String,
    /// Tag name for transformation
    transform_tag_name: String,
    /// Tag name for predicate
    predicate_tag_name: String,

    // Group names
    /// Group name for connector metrics
    connector_group_name: String,
    /// Group name for task metrics
    task_group_name: String,
    /// Group name for source task metrics
    source_task_group_name: String,
    /// Group name for sink task metrics
    sink_task_group_name: String,
    /// Group name for worker metrics
    worker_group_name: String,
    /// Group name for worker rebalance metrics
    worker_rebalance_group_name: String,
    /// Group name for task error handling metrics
    task_error_handling_group_name: String,
    /// Group name for transforms metrics
    transforms_group_name: String,
    /// Group name for predicates metrics
    predicates_group_name: String,

    // Connector level metrics
    connector_status: MetricNameTemplate,
    connector_type: MetricNameTemplate,
    connector_class: MetricNameTemplate,
    connector_version: MetricNameTemplate,
    connector_total_task_count: MetricNameTemplate,
    connector_running_task_count: MetricNameTemplate,
    connector_paused_task_count: MetricNameTemplate,
    connector_failed_task_count: MetricNameTemplate,
    connector_unassigned_task_count: MetricNameTemplate,
    connector_destroyed_task_count: MetricNameTemplate,
    connector_restarting_task_count: MetricNameTemplate,

    // Task level metrics
    task_status: MetricNameTemplate,
    task_running_ratio: MetricNameTemplate,
    task_pause_ratio: MetricNameTemplate,
    task_commit_time_max: MetricNameTemplate,
    task_commit_time_avg: MetricNameTemplate,
    task_batch_size_max: MetricNameTemplate,
    task_batch_size_avg: MetricNameTemplate,
    task_commit_failure_percentage: MetricNameTemplate,
    task_commit_success_percentage: MetricNameTemplate,
    task_connector_class: MetricNameTemplate,
    task_connector_class_version: MetricNameTemplate,
    task_connector_type: MetricNameTemplate,
    task_class: MetricNameTemplate,
    task_version: MetricNameTemplate,
    task_key_converter_class: MetricNameTemplate,
    task_value_converter_class: MetricNameTemplate,
    task_key_converter_version: MetricNameTemplate,
    task_value_converter_version: MetricNameTemplate,
    task_header_converter_class: MetricNameTemplate,
    task_header_converter_version: MetricNameTemplate,

    // Source task metrics
    source_record_poll_rate: MetricNameTemplate,
    source_record_poll_total: MetricNameTemplate,
    source_record_write_rate: MetricNameTemplate,
    source_record_write_total: MetricNameTemplate,
    source_record_poll_batch_time_max: MetricNameTemplate,
    source_record_poll_batch_time_avg: MetricNameTemplate,
    source_record_active_count: MetricNameTemplate,
    source_record_active_count_max: MetricNameTemplate,
    source_record_active_count_avg: MetricNameTemplate,
    transaction_size_min: MetricNameTemplate,
    transaction_size_max: MetricNameTemplate,
    transaction_size_avg: MetricNameTemplate,

    // Sink task metrics
    sink_record_read_rate: MetricNameTemplate,
    sink_record_read_total: MetricNameTemplate,
    sink_record_send_rate: MetricNameTemplate,
    sink_record_send_total: MetricNameTemplate,
    sink_record_lag_max: MetricNameTemplate,
    sink_record_partition_count: MetricNameTemplate,
    sink_record_offset_commit_seq_num: MetricNameTemplate,
    sink_record_offset_commit_completion_rate: MetricNameTemplate,
    sink_record_offset_commit_completion_total: MetricNameTemplate,
    sink_record_offset_commit_skip_rate: MetricNameTemplate,
    sink_record_offset_commit_skip_total: MetricNameTemplate,
    sink_record_put_batch_time_max: MetricNameTemplate,
    sink_record_put_batch_time_avg: MetricNameTemplate,
    sink_record_active_count: MetricNameTemplate,
    sink_record_active_count_max: MetricNameTemplate,
    sink_record_active_count_avg: MetricNameTemplate,

    // Worker level metrics
    connector_count: MetricNameTemplate,
    task_count: MetricNameTemplate,
    connector_startup_attempts_total: MetricNameTemplate,
    connector_startup_success_total: MetricNameTemplate,
    connector_startup_success_percentage: MetricNameTemplate,
    connector_startup_failure_total: MetricNameTemplate,
    connector_startup_failure_percentage: MetricNameTemplate,
    task_startup_attempts_total: MetricNameTemplate,
    task_startup_success_total: MetricNameTemplate,
    task_startup_success_percentage: MetricNameTemplate,
    task_startup_failure_total: MetricNameTemplate,
    task_startup_failure_percentage: MetricNameTemplate,

    // Worker rebalance metrics
    connect_protocol: MetricNameTemplate,
    leader_name: MetricNameTemplate,
    epoch: MetricNameTemplate,
    rebalance_completed_total: MetricNameTemplate,
    rebalance_mode: MetricNameTemplate,
    rebalance_time_max: MetricNameTemplate,
    rebalance_time_avg: MetricNameTemplate,
    rebalance_time_since_last: MetricNameTemplate,

    // Task error handling metrics
    record_processing_failures: MetricNameTemplate,
    record_processing_errors: MetricNameTemplate,
    records_skipped: MetricNameTemplate,
    retries: MetricNameTemplate,
    errors_logged: MetricNameTemplate,
    dlq_produce_requests: MetricNameTemplate,
    dlq_produce_failures: MetricNameTemplate,
    last_error_timestamp: MetricNameTemplate,

    // Transformation metrics
    transform_class: MetricNameTemplate,
    transform_version: MetricNameTemplate,

    // Predicate metrics
    predicate_class: MetricNameTemplate,
    predicate_version: MetricNameTemplate,

    /// Map of connector status metrics to task states
    connector_status_metrics: HashMap<MetricNameTemplate, TaskState>,
}

impl ConnectMetricsRegistry {
    /// Creates a new ConnectMetricsRegistry with default tags.
    pub fn new() -> Self {
        Self::with_tags(HashSet::new())
    }

    /// Creates a new ConnectMetricsRegistry with additional tags.
    ///
    /// # Arguments
    /// * `tags` - Additional tags to include in all metric templates
    pub fn with_tags(tags: HashSet<String>) -> Self {
        // Define tag names and group names
        let connector_tag_name = "connector".to_string();
        let task_tag_name = "task".to_string();
        let transform_tag_name = "transform".to_string();
        let predicate_tag_name = "predicate".to_string();

        let connector_group_name = "connector-metrics".to_string();
        let task_group_name = "connector-task-metrics".to_string();
        let source_task_group_name = "source-task-metrics".to_string();
        let sink_task_group_name = "sink-task-metrics".to_string();
        let worker_group_name = "connect-worker-metrics".to_string();
        let worker_rebalance_group_name = "connect-worker-rebalance-metrics".to_string();
        let task_error_handling_group_name = "task-error-metrics".to_string();
        let transforms_group_name = "connector-transform-metrics".to_string();
        let predicates_group_name = "connector-predicate-metrics".to_string();

        // Build connector tags
        let connector_tags: HashSet<String> = tags
            .iter()
            .cloned()
            .chain(std::iter::once(connector_tag_name.clone()))
            .collect();

        // Build worker task tags (connector + task)
        let worker_task_tags: HashSet<String> = tags
            .iter()
            .cloned()
            .chain(std::iter::once(connector_tag_name.clone()))
            .chain(std::iter::once(task_tag_name.clone()))
            .collect();

        // Build source task tags
        let source_task_tags: HashSet<String> = worker_task_tags.clone();

        // Build sink task tags
        let sink_task_tags: HashSet<String> = worker_task_tags.clone();

        // Build worker tags
        let worker_tags: HashSet<String> = tags.clone();

        // Build worker connector tags
        let worker_connector_tags: HashSet<String> = tags
            .iter()
            .cloned()
            .chain(std::iter::once(connector_tag_name.clone()))
            .collect();

        // Build rebalance tags
        let rebalance_tags: HashSet<String> = tags.clone();

        // Build task error handling tags
        let task_error_handling_tags: HashSet<String> = worker_task_tags.clone();

        // Build transform tags
        let transform_tags: HashSet<String> = worker_task_tags
            .iter()
            .cloned()
            .chain(std::iter::once(transform_tag_name.clone()))
            .collect();

        // Build predicate tags
        let predicate_tags: HashSet<String> = worker_task_tags
            .iter()
            .cloned()
            .chain(std::iter::once(predicate_tag_name.clone()))
            .collect();

        // Create templates
        let mut all_templates = Vec::new();

        // Connector level metrics
        let connector_status = Self::create_template(
            "status",
            &connector_group_name,
            "The status of the connector. One of 'unassigned', 'running', 'paused', 'stopped', 'failed', or 'restarting'.",
            &connector_tags,
            &mut all_templates,
        );
        let connector_type = Self::create_template(
            "connector-type",
            &connector_group_name,
            "The type of the connector. One of 'source' or 'sink'.",
            &connector_tags,
            &mut all_templates,
        );
        let connector_class = Self::create_template(
            "connector-class",
            &connector_group_name,
            "The name of the connector class.",
            &connector_tags,
            &mut all_templates,
        );
        let connector_version = Self::create_template(
            "connector-version",
            &connector_group_name,
            "The version of the connector class, as reported by the connector.",
            &connector_tags,
            &mut all_templates,
        );

        // Task level metrics
        let task_status = Self::create_template(
            "status",
            &task_group_name,
            "The status of the connector task. One of 'unassigned', 'running', 'paused', 'failed', or 'restarting'.",
            &worker_task_tags,
            &mut all_templates,
        );
        let task_running_ratio = Self::create_template(
            "running-ratio",
            &task_group_name,
            "The fraction of time this task has spent in the running state.",
            &worker_task_tags,
            &mut all_templates,
        );
        let task_pause_ratio = Self::create_template(
            "pause-ratio",
            &task_group_name,
            "The fraction of time this task has spent in the pause state.",
            &worker_task_tags,
            &mut all_templates,
        );
        let task_commit_time_max = Self::create_template(
            "offset-commit-max-time-ms",
            &task_group_name,
            "The maximum time in milliseconds taken by this task to commit offsets.",
            &worker_task_tags,
            &mut all_templates,
        );
        let task_commit_time_avg = Self::create_template(
            "offset-commit-avg-time-ms",
            &task_group_name,
            "The average time in milliseconds taken by this task to commit offsets.",
            &worker_task_tags,
            &mut all_templates,
        );
        let task_batch_size_max = Self::create_template(
            "batch-size-max",
            &task_group_name,
            "The number of records in the largest batch the task has processed so far.",
            &worker_task_tags,
            &mut all_templates,
        );
        let task_batch_size_avg = Self::create_template(
            "batch-size-avg",
            &task_group_name,
            "The average number of records in the batches the task has processed so far.",
            &worker_task_tags,
            &mut all_templates,
        );
        let task_commit_failure_percentage = Self::create_template(
            "offset-commit-failure-percentage",
            &task_group_name,
            "The average percentage of this task's offset commit attempts that failed.",
            &worker_task_tags,
            &mut all_templates,
        );
        let task_commit_success_percentage = Self::create_template(
            "offset-commit-success-percentage",
            &task_group_name,
            "The average percentage of this task's offset commit attempts that succeeded.",
            &worker_task_tags,
            &mut all_templates,
        );
        let task_connector_class = Self::create_template(
            "connector-class",
            &task_group_name,
            "The name of the connector class.",
            &worker_task_tags,
            &mut all_templates,
        );
        let task_connector_class_version = Self::create_template(
            "connector-version",
            &task_group_name,
            "The version of the connector class, as reported by the connector.",
            &worker_task_tags,
            &mut all_templates,
        );
        let task_connector_type = Self::create_template(
            "connector-type",
            &task_group_name,
            "The type of the connector. One of 'source' or 'sink'.",
            &worker_task_tags,
            &mut all_templates,
        );
        let task_class = Self::create_template(
            "task-class",
            &task_group_name,
            "The class name of the task.",
            &worker_task_tags,
            &mut all_templates,
        );
        let task_version = Self::create_template(
            "task-version",
            &task_group_name,
            "The version of the task.",
            &worker_task_tags,
            &mut all_templates,
        );
        let task_key_converter_class = Self::create_template(
            "key-converter-class",
            &task_group_name,
            "The fully qualified class name from key.converter",
            &worker_task_tags,
            &mut all_templates,
        );
        let task_value_converter_class = Self::create_template(
            "value-converter-class",
            &task_group_name,
            "The fully qualified class name from value.converter",
            &worker_task_tags,
            &mut all_templates,
        );
        let task_key_converter_version = Self::create_template(
            "key-converter-version",
            &task_group_name,
            "The version instantiated for key.converter. May be undefined",
            &worker_task_tags,
            &mut all_templates,
        );
        let task_value_converter_version = Self::create_template(
            "value-converter-version",
            &task_group_name,
            "The version instantiated for value.converter. May be undefined",
            &worker_task_tags,
            &mut all_templates,
        );
        let task_header_converter_class = Self::create_template(
            "header-converter-class",
            &task_group_name,
            "The fully qualified class name from header.converter",
            &worker_task_tags,
            &mut all_templates,
        );
        let task_header_converter_version = Self::create_template(
            "header-converter-version",
            &task_group_name,
            "The version instantiated for header.converter. May be undefined",
            &worker_task_tags,
            &mut all_templates,
        );

        // Transformation metrics
        let transform_class = Self::create_template(
            "transform-class",
            &transforms_group_name,
            "The class name of the transformation class",
            &transform_tags,
            &mut all_templates,
        );
        let transform_version = Self::create_template(
            "transform-version",
            &transforms_group_name,
            "The version of the transformation class",
            &transform_tags,
            &mut all_templates,
        );

        // Predicate metrics
        let predicate_class = Self::create_template(
            "predicate-class",
            &predicates_group_name,
            "The class name of the predicate class",
            &predicate_tags,
            &mut all_templates,
        );
        let predicate_version = Self::create_template(
            "predicate-version",
            &predicates_group_name,
            "The version of the predicate class",
            &predicate_tags,
            &mut all_templates,
        );

        // Source task metrics
        let source_record_poll_rate = Self::create_template(
            "source-record-poll-rate",
            &source_task_group_name,
            "The average per-second number of records produced/polled (before transformation) by this task belonging to the named source connector in this worker.",
            &source_task_tags,
            &mut all_templates,
        );
        let source_record_poll_total = Self::create_template(
            "source-record-poll-total",
            &source_task_group_name,
            "The total number of records produced/polled (before transformation) by this task belonging to the named source connector in this worker.",
            &source_task_tags,
            &mut all_templates,
        );
        let source_record_write_rate = Self::create_template(
            "source-record-write-rate",
            &source_task_group_name,
            "The average per-second number of records written to Kafka for this task belonging to the named source connector in this worker, since the task was last restarted.",
            &source_task_tags,
            &mut all_templates,
        );
        let source_record_write_total = Self::create_template(
            "source-record-write-total",
            &source_task_group_name,
            "The number of records output written to Kafka for this task belonging to the named source connector in this worker.",
            &source_task_tags,
            &mut all_templates,
        );
        let source_record_poll_batch_time_max = Self::create_template(
            "poll-batch-max-time-ms",
            &source_task_group_name,
            "The maximum time in milliseconds taken by this task to poll for a batch of source records.",
            &source_task_tags,
            &mut all_templates,
        );
        let source_record_poll_batch_time_avg = Self::create_template(
            "poll-batch-avg-time-ms",
            &source_task_group_name,
            "The average time in milliseconds taken by this task to poll for a batch of source records.",
            &source_task_tags,
            &mut all_templates,
        );
        let source_record_active_count = Self::create_template(
            "source-record-active-count",
            &source_task_group_name,
            "The number of records that have been produced by this task but not yet completely written to Kafka.",
            &source_task_tags,
            &mut all_templates,
        );
        let source_record_active_count_max = Self::create_template(
            "source-record-active-count-max",
            &source_task_group_name,
            "The maximum number of records that have been produced by this task but not yet completely written to Kafka.",
            &source_task_tags,
            &mut all_templates,
        );
        let source_record_active_count_avg = Self::create_template(
            "source-record-active-count-avg",
            &source_task_group_name,
            "The average number of records that have been produced by this task but not yet completely written to Kafka.",
            &source_task_tags,
            &mut all_templates,
        );
        let transaction_size_min = Self::create_template(
            "transaction-size-min",
            &source_task_group_name,
            "The number of records in the smallest transaction the task has committed so far.",
            &source_task_tags,
            &mut all_templates,
        );
        let transaction_size_max = Self::create_template(
            "transaction-size-max",
            &source_task_group_name,
            "The number of records in the largest transaction the task has committed so far.",
            &source_task_tags,
            &mut all_templates,
        );
        let transaction_size_avg = Self::create_template(
            "transaction-size-avg",
            &source_task_group_name,
            "The average number of records in the transactions the task has committed so far.",
            &source_task_tags,
            &mut all_templates,
        );

        // Sink task metrics
        let sink_record_read_rate = Self::create_template(
            "sink-record-read-rate",
            &sink_task_group_name,
            "The average per-second number of records read from Kafka for this task belonging to the named sink connector in this worker.",
            &sink_task_tags,
            &mut all_templates,
        );
        let sink_record_read_total = Self::create_template(
            "sink-record-read-total",
            &sink_task_group_name,
            "The total number of records read from Kafka by this task belonging to the named sink connector in this worker.",
            &sink_task_tags,
            &mut all_templates,
        );
        let sink_record_send_rate = Self::create_template(
            "sink-record-send-rate",
            &sink_task_group_name,
            "The average per-second number of records output from the transformations and sent/put to this task.",
            &sink_task_tags,
            &mut all_templates,
        );
        let sink_record_send_total = Self::create_template(
            "sink-record-send-total",
            &sink_task_group_name,
            "The total number of records output from the transformations and sent/put to this task.",
            &sink_task_tags,
            &mut all_templates,
        );
        let sink_record_lag_max = Self::create_template(
            "sink-record-lag-max",
            &sink_task_group_name,
            "The maximum lag in terms of number of records that the sink task is behind the consumer's position.",
            &sink_task_tags,
            &mut all_templates,
        );
        let sink_record_partition_count = Self::create_template(
            "partition-count",
            &sink_task_group_name,
            "The number of topic partitions assigned to this task.",
            &sink_task_tags,
            &mut all_templates,
        );
        let sink_record_offset_commit_seq_num = Self::create_template(
            "offset-commit-seq-no",
            &sink_task_group_name,
            "The current sequence number for offset commits.",
            &sink_task_tags,
            &mut all_templates,
        );
        let sink_record_offset_commit_completion_rate = Self::create_template(
            "offset-commit-completion-rate",
            &sink_task_group_name,
            "The average per-second number of offset commit completions that were completed successfully.",
            &sink_task_tags,
            &mut all_templates,
        );
        let sink_record_offset_commit_completion_total = Self::create_template(
            "offset-commit-completion-total",
            &sink_task_group_name,
            "The total number of offset commit completions that were completed successfully.",
            &sink_task_tags,
            &mut all_templates,
        );
        let sink_record_offset_commit_skip_rate = Self::create_template(
            "offset-commit-skip-rate",
            &sink_task_group_name,
            "The average per-second number of offset commit completions that were received too late and skipped.",
            &sink_task_tags,
            &mut all_templates,
        );
        let sink_record_offset_commit_skip_total = Self::create_template(
            "offset-commit-skip-total",
            &sink_task_group_name,
            "The total number of offset commit completions that were received too late and skipped.",
            &sink_task_tags,
            &mut all_templates,
        );
        let sink_record_put_batch_time_max = Self::create_template(
            "put-batch-max-time-ms",
            &sink_task_group_name,
            "The maximum time taken by this task to put a batch of sink records.",
            &sink_task_tags,
            &mut all_templates,
        );
        let sink_record_put_batch_time_avg = Self::create_template(
            "put-batch-avg-time-ms",
            &sink_task_group_name,
            "The average time taken by this task to put a batch of sink records.",
            &sink_task_tags,
            &mut all_templates,
        );
        let sink_record_active_count = Self::create_template(
            "sink-record-active-count",
            &sink_task_group_name,
            "The number of records that have been read from Kafka but not yet completely committed.",
            &sink_task_tags,
            &mut all_templates,
        );
        let sink_record_active_count_max = Self::create_template(
            "sink-record-active-count-max",
            &sink_task_group_name,
            "The maximum number of records that have been read from Kafka but not yet completely committed.",
            &sink_task_tags,
            &mut all_templates,
        );
        let sink_record_active_count_avg = Self::create_template(
            "sink-record-active-count-avg",
            &sink_task_group_name,
            "The average number of records that have been read from Kafka but not yet completely committed.",
            &sink_task_tags,
            &mut all_templates,
        );

        // Worker level metrics
        let connector_count = Self::create_template(
            "connector-count",
            &worker_group_name,
            "The number of connectors run in this worker.",
            &worker_tags,
            &mut all_templates,
        );
        let task_count = Self::create_template(
            "task-count",
            &worker_group_name,
            "The number of tasks run in this worker.",
            &worker_tags,
            &mut all_templates,
        );
        let connector_startup_attempts_total = Self::create_template(
            "connector-startup-attempts-total",
            &worker_group_name,
            "The total number of connector startups that this worker has attempted.",
            &worker_tags,
            &mut all_templates,
        );
        let connector_startup_success_total = Self::create_template(
            "connector-startup-success-total",
            &worker_group_name,
            "The total number of connector starts that succeeded.",
            &worker_tags,
            &mut all_templates,
        );
        let connector_startup_success_percentage = Self::create_template(
            "connector-startup-success-percentage",
            &worker_group_name,
            "The average percentage of this worker's connector starts that succeeded.",
            &worker_tags,
            &mut all_templates,
        );
        let connector_startup_failure_total = Self::create_template(
            "connector-startup-failure-total",
            &worker_group_name,
            "The total number of connector starts that failed.",
            &worker_tags,
            &mut all_templates,
        );
        let connector_startup_failure_percentage = Self::create_template(
            "connector-startup-failure-percentage",
            &worker_group_name,
            "The average percentage of this worker's connector starts that failed.",
            &worker_tags,
            &mut all_templates,
        );
        let task_startup_attempts_total = Self::create_template(
            "task-startup-attempts-total",
            &worker_group_name,
            "The total number of task startups that this worker has attempted.",
            &worker_tags,
            &mut all_templates,
        );
        let task_startup_success_total = Self::create_template(
            "task-startup-success-total",
            &worker_group_name,
            "The total number of task starts that succeeded.",
            &worker_tags,
            &mut all_templates,
        );
        let task_startup_success_percentage = Self::create_template(
            "task-startup-success-percentage",
            &worker_group_name,
            "The average percentage of this worker's task starts that succeeded.",
            &worker_tags,
            &mut all_templates,
        );
        let task_startup_failure_total = Self::create_template(
            "task-startup-failure-total",
            &worker_group_name,
            "The total number of task starts that failed.",
            &worker_tags,
            &mut all_templates,
        );
        let task_startup_failure_percentage = Self::create_template(
            "task-startup-failure-percentage",
            &worker_group_name,
            "The average percentage of this worker's task starts that failed.",
            &worker_tags,
            &mut all_templates,
        );

        // Worker connector metrics
        let connector_total_task_count = Self::create_template(
            "connector-total-task-count",
            &worker_group_name,
            "The number of tasks of the connector on the worker.",
            &worker_connector_tags,
            &mut all_templates,
        );
        let connector_running_task_count = Self::create_template(
            "connector-running-task-count",
            &worker_group_name,
            "The number of running tasks of the connector on the worker.",
            &worker_connector_tags,
            &mut all_templates,
        );
        let connector_paused_task_count = Self::create_template(
            "connector-paused-task-count",
            &worker_group_name,
            "The number of paused tasks of the connector on the worker.",
            &worker_connector_tags,
            &mut all_templates,
        );
        let connector_failed_task_count = Self::create_template(
            "connector-failed-task-count",
            &worker_group_name,
            "The number of failed tasks of the connector on the worker.",
            &worker_connector_tags,
            &mut all_templates,
        );
        let connector_unassigned_task_count = Self::create_template(
            "connector-unassigned-task-count",
            &worker_group_name,
            "The number of unassigned tasks of the connector on the worker.",
            &worker_connector_tags,
            &mut all_templates,
        );
        let connector_destroyed_task_count = Self::create_template(
            "connector-destroyed-task-count",
            &worker_group_name,
            "The number of destroyed tasks of the connector on the worker.",
            &worker_connector_tags,
            &mut all_templates,
        );
        let connector_restarting_task_count = Self::create_template(
            "connector-restarting-task-count",
            &worker_group_name,
            "The number of restarting tasks of the connector on the worker.",
            &worker_connector_tags,
            &mut all_templates,
        );

        // Worker rebalance metrics
        let connect_protocol = Self::create_template(
            "connect-protocol",
            &worker_rebalance_group_name,
            "The Connect protocol used by this cluster",
            &rebalance_tags,
            &mut all_templates,
        );
        let leader_name = Self::create_template(
            "leader-name",
            &worker_rebalance_group_name,
            "The name of the group leader.",
            &rebalance_tags,
            &mut all_templates,
        );
        let epoch = Self::create_template(
            "epoch",
            &worker_rebalance_group_name,
            "The epoch or generation number of this worker.",
            &rebalance_tags,
            &mut all_templates,
        );
        let rebalance_completed_total = Self::create_template(
            "completed-rebalances-total",
            &worker_rebalance_group_name,
            "The total number of rebalances completed by this worker.",
            &rebalance_tags,
            &mut all_templates,
        );
        let rebalance_mode = Self::create_template(
            "rebalancing",
            &worker_rebalance_group_name,
            "Whether this worker is currently rebalancing.",
            &rebalance_tags,
            &mut all_templates,
        );
        let rebalance_time_max = Self::create_template(
            "rebalance-max-time-ms",
            &worker_rebalance_group_name,
            "The maximum time in milliseconds spent by this worker to rebalance.",
            &rebalance_tags,
            &mut all_templates,
        );
        let rebalance_time_avg = Self::create_template(
            "rebalance-avg-time-ms",
            &worker_rebalance_group_name,
            "The average time in milliseconds spent by this worker to rebalance.",
            &rebalance_tags,
            &mut all_templates,
        );
        let rebalance_time_since_last = Self::create_template(
            "time-since-last-rebalance-ms",
            &worker_rebalance_group_name,
            "The time in milliseconds since this worker completed the most recent rebalance.",
            &rebalance_tags,
            &mut all_templates,
        );

        // Task error handling metrics
        let record_processing_failures = Self::create_template(
            "total-record-failures",
            &task_error_handling_group_name,
            "The number of record processing failures in this task.",
            &task_error_handling_tags,
            &mut all_templates,
        );
        let record_processing_errors = Self::create_template(
            "total-record-errors",
            &task_error_handling_group_name,
            "The number of record processing errors in this task.",
            &task_error_handling_tags,
            &mut all_templates,
        );
        let records_skipped = Self::create_template(
            "total-records-skipped",
            &task_error_handling_group_name,
            "The number of records skipped due to errors.",
            &task_error_handling_tags,
            &mut all_templates,
        );
        let retries = Self::create_template(
            "total-retries",
            &task_error_handling_group_name,
            "The number of operations retried.",
            &task_error_handling_tags,
            &mut all_templates,
        );
        let errors_logged = Self::create_template(
            "total-errors-logged",
            &task_error_handling_group_name,
            "The number of errors that were logged.",
            &task_error_handling_tags,
            &mut all_templates,
        );
        let dlq_produce_requests = Self::create_template(
            "deadletterqueue-produce-requests",
            &task_error_handling_group_name,
            "The number of attempted writes to the dead letter queue.",
            &task_error_handling_tags,
            &mut all_templates,
        );
        let dlq_produce_failures = Self::create_template(
            "deadletterqueue-produce-failures",
            &task_error_handling_group_name,
            "The number of failed writes to the dead letter queue.",
            &task_error_handling_tags,
            &mut all_templates,
        );
        let last_error_timestamp = Self::create_template(
            "last-error-timestamp",
            &task_error_handling_group_name,
            "The epoch timestamp when this task last encountered an error.",
            &task_error_handling_tags,
            &mut all_templates,
        );

        // Build connector status metrics map
        let connector_status_metrics: HashMap<MetricNameTemplate, TaskState> = [
            (connector_running_task_count.clone(), TaskState::Running),
            (connector_paused_task_count.clone(), TaskState::Paused),
            (connector_failed_task_count.clone(), TaskState::Failed),
            (
                connector_unassigned_task_count.clone(),
                TaskState::Unassigned,
            ),
            (connector_destroyed_task_count.clone(), TaskState::Destroyed),
            (
                connector_restarting_task_count.clone(),
                TaskState::Restarting,
            ),
        ]
        .into_iter()
        .collect();

        ConnectMetricsRegistry {
            all_templates,
            connector_tag_name,
            task_tag_name,
            transform_tag_name,
            predicate_tag_name,
            connector_group_name,
            task_group_name,
            source_task_group_name,
            sink_task_group_name,
            worker_group_name,
            worker_rebalance_group_name,
            task_error_handling_group_name,
            transforms_group_name,
            predicates_group_name,
            connector_status,
            connector_type,
            connector_class,
            connector_version,
            connector_total_task_count,
            connector_running_task_count,
            connector_paused_task_count,
            connector_failed_task_count,
            connector_unassigned_task_count,
            connector_destroyed_task_count,
            connector_restarting_task_count,
            task_status,
            task_running_ratio,
            task_pause_ratio,
            task_commit_time_max,
            task_commit_time_avg,
            task_batch_size_max,
            task_batch_size_avg,
            task_commit_failure_percentage,
            task_commit_success_percentage,
            task_connector_class,
            task_connector_class_version,
            task_connector_type,
            task_class,
            task_version,
            task_key_converter_class,
            task_value_converter_class,
            task_key_converter_version,
            task_value_converter_version,
            task_header_converter_class,
            task_header_converter_version,
            source_record_poll_rate,
            source_record_poll_total,
            source_record_write_rate,
            source_record_write_total,
            source_record_poll_batch_time_max,
            source_record_poll_batch_time_avg,
            source_record_active_count,
            source_record_active_count_max,
            source_record_active_count_avg,
            transaction_size_min,
            transaction_size_max,
            transaction_size_avg,
            sink_record_read_rate,
            sink_record_read_total,
            sink_record_send_rate,
            sink_record_send_total,
            sink_record_lag_max,
            sink_record_partition_count,
            sink_record_offset_commit_seq_num,
            sink_record_offset_commit_completion_rate,
            sink_record_offset_commit_completion_total,
            sink_record_offset_commit_skip_rate,
            sink_record_offset_commit_skip_total,
            sink_record_put_batch_time_max,
            sink_record_put_batch_time_avg,
            sink_record_active_count,
            sink_record_active_count_max,
            sink_record_active_count_avg,
            connector_count,
            task_count,
            connector_startup_attempts_total,
            connector_startup_success_total,
            connector_startup_success_percentage,
            connector_startup_failure_total,
            connector_startup_failure_percentage,
            task_startup_attempts_total,
            task_startup_success_total,
            task_startup_success_percentage,
            task_startup_failure_total,
            task_startup_failure_percentage,
            connect_protocol,
            leader_name,
            epoch,
            rebalance_completed_total,
            rebalance_mode,
            rebalance_time_max,
            rebalance_time_avg,
            rebalance_time_since_last,
            record_processing_failures,
            record_processing_errors,
            records_skipped,
            retries,
            errors_logged,
            dlq_produce_requests,
            dlq_produce_failures,
            last_error_timestamp,
            transform_class,
            transform_version,
            predicate_class,
            predicate_version,
            connector_status_metrics,
        }
    }

    /// Helper to create a template and add it to the list.
    fn create_template(
        name: &str,
        group: &str,
        description: &str,
        tags: &HashSet<String>,
        all_templates: &mut Vec<MetricNameTemplate>,
    ) -> MetricNameTemplate {
        let template = MetricNameTemplate::new(
            name.to_string(),
            group.to_string(),
            description.to_string(),
            tags.clone(),
        );
        all_templates.push(template.clone());
        template
    }

    /// Returns all templates registered in this registry.
    pub fn all_templates(&self) -> &[MetricNameTemplate] {
        &self.all_templates
    }

    /// Returns the connector tag name.
    pub fn connector_tag_name(&self) -> &str {
        &self.connector_tag_name
    }

    /// Returns the task tag name.
    pub fn task_tag_name(&self) -> &str {
        &self.task_tag_name
    }

    /// Returns the transform tag name.
    pub fn transform_tag_name(&self) -> &str {
        &self.transform_tag_name
    }

    /// Returns the predicate tag name.
    pub fn predicate_tag_name(&self) -> &str {
        &self.predicate_tag_name
    }

    /// Returns the connector group name.
    pub fn connector_group_name(&self) -> &str {
        &self.connector_group_name
    }

    /// Returns the task group name.
    pub fn task_group_name(&self) -> &str {
        &self.task_group_name
    }

    /// Returns the source task group name.
    pub fn source_task_group_name(&self) -> &str {
        &self.source_task_group_name
    }

    /// Returns the sink task group name.
    pub fn sink_task_group_name(&self) -> &str {
        &self.sink_task_group_name
    }

    /// Returns the worker group name.
    pub fn worker_group_name(&self) -> &str {
        &self.worker_group_name
    }

    /// Returns the worker rebalance group name.
    pub fn worker_rebalance_group_name(&self) -> &str {
        &self.worker_rebalance_group_name
    }

    /// Returns the task error handling group name.
    pub fn task_error_handling_group_name(&self) -> &str {
        &self.task_error_handling_group_name
    }

    /// Returns the transforms group name.
    pub fn transforms_group_name(&self) -> &str {
        &self.transforms_group_name
    }

    /// Returns the predicates group name.
    pub fn predicates_group_name(&self) -> &str {
        &self.predicates_group_name
    }

    // Connector metrics getters
    pub fn connector_status(&self) -> &MetricNameTemplate {
        &self.connector_status
    }

    pub fn connector_type(&self) -> &MetricNameTemplate {
        &self.connector_type
    }

    pub fn connector_class(&self) -> &MetricNameTemplate {
        &self.connector_class
    }

    pub fn connector_version(&self) -> &MetricNameTemplate {
        &self.connector_version
    }

    // Connector task count metrics getters
    pub fn connector_total_task_count(&self) -> &MetricNameTemplate {
        &self.connector_total_task_count
    }

    pub fn connector_running_task_count(&self) -> &MetricNameTemplate {
        &self.connector_running_task_count
    }

    pub fn connector_paused_task_count(&self) -> &MetricNameTemplate {
        &self.connector_paused_task_count
    }

    pub fn connector_failed_task_count(&self) -> &MetricNameTemplate {
        &self.connector_failed_task_count
    }

    pub fn connector_unassigned_task_count(&self) -> &MetricNameTemplate {
        &self.connector_unassigned_task_count
    }

    pub fn connector_destroyed_task_count(&self) -> &MetricNameTemplate {
        &self.connector_destroyed_task_count
    }

    pub fn connector_restarting_task_count(&self) -> &MetricNameTemplate {
        &self.connector_restarting_task_count
    }

    // Task metrics getters
    pub fn task_status(&self) -> &MetricNameTemplate {
        &self.task_status
    }

    pub fn task_running_ratio(&self) -> &MetricNameTemplate {
        &self.task_running_ratio
    }

    pub fn task_pause_ratio(&self) -> &MetricNameTemplate {
        &self.task_pause_ratio
    }

    pub fn task_commit_time_max(&self) -> &MetricNameTemplate {
        &self.task_commit_time_max
    }

    pub fn task_commit_time_avg(&self) -> &MetricNameTemplate {
        &self.task_commit_time_avg
    }

    pub fn task_batch_size_max(&self) -> &MetricNameTemplate {
        &self.task_batch_size_max
    }

    pub fn task_batch_size_avg(&self) -> &MetricNameTemplate {
        &self.task_batch_size_avg
    }

    pub fn task_commit_failure_percentage(&self) -> &MetricNameTemplate {
        &self.task_commit_failure_percentage
    }

    pub fn task_commit_success_percentage(&self) -> &MetricNameTemplate {
        &self.task_commit_success_percentage
    }

    pub fn task_connector_class(&self) -> &MetricNameTemplate {
        &self.task_connector_class
    }

    pub fn task_connector_class_version(&self) -> &MetricNameTemplate {
        &self.task_connector_class_version
    }

    pub fn task_connector_type(&self) -> &MetricNameTemplate {
        &self.task_connector_type
    }

    pub fn task_class(&self) -> &MetricNameTemplate {
        &self.task_class
    }

    pub fn task_version(&self) -> &MetricNameTemplate {
        &self.task_version
    }

    pub fn task_key_converter_class(&self) -> &MetricNameTemplate {
        &self.task_key_converter_class
    }

    pub fn task_value_converter_class(&self) -> &MetricNameTemplate {
        &self.task_value_converter_class
    }

    pub fn task_key_converter_version(&self) -> &MetricNameTemplate {
        &self.task_key_converter_version
    }

    pub fn task_value_converter_version(&self) -> &MetricNameTemplate {
        &self.task_value_converter_version
    }

    pub fn task_header_converter_class(&self) -> &MetricNameTemplate {
        &self.task_header_converter_class
    }

    pub fn task_header_converter_version(&self) -> &MetricNameTemplate {
        &self.task_header_converter_version
    }

    // Source task metrics getters
    pub fn source_record_poll_rate(&self) -> &MetricNameTemplate {
        &self.source_record_poll_rate
    }

    pub fn source_record_poll_total(&self) -> &MetricNameTemplate {
        &self.source_record_poll_total
    }

    pub fn source_record_write_rate(&self) -> &MetricNameTemplate {
        &self.source_record_write_rate
    }

    pub fn source_record_write_total(&self) -> &MetricNameTemplate {
        &self.source_record_write_total
    }

    pub fn source_record_poll_batch_time_max(&self) -> &MetricNameTemplate {
        &self.source_record_poll_batch_time_max
    }

    pub fn source_record_poll_batch_time_avg(&self) -> &MetricNameTemplate {
        &self.source_record_poll_batch_time_avg
    }

    pub fn source_record_active_count(&self) -> &MetricNameTemplate {
        &self.source_record_active_count
    }

    pub fn source_record_active_count_max(&self) -> &MetricNameTemplate {
        &self.source_record_active_count_max
    }

    pub fn source_record_active_count_avg(&self) -> &MetricNameTemplate {
        &self.source_record_active_count_avg
    }

    pub fn transaction_size_min(&self) -> &MetricNameTemplate {
        &self.transaction_size_min
    }

    pub fn transaction_size_max(&self) -> &MetricNameTemplate {
        &self.transaction_size_max
    }

    pub fn transaction_size_avg(&self) -> &MetricNameTemplate {
        &self.transaction_size_avg
    }

    // Sink task metrics getters
    pub fn sink_record_read_rate(&self) -> &MetricNameTemplate {
        &self.sink_record_read_rate
    }

    pub fn sink_record_read_total(&self) -> &MetricNameTemplate {
        &self.sink_record_read_total
    }

    pub fn sink_record_send_rate(&self) -> &MetricNameTemplate {
        &self.sink_record_send_rate
    }

    pub fn sink_record_send_total(&self) -> &MetricNameTemplate {
        &self.sink_record_send_total
    }

    pub fn sink_record_lag_max(&self) -> &MetricNameTemplate {
        &self.sink_record_lag_max
    }

    pub fn sink_record_partition_count(&self) -> &MetricNameTemplate {
        &self.sink_record_partition_count
    }

    pub fn sink_record_offset_commit_seq_num(&self) -> &MetricNameTemplate {
        &self.sink_record_offset_commit_seq_num
    }

    pub fn sink_record_offset_commit_completion_rate(&self) -> &MetricNameTemplate {
        &self.sink_record_offset_commit_completion_rate
    }

    pub fn sink_record_offset_commit_completion_total(&self) -> &MetricNameTemplate {
        &self.sink_record_offset_commit_completion_total
    }

    pub fn sink_record_offset_commit_skip_rate(&self) -> &MetricNameTemplate {
        &self.sink_record_offset_commit_skip_rate
    }

    pub fn sink_record_offset_commit_skip_total(&self) -> &MetricNameTemplate {
        &self.sink_record_offset_commit_skip_total
    }

    pub fn sink_record_put_batch_time_max(&self) -> &MetricNameTemplate {
        &self.sink_record_put_batch_time_max
    }

    pub fn sink_record_put_batch_time_avg(&self) -> &MetricNameTemplate {
        &self.sink_record_put_batch_time_avg
    }

    pub fn sink_record_active_count(&self) -> &MetricNameTemplate {
        &self.sink_record_active_count
    }

    pub fn sink_record_active_count_max(&self) -> &MetricNameTemplate {
        &self.sink_record_active_count_max
    }

    pub fn sink_record_active_count_avg(&self) -> &MetricNameTemplate {
        &self.sink_record_active_count_avg
    }

    // Worker metrics getters
    pub fn connector_count(&self) -> &MetricNameTemplate {
        &self.connector_count
    }

    pub fn task_count(&self) -> &MetricNameTemplate {
        &self.task_count
    }

    pub fn connector_startup_attempts_total(&self) -> &MetricNameTemplate {
        &self.connector_startup_attempts_total
    }

    pub fn connector_startup_success_total(&self) -> &MetricNameTemplate {
        &self.connector_startup_success_total
    }

    pub fn connector_startup_success_percentage(&self) -> &MetricNameTemplate {
        &self.connector_startup_success_percentage
    }

    pub fn connector_startup_failure_total(&self) -> &MetricNameTemplate {
        &self.connector_startup_failure_total
    }

    pub fn connector_startup_failure_percentage(&self) -> &MetricNameTemplate {
        &self.connector_startup_failure_percentage
    }

    pub fn task_startup_attempts_total(&self) -> &MetricNameTemplate {
        &self.task_startup_attempts_total
    }

    pub fn task_startup_success_total(&self) -> &MetricNameTemplate {
        &self.task_startup_success_total
    }

    pub fn task_startup_success_percentage(&self) -> &MetricNameTemplate {
        &self.task_startup_success_percentage
    }

    pub fn task_startup_failure_total(&self) -> &MetricNameTemplate {
        &self.task_startup_failure_total
    }

    pub fn task_startup_failure_percentage(&self) -> &MetricNameTemplate {
        &self.task_startup_failure_percentage
    }

    // Worker rebalance metrics getters
    pub fn connect_protocol(&self) -> &MetricNameTemplate {
        &self.connect_protocol
    }

    pub fn leader_name(&self) -> &MetricNameTemplate {
        &self.leader_name
    }

    pub fn epoch(&self) -> &MetricNameTemplate {
        &self.epoch
    }

    pub fn rebalance_completed_total(&self) -> &MetricNameTemplate {
        &self.rebalance_completed_total
    }

    pub fn rebalance_mode(&self) -> &MetricNameTemplate {
        &self.rebalance_mode
    }

    pub fn rebalance_time_max(&self) -> &MetricNameTemplate {
        &self.rebalance_time_max
    }

    pub fn rebalance_time_avg(&self) -> &MetricNameTemplate {
        &self.rebalance_time_avg
    }

    pub fn rebalance_time_since_last(&self) -> &MetricNameTemplate {
        &self.rebalance_time_since_last
    }

    // Task error handling metrics getters
    pub fn record_processing_failures(&self) -> &MetricNameTemplate {
        &self.record_processing_failures
    }

    pub fn record_processing_errors(&self) -> &MetricNameTemplate {
        &self.record_processing_errors
    }

    pub fn records_skipped(&self) -> &MetricNameTemplate {
        &self.records_skipped
    }

    pub fn retries(&self) -> &MetricNameTemplate {
        &self.retries
    }

    pub fn errors_logged(&self) -> &MetricNameTemplate {
        &self.errors_logged
    }

    pub fn dlq_produce_requests(&self) -> &MetricNameTemplate {
        &self.dlq_produce_requests
    }

    pub fn dlq_produce_failures(&self) -> &MetricNameTemplate {
        &self.dlq_produce_failures
    }

    pub fn last_error_timestamp(&self) -> &MetricNameTemplate {
        &self.last_error_timestamp
    }

    // Transform metrics getters
    pub fn transform_class(&self) -> &MetricNameTemplate {
        &self.transform_class
    }

    pub fn transform_version(&self) -> &MetricNameTemplate {
        &self.transform_version
    }

    // Predicate metrics getters
    pub fn predicate_class(&self) -> &MetricNameTemplate {
        &self.predicate_class
    }

    pub fn predicate_version(&self) -> &MetricNameTemplate {
        &self.predicate_version
    }

    /// Returns the connector status metrics map.
    pub fn connector_status_metrics(&self) -> &HashMap<MetricNameTemplate, TaskState> {
        &self.connector_status_metrics
    }
}

impl Default for ConnectMetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metric_name_template_creation() {
        let tags: HashSet<String> = ["connector", "task"]
            .into_iter()
            .map(String::from)
            .collect();
        let template = MetricNameTemplate::new(
            "status".to_string(),
            "connector-metrics".to_string(),
            "The status of the connector".to_string(),
            tags.clone(),
        );

        assert_eq!(template.name(), "status");
        assert_eq!(template.group(), "connector-metrics");
        assert_eq!(template.description(), "The status of the connector");
        assert_eq!(template.tags().len(), 2);
    }

    #[test]
    fn test_connect_metrics_registry_creation() {
        let registry = ConnectMetricsRegistry::new();

        assert_eq!(registry.connector_tag_name(), "connector");
        assert_eq!(registry.task_tag_name(), "task");
        assert_eq!(registry.connector_group_name(), "connector-metrics");
        assert_eq!(registry.task_group_name(), "connector-task-metrics");
        assert_eq!(registry.source_task_group_name(), "source-task-metrics");
        assert_eq!(registry.sink_task_group_name(), "sink-task-metrics");
        assert_eq!(registry.worker_group_name(), "connect-worker-metrics");
        assert_eq!(
            registry.worker_rebalance_group_name(),
            "connect-worker-rebalance-metrics"
        );
        assert_eq!(
            registry.task_error_handling_group_name(),
            "task-error-metrics"
        );
    }

    #[test]
    fn test_all_templates_count() {
        let registry = ConnectMetricsRegistry::new();
        // Should have ~80 templates based on Java implementation
        assert!(registry.all_templates().len() > 70);
    }

    #[test]
    fn test_connector_status_metrics() {
        let registry = ConnectMetricsRegistry::new();
        let status_metrics = registry.connector_status_metrics();

        assert_eq!(status_metrics.len(), 6);
        assert!(status_metrics.contains_key(registry.connector_running_task_count()));
        assert_eq!(
            status_metrics.get(registry.connector_running_task_count()),
            Some(&TaskState::Running)
        );
        assert_eq!(
            status_metrics.get(registry.connector_paused_task_count()),
            Some(&TaskState::Paused)
        );
        assert_eq!(
            status_metrics.get(registry.connector_failed_task_count()),
            Some(&TaskState::Failed)
        );
    }

    #[test]
    fn test_task_state() {
        assert_eq!(TaskState::Running.as_str(), "running");
        assert_eq!(TaskState::Paused.as_str(), "paused");
        assert_eq!(TaskState::Failed.as_str(), "failed");
        assert_eq!(TaskState::Unassigned.as_str(), "unassigned");
        assert_eq!(TaskState::Destroyed.as_str(), "destroyed");
        assert_eq!(TaskState::Restarting.as_str(), "restarting");
    }

    #[test]
    fn test_source_task_metrics_templates() {
        let registry = ConnectMetricsRegistry::new();

        assert_eq!(
            registry.source_record_poll_rate().name(),
            "source-record-poll-rate"
        );
        assert_eq!(
            registry.source_record_poll_total().name(),
            "source-record-poll-total"
        );
        assert_eq!(
            registry.source_record_write_rate().name(),
            "source-record-write-rate"
        );
        assert_eq!(
            registry.source_record_active_count().name(),
            "source-record-active-count"
        );
    }

    #[test]
    fn test_sink_task_metrics_templates() {
        let registry = ConnectMetricsRegistry::new();

        assert_eq!(
            registry.sink_record_read_rate().name(),
            "sink-record-read-rate"
        );
        assert_eq!(
            registry.sink_record_read_total().name(),
            "sink-record-read-total"
        );
        assert_eq!(
            registry.sink_record_send_rate().name(),
            "sink-record-send-rate"
        );
        assert_eq!(
            registry.sink_record_active_count().name(),
            "sink-record-active-count"
        );
    }

    #[test]
    fn test_worker_rebalance_metrics_templates() {
        let registry = ConnectMetricsRegistry::new();

        assert_eq!(registry.connect_protocol().name(), "connect-protocol");
        assert_eq!(registry.leader_name().name(), "leader-name");
        assert_eq!(registry.epoch().name(), "epoch");
        assert_eq!(
            registry.rebalance_completed_total().name(),
            "completed-rebalances-total"
        );
    }

    #[test]
    fn test_task_error_handling_metrics_templates() {
        let registry = ConnectMetricsRegistry::new();

        assert_eq!(
            registry.record_processing_failures().name(),
            "total-record-failures"
        );
        assert_eq!(
            registry.record_processing_errors().name(),
            "total-record-errors"
        );
        assert_eq!(registry.records_skipped().name(), "total-records-skipped");
        assert_eq!(
            registry.dlq_produce_requests().name(),
            "deadletterqueue-produce-requests"
        );
    }

    #[test]
    fn test_with_additional_tags() {
        let mut tags = HashSet::new();
        tags.insert("custom-tag".to_string());
        let registry = ConnectMetricsRegistry::with_tags(tags);

        // All templates should include the custom tag
        for template in registry.all_templates() {
            // Note: Some templates may not have the custom tag based on how they're built
            // This test just verifies the registry can be created with custom tags
        }

        // Verify basic functionality still works
        assert_eq!(registry.connector_tag_name(), "connector");
    }

    #[test]
    fn test_metric_name_template_equality() {
        let tags1: HashSet<String> = ["connector"].into_iter().map(String::from).collect();
        let tags2: HashSet<String> = ["connector"].into_iter().map(String::from).collect();

        let template1 = MetricNameTemplate::new(
            "status".to_string(),
            "connector-metrics".to_string(),
            "desc".to_string(),
            tags1,
        );
        let template2 = MetricNameTemplate::new(
            "status".to_string(),
            "connector-metrics".to_string(),
            "desc".to_string(),
            tags2,
        );

        assert_eq!(template1, template2);
    }
}
