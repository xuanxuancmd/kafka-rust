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

//! Runtime module for Kafka Connect.
//!
//! This module contains the core runtime components for Kafka Connect,
//! including workers, herders, tasks, and connectors.

pub mod abstract_herder;
pub mod abstract_worker_source_task;
pub mod cached_connectors;
pub mod exactly_once_worker_source_task;
pub mod closeable_connector_context;
pub mod connect;
pub mod connect_metrics;
pub mod connect_metrics_registry;
pub mod herder_connector_context;
pub mod herder_request;
pub mod internal_sink_record;
pub mod loggers;
pub mod restart_plan;
pub mod restart_request;
pub mod session_key;
pub mod source_task_offset_committer;
pub mod state_tracker;
pub mod status;
pub mod submitted_records;
pub mod target_state;
pub mod task_config;
pub mod task_plugins_metadata;
pub mod too_many_tasks_exception;
pub mod topic_creation_config;
pub mod topic_status;
pub mod transformation;
pub mod worker_config_transformer;
pub mod worker_connector;
pub mod worker_info;
pub mod worker_metrics_group;
pub mod worker_sink_task_context;
pub mod worker_source_task_context;
pub mod worker_transaction_context;

// Re-export commonly used types
pub use abstract_herder::AbstractHerder;
pub use abstract_worker_source_task::{
    AbstractWorkerSourceTask, AbstractWorkerSourceTaskHooks, ConnectorOffsetBackingStore,
    KafkaProducer, NewTopic, OffsetStorageReader, OffsetStorageWriter, SourceRecordWriteCounter,
    SourceTaskConverter, SourceTaskHeaderConverter, SourceTaskMetricsGroup,
    SourceTaskTransformationChain, TopicAdmin, TopicCreation, TopicCreationResponse,
    TopicDescription, WorkerSourceTaskConfig,
};
pub use exactly_once_worker_source_task::{
    ExactlyOnceWorkerSourceTask, MockTransactionalProducer, PreProducerCheck,
    PostProducerCheck, TransactionalProducer, TRANSACTION_BOUNDARY_INTERVAL_MS_CONFIG,
    TRANSACTION_BOUNDARY_INTERVAL_MS_DEFAULT,
};
pub use closeable_connector_context::CloseableConnectorContext;
pub use herder_connector_context::{HerderCallback, HerderConnectorContext};
pub use herder_request::HerderRequest;
pub use internal_sink_record::{InternalSinkRecord, ProcessingContext};
pub use state_tracker::StateTracker;
pub use status::{ConnectorStatus, ConnectorStatusListener, State, TaskStatus, TaskStatusListener};
pub use submitted_records::{CommittableOffsets, SubmittedRecord, SubmittedRecords};
pub use source_task_offset_committer::{CommittableSourceTask, SourceTaskOffsetCommitter};
pub use target_state::TargetState;
pub use task_plugins_metadata::{AliasedPluginInfo, ConnectorType, StageInfo, TaskPluginsMetadata};
pub use topic_creation_config::{
    TopicCreationConfig, TopicCreationGroup, DEFAULT_TOPIC_CREATION_GROUP, NO_PARTITIONS,
    NO_REPLICATION_FACTOR,
};
pub use transformation::{
    TransformationChain, TransformationChainBuilder, TransformationStage,
    AlwaysTruePredicate, AlwaysFalsePredicate, TopicNameMatchesPredicate,
    NoOpTransformation, FilterAllTransformation,
};
pub use worker_info::WorkerInfo;
pub use worker_sink_task_context::WorkerSinkTaskContext;
pub use worker_source_task_context::WorkerSourceTaskContext;
pub use worker_transaction_context::WorkerTransactionContext;
