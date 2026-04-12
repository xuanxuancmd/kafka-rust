# Connect Runtime 模块迁移工作计划

## TL;DR

> **快速摘要**：将 kafka connect/runtime 模块（191 个 Java 文件，40,426 行代码）完整迁移到 Rust，保存到 connect-rust-new/connect-runtime 目录。
>
> **技术方案**：
> - 插件隔离：编译期宏 + 插件注册（inventory crate）
> - 并发模型：Tokio + CompletableFuture（oneshot 封装）
> - 反射：编译期宏 + trait 对象
> - REST API：actix-web + serde
>
> **迁移策略**：完全按照类名、函数名 1:1 翻译，所有测试（153 个）都 1:1 迁移
>
> **依赖处理**：所有外部依赖的 trait 定义在 kafka-clients-trait 中，mock 实现在 kafka-clients-mock 中
>
> **预估工作量**：约 28-42 天（6-8 周）
> **并行执行**：是（按阶段分批）
> **关键路径**：基础设施 → 基础类型 → 存储层 → 插件系统 → 运行时核心 → REST API → 测试迁移

---

## Context

### Original Request

将 kafka connect/runtime 模块完整迁移到 Rust，保存到 connect-rust-new/connect-runtime 目录下（除 connect-file 和 connect-json 外所有均迁移，含测试）。

### Interview Summary

**用户确认的关键决策**：

1. **插件隔离和反射机制**：使用编译期宏 + 插件注册，不实现运行期动态机制
   - 使用 `inventory` crate 实现插件注册表
   - 使用编译期宏（`register_connector!`）注册插件
   - 使用 trait 对象（`Box<dyn Trait>`）实现多态
   - 避免运行时反射和动态库加载的复杂性

2. **并发模型**：使用 Tokio 作为并发类库
   - CompletableFuture 在 `kafka-clients-trait/util` 下实现基于 `oneshot` 的封装
   - `synchronized` → `Arc<Mutex<T>>`
   - `volatile` → `AtomicUsize`, `AtomicBool`
   - `ExecutorService` → `tokio::task::spawn`
   - `CountDownLatch` → `tokio::sync::Semaphore`
   - `wait/notify` → `tokio::sync::Notify`
   - `ConcurrentHashMap` → `dashmap::DashMap`

3. **依赖处理策略**：
   - kafka-clients 的接口定义在 `kafka-clients-trait` 中
   - 其他模块（如 common）的代码也一并添加到 `kafka-clients-trait` 中
   - 兼容 API 的内存版实现位于 `kafka-clients-mock` 中

4. **迁移和测试策略**：
   - 完全按照类名、函数名 1:1 翻译成 Rust
   - Kafka 的测试应该完全一并 1:1 迁移

5. **Web 框架选择**：actix-web
   - 成熟稳定、生态好、性能优秀、与 Tokio 兼容

### Research Findings

**代码规模**：
- Java 文件数：191 个（主代码）+ 153 个（测试）
- 代码行数：40,426 行（主代码）+ 58,512 行（测试）
- 已迁移进度：约 29%（11,751 行 / 40,426 行）
- 还需迁移：约 28,675 行代码

**已迁移模块**：
- connect-runtime-core：13 个文件，9,873 行
- connect-runtime-distributed：4 个文件，1,878 行

**未迁移的主要模块**：
- cli（3个文件）- 100% 未迁移
- connector/policy（5个文件）- 100% 未迁移
- converters（10个文件）- 100% 未迁移
- runtime/rest（37个文件）- 约 90% 未迁移
- runtime/standalone（3个文件）- 100% 未迁移
- storage 中的部分文件
- util（20个文件）- 约 80% 未迁移

---

## Work Objectives

### Core Objective

将 kafka connect/runtime 模块完整、准确地迁移到 Rust，确保：
1. 完整性：代码结构、功能、测试覆盖与 Java 版本相当
2. 正确性：代码编译通过，测试通过，功能验证正确
3. 兼容性：与现有 connect-runtime-core 和 connect-runtime-distributed 兼容

### Concrete Deliverables

1. **connect-runtime 目录**：包含所有迁移后的 Rust 代码
2. **kafka-clients-trait 补充**：所有外部依赖的 trait 定义
3. **kafka-clients-mock 补充**：所有 trait 的 mock 实现
4. **测试代码**：所有 Java 测试的 1:1 Rust 迁移
5. **验证报告**：完整性和正确性度量报告

### Definition of Done

- [ ] 所有 Java 文件都有对应的 Rust 文件（±10%）
- [ ] 所有 class/interface/enum 都有对应的 struct/trait/enum（±10%）
- [ ] 所有方法都有对应的函数（±10%）
- [ ] 代码行数相当（±20%）
- [ ] 无 TODO 或空实现
- [ ] `cargo check` 通过
- [ ] `cargo clippy` 通过
- [ ] `cargo test` 全部通过
- [ ] 测试覆盖率 ≥ 80%

### Must Have

- 所有 public 方法的 Rust 实现
- 所有配置项的 Rust 定义
- 所有错误情况的 Rust 错误处理
- 所有 Java 测试的 Rust 迁移
- 编译通过和测试通过

### Must NOT Have (Guardrails)

- TODO 或空实现
- `unimplemented!()` 或 `todo!()` 宏
- 不符合命名规范的代码（驼峰转蛇形）
- 缺少测试的公共方法
- 不符合 Rust 最佳实践的代码（如不必要的 `unsafe`）

---

## Verification Strategy

> **ZERO HUMAN INTERVENTION** — ALL verification is agent-executed. No exceptions.
> Acceptance criteria requiring "user manually tests/confirms" are FORBIDDEN.

### Test Decision

- **Infrastructure exists**: YES (已有 Cargo.toml)
- **Automated tests**: YES (Tests-after，所有测试 1:1 迁移)
- **Framework**: cargo test
- **测试策略**：所有 Java 测试（153 个）都 1:1 迁移到 Rust

### QA Policy

每个任务 MUST 包含 agent-executed QA scenarios（见 TODO 模板）。
Evidence 保存到 `.sisyphus/evidence/task-{N}-{scenario-slug}.{ext}`。

- **编译验证**：使用 `cargo check` 和 `cargo build`
- **测试验证**：使用 `cargo test`
- **代码质量**：使用 `cargo clippy` 和 `cargo fmt --check`

---

## Execution Strategy

### Parallel Execution Waves

```
Wave 1 (Start Immediately — 基础设施准备):
├── Task 1: 补充 kafka-clients-trait（Producer/Consumer/Admin trait）[deep]
├── Task 2: 补充 kafka-clients-trait（ConfigDef, Time, Utils 等工具 trait）[deep]
├── Task 4: 补充 kafka-clients-mock（Producer/Consumer/Admin mock）[deep]
└── Task 5: 补充 kafka-clients-mock（ConfigDef, Time, Utils mock）[deep]

Wave 2 (After Wave 1 — 基础类型和工具):
├── Task 7: 迁移 util 包（20个文件）[unspecified-high]
├── Task 8: 迁移 errors 包（10个文件）[unspecified-high]
└── Task 9: 迁移 converters 包（10个文件）[unspecified-high]

Wave 3 (After Wave 2 — 存储层):
└── Task 10: 完善 storage 包（19个文件）[deep]

Wave 4 (After Wave 3 — 插件系统):
└── Task 11: 迁移 isolation 包（17个文件，使用编译期宏）[deep]

Wave 5 (After Wave 4 — 运行时核心):
├── Task 12: 完善 Worker（Worker, WorkerConnector, WorkerSourceTask, WorkerSinkTask 等）[deep]
├── Task 13: 完善 Herder（AbstractHerder, DistributedHerder, StandaloneHerder）[deep]
└── Task 14: 迁移 cli 包（3个文件）[unspecified-high]

Wave 6 (After Wave 5 — REST API):
└── Task 15: 迁移 rest 包（37个文件，使用 actix-web）[deep]

Wave 7 (After Wave 6 — 测试迁移):
├── Task 16: 迁移单元测试（153个文件）[deep]
└── Task 17: 迁移集成测试（使用 embedded Kafka）[deep]

Wave FINAL (After ALL tasks — 验证和报告):
├── Task F1: 完整性度量（代码结构对比）[deep]
├── Task F2: 正确性度量（编译和测试）[deep]
├── Task F3: 功能验证（端到端测试）[deep]
└── Task F4: 生成最终报告[deep]

Critical Path: Task 1-5 → Task 7-9 → Task 10 → Task 11 → Task 12-14 → Task 15 → Task 16-17 → F1-F4
Parallel Speedup: 约 60% 更快（相比顺序执行）（Wave 1 已完成）
Max Concurrent: 5 (Wave 1)
```

---

## TODOs

- [x] 1. 补充 kafka-clients-trait：Producer/Consumer/Admin trait（已完成）（已完成）

  **What to do**:
  - 在 `kafka-clients-trait/src` 中添加 Producer 相关 trait：
    - `Producer` trait
    - `ProducerRecord` struct
    - `RecordMetadata` struct
    - `Callback` trait
    - `ProducerConfig` struct
  - 在 `kafka-clients-trait/src` 中添加 Consumer 相关 trait：
    - `Consumer` trait
    - `ConsumerRecord` struct
    - `ConsumerRecords` struct
    - `OffsetAndMetadata` struct
    - `ConsumerConfig` struct
    - `ConsumerRebalanceListener` trait
    - `OffsetCommitCallback` trait
  - 在 `kafka-clients-trait/src` 中添加了 Admin 相关 trait：
    - `Admin` trait
    - `NewTopic` struct
    - `ConfigEntry` struct
    - `AdminClientConfig` struct
    - 各种 Admin 操作 trait（ListOffsets, DeleteConsumerGroups 等）
  - 确保所有 trait 都有完整的文档注释
  - 确保所有 trait 都有 `Send + Sync` 约束（如果需要）

  **Must NOT do**:
  - 不要实现具体逻辑，只定义 trait
  - 不要添加不相关的 trait

  **Recommended Agent Profile**:
  - **Category**: `deep`
    - Reason: 需要深入分析 Java 代码，理解 API 设计
  - **Skills**: []
  - **Skills Evaluated but Omitted**: []

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential
  - **Blocks**: [Task 4]
  - **Blocked By**: None

  **References**:
  - **Pattern References**:
    - `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/Worker.java:19-100` - Producer/Consumer/Admin 使用示例
  - **API/Type References**:
    - `org.apache.kafka.clients.producer.Producer` - Producer 接口定义
    - `org.apache.kafka.clients.consumer.Consumer` - Consumer 接口定义
    - `org.apache.kafka.clients.admin.Admin` - Admin 接口定义

  **Acceptance Criteria**:
  - [ ] 所有 Producer 相关 trait 都已定义
  - [ ] 所有 Consumer 相关 trait 都已定义
  - [ ] 所有 Admin 相关 trait 都已定义
  - [ ] 所有 trait 都有文档注释
  - [ ] `cargo check` 通过

  **QA Scenarios (MANDATORY)**:
  ```
  Scenario: 编译验证
    Tool: Bash (cargo check)
    Preconditions: kafka-clients-trait 已有基础结构
    Steps:
      1. cd kafka-clients-trait
      2. cargo check
    Expected Result: 编译成功，无错误
    Failure Indicators: 编译错误，trait 定义不完整
    Evidence: .sisyphus/evidence/task-1-compile.txt

  Scenario: 文档验证
    Tool: Bash (cargo doc)
    Preconditions: 所有 trait 都有文档注释
    Steps:
      1. cd kafka-clients-trait
      2. cargo doc --no-deps
    Expected Result: 文档生成成功
    Failure Indicators: 文档生成失败，缺少文档注释
    Evidence: .sisyphus/evidence/task-1-doc.txt
  ```

  **Commit**: NO

---

- [x] 2. 补充 kafka-clients-trait：ConfigDef, Time, Utils 等工具 trait（已完成）

  **What to do**:
  - 在 `kafka-clients-trait/src` 中添加配置相关 trait：
    - `ConfigDef` trait
    - `ConfigValue` struct
    - `ConfigException` error type
    - `AbstractConfig` trait
    - `ConfigDefType` enum
    - `ConfigDefImportance` enum
    - `ConfigProvider` trait
  - 在 `kafka-clients-trait/src` 中添加工具类 trait：
    - `Time` trait
    - `Utils` trait（包含常用工具方法）
    - `Timer` trait
    - `AppInfoParser` trait
    - `ThreadUtils` trait
    - `LogContext` trait
    - `Exit` trait
  - 在 `kafka-clients-trait/src` 中添加 Kafka 核心类型 trait：
    - `TopicPartition` struct
    - `IsolationLevel` enum
    - `KafkaFuture` trait
    - `KafkaException` error type
    - `MetricNameTemplate` trait
  - 在 `kafka-clients-trait/src` 中添加指标相关 trait：
    - `Sensor` trait
    - `PluginMetrics` trait
    - `Max` struct
    - `CumulativeSum` struct
    - `Avg` struct
  - 在 `kafka-clients-trait/src` 中添加序列化 trait：
    - `ByteArraySerializer` trait
    - `ByteArrayDeserializer` trait
  - 在 `kafka-clients-trait/src` 中添加错误类型 trait：
    - `WakeupException` error type
    - `UnsupportedVersionException` error type
    - `GroupIdNotFoundException` error type
    - `GroupNotEmptyException` error type
    - `GroupSubscribedToTopicException` error type
    - `UnknownMemberIdException` error type

  **Must NOT do**:
  - 不要实现具体逻辑，只定义 trait 和 struct
  - 不要添加不相关的 trait

  **Recommended Agent Profile**:
  - **Category**: `deep`
    - Reason: 需要深入分析 Java 代码，理解工具类设计
  - **Skills**: []
  - **Skills Evaluated but Omitted**: []

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential
  - **Blocks**: [Task 3]
  - **Blocked By**: None

  **References**:
  - **Pattern References**:
    - `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/Worker.java:36-100` - ConfigDef, Time, Utils 使用示例
  - **API/Type References**:
    - `org.apache.kafka.common.config.ConfigDef` - ConfigDef 接口定义
    - `org.apache.kafka.common.utils.Time` - Time 接口定义
    - `org.apache.kafka.common.utils.Utils` - Utils 类定义

  **Acceptance Criteria**:
  - [ ] 所有配置相关 trait 都已定义
  - [ ] 所有工具类 trait 都已定义
  - [ ] 所有 Kafka 核心类型 trait 都已定义
  - [ ] 所有指标相关 trait 都已定义
  - [ ] 所有错误类型 trait 都已定义
  - [ ] `cargo check` 通过

  **QA Scenarios (MANDATORY)**:
  ```
  Scenario: 编译验证
    Tool: Bash (cargo check)
    Preconditions: kafka-clients-trait 已有基础结构
    Steps:
      1. cd kafka-clients-trait
      2. cargo check
    Expected Result: 编译成功，无错误
    Failure Indicators: 编译错误，trait 定义不完整
    Evidence: .sisyphus/evidence/task-2-compile.txt

  Scenario: 文档验证
    Tool: Bash (cargo doc)
    Preconditions: 所有 trait 都有文档注释
    Steps:
      1. cd kafka-clients-trait
      2. cargo doc --no-deps
    Expected Result: 文档生成成功
    Failure Indicators: 文档生成失败，缺少文档注释
    Evidence: .sisyphus/evidence/task-2-doc.txt
  ```

  **Commit**: NO

---

- [x] 3. 补充 kafka-clients-trait：CompletableFuture 等效接口（connect/runtime 中未使用，跳过）

  **What to do**:
  - 在 `kafka-clients-trait/src/util` 中添加 `CompletableFuture` 结构：
    - 使用 `tokio::sync::oneshot` 实现
    - 提供 `new()`, `complete()`, `await()`, `then()` 等方法
    - 提供与 Java CompletableFuture 等效的 API
  - 确保泛型参数正确
  - 确保所有方法都有文档注释
  - 确保错误处理正确

  **Must NOT do**:
  - 不要实现复杂的异步编排（只实现基础方法）
  - 不要添加不相关的方法

  **Recommended Agent Profile**:
  - **Category**: `deep`
    - Reason: 需要深入理解 CompletableFuture 语义，正确实现
  - **Skills**: []
  - **Skills Evaluated but Omitted**: []

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential
  - **Blocks**: [Task 4]
  - **Blocked By**: None

  **References**:
  - **Pattern References**:
    - `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/Worker.java` - CompletableFuture 使用示例
  - **External References**:
    - Java CompletableFuture 文档：https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletableFuture.html
    - tokio::sync::oneshot 文档：https://docs.rs/tokio/tokio/sync/oneshot/index.html

  **Acceptance Criteria**:
  - [ ] `CompletableFuture` 结构已定义
  - [ ] `new()` 方法已实现
  - [ ] `complete()` 方法已实现
  - [ ] `await()` 方法已实现
  - [ ] `then()` 方法已实现
  - [ ] 所有方法都有文档注释
  - [ ] `cargo check` 通过

  **QA Scenarios (MANDATORY)**:
  ```
  Scenario: 编译验证
    Tool: Bash (cargo check)
    Preconditions: kafka-clients-trait 已有基础结构
    Steps:
      1. cd kafka-clients-trait
      2. cargo check
    Expected Result: 编译成功，无错误
    Failure Indicators: 编译错误，CompletableFuture 定义不完整
    Evidence: .sisyphus/evidence/task-3-compile.txt

  Scenario: 功能验证（完成和等待）
    Tool: Bash (cargo test)
    Preconditions: CompletableFuture 已实现
    Steps:
      1. cd kafka-clients-trait
      2. cargo test --lib util -- tests test_complete_future
    Expected Result: 测试通过
    Failure Indicators: 测试失败，CompletableFuture 功能不正确
    Evidence: .sisyphus/evidence/task-3-test.txt

  Scenario: 功能验证（then 链式调用）
    Tool: Bash (cargo test)
    Preconditions: CompletableFuture 已实现
    Steps:
      1. cd kafka-clients-trait
      2. cargo test --lib util -- tests test_then_future
    Expected Result: 测试通过
    Failure Indicators: 测试失败，then 方法不正确
    Evidence: .sisyphus/evidence/task-3-then-test.txt
  ```

  **Commit**: NO

---

- [x] 4. 补充 kafka-clients-mock：Producer/Consumer/Admin mock（已完成）

  **What to do**:
  - 在 `kafka-clients-mock/src` 中实现 `MockProducer`：
    - 实现 `Producer` trait
    - 提供内存版的生产者实现
    - 支持发送记录到内存缓冲区
  - 在 `kafka-clients-mock/src` 中实现 `MockConsumer`：
    - 实现 `Consumer` trait
    - 提供内存版的消费者实现
    - 支持从内存缓冲区消费记录
  - 在 `kafka-clients-mock/src` 中实现 `MockAdmin`：
    - 实现 `Admin` trait
    - 提供内存版的管理客户端实现
    - 支持主题管理（创建、删除、描述等）
  - 确保所有 mock 都可以独立运行（不需要真实 Kafka）
  - 确保所有 mock 都有测试

  **Must NOT do**:
  - 不要连接真实 Kafka
  - 不要实现持久化存储（只使用内存）

  **Recommended Agent Profile**:
  - **Category**: `deep`
    - Reason: 需要实现完整的 mock，确保测试可以独立运行
  - **Skills**: []
  - **Skills Evaluated but Omitted**: []

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential
  - **Blocks**: [Task 5]
  - **Blocked By**: [Task 1]

  **References**:
  - **Pattern References**:
    - `kafka-clients-mock/src/producer.rs` - 已有的 mock 实现示例
  - **API/Type References**:
    - `kafka-clients-trait/src/producer.rs` - Producer trait 定义

  **Acceptance Criteria**:
  - [ ] `MockProducer` 已实现
  - [ ] `MockConsumer` 已实现
  - [ ] `MockAdmin` 已实现
  - [ ] 所有 mock 都可以生产者/消费者交互
  - [ ] `cargo check` 通过
  - [ ] `cargo test` 通过

  **QA Scenarios (MANDATORY)**:
  ```
  Scenario: 编译验证
    Tool: Bash (cargo check)
    Preconditions: kafka-clients-mock 已有基础结构
    Steps:
      1. cd kafka-clients-mock
      2. cargo check
    Expected Result: 编译成功，无错误
    Failure Indicators: 编译错误，mock 实现不完整
    Evidence: .sisyphus/evidence/task-4-compile.txt

  Scenario: 测试验证
    Tool: Bash (cargo test)
    Preconditions: 所有 mock 都已实现
    Steps:
           1. cd kafka-clients-mock
      2. cargo test
    Expected Result: 所有测试通过
    Failure Indicators: 测试失败，mock 功能不正确
    Evidence: .sisyphus/evidence/task-4-test.txt

  Scenario: 生产者-消费者交互测试
    Tool: Bash (cargo test)
    Preconditions: MockProducer 和 MockConsumer 已实现
    Steps:
      1. cd kafka-clients-mock
      2. cargo test --tests test_producer_consumer_interaction
    Expected Result: 测试通过，生产者发送的记录可以被消费者消费
    Failure Indicators: 测试失败，生产者-消费者交互不正确
    Evidence: .sisyphus/evidence/task-4-interaction-test.txt
  ```

  **Commit**: NO

---

- [x] 5. 补充 kafka-clients-mock：ConfigDef, Time, Utils mock（已完成）

  **What to do**:
  - 在 `kafka-clients-mock/src` 中实现 `MockConfigDef`：
    - 实现 `ConfigDef` trait
    - 提供内存版的配置定义实现
    - 支持配置项的添加和查询
  - 在 `kafka-clients-mock/src` 中实现 `MockTime`：
    - 实现 `Time` trait
    - 提供可控制的时间实现（支持测试时间控制）
  - 在 `kafka-clients-mock/src` 中实现 `MockUtils`：
    - 实现 `Utils` trait
    - 提供常用工具方法的 mock 实现
  - 确保所有 mock 都可以独立运行
  - 确保所有 mock 都有测试

  **Must NOT do**:
  - 不要实现`Utils` 的所有方法（只实现 connect/runtime 使用的）
  - 不要添加不相关的方法

  **Recommended Agent Profile**:
  - **Category**: `deep`
    - Reason: 需要实现完整的 mock，确保测试可以独立运行
  - **Skills**: []
  - **Skills Evaluated but Omitted**: []

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential
  - **Blocks**: [Task 6]
  - **Blocked By**: [Task 2]

  **References**:
  - **Pattern References**:
    - `kafka-clients-mock/src/config.rs` - 已有的 mock 实现示例
  - **API/Type References**:
    - `kafka-clients-trait/src/config.rs` - ConfigDef trait 定义

  **Acceptance Criteria**:
  - [ ] `MockConfigDef` 已实现
  - [ ] `MockTime` 已实现
  - [ ] `MockUtils` 已实现
  - [ ] `cargo check` 通过
  - [ ] `cargo test` 通过

  **QA Scenarios (MANDATORY)**:
  ```
  Scenario: 编译验证
    Tool: Bash (cargo check)
    Preconditions: kafka-clients-mock 已有基础结构
    Steps:
      1. cd kafka-clients-mock
      2. cargo check
    Expected Result: 编译成功，无错误
    Failure Indicators: 编译错误，mock 实现不完整
    Evidence: .sisyphus/evidence/task-5-compile.txt

  Scenario: 测试验证
    Tool: Bash (cargo test)
    Preconditions: 所有 mock 都已实现
    Steps:
      1. cd kafka-clients-mock
      2. cargo test
    Expected Result: 所有测试通过
    Failure Indicators: 测试失败，mock 功能不正确
    Evidence: .sisyphus/evidence/task-5-test.txt
  ```

  **Commit**: NO

---

- [x] 6. 补充 kafka-clients-mock：CompletableFuture mock（connect/runtime 中未使用，跳过）

  **What to do**:
  - 在 `kafka-clients-mock/src/util` 中实现 `MockCompletableFuture`：
    - 使用 `kafka-clients-trait` 中的 `CompletableFuture`
    - 提供测试友好的实现
    - 支持同步和异步测试
  - 确保所有方法都正确实现
  - 确保有测试覆盖

  **Must NOT do**:
  - 不要重新实现 `CompletableFuture`（直接使用 trait）
  - 不要添加不相关的方法

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: CompletableFuture 已在 kafka-clients-trait 中实现，mock 只需要简单包装
  - **Skills**: []
  - **Skills Evaluated but Omitted**: []

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential
  - **Blocks**: [Task 7]
  - **Blocked By**: [Task 3]

  **References**:
  - **Pattern References**:
    - `kafka-clients-trait/src/util/completable_future.rs` - CompletableFuture 定义
  - **API/Type References**:
    - `kafka-clients-trait/src/util/completable_future.rs` - CompletableFuture trait

  **Acceptance Criteria**:
  - [ ] `MockCompletableFuture` 已实现
  - [ ] `cargo check` 通过
  - [ ] `cargo test` 通过

  **QA Scenarios (MANDATORY)**:
  ```
  Scenario: 编译验证
    Tool: Bash (cargo check)
    Preconditions: kafka-clients-mock 已有基础结构
    Steps:
      1. cd kafka-clients-mock
      2. cargo check
    Expected Result: 编译成功，无错误
    Failure Indicators: 编译错误，mock 实现不完整
    Evidence: .sisyphus/evidence/task-6-compile.txt

  Scenario: 测试验证
    Tool: Bash (cargo test)
    Preconditions: MockCompletableFuture 已实现
    Steps:
      1. cd kafka-clients-mock
      2. cargo test --lib util
    Expected Result: 所有测试通过
    Failure Indicators: 测试失败，mock 功能不正确
    Evidence: .sisyphus/evidence/task-6-test.txt
  ```

  **Commit**: NO

---

## Final Verification Wave (MANDATORY — after ALL implementation tasks)

> 4 review agents run in PARALLEL. ALL must APPROVE. Present consolidated results to user and get explicit "okay" before completing.
>
> **Do NOT auto-proceed after verification. Wait for user's explicit approval before marking work complete.**
> **Never mark F1-F4 as checked before getting user's okay.** Rejection or user feedback -> fix -> re-run -> present again -> wait for okay.

- [ ] F1. **完整性度量**（代码结构对比）— `deep`
  对比 Java 和 Rust 代码结构，验证完整性：
  - Java 文件数（191）≈ Rust 文件数（±10%）
  - Java class/interface/enum 数量（147）≈ Rust struct/trait/enum 数量（±10%）
  - Java 方法数（1357）≈ Rust 函数数（±10%）
  - Java 代码行数（40,426）≈ Rust 代码行数（±20%）
  - 无 TODO 或空实现
  输出：`Files [N/N] | Types [N/N] | Methods [N/N] | Lines [N/N] | VERDICT: APPROVE/REJECT`

- [ ] F2. **正确性度量**（编译和测试）— `deep`
  运行编译和测试，验证正确性：
  - `cargo check` 通过（无编译错误）
  - `cargo clippy -- -D warnings` 通过（无警告）
  - `cargo fmt --check` 通过（代码格式正确）
  - `cargo test` 全部通过
  - 测试覆盖率 ≥ 80%
  输出：`Build [PASS/FAIL] | Lint [PASS/FAIL] | Tests [N pass/N fail] | Coverage [N%] | VERDICT`

- [ ] F3. **功能验证**（端到端测试）— `deep`
  运行端到端测试，验证功能：
  - Worker 可以启动和停止连接器
  - SourceTask 可以发送记录到 Kafka
  - SinkTask 可以从 Kafka 消费记录
  - 分布式协调正常工作
  - REST API 响应正确
  - 错误处理和重试机制正常
  输出：`Worker [PASS/FAIL] | SourceTask [PASS/FAIL] | SinkTask [PASS/FAIL] | Distributed [PASS/FAIL] | REST [PASS/FAIL] | ErrorHandling [PASS/FAIL] | VERDICT`

- [ ] F4. **生成最终报告**— `deep`
  生成完整的迁移报告，包括：
  - 完整性度量结果
  - 正确性度量结果
  - 功能验证结果
  - 迁移统计（文件数、代码行数、测试覆盖率）
  - 技术方案总结
  输出：保存到 `connect-runtime-migration-report.md`

---

## Commit Strategy

- **Wave 1**: `feat(kafka-clients-trait): add Producer/Consumer/Admin traits` — kafka-clients-trait/src/*.rs
- **Wave 1**: `feat(kafka-clients-trait): add ConfigDef, Time, Utils traits` — kafka-clients-trait/src/*.rs
- **Wave 1**: `feat(kafka-clients-trait): add CompletableFuture trait` — kafka-clients-trait/src/util/*.rs
- **Wave 1**: `feat(kafka-clients-mock): add Producer/Consumer/Admin mocks` — kafka-clients-mock/src/*.rs
- **Wave 1**: `feat(kafka-clients-mock): add ConfigDef, Time, Utils mocks` — kafka-clients-mock/src/*.rs
- **Wave 1**: `feat(kafka-clients-mock): add CompletableFuture mock` — kafka-clients-mock/src/util/*.rs
- **Wave 2**: `feat(connect-runtime): add util, errors, converters` — connect-runtime/src/util/*.rs, errors/*.rs, converters/*.rs
- **Wave 3**: `feat(connect-runtime): add storage layer` — connect-runtime/src/storage/*.rs
- **Wave 4**: `feat(connect-runtime): add plugin system with compile-time macros` — connect-runtime/src/isolation/*.rs
- **Wave 5**: `feat(connect-runtime): add Worker, Herder, CLI` — connect-runtime/src/runtime/*.rs, standalone/*.rs, cli/*.rs
- **Wave 6**: `feat(connect-runtime): add REST API with actix-web` — connect-runtime/src/rest/*.rs
- **Wave 7**: `feat(connect-runtime): add tests` — connect-runtime/tests/*.rs
- **Final**: `docs(connect-runtime): add migration report` — connect-runtime-migration-report.md

---

## Success Criteria

### Verification Commands
```bash
# 编译检查
cd connect-rust-new && cargo check

# 代码质量检查
cd connect-rust-new && cargo clippy -- -D warnings

# 格式检查
cd connect-rust-new && cargo fmt --check

# 测试执行
cd connect-rust-new && cargo test

# 测试覆盖率（需要安装 tarpaulin）
cd connect-rust-new && cargo tarpaulin --out Html
```

### Final Checklist
- [ ] 所有 Java 文件都有对应的 Rust 文件（±10%）
- [ ] 所有 class/interface/enum 都有对应的 struct/trait/enum（±10%）
- [ ] 所有方法都有对应的函数（±10%）
- [ ] 代码行数相当（±20%）
- [ ] 无 TODO 或空实现
- [ ] `cargo check` 通过
- [ ] `cargo clippy` 通过
- [ ] `cargo test` 全部通过
- [ ] 测试覆盖率 ≥ 80%
- [ ] 功能验证通过

---

## 附录：详细任务清单

由于任务数量较多（约 170+ 个），本计划只列出了主要的任务框架。完整迁移时，每个任务都需要按照以下模板详细展开：

### 任务模板

对于每个 Java 文件的迁移，都需要：

1. **阅读 Java 文件**：理解代码逻辑和设计
2. **创建 Rust 文件**：按照命名规范（驼峰转蛇形）
3. **翻译代码**：
   - class → struct 或 trait
   - interface → trait
   - enum → enum
   - 方法 → 函数（驼峰转蛇形）
   - 异常 → Result<T, E>
   - synchronized → Arc<Mutex<T>>
   - volatile → AtomicUsize/AtomicBool
4. **编写测试**：1:1 迁移 Java 测试
5. **验证编译**：`cargo check`
6. **验证测试**：`cargo test`

### 命名规范

- Java 类名 → Rust struct/trait 名（驼峰转蛇形）
  - `Worker.java` → `worker.rs`
  - `AbstractHerder.java` → `abstract_herder.rs`
  - `DistributedHerder.java` → `distributed_herder.rs`
- Java 方法名 → Rust 函数名（驼峰转蛇形）
  - `startConnector()` → `start_connector()`
  - `stopConnector()` → `stop_connector()`
  - `getConnectorStatus()` → `get_connector_status()`

### 并发原语映射

| Java | Rust |
|------|------|
| `synchronized` | `Arc<Mutex<T>>` |
| `volatile` | `AtomicUsize`, `AtomicBool` |
| `ExecutorService` | `tokio::task::spawn` |
| `CompletableFuture` | `kafka_clients_trait::util::CompletableFuture` |
| `CountDownLatch` | `tokio::sync::Semaphore` |
| `wait/notify` | `tokio::sync::Notify` |
| `ConcurrentHashMap` | `dashmap::DashMap` |
| `LinkedHashMap` | `indexmap::IndexMap` |
| `TreeMap` | `BTreeMap` |

### 集合映射

| Java | Rust |
|------|------|
| `Map<K, V>` | `std::collections::HashMap<K, V>` |
| `List<T>` | `Vec<T>` |
| `Set<T>` | `std::collections::HashSet<T>` |
| `Optional<T>` | `Option<T>` |

---

**计划生成时间**：2026-04-11
**计划版本**：1.0
