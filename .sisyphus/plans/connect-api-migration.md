# Kafka Connect API 模块迁移工作计划

## TL;DR

> **Quick Summary**: 将 Kafka `connect/api` 模块从 Java 1:1 迁移到 Rust `connect-rust/connect-api`，保证目录结构、类名、函数名一致，所有测试迁移并通过编译验证。
>
> **Deliverables**:
> - 57 个 Rust 源文件（struct/trait ≥ 50）
> - 22 个测试文件（#[test] ≥ 150）
> - common-trait 新增: KafkaHeaders trait + TimestampType enum
> - kafka-clients-mock 新增: RecordMetadata + OffsetAndMetadata structs
>
> **Estimated Effort**: Large (57 files, ~3000+ lines)
> **Parallel Execution**: YES - 6 waves + final verification
> **Critical Path**: Wave 1 → Wave 2 → Wave 3 → Wave 7 → Final

---

## Context

### Original Request
将 `connect/api` 模块 1:1 迁移到 `connect-rust/connect-api` 目录，保证：
- 目录、类名、函数名严格一致
- 功能完整，无遗漏（禁止 TODO/空实现）
- 编译通过，测试通过
- 对 kafka-common/kafka-clients 的依赖在 common-trait/kafka-clients-mock 中实现

### Interview Summary
**Key Discussions**:
- **taskClass() 返回类型**: TaskType enum (Source, Sink, Custom)
- **Map<String, ?> 翻译**: serde_json::Value
- **getter 方法**: 简化或移除，直接 pub 属性访问
- **Date/Time/Timestamp**:  使用chrono库
- **Headers 分离**: kafka.Headers → common-trait, connect.Headers → connect-api
- **异常体系**: ConnectError enum 分组，无基类
- **静态常量**: once_cell::Lazy (新增 once_cell 依赖)

**Research Findings**:

- Java connect/api: 57 源文件 + 22 测试文件
- 核心类: Schema (219行), ConnectSchema (367行), SchemaBuilder (444行), Struct (286行), Connector (154行)
- 测试覆盖: SchemaBuilderTest (380行, 30+ tests), StructTest (326行, 20+ tests)

### Metis Review
**Identified Gaps** (addressed in guardrails):
- 未验证 Rust edition 和 MSRV → Guardrail: 使用 Rust 2021 edition, MSRV 1.70+
- 未验证具体编译验证命令 → Guardrail: cargo build + cargo clippy + cargo fmt
- serde_json::Value 边界情况 → Guardrail: 处理 null/空集合/嵌套结构
- 异常链式结构 → Guardrail: ConnectError enum 覆盖所有 Java 异常类型

---

## Work Objectives

### Core Objective
将 connect/api 模块完整迁移到 connect-rust/connect-api，实现 Java→Rust 的 1:1 代码翻译。

### Concrete Deliverables
- `connect-rust/connect-api/src/` 下 57 个 .rs 文件
- `connect-rust/connect-api/tests/` 下 22 个 *_test.rs 文件
- `common-trait/src/kafka_headers.rs` (KafkaHeaders trait)
- `common-trait/src/timestamp_type.rs` (TimestampType enum)
- `kafka-clients-mock/src/producer.rs` (RecordMetadata struct)
- `kafka-clients-mock/src/consumer.rs` (OffsetAndMetadata struct)

### Definition of Done
- [ ] cargo build --release 成功，无错误
- [ ] cargo clippy 无 warnings
- [ ] cargo fmt 格式检查通过
- [ ] cargo test 全部通过 (150+ #[test])
- [ ] struct/trait 数量 ≥ 50
- [ ] 无 TODO 或空实现

### Must Have
- 目录结构与 Java 包结构 1:1 对应
- 所有 Java 类名映射到 Rust struct/trait
- 所有 Java 方法名映射到 Rust 函数名
- 所有 @Test 测试功能点迁移

### Must NOT Have (Guardrails from Metis)
- 不迁移 connect/api 外的其他模块（禁止 connect/runtime, connect/json 等）
- 不添加 Java 源码中不存在的新功能
- 不重构 Java 代码后再迁移
- 不修改公共 API 签名
- 不引入未经确认的外部依赖
- 不使用 unsafe Rust（除非必须且文档说明）
- 不进行性能优化（除非必要）
- 不添加额外文档（仅必要的 API 说明）
- 不添加额外测试（仅迁移 Java 测试）

---

## Verification Strategy (MANDATORY)

> **ZERO HUMAN INTERVENTION** - ALL verification is agent-executed. No exceptions.

### Test Decision
- **Infrastructure exists**: YES (cargo test)
- **Automated tests**: YES (迁移 Java UT)
- **Framework**: cargo test (Rust builtin)
- **Strategy**: Tests-after (先实现，后迁移测试)

### QA Policy
Every task MUST include agent-executed QA scenarios:
- **Frontend/UI**: 无 (纯 Rust 库)
- **TUI/CLI**: cargo 命令验证
- **API/Backend**: cargo test, cargo clippy
- **Library/Module**: cargo build, cargo check

---

## Execution Strategy

### Parallel Execution Waves

```
Wave 1 (基础设施 - 开始):
├── Task 1: common-trait 新增 KafkaHeaders trait [quick]
├── Task 2: common-trait 新增 TimestampType enum [quick]
├── Task 3: kafka-clients-mock 新增 RecordMetadata [quick]
├── Task 4: kafka-clients-mock 新增 OffsetAndMetadata [quick]
├── Task 5: connect-api Cargo.toml 配置 + once_cell 依赖 [quick]
└── Task 6: connect-api lib.rs 模块入口骨架 [quick]

Wave 2 (核心 data 模块 - MAX PARALLEL):
├── Task 7: data/schema.rs (Schema trait + SchemaType enum + 静态常量) [deep]
├── Task 8: data/schema_builder.rs (SchemaBuilder struct) [deep]
├── Task 9: data/connect_schema.rs (ConnectSchema struct) [deep]
├── Task 10: data/field.rs (Field struct) [quick]
├── Task 11: data/struct.rs (Struct struct) [deep]
├── Task 12: data/schema_and_value.rs (SchemaAndValue struct) [quick]
└── Task 13: data/logical_types.rs (Date/Time/Timestamp/Decimal 常量) [quick]

Wave 3 (connector 核心 - MAX PARALLEL):
├── Task 14: components/connect_plugin.rs (ConnectPlugin trait) [quick]
├── Task 15: components/versioned.rs (Versioned trait) [quick]
├── Task 16: connector/task.rs + task_type.rs (Task trait + TaskType enum) [deep]
├── Task 17: connector/connector.rs (Connector trait) [deep]
├── Task 18: connector/connector_context.rs (ConnectorContext trait) [quick]
├── Task 19: connector/connect_record.rs (ConnectRecord trait) [deep]
└── Task 20: connector/policy 模块 (2 files) [quick]

Wave 4 (errors + header + health - PARALLEL):
├── Task 21: errors/connect_error.rs (ConnectError enum + 各异常 struct) [deep]
├── Task 22: header 模块 (Header, Headers, ConnectHeader, ConnectHeaders) [deep]
├── Task 23: health 模块 (6 files) [unspecified-high]
└── Task 24: data/values.rs + data/schema_projector.rs [deep]

Wave 5 (sink + source 模块 - MAX PARALLEL):
├── Task 25: sink/sink_connector.rs (SinkConnector trait) [deep]
├── Task 26: sink/sink_task.rs (SinkTask trait) [deep]
├── Task 27: sink/sink_record.rs (SinkRecord struct) [deep]
├── Task 28: sink/sink_task_context.rs + sink_connector_context.rs [quick]
├── Task 29: source/source_connector.rs (SourceConnector trait) [deep]
├── Task 30: source/source_task.rs (SourceTask trait) [deep]
├── Task 31: source/source_record.rs (SourceRecord struct) [deep]
└── Task 32: source 其他文件 (5 files) [unspecified-high]

Wave 6 (storage + transforms + rest + util - PARALLEL):
├── Task 33: storage/converter.rs (Converter trait) [deep]
├── Task 34: storage 其他文件 (6 files) [unspecified-high]
├── Task 35: transforms 模块 (Transformation + Predicate) [quick]
├── Task 36: rest 模块 (2 files) [quick]
└── Task 37: util/connector_utils.rs [quick]

Wave 7 (测试迁移 - MAX PARALLEL):
├── Task 38: data 测试迁移 (11 test files) [unspecified-high]
├── Task 39: connector 测试迁移 (2 test files) [quick]
├── Task 40: header 测试迁移 (2 test files) [quick]
├── Task 41: sink 测试迁移 (2 test files) [quick]
├── Task 42: source 测试迁移 (2 test files) [quick]
├── Task 43: storage 测试迁移 (3 test files) [quick]
└── Task 44: util 测试迁移 (1 test file) [quick]

Wave FINAL (验证 - 4 parallel reviews):
├── Task F1: Plan compliance audit (oracle) - 验证 Must Have/Must NOT Have
├── Task F2: Code quality review (unspecified-high) - cargo clippy + cargo fmt
├── Task F3: Test execution QA (unspecified-high) - cargo test 全量执行
└── Task F4: Scope fidelity check (deep) - 文件数量/API数量验证
-> Present results -> Get explicit user okay
```

### Dependency Matrix (abbreviated)

- **Wave 1**: 无依赖，可立即开始
- **Wave 2**: 依赖 Wave 1 (Cargo.toml, lib.rs)
- **Wave 3**: 依赖 Wave 2 (Schema, Struct)
- **Wave 4**: 依赖 Wave 2, Wave 3
- **Wave 5**: 依赖 Wave 3 (Connector, Task)
- **Wave 6**: 依赖 Wave 2, Wave 3
- **Wave 7**: 依赖所有实现 Waves
- **Final**: 依赖所有 Waves

### Agent Dispatch Summary
- **Wave 1**: 6 tasks → `quick` (6)
- **Wave 2**: 7 tasks → `deep` (4), `quick` (3)
- **Wave 3**: 7 tasks → `deep` (4), `quick` (3)
- **Wave 4**: 4 tasks → `deep` (2), `unspecified-high` (1), `quick` (1)
- **Wave 5**: 8 tasks → `deep` (4), `unspecified-high` (1), `quick` (3)
- **Wave 6**: 5 tasks → `deep` (1), `unspecified-high` (1), `quick` (3)
- **Wave 7**: 7 tasks → `unspecified-high` (1), `quick` (6)
- **Final**: 4 tasks → `oracle` (1), `unspecified-high` (2), `deep` (1)

---

## TODOs

### Wave 1: 基础设施 (6 tasks, 立即开始)

- [ ] 1. **common-trait 新增 KafkaHeaders trait**

  **What to do**:
  - 在 `common-trait/src/kafka_headers.rs` 创建 KafkaHeaders trait
  - 对应 Java: `org.apache.kafka.common.header.Headers`
  - 方法: add(key, value), get(key), lastWithName(key), remove(key)
  
  **Must NOT do**:
  - 不实现 Connect Headers (那是 connect-api 的 Headers)
  
  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 2-6)
  - **Blocks**: Wave 2+ 所有依赖 common-trait 的任务
  
  **References**:
  - `common-trait/src/connect.rs` - 已有的 trait 定义模式
  - Java: `clients/src/main/java/org/apache/kafka/common/header/Headers.java`
  
  **Acceptance Criteria**:
  - [ ] KafkaHeaders trait 定义完成
  - [ ] common-trait/src/lib.rs 导出 KafkaHeaders
  
  **QA Scenarios**:
  ```
  Scenario: KafkaHeaders trait 编译通过
    Tool: Bash (cargo check)
    Steps:
      1. cd connect-rust && cargo check -p common-trait
    Expected Result: Compiling common-trait... Finished
    Evidence: .sisyphus/evidence/task-01-cargo-check.log
  ```

  **Commit**: YES (Group 1: 基础设施)
  - Message: `feat(common-trait): add KafkaHeaders trait`
  - Files: `common-trait/src/kafka_headers.rs`, `common-trait/src/lib.rs`

- [ ] 2. **common-trait 新增 TimestampType enum**

  **What to do**:
  - 在 `common-trait/src/timestamp_type.rs` 创建 TimestampType enum
  - 对应 Java: `org.apache.kafka.common.record.TimestampType`
  - 值: NoTimestampType, CreateTime, LogAppendTime
  
  **Must NOT do**:
  - 不添加额外方法
  
  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1
  
  **References**:
  - Java: `clients/src/main/java/org/apache/kafka/common/record/TimestampType.java`
  
  **Acceptance Criteria**:
  - [ ] TimestampType enum 定义完成
  - [ ] lib.rs 导出 TimestampType
  
  **QA Scenarios**:
  ```
  Scenario: TimestampType 编译通过
    Tool: Bash (cargo check)
    Steps:
      1. cargo check -p common-trait
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-02-cargo-check.log
  ```

  **Commit**: YES (Group 1)

- [ ] 3. **kafka-clients-mock 新增 RecordMetadata struct**

  **What to do**:
  - 在 `kafka-clients-mock/src/producer.rs` 创建 RecordMetadata struct
  - 对应 Java: `org.apache.kafka.clients.producer.RecordMetadata`
  - 字段: topic, partition, offset, timestamp, serialized_key_size, serialized_value_size
  
  **Must NOT do**:
  - 不实现 Kafka Producer 完整功能
  
  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1
  
  **References**:
  - Java: `clients/src/main/java/org/apache/kafka/clients/producer/RecordMetadata.java`
  - `common-trait/src/connect.rs:RecordMetadata` - 已有定义可参考
  
  **Acceptance Criteria**:
  - [ ] RecordMetadata struct 定义完成
  - [ ] lib.rs 导出 RecordMetadata
  
  **QA Scenarios**:
  ```
  Scenario: RecordMetadata 编译通过
    Tool: Bash (cargo check)
    Steps:
      1. cargo check -p kafka-clients-mock
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-03-cargo-check.log
  ```

  **Commit**: YES (Group 1)

- [ ] 4. **kafka-clients-mock 新增 OffsetAndMetadata struct**

  **What to do**:
  - 在 `kafka-clients-mock/src/consumer.rs` 创建 OffsetAndMetadata struct
  - 对应 Java: `org.apache.kafka.clients.consumer.OffsetAndMetadata`
  - 字段: offset, metadata, leader_epoch
  
  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1
  
  **References**:
  - Java: `clients/src/main/java/org/apache/kafka/clients/consumer/OffsetAndMetadata.java`
  
  **Acceptance Criteria**:
  - [ ] OffsetAndMetadata struct 定义完成
  - [ ] lib.rs 导出
  
  **QA Scenarios**:
  ```
  Scenario: OffsetAndMetadata 编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-04-cargo-check.log
  ```

  **Commit**: YES (Group 1)

- [ ] 5. **connect-api Cargo.toml 配置 + once_cell 依赖**

  **What to do**:
  - 更新 `connect-rust/connect-api/Cargo.toml`
  - 添加依赖: once_cell = "1.18"
  - 添加路径依赖: common-trait, kafka-clients-mock
  
  **Must NOT do**:
  - 不添加未确认的依赖
  
  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1
  
  **References**:
  - `connect-rust/Cargo.toml` - workspace 配置
  - `connect-rust/connect-api/Cargo.toml` - 当前配置
  
  **Acceptance Criteria**:
  - [ ] Cargo.toml 包含 once_cell 依赖
  - [ ] Cargo.toml 包含 common-trait 路径依赖
  - [ ] Cargo.toml 包含 kafka-clients-mock 路径依赖
  
  **QA Scenarios**:
  ```
  Scenario: Cargo.toml 有效
    Tool: Bash (cargo check)
    Steps:
      1. cargo check -p connect-api
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-05-cargo-check.log
  ```

  **Commit**: YES (Group 1)

- [ ] 6. **connect-api lib.rs 模块入口骨架**

  **What to do**:
  - 创建 `connect-rust/connect-api/src/lib.rs`
  - 定义模块结构: components, connector, data, errors, header, health, rest, sink, source, storage, transforms, util
  - 添加模块级文档
  
  **Must NOT do**:
  - 不创建具体实现文件
  
  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1
  - **Blocks**: 所有 Wave 2+ 任务
  
  **References**:
  - Java 包结构: `org.apache.kafka.connect.*`
  
  **Acceptance Criteria**:
  - [ ] lib.rs 创建完成
  - [ ] 所有子模块声明存在 (pub mod xxx)
  
  **QA Scenarios**:
  ```
  Scenario: lib.rs 编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished (可能有 module not found 警告)
    Evidence: .sisyphus/evidence/task-06-cargo-check.log
  ```

  **Commit**: YES (Group 1)

---

### Wave 2: 核心 data 模块 (7 tasks, MAX PARALLEL)

- [ ] 7. **data/schema.rs (Schema trait + SchemaType enum + 静态常量)**

  **What to do**:
  - 创建 `connect-api/src/data/schema.rs`
  - 定义 `SchemaType` enum: Int8, Int16, Int32, Int64, Float32, Float64, Boolean, String, Bytes, Array, Map, Struct
  - 定义 `Schema` trait: type(), is_optional(), default_value(), name(), version(), doc(), fields(), field(), keySchema(), valueSchema()
  - 使用 `once_cell::Lazy` 定义静态常量: INT8_SCHEMA, INT16_SCHEMA 等
  - 实现 `isPrimitive()` 方法
  
  **Must NOT do**:
  - 不在 trait 内定义 enum (Rust不支持)
  - 不使用 chrono 或其他未确认依赖
  
  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES (with Tasks 8-13)
  - **Parallel Group**: Wave 2
  - **Blocked By**: Wave 1 (Cargo.toml, lib.rs)
  - **Blocks**: Wave 3+ 所有使用 Schema 的任务
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/data/Schema.java` (219行)
  - `once_cell` crate 文档: Lazy<T> 用法
  
  **Acceptance Criteria**:
  - [ ] SchemaType enum 定义完成，包含所有类型
  - [ ] Schema trait 定义完成，方法签名与 Java 一致
  - [ ] 静态常量使用 once_cell::Lazy 定义
  - [ ] data/mod.rs 导出 Schema 和 SchemaType
  
  **QA Scenarios**:
  ```
  Scenario: Schema 编译通过
    Tool: Bash (cargo check)
    Steps:
      1. cargo check -p connect-api
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-07-cargo-check.log
    
  Scenario: SchemaType isPrimitive 正确
    Tool: Bash (cargo test)
    Steps:
      1. 创建临时测试验证 isPrimitive()
    Expected Result: Int8.is_primitive() == true
    Evidence: .sisyphus/evidence/task-07-test.log
  ```

  **Commit**: YES (Group 2: 核心 data)

- [ ] 8. **data/schema_builder.rs (SchemaBuilder struct)**

  **What to do**:
  - 创建 `connect-api/src/data/schema_builder.rs`
  - 定义 `SchemaBuilder` struct 实现 Builder 模式
  - 实现 Schema trait 的所有方法
  - 实现 fluent API: optional(), required(), defaultValue(), name(), version(), doc(), parameter(), field()
  - 实现 static 工厂方法: int8(), int16(), int32(), int64(), float32(), float64(), bool(), string(), bytes(), struct(), array(), map()
  - 实现 `build()` 方法返回 ConnectSchema
  - 实现 checkCanSet() 验证逻辑
  
  **Must NOT do**:
  - 不改变 Builder API 签名
  - 不添加额外校验逻辑
  
  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2
  - **Blocked By**: Task 7 (Schema trait)
  - **Blocks**: Task 9 (ConnectSchema)
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/data/SchemaBuilder.java` (444行)
  - Rust Builder 模式标准写法
  
  **Acceptance Criteria**:
  - [ ] SchemaBuilder struct 定义完成
  - [ ] 所有 fluent API 方法实现
  - [ ] 所有 static 工厂方法实现
  - [ ] build() 返回 ConnectSchema
  
  **QA Scenarios**:
  ```
  Scenario: SchemaBuilder fluent API 编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-08-cargo-check.log
  ```

  **Commit**: YES (Group 2)

- [ ] 9. **data/connect_schema.rs (ConnectSchema struct)**

  **What to do**:
  - 创建 `connect-api/src/data/connect_schema.rs`
  - 定义 `ConnectSchema` struct 实现 Schema trait
  - 字段: type, optional, default_value, fields, fields_by_name, key_schema, value_schema, name, version, doc, parameters
  - 实现 `validateValue()` 静态方法
  - 实现 `schemaType()` 静态方法
  - 实现 equals/hashCode
  
  **Must NOT do**:
  - 不改变 Schema 接口实现
  
  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES (but depends on Task 7-8)
  - **Blocked By**: Task 7, Task 8
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/data/ConnectSchema.java` (367行)
  
  **Acceptance Criteria**:
  - [ ] ConnectSchema struct 定义完成
  - [ ] Schema trait 实现完成
  - [ ] validateValue 静态方法实现
  
  **QA Scenarios**:
  ```
  Scenario: ConnectSchema 编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-09-cargo-check.log
  ```

  **Commit**: YES (Group 2)

- [ ] 10. **data/field.rs (Field struct)**

  **What to do**:
  - 创建 `connect-api/src/data/field.rs`
  - 定义 `Field` struct: pub name, pub index, pub schema
  - 实现构造函数 new(name, index, schema)
  - 实现 equals/hashCode
  
  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Blocked By**: Task 7 (Schema trait)
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/data/Field.java`
  
  **Acceptance Criteria**:
  - [ ] Field struct 定义完成
  - [ ] 字段 pub 可访问
  
  **QA Scenarios**:
  ```
  Scenario: Field 编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-10-cargo-check.log
  ```

  **Commit**: YES (Group 2)

- [ ] 11. **data/struct.rs (Struct struct)**

  **What to do**:
  - 创建 `connect-api/src/data/struct.rs`
  - 定义 `Struct` struct: pub schema, values (Vec<serde_json::Value>)
  - 实现 put(fieldName, value) fluent API
  - 实现 get(fieldName) 返回 Value
  - 实现 validate() 方法
  - **简化**: 不实现 getInt8/getInt16 等多个 getter，用户自行处理类型
  - 实现 equals/hashCode
  
  **Must NOT do**:
  - 不实现 getInt8(), getInt16() 等多个 getter 方法
  - 不添加额外验证逻辑
  
  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Blocked By**: Task 7, Task 10
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/data/Struct.java` (286行)
  - 用户确认: getter 方法简化，直接 pub 属性访问
  
  **Acceptance Criteria**:
  - [ ] Struct struct 定义完成
  - [ ] put() fluent API 实现
  - [ ] get() 方法实现
  - [ ] validate() 方法实现
  
  **QA Scenarios**:
  ```
  Scenario: Struct 编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-11-cargo-check.log
    
  Scenario: Struct put fluent API 工作
    Tool: Bash (cargo test)
    Steps:
      1. 创建临时测试验证链式调用
    Expected Result: struct.put("a", v1).put("b", v2) 编译通过
    Evidence: .sisyphus/evidence/task-11-test.log
  ```

  **Commit**: YES (Group 2)

- [ ] 12. **data/schema_and_value.rs (SchemaAndValue struct)**

  **What to do**:
  - 创建 `connect-api/src/data/schema_and_value.rs`
  - 定义 `SchemaAndValue` struct: pub schema, pub value
  - 简单包装类型
  
  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Blocked By**: Task 7
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/data/SchemaAndValue.java`
  
  **Acceptance Criteria**:
  - [ ] SchemaAndValue struct 定义完成
  
  **QA Scenarios**:
  ```
  Scenario: SchemaAndValue 编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-12-cargo-check.log
  ```

  **Commit**: YES (Group 2)

- [ ] 13. **data/logical_types.rs (Date/Time/Timestamp/Decimal 常量)**

  **What to do**:
  - 创建 `connect-api/src/data/logical_types.rs`
  - 定义常量字符串 (无 struct):
    - DATE_LOGICAL_NAME = "org.apache.kafka.connect.data.Date"
    - TIME_LOGICAL_NAME = "org.apache.kafka.connect.data.Time"
    - TIMESTAMP_LOGICAL_NAME = "org.apache.kafka.connect.data.Timestamp"
    - DECIMAL_LOGICAL_NAME = "org.apache.kafka.connect.data.Decimal"
  - Date/Time/Timestamp 不封装为 struct，直接使用 i32/i64
  - Decimal 使用 Vec<u8>
  
  **Must NOT do**:
  - 不创建 Date/Time/Timestamp struct 包装
  - 不使用 chrono 库
  
  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Blocked By**: 无
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/data/Date.java`
  - 用户确认: Date/Time/Timestamp 不需要 struct，直接数值类型
  
  **Acceptance Criteria**:
  - [ ] 常量字符串定义完成
  - [ ] 无 Date/Time/Timestamp struct
  
  **QA Scenarios**:
  ```
  Scenario: logical_types 编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-13-cargo-check.log
  ```

  **Commit**: YES (Group 2)

---

### Wave 3: connector 核心 (7 tasks, MAX PARALLEL)

- [ ] 14. **components/connect_plugin.rs (ConnectPlugin trait)**

  **What to do**:
  - 创建 `connect-api/src/components/connect_plugin.rs`
  - 定义 `ConnectPlugin` trait
  - 方法: version() -> &str, config() -> ConfigDef
  
  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Blocked By**: Wave 1, Task 7
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/components/ConnectPlugin.java`
  - `common-trait/src/config.rs` - ConfigDef 定义
  
  **Acceptance Criteria**:
  - [ ] ConnectPlugin trait 定义完成
  
  **QA Scenarios**:
  ```
  Scenario: ConnectPlugin 编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-14-cargo-check.log
  ```

  **Commit**: YES (Group 3: connector)

- [ ] 15. **components/versioned.rs (Versioned trait)**

  **What to do**:
  - 创建 `connect-api/src/components/versioned.rs`
  - 定义 `Versioned` trait
  - 方法: version() -> &str
  
  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/components/Versioned.java`
  
  **Acceptance Criteria**:
  - [ ] Versioned trait 定义完成
  
  **QA Scenarios**:
  ```
  Scenario: Versioned 编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-15-cargo-check.log
  ```

  **Commit**: YES (Group 3)

- [ ] 16. **connector/task.rs + task_type.rs (Task trait + TaskType enum)**

  **What to do**:
  - 创建 `connect-api/src/connector/task.rs`
  - 定义 `Task` trait: version(), start(props), stop()
  - 创建 `connect-api/src/connector/task_type.rs`
  - 定义 `TaskType` enum: Source, Sink, Custom(&'static str)
  
  **Must NOT do**:
  - 不使用 Class<?> 或泛型
  
  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Blocked By**: Wave 1, Task 14
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/connector/Task.java` (52行)
  - 用户确认: taskClass() → TaskType enum
  
  **Acceptance Criteria**:
  - [ ] Task trait 定义完成
  - [ ] TaskType enum 定义完成
  
  **QA Scenarios**:
  ```
  Scenario: Task trait 编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-16-cargo-check.log
  ```

  **Commit**: YES (Group 3)

- [ ] 17. **connector/connector.rs (Connector trait)**

  **What to do**:
  - 创建 `connect-api/src/connector/connector.rs`
  - 定义 `Connector` trait: ConnectPlugin + Versioned
  - 方法: initialize(ctx), start(props), stop(), task_class() -> TaskType, task_configs(max_tasks), validate(configs), config() -> ConfigDef
  - 默认实现: reconfigure(), validate()
  
  **Must NOT do**:
  - 不改变方法签名
  - task_class() 返回 TaskType enum (不是 Class<?>)
  
  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Blocked By**: Task 14, Task 16
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/connector/Connector.java` (154行)
  
  **Acceptance Criteria**:
  - [ ] Connector trait 定义完成
  - [ ] trait 默认实现 reconfigure, validate
  - [ ] task_class() 返回 TaskType
  
  **QA Scenarios**:
  ```
  Scenario: Connector trait 编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-17-cargo-check.log
  ```

  **Commit**: YES (Group 3)

- [ ] 18. **connector/connector_context.rs (ConnectorContext trait)**

  **What to do**:
  - 创建 `connect-api/src/connector/connector_context.rs`
  - 定义 `ConnectorContext` trait
  - 方法: requestTaskReconfiguration(), raiseError(err)
  
  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Blocked By**: Task 21 (ConnectError)
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/connector/ConnectorContext.java`
  
  **Acceptance Criteria**:
  - [ ] ConnectorContext trait 定义完成
  
  **QA Scenarios**:
  ```
  Scenario: ConnectorContext 编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-18-cargo-check.log
  ```

  **Commit**: YES (Group 3)

- [ ] 19. **connector/connect_record.rs (ConnectRecord trait)**

  **What to do**:
  - 创建 `connect-api/src/connector/connect_record.rs`
  - 定义 `ConnectRecord` trait
  - 方法: topic(), kafkaPartition(), keySchema(), key(), valueSchema(), value(), timestamp(), headers()
  - 方法: newRecord(topic, partition, keySchema, key, valueSchema, value, timestamp) -> Self
  - 方法: newRecord with headers
  
  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Blocked By**: Task 7 (Schema), Task 22 (Headers)
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/connector/ConnectRecord.java`
  
  **Acceptance Criteria**:
  - [ ] ConnectRecord trait 定义完成
  
  **QA Scenarios**:
  ```
  Scenario: ConnectRecord 编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-19-cargo-check.log
  ```

  **Commit**: YES (Group 3)

- [ ] 20. **connector/policy 模块 (2 files)**

  **What to do**:
  - 创建 `connect-api/src/connector/policy/mod.rs`
  - 创建 `connector_client_config_override_policy.rs`: trait ConnectorClientConfigOverridePolicy
  - 创建 `connector_client_config_request.rs`: struct ConnectorClientConfigRequest
  
  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/connector/policy/*.java`
  
  **Acceptance Criteria**:
  - [ ] policy 模块目录创建
  - [ ] 2 个 Rust 文件创建
  
  **QA Scenarios**:
  ```
  Scenario: policy 模块编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-20-cargo-check.log
  ```

  **Commit**: YES (Group 3)

---

### Wave 4: errors + header + health + values (4 tasks, PARALLEL)

- [ ] 21. **errors/connect_error.rs (ConnectError enum + 各异常 struct)**

  **What to do**:
  - 创建 `connect-api/src/errors/mod.rs`
  - 创建 `connect_error.rs`: 定义 `ConnectError` enum
  - enum 成员: Data(DataError), Retriable(RetriableError), NotFound(NotFoundError), AlreadyExists(AlreadyExistsError), IllegalWorkerState(IllegalWorkerStateError), SchemaBuilder(SchemaBuilderError), SchemaProjector(SchemaProjectorError)
  - 各异常 struct 定义: message 字段, impl Display, impl Error
  - **无基类**: 不创建 KafkaError 基类
  
  **Must NOT do**:
  - 不使用 thiserror (已禁止)
  - 不创建异常基类
  
  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Blocked By**: Wave 1
  - **Blocks**: 所有使用异常的模块
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/errors/*.java` (9 files)
  - 用户确认: 异常用 enum 分组，无基类
  
  **Acceptance Criteria**:
  - [ ] ConnectError enum 定义完成
  - [ ] 7 个异常 struct 定义完成
  - [ ] impl Display/Error for each
  
  **QA Scenarios**:
  ```
  Scenario: errors 模块编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-21-cargo-check.log
  ```

  **Commit**: YES (Group 4: errors)

- [ ] 22. **header 模块 (4 files)**

  **What to do**:
  - 创建 `connect-api/src/header/mod.rs`
  - 创建 `header.rs`: struct Header { pub key, pub value, pub schema }
  - 创建 `headers.rs`: trait Headers (add, remove, size, isEmpty, allWithName, lastWithName, duplicate, apply)
  - 创建 `connect_header.rs`: struct ConnectHeader impl Header trait
  - 创建 `connect_headers.rs`: struct ConnectHeaders impl Headers trait
  
  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Blocked By**: Task 7 (Schema), Task 21 (ConnectError)
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/header/*.java` (4 files)
  
  **Acceptance Criteria**:
  - [ ] header 模块目录创建
  - [ ] Header struct 定义
  - [ ] Headers trait 定义
  - [ ] ConnectHeader/ConnectHeaders struct 实现
  
  **QA Scenarios**:
  ```
  Scenario: header 模块编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-22-cargo-check.log
  ```

  **Commit**: YES (Group 4)

- [ ] 23. **health 模块 (6 files)**

  **What to do**:
  - 创建 `connect-api/src/health/mod.rs`
  - 创建 `abstract_state.rs`: struct AbstractState { pub state, pub trace, pub worker_id }
  - 创建 `connector_state.rs`: enum ConnectorState { Uninitialized, Running, Paused, Stopped, Failed, Destroyed }
  - 创建 `connector_type.rs`: enum ConnectorType { Source, Sink }
  - 创建 `task_state.rs`: struct TaskState { pub state, pub trace, pub worker_id }
  - 创建 `connect_cluster_details.rs`, `connect_cluster_state.rs`, `connector_health.rs`
  
  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/health/*.java` (6 files)
  
  **Acceptance Criteria**:
  - [ ] health 模块目录创建
  - [ ] 6 个 Rust 文件创建
  
  **QA Scenarios**:
  ```
  Scenario: health 模块编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-23-cargo-check.log
  ```

  **Commit**: YES (Group 4)

- [ ] 24. **data/values.rs + data/schema_projector.rs**

  **What to do**:
  - 创建 `values.rs`: struct Values 工具类，静态方法 parse/encode
  - 创建 `schema_projector.rs`: struct SchemaProjector，project 方法
  
  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Blocked By**: Task 7, Task 9, Task 11
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/data/Values.java`
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/data/SchemaProjector.java`
  
  **Acceptance Criteria**:
  - [ ] values.rs 创建
  - [ ] schema_projector.rs 创建
  
  **QA Scenarios**:
  ```
  Scenario: values/schema_projector 编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-24-cargo-check.log
  ```

  **Commit**: YES (Group 4)

---

### Wave 5: sink + source 模块 (8 tasks, MAX PARALLEL)

- [ ] 25. **sink/sink_connector.rs (SinkConnector trait)**

  **What to do**:
  - 创建 `connect-api/src/sink/sink_connector.rs`
  - 定义 `SinkConnector` trait: Connector
  - 方法: taskClass() -> TaskType::Sink, taskConfigs(max_tasks)
  
  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Blocked By**: Task 17 (Connector)
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/sink/SinkConnector.java`
  
  **Acceptance Criteria**:
  - [ ] SinkConnector trait 定义完成
  - [ ] taskClass() 返回 TaskType::Sink
  
  **QA Scenarios**:
  ```
  Scenario: SinkConnector 编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-25-cargo-check.log
  ```

  **Commit**: YES (Group 5: sink/source)

- [ ] 26. **sink/sink_task.rs (SinkTask trait)**

  **What to do**:
  - 创建 `sink/sink_task.rs`
  - 定义 `SinkTask` trait: Task
  - 方法: initialize(context), start(props), put(records), flush(offsets), preCommit(offsets), open(partitions), close(partitions), stop()
  - 静态常量: TOPICS_CONFIG, TOPICS_REGEX_CONFIG
  
  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Blocked By**: Task 16 (Task), Task 4 (OffsetAndMetadata)
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/sink/SinkTask.java` (180行)
  
  **Acceptance Criteria**:
  - [ ] SinkTask trait 定义完成
  - [ ] 所有方法签名与 Java 一致
  
  **QA Scenarios**:
  ```
  Scenario: SinkTask 编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-26-cargo-check.log
  ```

  **Commit**: YES (Group 5)

- [ ] 27. **sink/sink_record.rs (SinkRecord struct)**

  **What to do**:
  - 创建 `sink/sink_record.rs`
  - 定义 `SinkRecord` struct
  - 字段: kafka_offset, timestamp_type (TimestampType), original_topic, original_kafka_partition, original_kafka_offset
  - impl ConnectRecord trait
  
  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Blocked By**: Task 2 (TimestampType), Task 19 (ConnectRecord)
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/sink/SinkRecord.java` (231行)
  
  **Acceptance Criteria**:
  - [ ] SinkRecord struct 定义完成
  - [ ] ConnectRecord trait 实现
  
  **QA Scenarios**:
  ```
  Scenario: SinkRecord 编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-27-cargo-check.log
  ```

  **Commit**: YES (Group 5)

- [ ] 28. **sink/sink_task_context.rs + sink_connector_context.rs + errant_record_reporter.rs**

  **What to do**:
  - 创建 `sink_task_context.rs`: trait SinkTaskContext
  - 创建 `sink_connector_context.rs`: trait SinkConnectorContext
  - 创建 `errant_record_reporter.rs`: trait ErrantRecordReporter
  
  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/sink/*.java`
  
  **Acceptance Criteria**:
  - [ ] 3 个文件创建完成
  
  **QA Scenarios**:
  ```
  Scenario: sink context 模块编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-28-cargo-check.log
  ```

  **Commit**: YES (Group 5)

- [ ] 29. **source/source_connector.rs (SourceConnector trait)**

  **What to do**:
  - 创建 `source/source_connector.rs`
  - 定义 `SourceConnector` trait: Connector
  - 方法: taskClass() -> TaskType::Source, taskConfigs(max_tasks)
  
  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Blocked By**: Task 17 (Connector)
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/source/SourceConnector.java`
  
  **Acceptance Criteria**:
  - [ ] SourceConnector trait 定义完成
  
  **QA Scenarios**:
  ```
  Scenario: SourceConnector 编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-29-cargo-check.log
  ```

  **Commit**: YES (Group 5)

- [ ] 30. **source/source_task.rs (SourceTask trait)**

  **What to do**:
  - 创建 `source/source_task.rs`
  - 定义 `SourceTask` trait: Task
  - 方法: initialize(context), start(props), poll() -> Vec<SourceRecord>, commit(), stop(), commitRecord(record, metadata)
  - 静态常量: TRANSACTION_BOUNDARY_CONFIG
  - enum TransactionBoundary: Poll, Interval, Connector
  
  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Blocked By**: Task 16, Task 3 (RecordMetadata)
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/source/SourceTask.java` (155行)
  
  **Acceptance Criteria**:
  - [ ] SourceTask trait 定义完成
  - [ ] TransactionBoundary enum 定义
  
  **QA Scenarios**:
  ```
  Scenario: SourceTask 编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-30-cargo-check.log
  ```

  **Commit**: YES (Group 5)

- [ ] 31. **source/source_record.rs (SourceRecord struct)**

  **What to do**:
  - 创建 `source/source_record.rs`
  - 定义 `SourceRecord` struct
  - 字段: source_partition (HashMap<String, serde_json::Value>), source_offset (HashMap<String, serde_json::Value>)
  - impl ConnectRecord trait
  
  **Must NOT do**:
  - 不使用 ConnectValue enum，使用 serde_json::Value
  
  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Blocked By**: Task 19
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/source/SourceRecord.java` (136行)
  - 用户确认: Map<String, ?> → serde_json::Value
  
  **Acceptance Criteria**:
  - [ ] SourceRecord struct 定义完成
  - [ ] source_partition/source_offset 使用 serde_json::Value
  
  **QA Scenarios**:
  ```
  Scenario: SourceRecord 编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-31-cargo-check.log
  ```

  **Commit**: YES (Group 5)

- [ ] 32. **source 其他文件 (5 files)**

  **What to do**:
  - 创建 `source_task_context.rs`: trait SourceTaskContext
  - 创建 `source_connector_context.rs`: trait SourceConnectorContext
  - 创建 `transaction_context.rs`: trait TransactionContext
  - 创建 `exactly_once_support.rs`: enum ExactlyOnceSupport
  - 创建 `connector_transaction_boundaries.rs`: trait ConnectorTransactionBoundaries
  
  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/source/*.java`
  
  **Acceptance Criteria**:
  - [ ] 5 个文件创建完成
  
  **QA Scenarios**:
  ```
  Scenario: source context 模块编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-32-cargo-check.log
  ```

  **Commit**: YES (Group 5)

---

### Wave 6: storage + transforms + rest + util (5 tasks, PARALLEL)

- [ ] 33. **storage/converter.rs (Converter trait)**

  **What to do**:
  - 创建 `storage/converter.rs`
  - 定义 `Converter` trait: ConnectPlugin
  - 方法: configure(configs, isKey), fromConnectData(topic, schema, value), toConnectData(topic, value)
  - 默认方法: fromConnectData with headers, toConnectData with headers, close()
  
  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Blocked By**: Task 1 (KafkaHeaders), Task 7 (Schema)
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/storage/Converter.java` (119行)
  
  **Acceptance Criteria**:
  - [ ] Converter trait 定义完成
  - [ ] trait 默认实现 close()
  
  **QA Scenarios**:
  ```
  Scenario: Converter 编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-33-cargo-check.log
  ```

  **Commit**: YES (Group 6)

- [ ] 34. **storage 其他文件 (6 files)**

  **What to do**:
  - 创建 `header_converter.rs`: trait HeaderConverter
  - 创建 `offset_storage_reader.rs`: trait OffsetStorageReader
  - 创建 `simple_header_converter.rs`: struct SimpleHeaderConverter impl HeaderConverter
  - 创建 `string_converter.rs`: struct StringConverter impl Converter
  - 创建 `converter_config.rs`: struct ConverterConfig
  - 创建 `converter_type.rs`: enum ConverterType
  - 创建 `string_converter_config.rs`: struct StringConverterConfig
  
  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Blocked By**: Task 33
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/storage/*.java`
  
  **Acceptance Criteria**:
  - [ ] 6 个文件创建完成
  
  **QA Scenarios**:
  ```
  Scenario: storage 模块编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-34-cargo-check.log
  ```

  **Commit**: YES (Group 6)

- [ ] 35. **transforms 模块 (Transformation + Predicate)**

  **What to do**:
  - 创建 `transforms/mod.rs`
  - 创建 `transformation.rs`: trait Transformation<R: ConnectRecord>
  - 创建 `predicates/mod.rs`
  - 创建 `predicates/predicate.rs`: trait Predicate<R: ConnectRecord>
  
  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Blocked By**: Task 19 (ConnectRecord)
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/transforms/*.java`
  
  **Acceptance Criteria**:
  - [ ] transforms 目录创建
  - [ ] Transformation trait 定义
  - [ ] Predicate trait 定义
  
  **QA Scenarios**:
  ```
  Scenario: transforms 模块编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-35-cargo-check.log
  ```

  **Commit**: YES (Group 6)

- [ ] 36. **rest 模块 (2 files)**

  **What to do**:
  - 创建 `rest/mod.rs`
  - 创建 `connect_rest_extension.rs`: trait ConnectRestExtension
  - 创建 `connect_rest_extension_context.rs`: trait ConnectRestExtensionContext
  
  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/rest/*.java`
  
  **Acceptance Criteria**:
  - [ ] rest 目录创建
  - [ ] 2 个 trait 定义
  
  **QA Scenarios**:
  ```
  Scenario: rest 模块编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-36-cargo-check.log
  ```

  **Commit**: YES (Group 6)

- [ ] 37. **util/connector_utils.rs**

  **What to do**:
  - 创建 `util/mod.rs`
  - 创建 `connector_utils.rs`: struct ConnectorUtils (静态工具方法)
  - 方法: combineGroups(groupAssignment, maxGroups)
  
  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  
  **References**:
  - Java: `connect/api/src/main/java/org/apache/kafka/connect/util/ConnectorUtils.java`
  
  **Acceptance Criteria**:
  - [ ] util 目录创建
  - [ ] ConnectorUtils struct 定义
  
  **QA Scenarios**:
  ```
  Scenario: util 模块编译通过
    Tool: Bash (cargo check)
    Expected Result: Finished
    Evidence: .sisyphus/evidence/task-37-cargo-check.log
  ```

  **Commit**: YES (Group 6)

---

### Wave 7: 测试迁移 (7 tasks, MAX PARALLEL)

- [ ] 38. **data 测试迁移 (11 test files, ~100 #[test])**

  **What to do**:
  - 创建 `tests/data/mod.rs`
  - 创建 `connect_schema_test.rs`: 对应 Java ConnectSchemaTest (10+ #[test])
  - 创建 `schema_builder_test.rs`: 对应 Java SchemaBuilderTest (380行, 30+ #[test])
  - 创建 `struct_test.rs`: 对应 Java StructTest (326行, 20+ #[test])
  - 创建 `field_test.rs`: 对应 Java FieldTest (5+ #[test])
  - 创建 `values_test.rs`: 对应 Java ValuesTest (20+ #[test])
  - 创建 `schema_projector_test.rs`: 对应 Java SchemaProjectorTest (15+ #[test])
  - 创建 `date_test.rs`, `time_test.rs`, `timestamp_test.rs`, `decimal_test.rs`
  - 每个 #[test] 对应一个 Java @Test 方法，功能点覆盖
  
  **Must NOT do**:
  - 不添加 Java 中不存在的额外测试
  - 不跳过任何 @Test 方法
  
  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Blocked By**: Wave 2-3 (data 模块实现)
  
  **References**:
  - Java: `connect/api/src/test/java/org/apache/kafka/connect/data/*Test.java` (11 files)
  
  **Acceptance Criteria**:
  - [ ] 11 个测试文件创建
  - [ ] #[test] 数量 ≥ 100
  - [ ] 所有 #[test] 对应 Java @Test 功能
  
  **QA Scenarios**:
  ```
  Scenario: data 测试编译通过
    Tool: Bash (cargo test --no-run)
    Expected Result: Compiling tests... Finished
    Evidence: .sisyphus/evidence/task-38-cargo-test-compile.log
    
  Scenario: data 测试执行通过
    Tool: Bash (cargo test tests::data)
    Steps:
      1. cargo test tests::data --no-fail-fast
    Expected Result: 100+ tests passed, 0 failed
    Evidence: .sisyphus/evidence/task-38-cargo-test.log
  ```

  **Commit**: YES (Group 7: tests)

- [ ] 39. **connector 测试迁移 (2 test files, ~10 #[test])**

  **What to do**:
  - 创建 `tests/connector_test.rs`: 对应 Java ConnectorTest (5+ #[test])
  - 创建 `tests/connector_reconfiguration_test.rs`: 对应 Java ConnectorReconfigurationTest (3+ #[test])
  
  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Blocked By**: Wave 3 (connector 模块实现)
  
  **References**:
  - Java: `connect/api/src/test/java/org/apache/kafka/connect/connector/*Test.java`
  
  **Acceptance Criteria**:
  - [ ] 2 个测试文件创建
  - [ ] #[test] 数量 ≥ 8
  
  **QA Scenarios**:
  ```
  Scenario: connector 测试通过
    Tool: Bash (cargo test tests::connector)
    Expected Result: All passed
    Evidence: .sisyphus/evidence/task-39-cargo-test.log
  ```

  **Commit**: YES (Group 7)

- [ ] 40. **header 测试迁移 (2 test files, ~25 #[test])**

  **What to do**:
  - 创建 `tests/header/mod.rs`
  - 创建 `connect_headers_test.rs`: 对应 Java ConnectHeadersTest (15+ #[test])
  - 创建 `connect_header_test.rs`: 对应 Java ConnectHeaderTest (10+ #[test])
  
  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Blocked By**: Wave 4 (header 模块实现)
  
  **References**:
  - Java: `connect/api/src/test/java/org/apache/kafka/connect/header/*Test.java`
  
  **Acceptance Criteria**:
  - [ ] 2 个测试文件创建
  - [ ] #[test] 数量 ≥ 25
  
  **QA Scenarios**:
  ```
  Scenario: header 测试通过
    Tool: Bash (cargo test tests::header)
    Expected Result: All passed
    Evidence: .sisyphus/evidence/task-40-cargo-test.log
  ```

  **Commit**: YES (Group 7)

- [ ] 41. **sink 测试迁移 (2 test files, ~15 #[test])**

  **What to do**:
  - 创建 `tests/sink/mod.rs`
  - 创建 `sink_connector_test.rs`: 对应 Java SinkConnectorTest (5+ #[test])
  - 创建 `sink_record_test.rs`: 对应 Java SinkRecordTest (10+ #[test])
  
  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Blocked By**: Wave 5 (sink 模块实现)
  
  **References**:
  - Java: `connect/api/src/test/java/org/apache/kafka/connect/sink/*Test.java`
  
  **Acceptance Criteria**:
  - [ ] 2 个测试文件创建
  - [ ] #[test] 数量 ≥ 15
  
  **QA Scenarios**:
  ```
  Scenario: sink 测试通过
    Tool: Bash (cargo test tests::sink)
    Expected Result: All passed
    Evidence: .sisyphus/evidence/task-41-cargo-test.log
  ```

  **Commit**: YES (Group 7)

- [ ] 42. **source 测试迁移 (2 test files, ~15 #[test])**

  **What to do**:
  - 创建 `tests/source/mod.rs`
  - 创建 `source_connector_test.rs`: 对应 Java SourceConnectorTest (5+ #[test])
  - 创建 `source_record_test.rs`: 对应 Java SourceRecordTest (10+ #[test])
  
  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Blocked By**: Wave 5 (source 模块实现)
  
  **References**:
  - Java: `connect/api/src/test/java/org/apache/kafka/connect/source/*Test.java`
  
  **Acceptance Criteria**:
  - [ ] 2 个测试文件创建
  - [ ] #[test] 数量 ≥ 15
  
  **QA Scenarios**:
  ```
  Scenario: source 测试通过
    Tool: Bash (cargo test tests::source)
    Expected Result: All passed
    Evidence: .sisyphus/evidence/task-42-cargo-test.log
  ```

  **Commit**: YES (Group 7)

- [ ] 43. **storage 测试迁移 (3 test files, ~25 #[test])**

  **What to do**:
  - 创建 `tests/storage/mod.rs`
  - 创建 `converter_type_test.rs`: 对应 Java ConverterTypeTest (5+ #[test])
  - 创建 `simple_header_converter_test.rs`: 对应 Java SimpleHeaderConverterTest (10+ #[test])
  - 创建 `string_converter_test.rs`: 对应 Java StringConverterTest (10+ #[test])
  
  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Blocked By**: Wave 6 (storage 模块实现)
  
  **References**:
  - Java: `connect/api/src/test/java/org/apache/kafka/connect/storage/*Test.java`
  
  **Acceptance Criteria**:
  - [ ] 3 个测试文件创建
  - [ ] #[test] 数量 ≥ 25
  
  **QA Scenarios**:
  ```
  Scenario: storage 测试通过
    Tool: Bash (cargo test tests::storage)
    Expected Result: All passed
    Evidence: .sisyphus/evidence/task-43-cargo-test.log
  ```

  **Commit**: YES (Group 7)

- [ ] 44. **util 测试迁移 (1 test file, ~5 #[test])**

  **What to do**:
  - 创建 `tests/util/mod.rs`
  - 创建 `connector_utils_test.rs`: 对应 Java ConnectorUtilsTest (5+ #[test])
  
  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []
  
  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Blocked By**: Wave 6 (util 模块实现)
  
  **References**:
  - Java: `connect/api/src/test/java/org/apache/kafka/connect/util/ConnectorUtilsTest.java`
  
  **Acceptance Criteria**:
  - [ ] 1 个测试文件创建
  - [ ] #[test] 数量 ≥ 5
  
  **QA Scenarios**:
  ```
  Scenario: util 测试通过
    Tool: Bash (cargo test tests::util)
    Expected Result: All passed
    Evidence: .sisyphus/evidence/task-44-cargo-test.log
  ```

  **Commit**: YES (Group 7)

---

## Final Verification Wave (MANDATORY)

> 4 review agents run in PARALLEL. ALL must APPROVE.
> Present consolidated results to user and get explicit "okay" before completing.

- [ ] F1. **Plan Compliance Audit** — `oracle`
  验证所有 Must Have 存在，所有 Must NOT Have 不存在。
  Output: `Must Have [N/N] | Must NOT Have [N/N] | VERDICT: APPROVE/REJECT`

- [ ] F2. **Code Quality Review** — `unspecified-high`
  cargo clippy + cargo fmt + cargo build。检查 unsafe 使用、unused imports。
  Output: `Build [PASS/FAIL] | Clippy [PASS/FAIL] | Fmt [PASS/FAIL] | VERDICT`

- [ ] F3. **Test Execution QA** — `unspecified-high`
  cargo test 全量执行，验证 #[test] 数量 ≥ 150。
  Output: `Tests [N pass/N fail] | Count [N/N] | VERDICT`

- [ ] F4. **Scope Fidelity Check** — `deep`
  验证文件数量 ≥ 57，struct/trait 数量 ≥ 50，无 TODO/空实现。
  Output: `Files [N/N] | Types [N/N] | No TODO [PASS/FAIL] | VERDICT`

---

## Commit Strategy

由于这是全新创建的模块，按功能分组提交：
1. **基础设施**: common-trait + kafka-clients-mock 新增
2. **核心 data**: Schema/Struct 相关文件
3. **connector**: Connector/Task 相关文件
4. **errors**: 异常类型
5. **其余模块**: header, health, rest, sink, source, storage, transforms, util
6. **测试**: tests 目录

---

## Success Criteria

### Verification Commands
```bash
cargo build --release     # Expected: Compiling connect-api... Finished
cargo clippy --all        # Expected: 0 warnings
cargo fmt --check         # Expected: Passed
cargo test --all          # Expected: 150+ tests passed, 0 failed
```

### Final Checklist
- [ ] All "Must Have" present (57 files, 50+ types, 22 tests)
- [ ] All "Must NOT Have" absent (no TODO, no extra deps, no unsafe)
- [ ] cargo build success
- [ ] cargo clippy clean
- [ ] cargo test all pass
- [ ] User explicit "okay" after Final Verification