# Kafka Connect API 模块完整迁移计划

## TL;DR

> **Quick Summary**: 将 Kafka Connect Java API 模块（connect/api）完整迁移到 Rust（connect-rust-new/connect-api），确保 1:1 对应，零 TODO，测试覆盖率达到 68% 以上。
>
> **Deliverables**:
> - 完整的 Rust connect-api 模块（所有 65 个 Java 类/接口都有对应实现）
> - 扩展的 kafka-clients-trait（添加 Config, ConfigKey 等类）
> - 15+ 个测试文件（覆盖核心功能）
> - 零 TODO 标记的完整实现
>
> **Estimated Effort**: Large (20-30 工作日)
> **Parallel Execution**: NO - 顺序执行（依赖关系明确）
> **Critical Path**: 阶段 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8

---

## Context

### Original Request

完成 kafka connect 用 rust 语言重写到 connect-rust-new 目录下（除 connect-file 和 connect-json 外所有均迁移，含测试），当前已经完成了部分，但完整性和正确性较差，现在请具体分析比对 connect-rust-new/connect-api 目录和 connect/api 目录下的详细差异，完成全部的代码 1:1 翻译，保证迁移的完整性和正确性。

### Interview Summary

**Key Discussions**:
- **外部依赖处理**: 扩展现有 kafka-clients-trait，不创建独立的 mock 模块
- **模块组织方式**: 保持类名对应，允许扁平化（最后一级目录和类名、文件名与 Kafka 保持一致）
- **测试迁移策略**: 关键模块测试在实现时一并生成（FT 用例），对于原 Kafka 包含的 UT 一并翻译
- **代码处理策略**: 充当前已有代码要充分利用，差异较大的代码允许删除重新翻译

**Research Findings**:
- Java 源码：65 个文件，8,008 行代码，22 个测试文件
- Rust 实现：16 个文件，5,093 行代码，6 个测试文件
- 完整性差距：约 75% 的文件缺失，73% 的测试缺失
- 发现 6 处 TODO 标记在 connector_impl.rs 中

### Metis Review

**Identified Gaps** (addressed):
- **外部依赖缺口**: 需要在 kafka-clients-trait 中添加 Config, ConfigKey, ConfigType 等类
- **核心 API 缺失**: rest 包（2 个接口）、connector/policy 包（2 个类）、transforms/predicates 包（1 个接口）
- **模块组织问题**: header 模块混合在 data.rs 中，需要独立
- **测试覆盖不足**: 缺失 16 个测试文件，覆盖率仅 27%

---

## Work Objectives

### Core Objective

完成 Kafka Connect API 模块的 Rust 重写，确保：
1. **完整性**: 所有（65 个）Java 类/接口都有对应的 Rust 实现
2. **正确性**: 代码编译通过，核心功能有 UT 覆盖，且 UT 覆盖通过
3. **兼容性**: 外部依赖有对应的实现，API 语义与 Java 版本一致

### Concrete Deliverables

- 扩展的 kafka-clients-trait 模块（添加 Config, ConfigKey, ConfigType 等）
- 完整的 connect-api 模块（包括 rest, policy, pred, header 等独立模块）
- 15+ 个测试文件（覆盖核心功能）
- 零 TODO 标记的完整实现
- 完整的文档注释

### Definition of Done

- [ ] 所有 65 个 Java 类/接口都有对应的 Rust 实现
- [ ] `cargo check` 通过（无警告）
- [ ] `cargo clippy` 无警告
- [ ] `cargo build` 成功
- [ ] `cargo test` 全部通过（至少 15 个测试）
- [ ] 测试覆盖率达到 68% 以上
- [ ] 零 TODO 标记
- [ ] 所有公共 API 都有文档

### Must Have

- 所有 Java 类/接口的 1:1 Rust 对应
- 完整的测试覆盖（至少 15 个测试文件）
- 零 TODO 标记
- 代码编译通过
- 所有测试通过

### Must NOT Have (Guardrails)

- 不允许有空实现或 TODO 标记
- 不允许跳过任何 Java 类/接口
- 不允许修改已验证正确的现有代码（除非必要）
- 不允许降低测试覆盖率

---

## Verification Strategy

> **ZERO HUMAN INTERVENTION** — ALL verification is agent-executed. No exceptions.
> Acceptance criteria requiring "user manually tests/confirms" are FORBIDDEN.

### Test Decision

- **Infrastructure exists**: YES (已有 Cargo.toml 和测试框架)
- **Automated tests**: Tests-after (在实现完成后补充测试)
- **Framework**: cargo test (Rust 内置测试框架)
- **测试策略**: 关键模块测试在实现时一并生成（FT 用例），简单文件的用例可以省略

### QA Policy

Every task MUST include agent-executed QA scenarios. Evidence saved to `.sisyphus/evidence/task-{N}-{scenario-slug}.{ext}`.

- **编译验证**: 使用 `cargo check` 验证代码编译
- **测试验证**: 使用 `cargo test` 运行所有测试
- **代码质量**: 使用 `cargo clippy` 检查代码质量
- **文档验证**: 使用 `cargo doc` 生成文档

---

## Execution Strategy

### Parallel Execution Waves

> 由于任务之间有明确的依赖关系，采用顺序执行方式。

```
阶段 1 (外部依赖扩展 - 1-2 天):
└── Task 1: 扩展 kafka-clients-trait [deep]

阶段 2 (修复现有 TODO - 2-3 天):
└── Task 2: 修复 connector_impl.rs 中的 TODO [deep]

阶段 3 (创建缺失模块 - 3-5 天):
├── Task 3: 创建 rest 模块 [deep]
├── Task 4: 创建 policy 模块 [deep]
└── Task 5: 创建 pred 模块 [deep]

阶段 4 (补充 header 模块 - 2-3 天):
└── Task 6: 补充 header 模块 [deep]

阶段 5 (补充 source/sink 模块 - 3-4 天):
├── Task 7: 完善 source 模块 [deep]
└── Task 8: 完善 sink 模块 [deep]

阶段 6 (测试迁移 - 5-7 天):
└── Task 9: 迁移测试文件 [deep]

阶段 7 (代码重构和优化 - 3-4 天):
└── Task 10: 代码重构和优化 [unspecified-high]

阶段 8 (最终验证 - 1-2 天):
└── Task 11: 最终验证 [deep]

Critical Path: Task 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9 → 10 → 11
Parallel Speedup: N/A (顺序执行)
Max Concurrent: 1
```

### Dependency Matrix

- **1**: — — 2, 1
- **2**: 1 — 3, 1
- **3**: 2 — 4, 1
- **4**: 2 — 5, 1
- **5**: 2 — 6, 1
- **6**: 3, 4, 5 — 7, 1
- **7**: 6 — 8, 1
- **8**: 6, 7 — 9, 1
- **9**: 8 — 10, 1
- **10**: 9 — 11, 1
- **11**: 10 — 12, 4

### Agent Dispatch Summary

- **1**: **1** — T1 → `deep`
- **2**: **1** — T2 → `deep`
- **3**: **3** — T3-T5 → `deep`
- **4**: **1** — T6 → `deep`
- **5**: **2** — T7-T8 → `deep`
- **6**: **1** — T9 → `deep`
- **7**: **1** — T10 → `unspecified-high`
- **8**: **1** — T11 → `deep`

---

## TODOs

> Implementation + Test = ONE Task. Never separate.
> EVERY task MUST have: Recommended Agent Profile + Parallelization info + QA Scenarios.
> **A task WITHOUT QA Scenarios is INCOMPLETE. No exceptions.**

- [x] 1. **扩展 kafka-clients-trait**

  **What to do**:
  - 在 `kafka-clients-trait/src/config.rs` 中添加以下类：
    - `Config` 结构体（配置验证和错误收集）
    - `ConfigKey` 结构体（配置键定义）
    - `ConfigType` 枚举（配置类型：Boolean, String, Int 等）
  - 更新 `ConfigValue` 结构体以支持错误消息
  - 添加必要的测试用例

  **Must NOT do**:
  - 不创建独立的 mock 模块
  - 不修改已有的 ConfigDef, ConfigValue 结构（除非必要）

  **Recommended Agent Profile**:
  > Select category + skills based on task domain. Justify each choice.
  - **Category**: `deep`
    - Reason: 需要深入理解 Java Config API 并准确映射到 Rust
  - **Skills**: []
    - 无特定技能需求，使用通用深度分析能力

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential
  - **Blocks**: Task 2
  - **Blocked By**: None

  **References** (CRITICAL - Be Exhaustive):

  > The executor has NO context from your interview. References are their ONLY guide.
  > Each reference must answer: "What should I look at and WHY?"

  **Pattern References** (existing code to follow):
  - `kafka-clients-trait/src/config.rs` - 现有的 ConfigDef 和 ConfigValue 实现

  **API/Type References** (contracts to implement against):
  - `connect/api/src/main/java/org/apache/kafka/common/config/Config.java` - Java Config 类定义
  - `connect/api/src/main/java/org/apache/kafka/common/config/ConfigKey.java` - Java ConfigKey 类定义

  **Test References** (testing patterns to follow):
  - `connect-rust-new/connect-api/tests/config_test.rs` - 现有的 Config 测试模式

  **External References** (libraries and frameworks):
  - Kafka 官方文档：https://kafka.apache.org/documentation/ - Kafka 配置 API 文档

  **WHY Each Reference Matters** (explain the relevance):
  - `kafka-clients-trait/src/config.rs`: 理解现有的 Config 实现模式，保持一致性
  - Java Config 类: 确保 Rust 实现与 Java API 1:1 对应
  - `config_test.rs`: 遵循现有的测试模式，保持测试风格一致

  **Acceptance Criteria**:

  > **AGENT-EXECUTABLE VERIFICATION ONLY** — No human action permitted.
  > Every criterion MUST be verifiable by running a command or using a tool.

  **If TDD (tests enabled):**
  - [ ] 测试文件创建: kafka-clients-trait/tests/config_test.rs
  - [ ] cargo test --package kafka-clients-trait → PASS

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  > **This is NOT optional. A task without QA scenarios WILL BE REJECTED.**
  >
  > Write scenario tests that verify the ACTUAL BEHAVIOR of what you built.
  > Minimum: 1 happy path + 1 failure/edge case per task.
  > Each scenario = exact tool + exact steps + exact assertions + evidence path.
  >
  > **The executing agent MUST run these scenarios after implementation.**
  > **The executing agent WILL verify evidence files exist before marking task complete.**

  \`\`\`
  Scenario: Config 类基本功能测试
    Tool: Bash (cargo test)
    Preconditions: kafka-clients-trait 模块已更新
    Steps:
      1. cd kafka-clients-trait
      2. cargo test config_test
    Expected Result: 所有测试通过，无失败
    Failure Indicators: 测试失败，编译错误
    Evidence: .sisyphus/evidence/task-1-config-test.txt

  Scenario: ConfigKey 类基本功能测试
    Tool: Bash (cargo test)
    Preconditions: ConfigKey 已实现
    Steps:
      1. cd kafka-clients-trait
      2. cargo test -- --nocapture
    Expected Result: ConfigKey 相关测试通过
    Failure Indicators: ConfigKey 测试失败
    Evidence: .sisyphus/evidence/task-1-configkey-test.txt

  Scenario: ConfigType 枚举完整性测试
    Tool: Bash (cargo test)
    Preconditions: ConfigType 已实现
    Steps:
      1. cd kafka-clients-trait
      2. cargo test
    Expected Result: 所有 ConfigType 变体都能正确使用
    Failure Indicators: ConfigType 相关测试失败
    Evidence: .sisyphus/evidence/task-1-configtype-test.txt
  \`\`\`

  > **Specificity requirements — every scenario MUST use:**
  > - **Selectors**: Specific test names or patterns
  > - **Data**: Concrete test data
  > - **Assertions**: Exact expected results
  > - **Timing**: Reasonable timeout
  > - **Negative**: At least ONE failure/error scenario per task
  >
  > **Anti-patterns (your scenario is INVALID if it looks like this):**
  > - ❌ "Verify it works correctly" — HOW? What does "correctly" mean?
  > - ❌ "Check the API returns data" — WHAT data? What fields? What values?
  > - ❌ "Test the struct works" — WHERE? What method? What content?
  > - ❌ Any scenario without an evidence path

  **Evidence to Capture**:
  - [ ] 每个证据文件命名: task-{N}-{scenario-slug}.{ext}
  - [ ] 测试输出保存到证据文件

  **Commit**: YES
  - Message: "feat(kafka-clients-trait): add Config, ConfigKey, ConfigType classes"
  - Files: kafka-clients-trait/src/config.rs, kafka-clients-trait/tests/config_test.rs
  - Pre-commit: cargo test --package kafka-clients-trait

---

- [x] 2. **修复 connector_impl.rs 中的 TODO**

  **What to do**:
  - 修复 connector_impl.rs 中的 6 处 TODO 标记：
    - 实现 proper key_schema
    - 实现 proper conversion from Box to Arc
    - 实现 proper value_schema
    - 实现 proper headers conversion
    - 实现 proper new_record
  - 确保所有实现都完整，无 TODO 标记残留
  - 运行 `cargo check` 和 `cargo test` 验证

  **Must NOT do**:
  - 不允许跳过任何 TODO
  - 不允许使用 todo!() 或 unimplemented!()
  - 不允许破坏现有的正确代码

  **Recommended Agent Profile**:
  > Select category + skills based on task domain. Justify each choice.
  - **Category**: `deep`
    - Reason: 需要深入理解现有代码和 TODO 上下文，实现正确的类型转换
  - **Skills**: []
    - 无特定技能需求，使用通用深度分析能力

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential
  - **Blocks**: Task 3
  - **Blocked By**: Task 1

  **References** (CRITICAL - Be Exhaustive):

  > The executor has NO context from your interview. References are their ONLY guide.
  > Each reference must answer: "What should I look at and WHY?"

  **Pattern References** (existing code to follow):
  - `connect-rust-new/connect-api/src/connector_impl.rs` - 包含 TODO 的源文件
  - `connect-rust-new/connect-api/src/data.rs` - Schema 相关实现参考

  **API/Type References** (contracts to implement against):
  - `connect/api/src/main/java/org/apache/kafka/connect/connector/ConnectRecord.java` - Java ConnectRecord 接口
  - `connect/api/src/main/java/org/apache/kafka/connect/data/Schema.java` - Java Schema 接口

  **Test References** (testing patterns to follow):
  - `connect-rust-new/connect-api/tests/connector_test.rs` - 现有的 connector 测试

  **External References** (libraries and frameworks):
  - Rust 文档：https://doc.rust.org/std/ - Rust 标准库文档

  **WHY Each Reference Matters** (explain the relevance):
  - `connector_impl.rs`: 理解 TODO 的上下文和预期行为
  - Java ConnectRecord: 确保 Rust 实现与 Java API 1:1 对应
  - `connector_test.rs`: 遵循现有的测试模式

  **Acceptance Criteria**:

  > **AGENT-EXECUTABLE VERIFICATION ONLY** — No human action permitted.
  > Every criterion MUST be verifiable by running a command or using a tool.

  **If TDD (tests enabled):**
  - [ ] connector_impl.rs 中无 TODO 标记
  - [ ] cargo check --package connect-api → PASS
  - [ ] cargo test --package connect-api → PASS

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  > **This is NOT optional. A task without QA scenarios WILL BE REJECTED.**
  >
  > Write scenario tests that verify the ACTUAL BEHAVIOR of what you built.
  > Minimum: 1 happy path + 1 failure/edge case per task.
  > Each scenario = exact tool + exact steps + exact assertions + evidence path.
  >
 arezhe executing agent MUST run these scenarios after implementation.**
  > **The executing agent WILL verify evidence files exist before marking task complete.**

  \`\`\`
  Scenario: 编译验证
    Tool: Bash (cargo check)
    Preconditions: 所有 TODO 已修复
    Steps:
      1. cd connect-rust-new/connect-api
      2. cargo check
    Expected Result: 编译成功，无错误和警告
    Failure Indicators: 编译错误，警告
    Evidence: .sisyphus/evidence/task-2-compile-check.txt

  Scenario: TODO 标记检查
    Tool: Bash (grep)
    Preconditions: connector_impl.rs 已修复
    Steps:
      1. cd connect-rust-new/connect-api/src
      2. grep -n "TODO\|todo!\|unimplemented!" connector_impl.rs
    Expected Result: 无输出（无 TODO 标记）
    Failure Indicators: 发现 TODO 标记
    Evidence: .sisyphus/evidence/task-2-todo-check.txt

  Scenario: 测试验证
    Tool: Bash (cargo test)
    Preconditions: 代码已编译
    Steps:
      1. cd connect-rust-new/connect-api
      2. cargo test
    Expected Result: 所有测试通过
    Failure Indicators: 测试失败
    Evidence: .sisyphus/evidence/task-2-test-run.txt
  \`\`\`

  > **Specificity requirements — every scenario MUST use:**
  > - **Selectors**: Specific grep patterns
  > - **Data**: Concrete file paths
  > - **Assertions**: Exact expected results
  > - **Timing**: Reasonable timeout
  > - **Negative**: At least ONE failure/error scenario per task
  >
  > **Anti-patterns (your scenario is INVALID if it looks like this):**
  > - ❌ "Verify it works correctly" — HOW? What does "correctly" mean?
  > - ❌ "Check the code compiles" — WHAT command? WHAT output?
  > - ❌ "Test the implementation" — WHERE? What test?
  > - ❌ Any scenario without an evidence path

  **Evidence to Capture**:
  - [ ] 每个证据文件命名: task-{N}-{scenario-slug}.{ext}
  - [ ] 编译和测试输出保存到证据文件

  **Commit**: YES
  - Message: "fix(connect-api): resolve all TODO markers in connector_impl.rs"
  - Files: connect-rust-new/connect-api/src/connector_impl.rs
  - Pre-commit: cargo test --package connect-api

---

- [x] 3. **创建 rest 模块**

  **What to do**:
  - 创建 `connect-rust-new/connect-api/src/rest.rs` 文件
  - 实现 `ConnectRestExtension` trait：
    - 继承：Configurable + Versioned + Closeable
    - 方法：register(&self, context: &ConnectRestExtensionContext)
  - 实现 `ConnectRestExtensionContext` struct：
    - 提供 JAX-RS Configurable 访问
    - 提供 ConnectClusterState 访问
  - 在 `lib.rs` 中添加模块声明
  - 添加 FT 用例测试

  **Must NOT do**:
  - 不允许跳过任何方法或功能
  - 不允许使用 TODO 标记
  - 不允许与 Java API 不一致

  **Recommended Agent Profile**:
  > Select category + skills based on task domain. Justify each choice.
  - **Category**: `deep`
    - Reason: 需要深入理解 Java REST 扩展 API 并准确映射到 Rust
  - **Skills**: []
    - 无特定技能需求，使用通用深度分析能力

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential
  - **Blocks**: Task 4
  - **Blocked By**: Task 2

  **References** (CRITICAL - Be Exhaustive):

  > The executor has NO context from your interview. References are their ONLY guide.
  > Each reference must answer: "What should I look at and WHY?"

  **Pattern References** (existing code to follow):
  - `connect-rust-new/connect-api/src/connector_impl.rs` - 现有的 trait 实现模式
  - `connect-rust-new/connect-api/src/config.rs` - Configurable trait 参考

  **API/Type References** (contracts to implement against):
  - `connect/api/src/main/java/org/apache/kafka/connect/rest/ConnectRestExtension.java` - Java ConnectRestExtension 接口
  - `connect/api/src/main/java/org/apache/kafka/connect/rest/ConnectRestExtensionContext.java` - Java ConnectRestExtensionContext 接口

  **Test References** (testing patterns to follow):
  - `connect-rust-new/connect-api/tests/connector_test.rs` - 现有的测试模式

  **External References** (libraries and frameworks):
  - JAX-RS 文档：https://eclipse-ee4j.github.io/jax-rs/ - JAX-RS API 文档

  **WHY Each Reference Matters** (explain the relevance):
  - `connector_impl.rs`: 理解现有的 trait 实现模式
  - Java ConnectRestExtension: 确保 Rust 实现与 Java API 1:1 对应
  - `connector_test.rs`: 遵循现有的测试模式

  **Acceptance Criteria**:

  > **AGENT-EXECUTABLE VERIFICATION ONLY** — No human action permitted.
  > Every criterion MUST be verifiable by running a command or using a tool.

  **If TDD (tests enabled):**
  - [ ] rest.rs 文件已创建
  - [ ] ConnectRestExtension trait 已实现
  - [ ] ConnectRestExtensionContext struct 已实现
  - [ ] lib.rs 中已添加模块声明
  - [ ] cargo check --package connect-api → PASS
  - [ ] cargo test --package connect-api → PASS

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  > **This is NOT optional. A task without QA scenarios WILL BE REJECTED.**
  >
  > Write scenario tests that verify the ACTUAL BEHAVIOR of what you built.
  > Minimum: 1 happy path + 1 failure/edge case per task.
  > Each scenario = exact tool + exact steps + exact assertions + evidence path.
  >
  > **The executing agent MUST run these scenarios after implementation.**
  > **The executing agent WILL verify evidence files exist before marking task complete.**

  \`\`\`
  Scenario: 编译验证
    Tool: Bash (cargo check)
    Preconditions: rest 模块已创建
    Steps:
      1. cd connect-rust-new/connect-api
      2. cargo check
    Expected Result: 编译成功，无错误和警告
    Failure Indicators: 编译错误，警告
    Evidence: .sisyphus/evidence/task-3-compile-check.txt

  Scenario: ConnectRestExtension trait 验证
    Tool: Bash (cargo test)
    Preconditions: 代码已编译
    Steps:
      1. cd connect-rust-new/connect-api
      2. cargo test -- --nocapture
    Expected Result: ConnectRestExtension 相关测试通过
    Failure Indicators: ConnectRestExtension 测试失败
    Evidence: .sisyphus/evidence/task-3-rest-extension-test.txt

  Scenario: ConnectRestExtensionContext struct 验证
    Tool: Bash (cargo test)
    Preconditions: 代码已编译
    Steps:
      1. cd connect-rust-new/connect-api
      2. cargo test
    Expected Result: ConnectRestExtensionContext 相关测试通过
    Failure Indicators: ConnectRestExtensionContext 测试失败
    Evidence: .sisyphus/evidence/task-3-rest-context-test.txt
  \`\`\`

  > **Specificity requirements — every scenario MUST use:**
  > - **Selectors**: Specific test names or patterns
  > - **Data**: Concrete test data
  > - **Assertions**: Exact expected results
  > - **Timing**: Reasonable timeout
  > - **Negative**: At least ONE failure/error scenario per task
  >
  > **Anti-patterns (your scenario is INVALID if it looks like this):**
  > - ❌ "Verify it works correctly" — HOW? What does "correctly" mean?
  > - ❌ "Check the API is implemented" — WHAT API? WHAT methods?
  > - ❌ "Test the rest module" — WHERE? What test?
  > - ❌ Any scenario without an evidence path

  **Evidence to Capture**:
  - [ ] 每个证据文件命名: task-{N}-{scenario-slug}.{ext}
  - [ ] 编译和测试输出保存到证据文件

  **Commit**: YES
  - Message: "feat(connect-api): add rest module with ConnectRestExtension"
  - Files: connect-rust-new/connect-api/src/rest.rs, connect-rust-new/connect-api/src/lib.rs
  - Pre-commit: cargo test --package connect-api

---

- [x] 4. **创建 policy 模块**

  **What to do**:
  - 创建 `connect-rust-new/connect-api/src/policy.rs` 文件
  - 实现 `ConnectorClientConfigOverridePolicy` trait：
    - 继承：Configurable + Closeable
    - 方法：validate(&self, request: &ConnectorClientConfigRequest) -> Vec<ConfigValue>
  - 实现 `ConnectorClientConfigRequest` struct：
    - 包含配置和上下文信息
    - 字段：connector_name, configs, config_type
  - 在 `lib.rs` 中添加模块声明
  - 添加 FT 用例测试

  **Must NOT do**:
  - 不允许跳过任何方法或功能
  - 不允许使用 TODO 标记
  - 不允许与 Java API 不一致

  **Recommended Agent Profile**:
  > Select category + skills based on task domain. Justify each choice.
  - **Category**: `deep`
    - Reason: 需要深入理解 Java Policy API 并准确映射到 Rust
  - **Skills**: []
    - 无特定技能需求，使用通用深度分析能力

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential
  - **Blocks**: Task 5
  - **Blocked By**: Task 3

  **References** (CRITICAL - Be Exhaustive):

  > The executor has NO context from your interview. References are their ONLY guide.
  > Each reference must answer: "What should I look at and WHY?"

  **Pattern References** (existing code to follow):
  - `connect-rust-new/connect-api/src/connector_impl.rs` - 现有的 trait 实现模式
  - `connect-rust-new/connect-api/src/config.rs` - Configurable trait 参考

  **API/Type References** (contracts to implement against):
  - `connect/api/src/main/java/org/apache/kafka/connect/connector/policy/ConnectorClientConfigOverridePolicy.java` - Java Policy 接口
  - `connect/api/src/main/java/org/apache/kafka/connect/connector/policy/ConnectorClientConfigRequest.java` - Java Request 类

  **Test References** (testing patterns to follow):
  - `connect-rust-new/connect-api/tests/connector_test.rs` - 现有的测试模式

  **External References** (libraries and frameworks):
  - Kafka 官方文档：https://kafka.apache.org/documentation/ - Kafka 配置策略文档

  **WHY Each Reference Matters** (explain the relevance):
  - `connector_impl.rs`: 理解现有的 trait 实现模式
  - Java Policy: 确保 Rust 实现与 Java API 1:1 对应
  - `connector_test.rs`: 遵循现有的测试模式

  **Acceptance Criteria**:

  > **AGENT-EXECUTABLE VERIFICATION ONLY** — No human action permitted.
  > Every criterion MUST be verifiable by running a command or using a tool.

  **If TDD (tests enabled):**
  - [ ] policy.rs 文件已创建
  - [ ] ConnectorClientConfigOverridePolicy trait 已实现
  - [ ] ConnectorClientConfigRequest struct 已实现
  - [ ] lib.rs 中已添加模块声明
  - [ ] cargo check --package connect-api → PASS
  - [ ] cargo test --package connect-api → PASS

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  > **This is NOT optional. A task without QA scenarios WILL BE REJECTED.**
  >
  > Write scenario tests that verify the ACTUAL BEHAVIOR of what you built.
  > Minimum: 1 happy path + 1 failure/edge case per task.
  > Each scenario = exact tool + exact steps + exact assertions + evidence path.
  >
  > **The executing agent MUST run these scenarios after implementation.**
  > **The executing agent WILL verify evidence files exist before marking task complete.**

  \`\`\`
  Scenario: 编译验证
    Tool: Bash (cargo check)
    Preconditions: policy 模块已创建
    Steps:
      1. cd connect-rust-new/connect-api
      2. cargo check
    Expected Result: 编译成功，无错误和警告
    Failure Indicators: 编译错误，警告
    Evidence: .sisyphus/evidence/task-4-compile-check.txt

  Scenario: ConnectorClientConfigOverridePolicy trait 验证
    Toolser: Bash (cargo test)
    Preconditions: 代码已编译
    Steps:
      1. cd connect-rust-new/connect-api
      2. cargo test -- --nocapture
    Expected Result: Policy 相关测试通过
    Failure Indicators: Policy 测试失败
    Evidence: .sisyphus/evidence/task-4-policy-test.txt

  Scenario: ConnectorClientConfigRequest struct 验证
    Tool: Bash (cargo test)
    Preconditions: 代码已编译
    Steps:
      1. cd connect-rust-new/connect-api
      2. cargo test
    Expected Result: Request 相关测试通过
    Failure Indicators: Request 测试失败
    Evidence: .sisyphus/evidence/task-4-request-test.txt

  \`\`\`

  > **Specificity requirements — every scenario MUST use:**
  > - **Selectors**: Specific test names or patterns
  > - **Data**: Concrete test data
  > - **Assertions**: Exact expected results
  > - **Timing**: Reasonable timeout
  > - **Negative**: At least ONE failure/error scenario per task
  >
  > **Anti-patterns (your scenario is INVALID if it looks like this):**
  > - ❌ "Verify it works correctly" — HOW? What does "correctly" mean?
  > - ❌ "Check the API is implemented" — WHAT API? WHAT methods?
  > - ❌ "Test the policy module" — WHERE? What test?
  > - ❌ Any scenario without an evidence path

  **Evidence to Capture**:
  - [ ] 每个证据文件命名: task-{N}-{scenario-slug}.{ext}
  - [ ] 编译和测试输出保存到证据文件

  **Commit**: YES
  - Message: "feat(connect-api): add policy module with ConnectorClientConfigOverridePolicy"
  - Files: connect-rust-new/connect-api/src/policy.rs, connect-rust-new/connect-api/src/lib.rs
  - Pre-commit: cargo test --package connect-api

---

- [x] 5. **创建 pred 模块**

  **What to do**:
  - 创建 `connect-rust-new/connect-api/src/pred.rs` 文件
  - 实现 `Predicate` trait：
    - 泛型：Predicate<R: ConnectRecordTrait>
    - 继承：Configurable + Closeable
    - 方法：config(&self) -> ConfigDef, test(&self, record: &R) -> bool
  - 在 `lib.rs` 中添加模块声明
  - 添加 FT 用例测试

  **Must NOT do**:
  - 不允许跳过任何方法或功能
  - 不允许使用 TODO 标记
  - 不允许与 Java API 不一致

  **Recommended Agent Profile**:
  > Select category + skills based on task domain. Justify each choice.
  - **Category**: `deep`
    - Reason: 需要深入理解 Java Predicate API 并准确映射到 Rust
  - **Skills**: []
    - 无特定技能需求，使用通用深度分析能力

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential
  - **Blocks**: Task 6
  - **Blocked By**: Task 4

  **References** (CRITICAL - Be Exhaustive):

  > The executor has NO context from your interview. References are their ONLY guide.
  > Each reference must answer: "What should I look at and WHY?"

  **Pattern References** (existing code to follow):
  - `connect-rust-new/connect-api/src/connector_impl.rs` - 现有的 trait 实现模式
  - `connect-rust-new/connect-api/src/config.rs` - Configurable trait 参考

  **API/Type References** (contracts to implement against):
  - `connect/api/src/main/java/org/apache/kafka/connect/transforms/predicates/Predicate.java` - Java Predicate 接口

  **Test References** (testing patterns to follow):
  - `connect-rust-new/connect-api/tests/connector_test.rs` - 现有的测试模式

  **External References** (libraries and frameworks):
  - Kafka 官方文档：https://kafka.apache.org/documentation/ - Kafka 转换和谓词文档

  **WHY Each Reference Matters** (explain the relevance):
  - `connector_impl.rs`: 理解现有的 trait 实现模式
  - Java Predicate: 确保 Rust 实现与 Java API 1:1 对应
  - `connector_test.rs`: 遵循现有的测试模式

  **Acceptance Criteria**:

  > **AGENT-EXECUTABLE VERIFICATION ONLY** — No human action permitted.
  > Every criterion MUST be verifiable by running a command or using a tool.

  **If TDD (tests enabled):**
  - [ ] pred.rs 文件已创建
  - [ ] Predicate trait 已实现
  - [ ] lib.rs 中已添加模块声明
  - [ ] cargo check --package connect-api → PASS
  - [ ] cargo test --package connect-api → PASS

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  > **This is NOT optional. A task without QA scenarios WILL BE REJECTED.**
  >
  > Write scenario tests that verify the ACTUAL BEHAVIOR of what you built.
  > Minimum: 1 happy path + 1 failure/edge case per task.
  > Each scenario = exact tool + exact steps + exact assertions + evidence path.
  >
  > **The executing agent MUST run these scenarios after implementation.**
  > **The executing agent WILL verify evidence files exist before marking task complete.**

  \`\`\`
  Scenario: 编译验证
    Tool: Bash (cargo check)
    Preconditions: pred 模块已创建
    Steps:
      1. cd connect-rust-new/connect-api
      2. cargo check
    Expected Result: 编译成功，无错误和警告
    Failure Indicators: 编译错误，警告
    Evidence: .sisyphus/evidence/task-5-compile-check.txt

  Scenario: Predicate trait 验证
    Tool: Bash (cargo test)
    Preconditions: 代码已编译
    Steps:
      1. cd connect-rust-new/connect-api
      2. cargo test -- --nocapture
    Expected Result: Predicate 相关测试通过
    Failure Indicators: Predicate 测试失败
    Evidence: .sisyphus/evidence/task-5-predicate-test.txt
  \`\`\`

  > **Specificity requirements — every scenario MUST use:**
  > - **Selectors**: Specific test names or patterns
  > - **Data**: Concrete test data
  > - **Assertions**: Exact expected results
  > - **Timing**: Reasonable timeout
  > - **Negative**: At least ONE failure/error scenario per task
  >
  > **Anti-patterns (your scenario is INVALID if it looks like this):**
  > - ❌ "Verify it works correctly" — HOW? What does "correctly" mean?
  > - ❌ "Check the API is implemented" — WHAT API? WHAT methods?
  > - ❌ "Test the pred module" — WHERE? What test?
  > - ❌ Any scenario without an evidence path

  **Evidence to Capture**:
  - [ ] 每个证据文件命名: task-{N}-{scenario-slug}.{ext}
  - [ ] 编译和测试输出保存到证据文件

  **Commit**: YES
  - Message: "feat(connect-api): add pred module with Predicate trait"
  - Files: connect-rust-new/connect-api/src/pred.rs, connect-rust-new/connect-api/src/lib.rs
  - Pre-commit: cargo test --package connect-api

---

- [x] 6. **补充 header 模块**

  **What to do**:
  - 从 `data.rs` 提取 header 相关代码
  - 创建独立的 `connect-rust-new/connect-api/src/header.rs` 文件
  - 确保 `Header`, `Headers` trait 完整
  - 实现 `ConnectHeader`, `ConnectHeaders` struct
  - 更新 `data.rs` 中的引用
  - 在 `lib.rs` 中添加模块声明
  - 添加 FT 用例测试

  **Must NOT do**:
  - 不允许跳过任何方法或功能
  - 不允许使用 TODO 标记
  - 不允许与 Java API 不一致

  **Recommended Agent Profile**:
  > Select category + skills based on task domain. Justify each choice.
  - **Category**: `deep`
    - Reason: 需要深入理解现有代码并正确重构模块结构
  - **Skills**: []
    - 无特定技能需求，使用通用深度分析能力

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential
  - **Blocks**: Task 7
  - **Blocked By**: Task 5

  **References** (CRITICAL - Be Exhaustive):

  > The executor has NO context from your interview. References are their ONLY guide.
  > Each reference must answer: "What should I look at and WHY?"

  **Pattern References** (existing code to follow):
  - `connect-rust-new/connect-api/src/data.rs` - 现有的 header 相关代码
  - `connect-r.rust-new/connect-api/src/connector_impl.rs` - 现有的 trait 实现模式

  **API/Type References** (contracts to implement against):
  - `connect/api/src/main/java/org/apache/kafka/connect/header/Header.java` - Java Header 接口
  - `connect/api/src/main/java/org/apache/kafka/connect/header/Headers.java` - Java Headers 接口
  - `connect/api/src/main/java/org/apache/kafka/connect/header/ConnectHeader.java` - Java ConnectHeader 类
  - `connect/api/src/main/java/org/apache/kafka/connect/header/ConnectHeaders.java` - Java ConnectHeaders 类

  **Test References** (testing patterns to follow):
  - `connect-rust-new/connect-api/tests/struct_test.rs` - 现有的测试模式

  **External References** (libraries and frameworks):
  - Kafka 官方文档：https://kafka.apache.org/documentation/ - Kafka Headers 文档



  **WHY Each Reference Matters** (explain the relevance):
  - `data.rs`: 理解现有的 header 实现并正确提取
  - Java Header: 确保 Rust 实现与 Java API 1:1 对应
  - `struct_test.rs`: 遵循现有的测试模式

  **Acceptance Criteria**:

  > **AGENT-EXECUTABLE VERIFICATION ONLY** — No human action permitted.
  > Every criterion MUST be verifiable by running a command or using a tool.

  **If TDD (tests enabled):**
  - [ ] header.rs 文件已创建
  - [ ] Header, Headers trait 已实现
  - [ ] ConnectHeader, ConnectHeaders struct 已实现
  - [ ] data.rs 中的引用已更新
  - [ ] lib.rs 中已添加模块声明
  - [ ] cargo check --package connect-api → PASS
  - [ ] cargo test --package connect-api → PASS

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  > **This is NOT optional. A task without QA scenarios WILL BE REJECTED.**
  >
  > Write scenario tests that verify the ACTUAL BEHAVIOR of what you built.
  > Minimum: 1 happy path + 1 failure/edge case per task.
  > Each scenario = exact tool + exact steps + exact assertions + evidence path.
  >
  > **The executing agent MUST run these scenarios after implementation.**
  > **The executing agent WILL verify evidence files exist before marking task complete.**

  \`\`\`
  Scenario: 编译验证
    Tool: Bash (cargo check)
    Preconditions: header 模块已创建
    Steps:
      1. cd connect-rust-new/connect-api
      2. cargo check
    Expected Result: 编译成功，无错误和警告
    Failure Indicators: 编译错误，警告
    Evidence: .sisyphus/evidence/task-6-compile-check.txt

  Scenario: Header trait 验证
    Tool: Bash (cargo test)
    Preconditions: 代码已编译
    Steps:
      1. cd connect-rust-new/connect-api
      2. cargo test -- --nocapture
    Expected Result: Header 相关测试通过
    Failure Indicators: Header 测试失败
    Evidence: .sisyphus/evidence/task-6-header-test.txt

  Scenario: ConnectHeaders struct 验证
    Tool: Bash (cargo test)
    Preconditions: 代码已编译
    Steps:
      1. cd connect-rust-newly/connect-api
      2. cargo test
    Expected Result: ConnectHeaders 相关测试通过
    Failure Indicators: ConnectHeaders 测试失败
    Evidence: .sisyphus/evidence/task-6-connect-headers-test.txt
  \`\`\`

  > **Specificity requirements — every scenario MUST use:**
  > - **Selectors**: Specific test names or patterns
  > - **Data**: Concrete test data
  > - **Assertions**: Exact expected results
  > - **Timing**: Reasonable timeout
  > - **Negative**: At least ONE failure/error scenario per task
  >
  > **Anti-patterns (your scenario is INVALID if it looks like this):**
  > - ❌ "Verify it works correctly" — HOW? What does "correctly" mean?
  > - ❌ "Check the API is implemented" — WHAT API? WHAT methods?
  > - ❌ "Test the header module" — WHERE? What test?
  > - ❌ Any scenario without an evidence path

  **Evidence to Capture**:
  - [ ] 每个证据文件命名: task-{N}-{scenario-slug}.{ext}
  - [ ] 编译和测试输出保存到证据文件

  **Commit**: YES
  - Message: "refactor(connect-api): extract header module from data.rs"
  - Files: connect-rust-new/connect-api/src/header.rs, connect-rust-new/connect-api/src/data.rs, connect-rust-new/connect-api/src/lib.rs
  - Pre-commit: cargo test --package connect-api

---

- [x] 7. **完善 source 模块**

  **What to do**:
  - 从 `connector_impl.rs` 提取 source 相关代码
  - 创建独立的 `connect-rust-new/connect-api/src/source.rs` 文件
  - 确保所有 source trait 完整：
    - SourceConnector
    - SourceConnectorContext
    - SourceTask
    - SourceTaskContext
    - SourceRecord
    - TransactionContext
    - ExactlyOnceSupport (enum)
    - ConnectorTransactionBoundaries (enum)
  - 添加缺失的接口实现
  - 在 `lib.rs` 中添加模块声明
  - 添加 FT 用例测试

  **Must NOT do**:
  - 不允许跳过任何方法或功能
  - 不允许使用 TODO 标记
  - 不允许与 Java API 不一致

  **Recommended Agent Profile**:
  > Select category + skills based on task domain. Justify each choice.
  - **Category**: `deep`
    - Reason: 需要深入理解现有代码并正确重构模块结构
  - **Skills**: []
    - 无特定技能需求，使用通用深度分析能力

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential
  - **Blocks**: Task 8
  - **Blocked By**: Task 6

  **References** (CRITICAL - Be Exhaustive):

  > The executor has NO context from your interview. References are their ONLY guide.
  > Each reference must answer: "What should I look at and WHY?"

  **Pattern References** (existing code to follow):
  - `connect-rust-new/connect-api/src/connector_impl.rs` - 现有的 source 相关代码
  - `connect-rust-new/connect-api/src/data.rs` - Schema 相关实现参考

  **API/Type References** (contracts to implement against):
  - `connect/api/src/main/java/org/apache/kafka/connect/source/SourceConnector.java` - Java SourceConnector 接口
  - `connect/api/src/main/java/org/apache/kafka/connect/source/SourceTask.java` - Java SourceTask 接口
  - `connect/api/src/main/java/org/apache/kafka/connect/source/SourceRecord.java` - Java SourceRecord 类
  - `connect/api/src/main/java/org/apache/kafka/connect/source/TransactionContext.java` - Java TransactionContext 接口

  **Test References** (testing patterns to follow):
  - `connect-rust-new/connect-api/tests/connector_test.rs` - 现有的测试模式

  **External References** (libraries and frameworks):
  - Kafka 官方文档：https://kafka.apache.org/documentation/ - Kafka Source Connector 文档

  **WHY Each Reference Matters** (explain the relevance):
  - `connector_impl.rs`: 理解现有的 source 实现并正确提取
  - Java Source: 确保 Rust 实现与 Java API 1:1 对应
  - `connector_test.rs`: 遵循现有的测试模式

  **Acceptance Criteria**:

  > **AGENT-EXECUTABLE VERIFICATION ONLY** — No human action permitted.
  > Every criterion MUST be verifiable by running a command or using a tool.

  **If TDD (tests enabled):**
  - [ ] source.rs 文件已创建
  - [ ] 所有 source trait 已实现
  - [ ] lib.rs 中已添加模块声明
  - [ ] cargo check --package connect-api → PASS
  - [ ] cargo test --package connect-api → PASS

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  > **This is NOT optional. A task without QA scenarios WILL BE REJECTED.**
  >
  > Write scenario tests that verify the ACTUAL BEHAVIOR of what you built.
  > Minimum: 1 happy path + 1 failure/edge case per task.
  > Each scenario = exact tool + exact steps + exact assertions + evidence path.
  >
  > **The executing agent MUST run these scenarios after implementation.**
  > **The executing agent WILL verify evidence files exist before marking task complete.**

  \`\`\`
  Scenario: 编译验证
    Tool: Bash (cargo check)
    Preconditions: source 模块已创建
    Steps:
      1. cd connect-rust-new/connect-api
      2. cargo check
    Expected Result: 编译成功，无错误和警告
    Failure Indicators: 编译错误，警告
    Evidence: .sisyphus/evidence/task-7-compile-check.txt

  Scenario: SourceConnector trait 验证
    Tool: Bash (cargo test)
    Preconditions: 代码已编译
    Steps:
      1. cd connect-rust-new/connect-api
      2. cargo test -- --nocapture
    Expected Result: SourceConnector 相关测试通过
    Failure Indicators: SourceConnector 测试失败
    Evidence: .sisyphus/evidence/task-7-source-connector-test.txt

  Scenario: SourceRecord struct 验证
    Tool: Bash (cargo test)
    Preconditions: 代码已编译
    Steps:
      1. cd connect-rust-new/connect-api
      2. cargo test
    Expected Result: SourceRecord 相关测试通过
    Failure Indicators: SourceRecord 测试失败
    Evidence: .sisyphus/evidence/task-7-source-record-test.txt
  \`\`\`

  > **Specificity requirements — every scenario MUST use:**
  > - **Selectors**: Specific test names or patterns
  > - **Data**: Concrete test data
  > - **Assertions**: Exact expected results
  > - **Timing**: Reasonable timeout
  > - **Negative**: At least ONE failure/error scenario per task
  >
  > **Anti-patterns (your scenario is INVALID if it looks like this):**
  > - ❌ "Verify it works correctly" — HOW? What does "correctly" mean?
  > - ❌ "Check the API is implemented" — WHAT API? WHAT methods?
  > - ❌ "Test the source module" — WHERE? What test?
  > - ❌ Any scenario without an evidence path

  **Evidence to Capture**:
  - [ ] 每个证据文件命名: task-{N}-{scenario-slug}.{ext}
  - [ ] 编译和测试输出保存到证据文件

  **Commit**: YES
  - Message: "refactor(connect-api): extract and complete source module"
  - Files: connect-rust-new/connect-api/src/source.rs, connect-rust-new/connect-api/src/connector_impl.rs, connect-rust-new/connect-api/src/lib.rs
  - Pre-commit: cargo test --package connect-api

---

- [x] 8. **完善 sink 模块**

  **What to do**:
  - 从 `connector_impl.rs` 提取 sink 相关代码
  - 创建独立的 `connect-rust-new/connect-api/src/sink.rs` 文件
  - 确保所有 sink trait 完整：
    - SinkConnector
    - SinkConnectorContext
    - SinkTask
    - SinkTaskContext
    - SinkRecord
    - ErrantRecordReporter
  - 添加缺失的接口实现
  - 在 `lib.rs` 中添加模块声明
  - 添加 FT 用例测试

  **Must NOT do**:
  - 不允许跳过任何方法或功能
  - 不允许使用 TODO 标记
  - 不允许与 Java API 不一致

  **Recommended Agent Profile**:
  > Select category + skills based on task domain. Justify each choice.
  - **Category**: `deep`
    - Reason: 需要深入理解现有代码并正确重构模块结构
  - **Skills**: []
    - 无特定技能需求，使用通用深度分析能力

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential
  - **Blocks**: Task 9
  - **Blocked By**: Task 7

  **References** (CRITICAL - Be Exhaustive):

  > The executor has NO context from your interview. References are their ONLY guide.
  > Each reference must answer: "What should I look at and WHY?"

  **Pattern References** (existing code to follow):
  - `connect-rust-new/connect-api/src/connector_impl.rs` - 现有的 sink 相关代码
  - `connect-rust-new/connect-api/src/data.rs` - Schema 相关实现参考

  **API/Type References** (contracts to implement against):
  - `connect/api/src/main/java/org/apache/kafka/connect/sink/SinkConnector.java` - Java SinkConnector 接口
  - `connect/api/src/main/java/org/apache/kafka/connect/sink/SinkTask.java` - Java SinkTask 接口
  - `connect/api/src/main/java/org/apache/kafka/connect/sink/SinkRecord.java` - Java SinkRecord 类
  - `connect/api/src/main/java/org/apache/kafka/connect/sink/ErrantRecordReporter.java` - Java ErrantRecordReporter 接口

  **Test References** (testing patterns to follow):
  - `connect-rust-new/connect-api/tests/connector_test.rs` - 现有的测试模式

  **External References** (libraries and frameworks):
  - Kafka 官方文档：https://kafka.apache.org/documentation/ - Kafka Sink Connector 文档

  **WHY Each Reference Matters** (explain the relevance):
  - `connector_impl.rs`: 理解现有的 sink 实现并正确提取
  - Java Sink: 确保 Rust 实现与 Java API 1:1 对应
  - `connector_test.rs`: 遵循现有的测试模式

  **Acceptance Criteria**:

  > **AGENT-EXECUTABLE VERIFICATION ONLY** — No human action permitted.
  > Every criterion MUST be verifiable by running a command or using a tool.

  **If TDD (tests enabled):**
  - [ ] sink.rs 文件已创建
  - [ ] 所有 sink trait 已实现
  - [ ] lib.rs 中已添加模块声明
  - [ ] cargo check --package connect-api → PASS
  - [ ] cargo test --package connect-api → PASS

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  > **This is NOT optional. A task without QA scenarios WILL BE REJECTED.**
  >
  > Write scenario tests that verify the ACTUAL BEHAVIOR of what you built.
  > Minimum: 1 happy path + 1 failure/edge case per task.
  > Each scenario = exact tool + exact steps + exact assertions + evidence path.
  >
  > **The executing agent MUST run these scenarios after implementation.**
  > **The executing agent WILL verify evidence files exist before marking task complete.**

  \`\`\`
  Scenario: 编译验证
    Tool: Bash (cargo check)
    Preconditions: sink 模块已创建
    Steps:
      1. cd connect-rust-new/connect-api
      2. cargo check
    Expected Result: 编译成功，无错误和警告
    Failure Indicators: 编译错误，警告
    Evidence: .sisyphus/evidence/task-8-compile-check.txt

  Scenario: SinkConnector trait 验证
    Tool: Bash (cargo test)
    Preconditions: 代码已编译
    Steps:
      1. cd connect-rust-new/connect-api
      2. cargo test -- --nocapture
    Expected Result: SinkConnector 相关测试通过
    Failure Indicators: SinkConnector 测试失败
    Evidence: .sisyphus/evidence/task-8-sink-connector-test.txt

  Scenario: SinkRecord struct 验证
    Tool: Bash (cargo test)
    Preconditions: 代码已编译
    Steps:
      1. cd connect-rust-new/connect-api
      2. cargo test
    Expected Result: SinkRecord 相关测试通过
    Failure Indicators: SinkRecord 测试失败
    Evidence: .sisyphus/evidence/task-8-sink-record-test.txt
  \`\`\`

  > **Specificity requirements — every scenario MUST use:**
  > - **Selectors**: Specific test names or patterns
  > - **Data**: Concrete test data
  > - **Assertions**: Exact expected results
  > - **Timing**: Reasonable timeout
  > - **Negative**: At least ONE failure/error scenario per task
  >
  > **Anti-patterns (your scenario is INVALID if it looks like this):**
  > - ❌ "Verify it works correctly" — HOW? What does "correctly" mean?
  > - ❌ "Check the API is implemented" — WHAT API? WHAT methods?
  > - ❌ "Test the sink module" — WHERE? What test?
  > - ❌ Any scenario without an evidence path

  **Evidence to Capture**:
  - [ ] 每个证据文件命名: task-{N}-{scenario-slug}.{ext}
  - [ ] 编译和测试输出保存到证据文件

  **Commit**: YES
  - Message: "refactor(connect-api): extract and complete sink module"
  - Files: connect-rust-new/connect-api/src/sink.rs, connect-rust-new/connect-api/src/connector_impl.rs, connect-rust-new/connect-api/src/lib.rs
  - Pre-commit: cargo test --package connect-api

---

- [x] 9. **迁移测试文件**

  **What to do**:
  - 迁移高优先级测试（8 个）：
    - StringConverterTest
    - SimpleHeaderConverterTest
    - SourceRecordTest
    - SinkRecordTest
    - SourceConnectorTest
    - SinkConnectorTest
    - ConnectorUtilsTest
    - ConnectHeadersTest
  - 迁移中优先级测试（6 个）：
    - ConnectHeaderTest
    - ValuesTest
    - TimestampTest
    - TimeTest
    - DateTest
    - DecimalTest
    - ConnectSchemaTest
    - SchemaProjectorTest
  - 确保所有测试通过
  - 达到 15+ 个测试文件

  **Must NOT do**:
  - 不允许跳过任何测试
  - 不允许使用 TODO 标记
  - 不允许与 Java 测试逻辑不一致

  **Recommended Agent Profile**:
  > Select category + skills based on task domain. Justify each choice.
  - **Category**: `deep`
    - Reason: 需要深入理解 Java 测试并准确翻译到 Rust
  - **Skills**: []
    - 无特定技能需求，使用通用深度分析能力

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential
  - **Blocks**: Task 10
  - **Blocked By**: Task 8

  **References** (CRITICAL - Be Exhaustive):

  > The executor has NO context from your interview. References are their ONLY guide.
  > Each reference must answer: "What should I look at and WHY?"

  **Pattern References** (existing code to follow):
  - `connect-rust-new/connect-api/tests/connector_test.rs` - 现有的测试模式
  - `connect-rust-new/connect-api/tests/schema_builder_test.rs` - 现有的测试模式

  **API/Type References** (contracts to implement against):
  - `connect/api/src/test/java/org/apache/kafka/connect/storage/StringConverterTest.java` - Java StringConverterTest
  - `connect/api/src/test/java/org/apache/kafka/connect/source/SourceRecordTest.java` - Java SourceRecordTest
  - `connect/api/src/test/java/org/apache/kafka/connect/sink/SinkRecordTest.java` - Java SinkRecordTest

  **Test References** (testing patterns to follow):
  - `connect-rust-new/connect-api/tests/` - 现有的所有测试文件

  **External References** (libraries and frameworks):
  - Rust 测试文档：https://doc.rust.org/book/ch11-00-testing.html - Rust 测试文档

  **WHY Each Reference Matters** (explain the relevance):
  - `connector_test.rs`: 理解现有的测试模式并保持一致性
  - Java 测试: 确保 Rust 测试与 Java 测试逻辑一致
  - 现有测试: 遵循现有的测试风格

  **Acceptance Criteria**:

  > **AGENT-EXECUTABLE VERIFICATION ONLY** — No human action permitted.
  > Every criterion MUST be verifiable by running a command or using a tool.

  **If TDD (tests enabled):**
  - [ ] 15+ 个测试文件已创建
  - [ ] cargo test --package connect-api → PASS
  - [ ] 测试覆盖率达到 68% 以上

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  > **This is NOT optional. A task without QA scenarios WILL BE REJECTED.**
  >
  > Write scenario tests that verify the ACTUAL BEHAVIOR of what you built.
  > Minimum: 1 happy path + 1 failure/edge case per task.
  > Each scenario = exact tool + exact steps + exact assertions + evidence path.
  >
  > **The executing agent MUST run these scenarios after implementation.**
  > **The executing agent WILL verify evidence files exist before marking task complete.**

  \`\`\`
  Scenario: 测试数量验证
    Tool: Bash (find)
    Preconditions: 测试文件已创建
    Steps:
      1. cd connect-rust-new/connect-api/tests
      2. find . -name "*.rs" | wc -l
    Expected Result: 至少 15 个测试文件
    Failure Indicators: 测试文件数量不足
    Evidence: .sisyphus/evidence/task-9-test-count.txt

  Scenario: 所有测试通过
    Tool: Bash (cargo test)
    Preconditions: 测试文件已创建
    Steps:
      1. cd connect-rust-new/connect-api
      2. cargo test -- --nocapture
    Expected Result: 所有测试通过
    Failure Indicators: 有测试失败
    Evidence: .sisyphusus/evidence/task-9-test-run.txt

  Scenario: 测试覆盖率验证
    Tool: Bash (cargo test)
    Preconditions: 测试已通过
    Steps:
      1. cd connect-rust-new/connect-api
      2. cargo test -- --nocapture
    Expected Result: 测试覆盖率达到 68% 以上
    Failure Indicators: 测试覆盖率不足
    Evidence: .sisyphus/evidence/task-9-test-coverage.txt
  \`\`\`

  > **Specificity requirements — every scenario MUST use:**
  > - **Selectors**: Specific test命令和参数
  > - **Data**: Concrete 文件数量和覆盖率阈值
  > - **Assertions**: Exact expected results
  > - **Timing**: Reasonable timeout
  > - **Negative**: At least ONE failure/error scenario per task
  >
  > **Anti-patterns (your scenario is INVALID if it looks like this):**
  > - ❌ "Verify tests pass" — HOW? What command? WHAT output?
  > - ❌ "Check test coverage" — WHAT threshold? HOW to measure?
  > - ❌ "Test the migration" — WHERE? What tests?
  > - ❌ Any scenario without an evidence path

  **Evidence to Capture**:
  - [ ] 每个证据文件命名: task-{N}-{scenario-slug}.{ext}
  - [ ] 测试输出保存到证据文件

  **Commit**: YES
  - Message: "test(connect-api): migrate 14 test files from Java"
  - Files: connect-rust-new/connect-api/tests/*.rs
  - Pre-commit: cargo test --package connect-api

---

- [x] 10. **代码重构和优化**

  **What to do**:
  - 模块结构优化：
    - 确保模块组织符合 Rust 惯例
    - 检查所有公共 API 的导出
    - 优化模块依赖关系
  - 性能优化：
    - 优化 Arc/Box 使用
    - 减少不必要的克隆
    - 优化热点路径
  - 文档完善：
    - 为所有公共 API 添加文档注释
    - 提供使用示例
    - 更新 README

  **Must NOT do**:
  - 不允许破坏现有的正确功能
  - 不允许降低测试覆盖率
  - 不允许引入 TODO 标记

  **Recommended Agent Profile**:
  > Select category + skills based on task domain. Justify each choice.
  - **Category**: `unspecified-high`
    - Reason: 需要进行代码重构、性能优化和文档完善，涉及多个方面
  - **Skills**: []
    - 无特定技能需求，使用通用高优先级能力

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential
  - **Blocks**: Task 11
  - **Blocked By**: Task 9

  **References** (CRITICAL - Be Exhaustive):

  > The executor has NO context from your interview. References are their ONLY guide.
  > Each reference must answer: "What should I look at and WHY?"

  **Pattern References** (existing code to follow):
  - `connect-rust-new/connect-api/src/*.rs` - 所有源文件
  - `connect-rust-new/connect-api/tests/*.rs` - 所有测试文件

  **API/Type References** (contracts to implement against):
  - `connect/api/src/main/java/` - 所有 Java 源文件

  **Test References** (testing patterns to follow):
  - `connect-rust-new/connect-api/tests/*.rs` - 所有测试文件

  **External References** (libraries and frameworks):
  - Rust 文档：https://doc.rust.org/ - Rust 官方文档
  - Rust Clippy：https://github.com/rust-lang/rust-clippy - Rust 代码质量工具

  **WHY Each Reference Matters** (explain the relevance):
  - 所有源文件: 理解现有代码结构并进行优化
  - 所有测试文件: 确保优化后测试仍然通过
  - Rust 文档: 遵循 Rust 最佳实践

  **Acceptance Criteria**:

  > **AGENT-EXECUTABLE VERIFICATION ONLY** — No human action permitted.
  > Every criterion MUST be verifiable by running a command or using a tool.

  **If TDD (tests enabled):**
  - [ ] `cargo clippy` 无警告
  - [ ] `cargo doc` 成功生成文档
  - [ ] 所有公共 API 都有文档
  - [ ] `cargo test` 全部通过

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  > **This is NOT optional. A task without QA scenarios WILL BE REJECTED.**
  >
  > Write scenario tests that verify the ACTUAL BEHAVIOR of what you built.
  > Minimum: 1 happy path + 1 failure/edge case per task.
  > Each scenario = exact tool + exact steps + exact assertions + evidence path.
  >
  > **The executing agent MUST run these scenarios after implementation.**
  > **The executing agent WILL verify evidence files exist before marking task complete.**

  \`\`\`
  Scenario: Clippy 检查
    Tool: Bash (cargo clippy)
    Preconditions: 代码已优化
    Steps:
      1. cd connect-rust-new/connect-api
      2. cargo clippy -- -D warnings
    Expected Result: 无警告
    Failure Indicators: 有 clippy 警告
    Evidence: .sisyphus/evidence/task-10-clippy-check.txt

  Scenario: 文档生成
    Tool: Bash (cargo doc)
    Preconditions: 代码已优化
    Steps:
      1. cd connect-rust-new/connect-api
      2. cargo doc --no-deps
    Expected Result: 文档成功生成
    Failure Indicators: 文档生成失败
    Evidence: .sisyphus/evidence/task-10-doc-gen.txt

  Scenario: 测试验证
    Tool: Bash (cargo test)
    Preconditions: 代码已优化
    Steps:
      1. cd connect-rust-new/connect-api
      2. cargo test
    Expected Result: 所有测试通过
    Failure Indicators: 有测试失败
    Evidence: .sisyphus/evidence/task-10-test-run.txt
  \`\`\`

  > **Specificity requirements — every scenario MUST use:**
  > - **Selectors**: Specific cargo 命令和参数
  > - **Data**: Concrete 预期结果
  > - **Assertions**: Exact expected results
  > - **Timing**: Reasonable timeout
  > - **Negative**: At least ONE failure/error scenario per task
  >
  > **Anti-patterns (your scenario is INVALID if it looks like this):**
  > - ❌ "Verify code quality" — HOW? What tool? WHAT output?
  > - ❚ "Check documentation" — WHAT command? WHAT result?
  > - ❌ "Test the optimized code" — WHERE? What test?
  > - ❌ Any scenario without an evidence path

  **Evidence to Capture**:
  - [ ] 每个证据文件命名: task-{N}-{scenario-slug}.{ext}
  - [ ] Clippy 和文档输出保存到证据文件

  **Commit**: YES
  - Message: "refactor(connect-api): optimize code structure and add documentation"
  - Files: connect-rust-new/connect-api/src/*.rs
  - Pre-commit: cargo clippy && cargo test --package connect-api

---

- [x] 11. **最终验证**

  **What to do**:
  - 完整性验证：
    - 所有 65 个 Java 类/接口都有对应实现
    - 代码行数合理（~6000-7000 行）
    - 零 TODO 标记
    - 所有公共 API 都有文档
  - 正确性验证：
    - `cargo check` 通过（无警告）
    - `cargo clippy` 无警告
    - `cargo build` 成功
    - `cargo test` 全部通过
    - 测试覆盖率达到 68% 以上
  - 兼容性验证：
    - 所有外部依赖都有对应实现
    - API 语义与 Java 版本一致
  - 生成最终报告：
    - 完整性度量报告
    - 正确性度量报告
    - 迁移总结

  **Must NOT do**:
  - 不允许跳过任何验证步骤
  - 不允许降低验证标准

  **Recommended Agent Profile**:
  > Select category + skills based on task domain. Justify each choice.
  - **Category**: `deep`
    - Reason: 需要进行全面的验证和报告生成
  - **Skills**: []
    - 无特定技能需求，使用通用深度分析能力

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential
  - **Blocks**: None
  - **Blocked By**: Task 10

  **References** (CRITICAL - Be Exhaustive):

  > The executor has NO context from your interview. References are their ONLY guide.
  > Each reference must answer: "What should I look at and WHY?"

  **Pattern References** (existing code to follow):
  - `connect-rust-new/connect-api/src/*.rs` - 所有源文件
  - `connect/api/src/main/java/` - 所有 Java 源文件

  **API/Type References** (contracts to implement against):
  - `connect/api/src/main/java/` - 所有 Java 源文件

  **Test References** (testing) patterns to follow):
  - `connect-rust-new/connect-api/tests/*.rs` - 所有测试文件

  **External References** (libraries and frameworks):
  - Rust 文档：https://doc.rust.org/ - Rust 官方文档

  **WHY Each Reference Matters** (explain the relevance):
  - 所有源文件: 验证完整性
  - 所有测试文件: 验证正确性
  - Rust 文档: 遵循 Rust 最佳实践

  **Acceptance Criteria**:

  > **AGENT-EXECUTABLE VERIFICATION ONLY** — No human action permitted.
  > Every criterion MUST be verifiable by running a command or using a tool.

  **If TDD (tests enabled):**
  - [ ] 所有 65 个 Java 类/接口都有对应实现
  - [ ] 代码行数合理（~6000-7000 行）
  - [ ] 零 TODO 标记
  - [ ] 所有公共 API 都有文档
  - [ ] `cargo check` 通过（无警告）
  - [ ] `cargo clippy` 无警告
  - [ ] `cargo build` 成功
  - [ ] `cargo test` 全部通过
  - [ ] 测试覆盖率达到 68% 以上

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  > **This is NOT optional. A task without QA scenarios WILL BE REJECTED.**
  >
  > Write scenario tests that verify the ACTUAL BEHAVIOR of what you built.
  > Minimum: 1 happy path + 1 failure/edge case per task.
  > Each scenario = exact tool + exact steps + exact assertions + evidence path.
  >
  > **The executing agent MUST run these scenarios after implementation.**
  > **The executing agent WILL verify evidence files exist before marking task complete.**

  \`\`\`
  Scenario: 完整性验证
    Tool: Bash (cargo check + grep)
    Preconditions: 所有实现已完成
    Steps:
      1. cd connect-rust-new/connect-api
      2. cargo check
      3. grep -r "TODO\|todo!\|unimplemented!" src/
    Expected Result: 编译成功，无 TODO 标记
    Failure Indicators: 编译失败，发现 TODO 标记
    Evidence: .sisyphus/evidence/task-11-completeness-check.txt

  Scenario: 正确性验证
    Tool: Bash (cargo test)
    Preconditions: 代码已编译
    Steps:
      1. cd connect-rust-new/connect-api
      2. cargo test -- --nocapture
    Expected Result: 所有测试通过
    Failure Indicators: 有测试失败
    Evidence: .sisyphus/evidence/task-11-correctness-check.txt

  Scenario: 测试覆盖率验证
    Tool: Bash (cargo test)
    Preconditions: 测试已通过
    Steps:
      1. cd connect-rust-new/connect-api
      2. cargo test -- --nocapture
    Expected Result: 测试覆盖率达到 68% 以上
    Failure Indicators: 测试覆盖率不足
    Evidence: .sisyphus/evidence/task-11-coverage-check.txt
  \`\`\`

  > **Specificity requirements — every scenario MUST use:**
  > - **Selectors**: Specific cargo 命令和参数
  > - **Data**: Concrete 预期结果
  > - **Assertions**: Exact expected results
  > - **Timing**: Reasonable timeout
  > - **Negative**: At least ONE failure/error scenario per task
  >
  > **Anti-patterns (ar scenario is INVALID if it looks like this):**
  > - ❌ "Verify everything is complete" — HOW? What checks?
  > - ❌ "Check correctness" — WHAT command? WHAT output?
  > - ❌ "Test the final code" — WHERE? What test?
  > - ❌ Any scenario without an evidence path

  **Evidence to Capture**:
  - [ ] 每个证据文件命名: task-{N}-{scenario-slug}.{ext}
  - [ ] 所有验证输出保存到证据文件

  **Commit**: NO
  - Message: N/A (最终验证，无提交)
  - Files: N/A
  - Pre-commit: N/A

---

## Final Verification Wave (MANDATORY — after ALL implementation tasks)

> 4 review agents run in PARALLEL. ALL must APPROVE. Present consolidated results to user and get explicit "okay" before completing.
>
> **Do NOT auto-proceed after verification. Wait for user's explicit approval before marking work complete.**
> **Never mark F1-F4 as checked before getting user's okay.** Rejection or user feedback -> fix -> re-run -> present again -> wait for okay.

- [ ] F1. **完整性验证** — `deep`
  验证所有 65 个 Java 类/接口都有对应的 Rust 实现。检查代码行数是否合理（~6000-7000 行）。验证零 TODO 标记。验证所有公共 API 都有文档。
  输出: `Java Classes [65/65] | Rust Implementations [N/N] | TODO Markers [0] | Documentation [N/N] | VERDICTA: APPROVE/REJECT`

- [ ] F2. **正确性验证** — `unspecified-high`
  运行 `cargo check`, `cargo clippy`, `cargo build`, `cargo test`。验证所有测试通过。验证测试覆盖率达到 68% 以上。
  输出: `cargo check [PASS/FAIL] | cargo clippy [PASS/FAIL] | cargo build [PASS/FAIL] | Tests [N pass/N fail] | Coverage [N%] | VERDICT`

- [ ] F3. **兼容性验证** — `deep`
  验证所有外部依赖都有对应的实现。验证 API 语义与 Java 版本一致。检查序列化格式兼容性。
  输出: `External Dependencies [N/N] | API Compatibility [PASS/FAIL] | Serialization [PASS/FAIL] | VERDICT`

- [ ] F4. **生成最终报告** — `deep`
  生成完整性度量报告、正确性度量报告和迁移总结。统计最终指标。
  输出: `Completeness Report [OK/FAIL] | Correctness Report [OK/FAIL] | Summary Report [OK/FAIL] | VERDICT`

---

## Commit Strategy

- **1**: `feat(kafka-clients-trait): add Config, ConfigKey, ConfigType classes` — kafka-clients-trait/src/config.rs, kafka-clients-trait/tests/config_test.rs
- **2**: `fix(connect-api): resolve all TODO markers in connector_impl.rs` — connect-rust-new/connect-api/src/connector_impl.rs
- **3**: `feat(connect-api): add rest module with ConnectRestExtension` — connect-rust-new/connect-api/src/rest.rs, connect-rust-new/connect-api/src/lib.rs
- **4**: `feat(connect-api): add policy module with ConnectorClientConfigOverridePolicy` — connect-rust-new/connect-api/src/policy.rs, connect-r-rust-new/connect-api/src/lib.rs
- **5**: `feat(connect-api): add pred module with Predicate trait` — connect-rust-new/connect-api/src/pred.rs, connect-rust-new/connect-api/src/lib.rs
- **6**: `refactor(connect-api): extract header module from data.rs` — connect-rust-new/connect-api/src/header.rs, connect-rust-new/connect-api/src/data.rs, connect-rust-new/connect-api/src/lib.rs
- **7**: `refactor(connect-api): extract and complete source module` — connect-rust-new/connect-api/src/source.rs, connect-rust-new/connect-api/src/connector_impl.rs, connect-rust-new/connect-api/src/lib.rs
- **8**: `refactor(connect-api): extract and complete sink module` — connect-rust-new/connect-api/src/sink.rs, connect-rust-new/connect-api/src/connector_impl.rs, connect-rust-new/connect-api/src/lib.rs
- **9**: `test(connect-api): migrate 14 test files from Java` — connect-rust-new/connect-api/tests/*.rs
- **10**: `refactor(connect-api): optimize code structure and add documentation` — connect-rust-new/connect-api/src/*.rs

---

## Success Criteria

### Verification Commands

```bash
# 编译验证
cd connect-rust-new/connect-api && cargo check

# 代码质量检查
cd connect-rust-new/connect-api && cargo clippy

# 构建验证
cd connect-rust-new/connect-api && cargo build

# 测试验证
cd connect-rust-new/connect-api && cargo test

# 文档生成
cd connect-rust-new/connect-api && cargo doc

# TODO 标记检查
cd connect-rust-new/connect-api/src && grep -r "TODO\|todo!\|unimplemented!" .

# 测试文件数量检查
cd connect-rust-new/connect-api/tests && find . -name "*.rs" | wc -l
```

### Final Checklist

- [ ] 所有 65 个 Java 类/接口都有对应的 Rust 实现
- [ ] 代码行数合理（~6000-7000 行）
- [ ] 零 TODO 标记
- [ ] 所有公共 API 都有文档
- [ ] `cargo check` 通过（无警告）
- [ ] `cargo clippy` 无警告
- [ ] `cargo build` 成功
- [ ] `cargo test` 全部通过（至少 15 个测试）
- [ ] 测试覆盖率达到 68% 以上
- [ ] 所有外部依赖都有对应的实现
- [ ] API 语义与 Java 版本一致
- [ ] 序列化格式与 Java 版本兼容
