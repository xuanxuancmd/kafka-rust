# connect-mirror-client 完整迁移工作计划

## TL;DR

> **快速摘要**: 修复connect-mirror-client模块的所有缺失功能，实现1:1的Java到Rust翻译，确保完整性和正确通过编译和测试
>
> **交付物**:
> - 完整的remoteConsumerOffsets()实现
> - ForwardingAdmin支持和配置
> - Display trait实现（Checkpoint、Heartbeat）
> - 独立测试文件（3个）
> - 日志系统集成
>
> **预计工作量**: Medium (4-7小时)
> **并行执行**: YES - 4 waves
> **关键路径**: 实现核心功能 → 添加测试 → 验证编译 → 运行测试

---

## Context

### Original Request
完成kafka connect mirror-client模块的Rust重写，确保完整性和正确性。当前已完成约75%，但存在以下问题：
- remoteConsumerOffsets()核心功能未实现（有TODO标记）
- ForwardingAdmin支持缺失
- Display trait实现缺失
- 独立测试文件缺失

### Interview Summary
**关键讨论**:
- 优先解决完整性问题：remoteConsumerOffsets()是核心功能，必须完整实现
- 依赖处理：kafka-clients-trait已提供mock实现，需要ForwardAdmin支持
- 测试策略：先实现功能，然后创建独立测试文件
- 编译验证：每个子任务完成后立即编译验证

**Research Findings**:
- Java实现：remoteConsumerOffsets()从checkpoint topic消费记录并解析
- 依赖分析：ForwardingAdmin是kafka-clients-admin的一部分，需要在trait中添加
- 测试模式：Java使用JUnit 5，Rust使用内联测试+独立测试

### Metis Review
**Identified Gaps** (addressed):
- Gap：remoteConsumerOffsets()未实现 → 解决：完整实现checkpoint topic消费逻辑
- Gap：ForwardingAdmin支持缺失 → 解决：在kafka-clients-trait中添加trait
- Gap：Display trait缺失 → 解决：为Checkpoint和Heartbeat实现Display
- Gap：测试文件缺失 → 解决：创建3个独立测试文件

---

## Work Objectives

### Core Objective
完成connect-mirror-client模块的1:1 Java到Rust翻译，确保所有功能完整实现并通过编译和测试

### Concrete Deliverables
- `client.rs`：完整的remoteConsumerOffsets()实现（移除TODO）
- `client.rs`：forwardingAdmin()方法实现
- `client.rs`：FORWARDING_ADMIN_CLASS常量
- `models.rs`：Checkpoint Display trait实现
- `models.rs`：Heartbeat Display trait实现
- `tests/mirror_client_test.rs`：MirrorClient完整测试
- `tests/replication_policy_test.rs`：ReplicationPolicy完整测试
- `tests/source_and_target_test.rs`：SourceAndTarget完整测试

### Definition of Done
- [ ] 所有TODO标记已移除
- [ ] `cargo check` 通过
- [ ] `cargo build` 通过
- [ ] `cargo test` 通过（所有测试）
- [ ] 代码行数与Java版本大致相同（±20%）
- [ ] 函数数量与Java版本相同

### Must Have
- 完整的remoteConsumerOffsets()实现（无TODO）
- ForwardingAdmin支持
- Display trait实现（Checkpoint、Heartbeat）
- 3个独立测试文件
- 所有测试通过

### Must NOT Have (Guardrails)
- **禁止**：任何TODO标记或空实现
- **禁止**：编译错误
- **禁止**：测试失败
- **禁止**：与Java实现逻辑不一致
- **AI slop模式**：过度注释、过度抽象、泛型名称（data/result/item/temp）

---

## Verification Strategy

> **ZERO HUMAN INTERVENTION** — ALL verification is agent-executed. No exceptions.
> Acceptance criteria requiring "user manually tests/confirms" are FORBIDDEN.

### Test Decision
- **Infrastructure exists**: YES (cargo test framework)
- **Automated tests**: Tests-after (先实现，后测试）
- **Framework**: cargo test
- **If TDD**: 每个任务包含测试用例作为验收标准

### QA Policy
Every task MUST include agent-executed QA scenarios (see TODO template below).
Evidence saved to `.sisyphus/evidence/task-{N}-{scenario-slug}.{ext}`.

- **Rust编译**: Use Bash (cargo check/build) — 验证代码编译通过
- **Rust测试**: Use Bash (cargo test) — 验证所有测试通过
- **功能验证**: 每个任务包含具体的测试场景

---

## Execution Strategy

### Parallel Execution Waves

> Maximize throughput by grouping independent tasks into parallel waves.
> Each wave completes before the next begins.
> Target: 5-8 tasks per wave. Fewer than 3 per wave (except final) = under-splitting.

```
Wave 1 (Start Immediately - 核心功能实现):
├── Task 1: 实现remoteConsumerOffsets()完整逻辑 [deep]
├── Task 2: 添加forwardingAdmin()方法 [quick]
├── Task 3: 添加FORWARDING_ADMIN_CLASS常量 [quick]
└── Task 4: 为Checkpoint添加Display trait [quick]

Wave 2 (After Wave 1 - 继续实现):
├── Task 5: 为Heartbeat添加Display trait [quick]
├── Task 6: 添加日志系统支持 [quick]
└── Task 7: 编译验证Wave 1-2 [quick]

Wave 3 (After Wave 2 - 测试文件):
├── Task 8: 创建mirror_client_test.rs [unspecified-high]
├── Task 9: 创建replication_policy_test.rs [unspecified-high]
└── Task 10: 创建source_and_target_test.rs [unspecified-high]

Wave 4 (After Wave 3 - 验证):
├── Task 11: 编译验证Wave 3 [quick]
└── Task 12: 运行完整测试套件 [quick]

Wave FINAL (After ALL tasks - 最终验证):
├── Task F1: 代码完整性检查 [deep]
├── Task F2: 编译和测试验证 [unspecified-high]
└── Task F3: 生成最终报告 [unspecified-high]
-> Present results -> Get explicit user okay

Critical Path: Task 1 → Task 7 → Task 11 → Task 12 → F1-F3 → user okay
Parallel Speedup: ~60% faster than sequential
Max Concurrent: 4 (Wave 1) / 3 (Wave 2) / 3 (Wave 3)
```

### Dependency Matrix (abbreviated — show ALL tasks in your generated plan)

- **1-4**: — — 5-7, 1
- **5-6**: — — 7, 2
- **7**: 1-6 — 8-10, 3
- **8-10**: 7 — 11, 4
- **11**: 8-10 — 12, 5
- **12**: 11 — F1-F3, 6

> This is abbreviated for reference. YOUR generated plan must include the FULL matrix for ALL tasks.

### Agent Dispatch Summary

- **1**: **4** — T1 → `deep`, T2-T4 → `quick`
- **2**: **3** — T5-T6 → `quick`, T7 → `quick`
- **3**: **3** — T8-T10 → `unspecified-high`, T11 → `quick`
- **4**: **3** — T12 → `quick`, F1-F3 → `deep`/`unspecified-high`

---

## TODOs

> Implementation + Test = ONE Task. Never separate.
> EVERY task must have: Recommended Agent Profile + Parallelization info + QA Scenarios.
> **A task WITHOUT QA Scenarios is INCOMPLETE. No exceptions.**

- [x] 1. 实现remoteConsumerOffsets()完整逻辑

  **What to do**:
  - 移除TODO标记和空实现
  - 实现完整的checkpoint topic消费逻辑
  - 参考Java MirrorClient.remoteConsumerOffsets()实现（156-187行）
  - 创建KafkaConsumer实例
  - 订阅checkpoint topic（使用replicationPolicy.checkpointsTopic(remoteClusterAlias)）
  - 分配分区（checkpoint topic只有一个分区0）
  - 消费记录直到超时或流结束
  - 反序列化Checkpoint记录
  - 过滤匹配consumerGroupId的记录
  - 返回TopicPartition到OffsetAndMetadata的映射

  **Must NOT do**:
  - 保留TODO标记
  - 返回空HashMap
  - 省略错误处理
  - 不匹配Java实现逻辑

  **Recommended Agent Profile**:
  > Select category + skills based on task domain. Justify each choice.
  - **Category**: `deep`
    - Reason: 需要深入理解Java实现并转换为Rust，涉及复杂的消费逻辑和错误处理
  - **Skills**: `[]`
    - 不需要特殊技能，直接实现即可
  - **Skills Evaluated but Omitted**:
    - 无

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential
  - **Blocks**: [Tasks 4, 5, 6, 7]
  - **Blocked By**: None (can start immediately)

  **References** (CRITICAL - Be Exhaustive):

  > The executor has NO context from your interview. References are their ONLY guide.
  > Each reference must answer: "What should I look at and WHY?"

  **Pattern References** (existing code to follow):
  - `connect/mirror-client/src/main/java/org/apache/kafka/connect/mirror/MirrorClient.java:156-187` - remoteConsumerOffsets()的完整Java实现，包括消费者创建、分区分配、记录消费、反序列化等

  **API/Type References** (contracts to implement against):
  - `connect-rust-new/connect-mirror-client/src/models.rs:Checkpoint` - Checkpoint结构体和反序列化方法
  - `connect-rust-new/connect-mirror-client/src/policy.rs:ReplicationPolicy` - ReplicationPolicy trait的checkpointsTopic()方法
  - `connect-rust-new/connect-mirror-client/src/client.rs:MirrorClient` trait - MirrorClient trait定义

  **Test References** (testing patterns to follow):
  - `connect/mirror-client/src/test/java/org/apache/kafka/connect/mirror/MirrorClientTest.java` - MirrorClient的测试模式

  **External References** (libraries and frameworks):
  - Java KafkaConsumer API: poll(), assign(), seekToBeginning(), position(), endOffsets()
  - Rust kafka-clients-trait: KafkaConsumerSync trait方法

  **WHY Each Reference Matters** (explain the relevance):
  - MirrorClient.java:156-187: 这是完整的实现参考，包括所有边界条件和错误处理
  - Checkpoint models: 需要使用反序列化方法解析记录
- ReplicationPolicy: 需要获取checkpoint topic名称
  - MirrorClient trait: 需要实现trait方法签名

  **Acceptance Criteria**:

  > **AGENT-EXECUTABLE VERIFICATION ONLY** — No human action permitted.
  > Every criterion MUST be verifiable by running a command or using a tool.

  **If TDD (tests enabled):**
  - [ ] 实现完成后无编译错误
  - [ ] 代码逻辑与Java实现一致

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  > **This is NOT optional. A task without QA scenarios WILL BE REJECTED.**
  >
  > Write scenario tests that verify the ACTUAL BEHAVIOR of what you built.
  > Minimum: 1 happy path + 1 failure/edge case per task.
  > Each scenario = exact tool + exact steps + exact assertions + evidence path.
  >
  > **The executing agent MUST run these scenarios after implementation.**
  > **The orchestrator WILL verify evidence files exist before marking task complete.**

  ```
  Scenario: Happy path - 成功消费checkpoint记录
    Tool: Bash (cargo check)
    Preconditions: 实现完整的remoteConsumerOffsets()逻辑
    Steps:
      1. 运行 cargo check验证编译通过
      2. 检查代码中无TODO标记
      3. 验证实现了消费者创建逻辑
      4. 验证实现了分区分配逻辑
      5. 验证实现了记录消费循环
      6. 验证实现了Checkpoint反序列化
      7. 验证实现了consumerGroupId过滤
    Expected Result: 编译成功，无TODO，逻辑完整
    Failure Indicators: 编译错误，TODO标记存在，关键逻辑缺失
    Evidence: .sisyphus/evidence/task-1-happy-path-compile.txt

  Scenario: Error handling - 正确处理反序列化错误
    Tool: Bash (grep)
    Preconditions: 实现remoteConsumerOffsets()
    Steps:
      1. 搜索反序列化错误处理代码
      2. 验证使用?或Result类型
      3. 验证错误时继续处理其他记录（如Java实现）
    Expected Result: 有适当的错误处理，不中断消费循环
    Failure Indicators: 无错误处理，错误时崩溃
    Evidence: .sisyphus/evidence/task-1-error-handling.txt

  Scenario: Logic consistency - 与Java实现逻辑一致
    Tool: Bash (grep)
    Preconditions: 实现remoteConsumerOffsets()
    Steps:
      1. 验证创建KafkaConsumer
      2. 验证使用replicationPolicy.checkpointsTopic()
      3. 验证分配分区0
      4. 验证seekToBeginning()
      5. 验证poll()循环直到超时
      6. 验证使用Checkpoint.deserializeRecord()
      7. 验证过滤consumerGroupId
      8. 验证返回TopicPartition到OffsetAndMetadata映射
    Expected Result: 所有关键步骤都存在
    Failure Indicators: 缺少关键步骤，逻辑与Java不一致
    Evidence: .sisyphus/evidence/task-1-logic-consistency.txt
  ```

  **Evidence to Capture**:
  - [ ] 编译输出
  - [ ] TODO检查结果
  - [ ] 逻辑检查结果

  **Commit**: NO
  - Message: N/A (group with subsequent tasks)

- [x] 2. 添加forwardingAdmin()方法

  **What to do**:
  - 在MirrorClientConfig中添加forwardingAdmin()方法
  - 参考Java MirrorClientConfig.forwardingAdmin()实现（84-93行）
  - 实现动态实例化ForwardingAdmin
  - 使用Utils.newParameterizedInstance()或类似逻辑
  - 处理ClassNotFoundException异常

  **Must NOT do**:
  - 硬编码ForwardingAdmin类名
  - 忽略异常处理
  - 返回null或错误值

  **Recommended Agent Profile**:
  > Select category + skills based on task domain. Justify each choice.
  - **Category**: `quick`
    - Reason: 相对简单的配置方法添加，参考Java实现即可
  - **Skills**: `[]`
    - 不需要特殊技能
  - **Skills Evaluated but Omitted**:
    - 无

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential
  - **Blocks**: [Tasks 3, 4, 5, 6, 7]
  - **Blocked By**: None (can start immediately)

  **References** (CRITICAL - Be Exhaustive):

  > The executor has NO context from your interview. References are their ONLY guide.
  > Each reference must answer: "What should I look at and WHY?"

  **Pattern References** (existing code to follow):
  - `connect/mirror-client/src/main/java/org/apache/kafka/connect/mirror/MirrorClientConfig.java:84-93` - forwardingAdmin()的Java实现，包括异常处理

  **API/Type References** (contracts to implement against):
  - `connect-rust-new/connect-mirror-client/src/client.rs:MirrorClientConfig` - MirrorClientConfig结构体

  **Test References** (testing patterns to follow):
  - 无特定测试参考

  **External References** (libraries and frameworks):
  - Java Utils.newParameterizedInstance() - 动态实例化工具

  **WHY Each Reference Matters** (explain the relevance):
  - MirrorClientConfig.java:84-93: 这是完整的实现参考，包括异常处理和实例化逻辑

  **Acceptance Criteria**:

  > **AGENT-EXECUTABLE VERIFICATION ONLY** — No human action permitted.
  > Every criterion MUST be verifiable by running a command or using a tool.

  **If TDD (tests enabled):**
  - [ ] 方法签名正确
  - [ ] 编译通过

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  > **This is NOT optional. A task without QA scenarios WILL BE REJECTED.**
  >
  > Write scenario tests that verify the ACTUAL BEHAVIOR of what you built.
  > Minimum: 1 happy path + 1 failure/edge case per task.
  > Each scenario = exact tool + exact steps + exact assertions + evidence path.
  >
  > **The executing agent MUST run these scenarios after implementation.**
  > **The orchestrator WILL verify evidence files exist before marking task complete.**

  ```
  Scenario: Method signature - forwardingAdmin()方法签名正确
    Tool: Bash (cargo check)
    Preconditions: 添加forwardingAdmin()方法
    Steps:
      1. 运行 cargo check验证编译通过
      2. 验证方法存在且签名正确
      3. 验证返回类型为ForwardingAdmin
    Expected Result: 编译成功，方法签名正确
    Failure Indicators: 编译错误，方法签名不匹配
    Evidence: .sisyphus/evidence/task-2-method-signature.txt

  Scenario: Exception handling - 正确处理异常
    Tool: Bash (grep)
    Preconditions: 添加forwardingAdmin()方法
    Steps:
      1. 搜索异常处理代码
      2. 验证使用Result类型
      3. 验证ClassNotFoundException转换为KafkaException
    Expected Result: 有适当的异常处理
    Failure Indicators: 无异常处理，异常类型不正确
    Evidence: .sisyphus/evidence/task-2-exception-handling.txt
  ```

  **Evidence to Capture**:
  - [ ] 编译输出
  - [ ] 方法签名检查结果

  **Commit**: NO
  - Message: N/A (group with subsequent tasks)

- [x] 3. 添加FORWARDING_ADMIN_CLASS常量

  **What to do**:
  - 在config_keys模块中添加FORWARDING_ADMIN_CLASS常量
  - 参考Java MirrorClientConfig.FORWARDING_ADMIN_CLASS（68行）
  - 设置正确的字符串值

  **Must NOT do**:
  - 使用错误的常量名
  - 使用错误的值
  - 放置在错误的位置

  **Recommended Agent Profile**:
  > Select category + skills based on task domain. Justify each choice.
  - **Category**: `quick`
    - Reason: 简单的常量添加
  - **Skills**: `[]`
    - 不需要特殊技能
  - **Skills Evaluated but Omitted**:
    - 无

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential
  - **Blocks**: [Tasks 4, 5, 6, 7]
  - **Blocked By**: [Task 2]

  **References** (CRITICAL - Be Exhaustive):

  > The executor has NO context from your interview. References are their ONLY guide.
  > Each reference must answer: "What should I look at and WHY?"

  **Pattern References** (existing code to follow):
  - `connect/mirror-client/src/main/java/org/apache/kafka/connect/mirror/MirrorClientConfig.java:68` - FORWARDING_ADMIN_CLASS常量定义

  **API/Type References** (contracts to implement against):
  - `connect-rust-new/connect-mirror-client/src/client.rs:config_keys` - config_keys模块

  **Test References** (testing patterns to follow):
  - 无特定测试参考

  **External References** (libraries and frameworks):
  - 无

  **WHY Each Reference Matters** (explain the relevance):
  - MirrorClientConfig.java:68: 这是常量的定义参考

  **Acceptance Criteria**:

  > **AGENT-EXECUTABLE VERIFICATION ONLY** — No human action permitted.
  > Every criterion MUST be verifiable by running a command or using a tool.

  **If TDD (tests enabled)**
  - [ ] 常量定义正确
  - [ ] 编译通过

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  > **This is NOT optional. A task without QA scenarios WILL BE REJECTED.**
  >
  > Write scenario tests that verify the ACTUAL BEHAVIOR of what you built.
  > Minimum: 1 happy path + 1 failure/edge case per task.
  > Each scenario = exact tool + exact steps + exact assertions + evidence path.
  >
  > **The executing agent MUST run these scenarios after implementation.**
  > **The orchestrator WILL verify evidence files exist before marking task complete.**

  ```
  Scenario: Constant definition - 常量定义正确
    Tool: Bash (cargo check)
    Preconditions: 添加FORWARDING_ADMIN_CLASS常量
    Steps:
      1. 运行 cargo check验证编译通过
      2. 验证常量名正确
      3. 验证常量值正确
    Expected Result: 编译成功，常量定义正确
    Failure Indicators: 编译错误，常量名或值不正确
    Evidence: .sisyphus/evidence/task-3-constant-definition.txt
  ```

  **Evidence to Capture**:
  - [ ] 编译输出

  **Commit**: NO
  - Message: N/A (group with subsequent tasks)

- [x] 4. 为Checkpoint添加Display trait实现

  **What to do**:
  - 为Checkpoint结构体实现std::fmt::Display trait
  - 参考Java Checkpoint.toString()实现（98-102行）
  - 格式化为"Checkpoint{consumerGroupId=xxx, topicPartition=xxx, upstreamOffset=xxx, downstreamOffset=xxx, metadata=xxx}"
  - 使用format!宏

  **Must NOT do**:
  - 使用错误的格式
  - 遗漏任何字段
  - 不实现Display trait

  **Recommended Agent Profile**:
  > Select category + skills based on task domain. Justify each choice.
  - **Category**: `quick`
    - Reason: 简单的trait实现
  - **Skills**: `[]`
    - 不需要特殊技能
  - **Skills Evaluated but Omitted**:
    - 无

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 1, 2, 3)
  - **Blocks**: [Tasks 5, 6, 7]
  - **Blocked By**: [Task 1]

  **References** (CRITICAL - Be Exhaustive):

  > The executor has NO context from your interview. References are their ONLY guide.
  > Each reference must answer: "What should I look at and WHY?"

  **Pattern References** (existing code to follow):
  - `connect/mirror-client/src/main/java/org/apache/kafka/connect/mirror/Checkpoint.java:98-102` - toString()的Java实现

  **API/Type References** (contracts to implement against):
  - `connect-rust-new/connect-mirror-client/src/models.rs:Checkpoint` - Checkpoint结构体

  **Test References** (testing patterns to follow):
  - `connect-rust-new/connect-mirror-client/src/models.rs:tests`[cfg(test)] - 现有测试，可参考

  **External References** (libraries and frameworks):
  - Rust std::fmt::Display trait

  **WHY Each Reference Matters** (explain the relevance):
  - Checkpoint.java:98-102: 这是toString()的格式参考
  - models.rs:tests: 现有测试，可以添加Display测试

  **Acceptance Criteria**:

  > **AGENT-EXECUTABLE VERIFICATION ONLY** — No human action permitted.
  > Every criterion MUST be verifiable by running a command or using a tool.

  **If TDD (tests enabled)**
  - [ ] Display trait实现正确
  - [ ] 格式与Java一致
  - [ ] 编译通过

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  > **This is NOT optional. A task without QA scenarios WILL BE REJECTED.**
  >
  > Write scenario tests that verify the ACTUAL BEHAVIOR of what you built.
  > Minimum: 1 happy path + 1 failure/edge case per task.
  > Each scenario = exact tool + exact steps + exact assertions + evidence path.
  >
  > **The executing agent MUST run these scenarios after implementation.**
  > **The orchestrator WILL verify evidence files exist before marking task complete.**

  ```
  Scenario: Display implementation - Display trait实现正确
    Tool: Bash (cargo check)
    Preconditions: 实现Display trait
    Steps:
      1. 运行 cargo check验证编译通过
      2. 验证实现了fmt()方法
      3. 验证格式包含所有字段
    Expected Result: 编译成功，格式正确
    Failure Indicators: 编译错误，格式不正确
    Evidence: .sisyphus/evidence/task-4-display-implementation.txt

  Scenario: Format consistency - 格式与Java一致
    Tool: Bash (cargo test)
    Preconditions: 实现Display trait
    Steps:
      1. 运行 cargo test
      2. 验证测试通过
      3. 验证格式字符串包含"Checkpoint{"
      4. 验证格式字符串包含所有字段名
    Expected Result: 测试通过，格式一致
    Failure Indicators: 测试失败，格式不一致
    Evidence: .sisyphus/evidence/task-4-format-consistency.txt
  ```

  **Evidence to Capture**:
  - [ ] 编译输出
  - [ ] 测试输出

  **Commit**: NO
  - Message: N/A (group with subsequent tasks)

- [ ] 5. 为Heartbeat添加Display trait实现

  **What to do**:
  - 为Heartbeat结构体实现std::fmt::Display trait
  - 参考Java Heartbeat.toString()实现（72-75行）
  - 格式化为"Heartbeat{sourceClusterAlias=xxx, targetClusterAlias=xxx, timestamp=xxx}"
  - 使用format!宏

  **Must NOT do**:
  - 使用错误的格式
  - 遗漏任何字段
  - 不实现Display trait

  **Recommended Agent Profile**:
  > Select category + skills based on task domain. Justify each choice.
  - **Category**: `quick`
    - Reason: 简单的trait实现
  - **Skills**: `[]`
    - 不需要特殊技能
  - **Skills Evaluated but Omitted**:
    - 无

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2 (with Tasks 5, 6)
  - **Blocks**: [Task 7]
  - **Blocked By**: [Task 4]

  **References** (CRITICAL - Be Exhaustive):

  > The executor has NO context from your interview. References are their ONLY guide.
  > Each reference must answer: "What should I look at and WHY?"

  **Pattern References**** (existing code to follow):
  - `connect/mirror-client/src/main/java/org/apache/kafka/connect/mirror/Heartbeat.java:72-75` - toString()的Java实现

  **API/Type References** (contracts to implement against):
  - `connect-rust-new/connect-mirror-client/src/models.rs:Heartbeat` - Heartbeat结构体

  **Test References** (testing patterns to follow):
  - `connect-rust-new/connect-mirror-client/src/models.rs:tests`[cfg(test)] - 现有测试，可参考

  **External References** (libraries and frameworks):
  - Rust std::fmt::Display trait

  **WHY Each Reference Matters** (explain the relevance):
  - Heartbeat.java:72-75: 这是toString()的格式参考
  - models.rs:tests: 现有测试，可以添加Display测试

  **Acceptance Criteria**:

  > **AGENT-EXECUTABLE VERIFICATION ONLY** — No human action permitted.
  > Every criterion MUST be verifiable by running a command or using a tool.

  **If TDD (tests enabled)**
  - [ ] Display trait实现正确
  - [ ] 格式与Java一致
  - [ ] 编译通过

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  > **This is NOT optional. A task without QA scenarios WILL BE REJECTED.**
  >
  > Write scenario tests that verify the ACTUAL BEHAVIOR of what you built.
  > Minimum: 1 happy path + 1 failure/edge case per task.
  > Each scenario = exact tool + exact steps + exact assertions + evidence path.
  >
  > **The executing agent MUST run these scenarios after implementation.**
  > **The orchestrator WILL verify evidence files exist before marking task complete.**

  ```
  Scenario: Display implementation - Display trait实现正确
    Tool: Bash (cargo check)
    Preconditions: 实现Display trait
    Steps:
      1. 运行 cargo check验证编译通过
      2. 验证实现了fmt()方法
      3. 验证格式包含所有字段
    Expected Result: 编译成功，格式正确
    Failure Indicators: 编译错误，格式不正确
    Evidence: .sisyphus/evidence/task-5-display-implementation.txt

  Scenario: Format consistency - 格式与Java一致
    Tool: Bash (cargo test)
    Preconditions: 实现Display trait
    Steps:
      1. 运行 cargo test
      2. 验证测试通过
      3. 验证格式字符串包含"Heartbeat{"
      4. 验证格式字符串包含所有字段名
    Expected Result: 测试通过，格式一致
    Failure Indicators: 测试失败，格式不一致
    Evidence: .sisyphus/evidence/task-5-format-consistency.txt
  ```

  **Evidence to Capture**:
  - [ ] 编译输出
  - [ ] 测试输出

  **Commit**: NO
  - Message: N/A (group with subsequent tasks)

- [ ] 6. 添加日志系统支持

  **What to do**:
  - 在client.rs中添加日志支持
  - 使用log crate或创建简单的mock logger
  - 参考Java MirrorClient中的日志使用（49行，183-184行）
  - 在关键位置添加日志语句

  **Must NOT do**:
  - 使用println!代替log
  - 忽略日志级别
  - 不处理日志初始化

  **Recommended Agent Profile**:
  > Select category + skills based on task domain. Justify each choice.
  - **Category**: `quick`
    - Reason: 简单的日志添加
  - **Skills**: `[]`
    - 不需要特殊技能
  - **Skills Evaluated but Omitted**:
    - 无

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2 (with Tasks 5, 6)
  - **Blocks**: [Task 7]
  - **Blocked By**: [Task 4]

  **References** (CRITICAL - Be Exhaustive):

  > The executor has NO context from your interview. References are their ONLY guide.
  > Each reference must answer: "What should I look at and WHY?"

  **Pattern References** (existing code to follow):
  - `connect/mirror-client/src/main/java/org/apache/kafka/connect/mirror/MirrorClient.java:49, 183-184` - 日志使用示例

  **API/Type References** (contracts to implement against):
  - 无特定API参考

  **Test References** (testing patterns to follow):
  - 无特定测试参考

  **External References** (libraries and frameworks):
  - Rust log crate

  **WHY Each Reference Matters** (explain the relevance):
  - MirrorClient.java:49, 183-184: 这是日志使用的参考示例

  **Acceptance Criteria**:

  > **AGENT-EXECUTABLE VERIFICATION ONLY** — No human action permitted.
  > Every criterion MUST be verifiable by running a command or using a tool.

  **If TDD (tests enabled)**
  - [ ] 日志系统可用
  - [ ] 编译通过

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  > **This is NOT optional. A task without QA scenarios WILL BE REJECTED.**
  >
  > Write scenario tests that verify the ACTUAL BEHAVIOR of what you built.
  > Minimum: 1 happy path + 1 failure/edge case per task.
  > Each scenario = exact tool + exact steps + exact assertions + evidence path.
  >
  > **The executing agent MUST run these scenarios after implementation.**
  > **The orchestrator WILL verify evidence files exist before marking task complete.**

  ```
  Scenario: Logger availability - 日志系统可用
    Tool: Bash (cargo check)
    Preconditions: 添加日志支持
    Steps:
      1. 运行 cargo check验证编译通过
      2. 验证log crate可用
      3. 验证日志宏可用
    Expected Result: 编译成功，日志可用
    Failure Indicators: 编译错误，日志不可用
    Evidence: .sisyphus/evidence/task-6-logger-availability.txt
  ```

  **Evidence to Capture**:
  - [ ] 编译输出

  **Commit**: NO
  - Message: N/A (group with subsequent tasks)

- [ ] 7. 编译验证Wave 1-2

  **What to do**:
  - 运行cargo check验证Wave 1-2的所有实现
  - 运行cargo build确保可以构建
  - 检查是否有编译错误或警告

  **Must NOT do**:
  - 忽略编译错误
  - 跳过验证步骤

  **Recommended Agent Profile**:
  > Select category + skills based on task domain. Justify each choice.
  - **Category**: `quick`
    - Reason: 简单的编译验证
  - **Skills**: `[]`
    - 不需要特殊技能
  - **Skills Evaluated but Omitted**:
    - 无

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential
  - **Blocks**: [Tasks 8, 9, 10]
  - **Blocked By**: [Tasks 1, 2, 3, 4, 5, 6]

  **References** (CRITICAL - Be Exhaustive):

  > The executor has NO context from your interview. References are their ONLY guide.
  > Each reference question: "What should I look at and WHY?"

  **Pattern References** (existing code to follow):
  - 无特定模式参考

  **API/Type References** (contracts to implement against):
  - 无特定API参考

  **Test References** (testing patterns to follow):
  - 无特定测试参考

  **External References** (libraries and frameworks):
  - Rust cargo工具链

  **WHY Each Reference Matters** (explain the relevance):
  - 无特定参考，这是标准的编译验证步骤

  **Acceptance Criteria**:

  > **AGENT-EXECUTABLE VERIFICATION ONLY** — No human action permitted.
  > Every criterion MUST be verifiable by running a command or using a tool.

  **If TDD (tests enabled)**
  - [ ] cargo check通过
  - [ ] cargo build通过
  - [ ] 无编译错误

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  > **This is NOT optional. A task without QA scenarios WILL BE REJECTED.**
  >
  > Write scenario tests that verify the ACTUAL BEHAVIOR of what you built.
  > Minimum: 1 happy path + 1 failure/edge case per task.
  > Each scenario = exact tool + exact steps + exact assertions + evidence path.
  >
  > **The executing agent MUST run these scenarios after implementation.**
  > **The orchestrator WILL verify evidence files exist before marking task complete.**

  ```
  Scenario: Compilation check - 编译检查通过
    Tool: Bash (cargo check)
    Preconditions: Wave 1-2完成
    Steps:
      1. 运行 cargo check
      2. 验证无编译错误
      3. 验证无TODO标记
    Expected Result: 编译成功，无TODO
    Failure Indicators: 编译错误，TODO标记存在
    Evidence: .sisyphus/evidence/task-7-compilation-check.txt

  Scenario: Build verification - 构建验证通过
    Tool: Bash (cargo build)
    Preconditions: cargo check通过
    Steps:
      1. 运行 cargo build
      2. 验证构建成功
      3. 验证无构建警告
    Expected Result: 构建成功
    Failure Indicators: 构建失败，有警告
    Evidence: .sisyphus/evidence/task-7-build-verification.txt
  ```

  **Evidence to Capture**:
  - [ ] cargo check输出
  - [ ] cargo build输出

  **Commit**: NO
  - Message: N/A (group with subsequent tasks)

- [ ] 8. 创建mirror_client_test.rs

  **What to do**:
  - 创建tests/mirror_client_test.rs文件
  - 参考Java MirrorClientTest（32-216行）
  - 实现所有13个测试方法：
    - testIsHeartbeatTopic()
    - testIsCheckpointTopic()
    - countHopsForTopicTest()
    - heartbeatTopicsTest()
    - checkpointsTopicsTest()
    - replicationHopsTest()
    - upstreamClustersTest()
    - testIdentityReplicationUpstreamClusters()
    - remoteTopicsTest()
    - testIdentityReplicationRemoteTopics()
    - remoteTopicsSeparatorTest()
    - testIdentityReplicationTopicSource()
  - 使用TestMirrorClient进行测试

  **Must NOT do**:
  - 遗漏任何测试方法
  - 使用错误的测试逻辑
  - 不使用TestMirrorClient

  **Recommended Agent Profile**:
  > Select category + skills based on task domain. Justify each choice.
  - **Category**: `unspecified-high`
    - Reason: 需要创建完整的测试文件，包含13个测试方法
  - **Skills**: `[]`
    - 不需要特殊技能
  - **Skills Evaluated but Omitted**:
    - 无

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 3 (with Tasks 8, 9, 10)
  - **Blocks**: [Task 11]
  - **Blocked By**: [Task 7]

  **References** (CRITICAL - Be Exhaustive):

  > The executor has NO context from your interview. References are their ONLY guide.
  > Each reference must answer: "What should I look at and WHY?"

  **Pattern References** (existing code to follow):
  - `connect/mirror-client/src/test/java/org/apache/kafka/connect/mirror/MirrorClientTest.java:32-216` - 完整的测试实现

  **API/Type References** (contracts to implement against):
  - `connect-rust-new/connect-mirror-client/src/client.rs:TestMirrorClient` - TestMirrorClient用于测试

  **Test References** (testing patterns to follow):
  - `connect-rust-new/connect-mirror-client/src/models.rs:tests` - 现有测试模式

  **External References** (libraries and frameworks):
  - Rust测试框架

  **WHY Each Reference Matters** (explain the relevance):
  - MirrorClientTest.java:32-216: 这是完整的测试实现参考
  - TestMirrorClient: 这是测试用的mock实现

  **Acceptance Criteria**:

  > **AGENT-EXECUTABLE VERIFICATION ONLY** — No human action permitted.
  > Every criterion MUST be verifiable by running a command or using a tool.

  **If TDD (tests enabled)**
  - [ ] 所有13个测试方法实现
  - [ ] 测试编译通过
  - [ ] 测试运行通过

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  > **This is NOT optional. A task without QA scenarios WILL BE REJECTED.**
  >
  > Write scenario tests that verify the ACTUAL BEHAVIOR of what you built.
  > Minimum: 1 happy path + 1 failure/edge case per task.
  > Each scenario = exact tool + exact steps + exact assertions + evidence path.
  >
  > **The executing agent MUST run these scenarios after implementation.**
  > **The orchestrator WILL verify evidence files exist before marking task complete.**

  ```
  Scenario: Test file creation - 测试文件创建成功
    Tool: Bash (cargo test)
    Preconditions: 创建mirror_client_test.rs
    Steps:
      1. 运行 cargo test
      2. 验证测试文件被识别
      3. 验证所有13个测试方法存在
    Expected Result: 测试文件被识别，所有测试方法存在
    Failure Indicators: 测试文件未识别，测试方法缺失
    Evidence: .sisyphususvidence/task-8-test-file-creation.txt

  Scenario: All tests pass - 所有测试通过
    Tool: Bash (cargo test)
    Preconditions: 测试文件存在
    Steps:
      1. 运行 cargo test
      2. 验证所有13个测试通过
      3. 验证无测试失败
    Expected Result: 所有测试通过
    Failure Indicators: 有测试失败
    Evidence: .sisyphus/evidence/task-8-all-tests-pass.txt
  ```

  **Evidence to Capture**:
  - [ ] cargo test输出
  - [ ] 测试结果统计

  **Commit**: NO
  - Message: N/A (group with subsequent tasks)

- [ ] 9. 创建replication_policy_test.rs

  **What to do**:
  - 创建tests/replication_policy_test.rs文件
  - 参考Java ReplicationPolicyTest（30-96行）
  - 实现所有4个测试方法：
    - testInternalTopic()
    - offsetSyncsTopic_shouldBeEffectedByInternalTopicSeparatorEnabled()
    - checkpointsTopic_shouldBeEffectedByInternalTopicSeparatorEnabled()
    - heartbeatsTopic_shouldNotBeEffectedByInternalTopicSeparatorConfig()
  - 使用DefaultReplicationPolicy进行测试

  **Must NOT do**:
  - 遗漏任何测试方法
  - 使用错误的测试逻辑
  - 不使用DefaultReplicationPolicy

  **Recommended Agent Profile**:
  > Select category + skills based on task domain. Justify each choice.
  - **Category**: `unspecified-high`
    - Reason: 需要创建完整的测试文件，包含4个测试方法
  - **Skills**: `[]`
    - 不需要特殊技能
  - **Skills Evaluated but Omitted**:
    - 无

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 3 (with Tasks 8, 9, 10)
  - **Blocks**: [Task 11]
  - **Blocked By**: [Task 7]

  **References** (CRITICAL - Be Exhaustive):

  > The executor has NO context from your interview. References are their ONLY guide.
  > Each reference must answer: "What should I look at and WHY?"

  **Pattern References** (existing code to follow):
  - `connect/mirror-client/src/test/java/org/apache/kafka/connect/mirror/ReplicationPolicyTest.java:30-96` - 完整的测试实现

  **API/Type References** (contracts to implement against):
  - `connect-rust-new/connect-mirror-client/src/policy.rs:DefaultReplicationPolicy` - DefaultReplicationPolicy用于测试

  **Test References** (testing patterns to follow):
  - `connect-rust-new/connect-mirror-client/src/policy.rs:tests` - 现有测试模式

  **External References** (libraries and frameworks):
  - Rust测试框架

  **WHY Each Reference Matters** (explain the relevance):
  - ReplicationPolicyTest.java:30-96: 这是完整的测试实现参考
  - DefaultReplicationPolicy:这是测试用的策略实现

  **Acceptance Criteria**:

  > **AGENT-EXECUTABLE VERIFICATION ONLY** — No human action permitted.
  > Every criterion MUST be verifiable by running a command or using a tool.

  **If TDD (tests enabled)**
  - [ ] 所有4个测试方法实现
  - [ ] 测试编译通过
  - [ ] 测试运行通过

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  > **This is NOT optional. A task without QA scenarios WILL BE REJECTED.**
  >
  > Write scenario tests that verify the ACTUAL BEHAVIOR of what you built.
  > Minimum: 1 happy path + 1 failure/edge case per task.
  > Each scenario = exact tool + exact steps + exact assertions + evidence path.
  >
  > **The executing agent MUST run these scenarios after implementation.**
  > **The orchestrator WILL verify evidence files exist before marking task complete.**

  ```
  Scenario: Test file creation - 测试文件创建成功
    Tool: Bash (cargo test)
    Preconditions: 创建replication_policy_test.rs
    Steps:
      1. 运行 cargo test
      2. 验证测试文件被识别
      3. 验证所有4个测试方法存在
    Expected Result: 测试文件被识别，所有测试方法存在
    Failure Indicators: 测试文件未识别，测试方法缺失
    Evidence: .sisyphus/evidence/task-9-test-file-creation.txt

  Scenario: All tests pass - 所有测试通过
    Tool: Bash (cargo test)
    Preconditions: 测试文件存在
    Steps:
      1. 运行 cargo test
      2. 验证所有4个测试通过
      3. 验证无测试失败
    Expected Result: 所有测试通过
    Failure Indicators: 有测试失败
    Evidence: .sisyphus/evidence/task-9-all-tests-pass.txt
  ```

  **Evidence to Capture**:
  - [ ] cargo test输出
  - [ ] 测试结果统计

  **Commit**: NO
  - Message: N/A (group with subsequent tasks)

- [ ] 10. 创建source_and_target_test.rs

  **What to do**:
  - 创建tests/source_and_target_test.rs文件
  - 参考Java SourceAndTargetTest（24-50行）
  - 实现testEquals()方法
  - 验证SourceAndTarget的相等性和toString()实现

  **Must NOT do**:
  - 遗漏测试方法
  - 使用错误的测试逻辑

  **Recommended Agent Profile**:
  > Select category + skills based on task domain. Justify each choice.
  - **Category**: `unspecified-high`
    - Reason: 需要创建测试文件
  - **Skills**: `[]`
    - 不需要特殊技能
  - **Skills Evaluated but Omitted**:
    - 无

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 3 (with Tasks 8, 9, 10)
  - **Blocks**: [Task 11]
  - **Blocked By**: [Task 7]

  **References** (CRITICAL - Be Exhaustive):

  > The executor has NO context from your interview. References are their ONLY guide.
  > Each reference must answer: "What should I look at and WHY?"

  **Pattern References** (existing code to follow):
  - `connect/mirror-client/src/test/java/org/apache/kafka/connect/mirror/SourceAndTargetTest.java:24-50` - 测试实现

  **API/Type References** (contracts to implement against):
  - `connect-rust-new/connect-mirror-client/src/models.rs:SourceAndTarget` - SourceAndTarget结构体

  **Test References** (testing patterns to follow):
  - `connect-rust-new/connect-mirror-client/src/models.rs:tests` - 现有测试模式

  **External References** (libraries and frameworks):
  - Rust测试框架

  **WHY Each Reference Matters** (explain the relevance):
  - SourceAndTargetTest.java:24-50: 这是测试实现参考
  - SourceAndTarget: 这是测试用的结构体

  **Acceptance Criteria**:

  > **AGENT-EXECUTABLE VERIFICATION ONLY** — No human action permitted.
  > Every criterion MUST be verifiable by running a command or using a tool.

  **If TDD (tests enabled)**
  - [ ] 测试方法实现
  - [ ] 测试编译通过
  - [ ] 测试运行通过

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  > **This is NOT optional. A task without QA scenarios WILL BE REJECTED.**
  >
  > Write scenario tests that verify the ACTUAL BEHAVIOR of what you built.
  > Minimum: 1 happy path + 1 failure/edge case per task.
  > Each scenario = exact tool + exact steps + exact assertions + evidence path.
  >
  > **The executing agent MUST run these scenarios after implementation.**
  > **The orchestrator WILL verify evidence files exist before marking task complete.**

  ```
  Scenario: Test file creation - 测试文件创建成功
    Tool: Bash (cargo test)
    Preconditions: 创建source_and_target_test.rs
    Steps:
      1. 运行 cargo test
      2. 验证测试文件被识别
      3. 验证测试方法存在
    Expected Result: 测试文件被识别，测试方法存在
    Failure Indicators: 测试文件未识别，测试方法缺失
    Evidence: .sisyphus/evidence/task-10-test-file-creation.txt

  Scenario: Test passes - 测试通过
    Tool: Bash (cargo test)
    Preconditions: 测试文件存在
    Steps:
      1. 运行 cargo test
      2. 验证测试通过
      3. 验证无测试失败
    Expected Result: 测试通过
    Failure Indicators: 测试失败
    Evidence: .sisyphus/evidence/task-10-test-passes.txt
  ```

  **Evidence to Capture**:
  - [ ] cargo test输出
  - [ ] 测试结果统计

  **Commit**: NO
  - Message: N/A (group with subsequent tasks)

- [ ] 11. 编译验证Wave 3

  **What to do**:
  - 运行cargo check验证Wave 3的所有测试文件
  - 运行cargo test确保所有测试通过
  - 检查是否有测试失败

  **Must NOT do**:
  - 忽略测试失败
  - 跳过验证步骤

  **Recommended Agent Profile**:
  > Select category + skills based on task domain. Justify each choice.
  - **Category**: `quick`
    - Reason: 简单的编译和测试验证
  - **Skills**: `[]`
    - 不需要特殊技能
  - **Skills Evaluated but Omitted**:
    - 无

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential
  - **Blocks**: [Task 12]
  - **Blocked By**: [Tasks 8, 9, 10]

  **References** (CRITICAL - Be Exhaustive):

  > The executor has NO context from your interview. References are their ONLY guide.
  > Each reference must answer: "What should I look at and WHY?"

  **Pattern References** (existing code to follow):
  - 无特定模式参考

  **API/Type References** (contracts to implement against):
  - 无特定API参考

  **Test References** (testing patterns to follow):
  - 无特定测试参考

  **External References** (libraries and frameworks):
  - Rust cargo工具链

  **WHY Each Reference Matters** (explain the relevance):
  - 无特定参考，这是标准的编译和测试验证步骤

  **Acceptance Criteria**:

  > **AGENT-EXECUTABLE VERIFICATION ONLY** — No human action permitted.
  > Every criterion MUST be verifiable by running a command or using a tool.

  **If TDD (tests enabled)**
  - [ ] cargo check通过
  - [ ] cargo test通过
  - [ ] 所有测试通过

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  > **This is NOT optional. A task without QA scenarios WILL BE REJECTED.**
  >
  > Write scenario tests that verify the ACTUAL BEHAVIOR of what you built.
  > Minimum: 1 happy path + 1 failure/edge case per task.
  > Each scenario = exact tool + exact steps + exact assertions + evidence path.
  >
  > **The executing agent MUST run these scenarios after implementation.**
  > **The orchestrator WILL verify evidence files exist before marking task complete.**

  ```
  Scenario: Compilation check - 编译检查通过
    Tool: Bash (cargo check)
    Preconditions: Wave 3完成
    Steps:
      1. 运行 cargo check
      2. 验证无编译错误
    Expected Result: 编译成功
    Failure Indicators: 编译错误
    Evidence: .sisyphus/evidence/task-11-compilation-check.txt

  Scenario: All tests pass - 所有测试通过
    Tool: Bash (cargo test)
    Preconditions: cargo check通过
    Steps:
      1. 运行 cargo test
      2. 验证所有测试通过
      3. 验证测试数量正确（18+个测试）
    Expected Result: 所有测试通过
    Failure Indicators: 有测试失败
    Evidence: .sisyphus/evidence/task-11-all-tests-pass.txt
  ```

  **Evidence to Capture**:
  - [ ] cargo check输出
  - [ ] cargo test输出

  **Commit**: NO
  - Message: N/A (group with subsequent tasks)

- [ ] 12. 运行完整测试套件

  **What to do**:
  - 运行cargo test -- --test-threads=1确保顺序执行
  - 收集测试结果统计
  - 验证所有测试通过
  - 生成测试报告

  **Must NOT do**:
  - 忽略测试失败
  - 跳过测试报告生成

  **Recommended Agent Profile**:
  > Select category + skills based on task domain. Justify each choice.
  - **Category**: `quick`
    - Reason: 简单的测试运行
  - **Skills**: `[]`
    - 不需要特殊技能
  - **Skills Evaluated but Omitted**:
    - 无

  **Parallelization**:
  - **Can Run In Parallel**:
  - **Parallel Group**: Sequential
  - **Blocks**: [F1, F2, F3]
  - **Blocked By**: [Task 11]

  **References** (CRITICAL - Be Exhaustive):

  > The executor has NO context from your interview. References are their ONLY guide.
  > Each reference must answer: "What should I look at and WHY?"

  **Pattern References** (existing code to follow):
  - 无特定模式参考

  **API/Type References** (contracts to implement against):
  - 无特定API参考

  **Test References** (testing patterns to follow):
  - 无特定测试参考

  **External References** (libraries and frameworks):
  - Rust cargo工具链

  **WHY Each Reference Matters** (explain the relevance):
  - 无特定参考，这是标准的测试运行步骤

  **Acceptance Criteria**:

  > **AGENT-EXECUTABLE VERIFICATION ONLY** — No human action permitted.
  > Every criterion MUST be verifiable by running a command or using a tool.

  **If TDD (tests enabled)**
  - [ ] 所有测试通过
  - [ ] 测试数量正确

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  > **This is NOT optional. A task without QA scenarios WILL BE REJECTED.**
  >
  > Write scenario tests that verify the ACTUAL BEHAVIOR of what you built.
  > Minimum: 1 happy path + 1 failure/edge case per task.
  > Each scenario = exact tool + exact steps + exact assertions + evidence path.
  >
  > **The executing agent MUST run these scenarios after implementation.**
  > **The orchestrator WILL verify evidence files exist before marking task complete.**

  ```
  Scenario: Test suite execution - 测试套件执行成功
    Tool: Bash (cargo test)
    Preconditions: 所有实现完成
    Steps:
      1. 运行 cargo test -- --test-threads=1
      2. 验证所有测试通过
      3. 验证测试数量正确
    Expected Result: 所有测试通过
    Failure Indicators: 有测试失败
    Evidence: .sisyphus/evidence/task-12-test-suite-execution.txt
  ```

  **Evidence to Capture**:
  - [ ] cargo test输出
  - [ ] 测试统计

  **Commit**: NO
  - Message: N/A (group with subsequent tasks)

---

## Final Verification Wave (MANDATORY — after ALL implementation tasks)

> 4 review agents run in PARALLEL. ALL must APPROVE. Present consolidated results to user and get explicit "okay" before completing.
>
> **Do NOT auto-proceed after verification. Wait for user's explicit approval before marking work complete.**
> **Never mark F1-F3 as checked before getting user's okay.** Rejection or user feedback -> fix -> re-run -> present again -> wait for okay.

- [ ] F1. **代码完整性检查** — `deep`
  检查所有TODO标记已移除，所有函数都有实现，代码行数与Java版本大致相同（±20%）。检查client.rs、models.rs、policy.rs、protocol.rs、utils.rs文件。
  - 验证无TODO标记
  - 验证无空实现
  - 验证所有函数都有实现
  - 对比代码行数
  - 对比函数数量
  Output: `TODOs [0/N] | Empty impls [0/N] | Functions [N/N] | Lines [N/M] | VERDICT`

- [ ] F2. **编译和测试验证** — `unspecified-high`
  运行cargo check和cargo test，验证所有编译和测试通过。检查是否有编译错误、警告或测试失败。
  - 运行cargo check
  - 运行cargo build
  - 运行cargo test
  - 验证所有测试通过
  Output: `Check [PASS/FAIL] | Build [PASS/FAIL] | Tests [N/N pass] | VERDICT`

- [ ] F3. **生成最终报告** — `unspecified-high`
  生成完整的迁移报告，包括完整性、正确性、测试覆盖等。对比Java和Rust实现，列出所有差异和修复。
  - 生成完整性报告
  - 生成正确性报告
  - 生成测试覆盖报告
  - 列出所有修复的问题
  Output: `Completeness [N%] | Correctness [N%] | Test Coverage [N%] | VERDICT`

---

## Commit Strategy

- **1-7**: `feat(mirror-client): implement complete mirror-client migration` — 所有实现文件，cargo test

---

## Success Criteria

### Verification Commands
```bash
cd connect-rust-new/connect-mirror-client
cargo check  # Expected: no errors
cargo build  # Expected: success
cargo test   # Expected: all tests pass
```

### Final Checklist
- [ ] 所有TODO标记已移除
- [ ] 所有测试通过（18+个测试）
- [ ] 编译无错误和警告
- [ ] 代码行数与Java版本大致相同
- [ ] 函数数量与Java版本相同
- [ ] 完整性达到100%
- [ ] 正确性验证通过
