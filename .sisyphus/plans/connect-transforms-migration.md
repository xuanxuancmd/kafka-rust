# Kafka Connect Transforms 模块完整迁移计划

## TL;DR

> **Quick Summary**: 完成connect-transforms模块的1:1 Java到Rust翻译，确保完整性和正确性。
>
> **Deliverables**:
> - 修复所有编译错误和未完成实现
> - 补充13个缺失的Java类（4个Transformation + 3个Predicate + 6个工具类）
> - 在kafka-clients-trait中实现6个外部依赖工具类
> - 补充17个测试文件，确保测试覆盖率≥80%
>
> **Estimated Effort**: Medium (约30小时)
> **Parallel Execution**: YES - 部分任务可并行
> **Critical Path**: 修复编译错误 → 完成unimplemented → 实现外部依赖 → 补充缺失类 → 补充测试

---

## Context

### Original Request
完成kafka connect用rust语言重写到connect-rust-new目录下（除connect-file和connect-json外所有均迁移，含测试），当前已经完成了部分，但完整性和正确性较差，现在请具体分析比对connect-rust-new/connect-transforms目录和connect/transforms目录下的详细差异，完成全部的代码1:1翻译，保证迁移的完整性和正确性。

### Interview Summary
**Key Discussions**:
- 优化反射和动态加载的翻译：允许通过Rust编译期宏解决
- 在kafka-clients-trait中实现外部依赖的无业务属性公共工具类（如org.apache.kafka.common.cache.*）
- 同意P0-P3优先级划分

**Research Findings**:
- Java模块：26个类，8,687行代码（4,135主代码 + 4,552测试）
- Rust模块：15个模块，10,180行代码（8,970主代码 + 1,210测试）
- 缺失13个Java类，测试覆盖率仅19%
- 存在1个编译错误和6处unimplemented!()

### Metis Review
**Identified Gaps** (addressed):
- 编译期宏方案优化了反射和性能
- 外部依赖统一在模块间复用
- 工作计划按依赖关系和优先级组织

---

## Work Objectives

### Core Objective
完成connect-transforms模块的完整1:1翻译，确保：
1. 所有Java类都有对应的Rust实现
2. 代码编译通过，无错误和警告
3. 核心功能有单元测试覆盖，且测试通过
4. 完整性和正确性达到度量标准

### Concrete Deliverables
- 修复后的connect-transforms模块（编译通过）
- 13个新增的Rust文件（对应缺失的Java类）
- 6个新增的外部依赖工具类（在kafka-clients-trait中）
- 17个新增的测试文件
- 完整性验证报告

### Definition of Done
- [ ] `cargo check`在connect-transforms模块无错误
- [ ] `cargo test`在connect-transforms模块所有测试通过
- [ ] Java类数量（26）≈ Rust struct+trait数量（26）
- [ ] 测试文件数≥20个（Java的95%）
- [ ] 代码中无unimplemented!()或TODO
- [ ] 外部依赖在kafka-clients-trait中实现并可用

### Must Have
- 所有Java类1:1对应到Rust
- 编译通过，无错误
- 核心功能有测试覆盖
- 测试全部通过

### Must NOT Have (Guardrails)
- 不允许存在unimplemented!()或TODO
- 不允许编译错误
- 不允许跳过测试
- 不允许遗漏任何Java类
- 不允许修改Java原始代码

---

## Verification Strategy (MANDATORY)

> **ZERO HUMAN INTERVENTION** — ALL verification is agent-executed. No exceptions.
> Acceptance criteria requiring "user manually tests/confirms" are FORBIDDEN.

### Test Decision
- **Infrastructure exists**: YES (Rust测试框架)
- **Automated tests**: Tests-after (先实现，后测试)
- **Framework**: cargo test
- **If TDD**: 每个任务先实现，然后添加测试验证

### QA Policy
Every task MUST include agent-executed QA scenarios (see TODO template below).
Evidence saved to `.sisyphus/evidence/task-{N}-{scenario-slug}.{ext}`.

- **Rust代码**: Use Bash (cargo check, cargo test) — 编译检查，运行测试
- **外部依赖**: Use Bash (cargo check) — 验证编译通过
- **测试文件**: Use Bash (cargo test) — 运行特定测试

---

## Execution Strategy

### Parallel Execution Waves

> Maximize throughput by grouping independent tasks into parallel waves.
> Each wave completes before the next begins.
> Target: 5-8 tasks per wave. Fewer than 3 per wave (except final) = under-splitting.

```
Wave 1 (Start Immediately — 修复现有代码):
├── Task 1: 修复hoist_field.rs编译错误 [quick]
├── Task 2: 完成extract_field.rs的unimplemented [quick]
├── Task 3: 完成flatten.rs的unimplemented [quick]
└── Task 4: 完成header_from.rs的unimplemented [quick]

Wave 2 (After Wave 1 — 实现外部依赖工具类，MAX PARALLEL):
├── Task 5: 实现LRUCache和Cache trait [quick]
├── Task 6: 实现Base64工具类 [quick]
├── Task 7: 实现ByteBuffer工具类 [quick]
├── Task 8: 实现Pattern和Matcher工具类 [quick]
├── Task 9: 实现Logger工具类 [quick]
└── Task 10: 实现AppInfoParser工具类 [quick]

Wave 3 (After Wave 2 — 补充工具类，MAX PARALLEL):
├── Task 11: 实现FieldSyntaxVersion枚举 [quick]
├── Task 12: 实现SingleFieldPath [quick]
├── Task 13: 实现NonEmptyListValidator [quick]
├── Task 14: 实现RegexValidator [quick]
├── Task 15: 实现Requirements工具类 [quick]
├── Task 16: 实现SchemaUtil工具类 [quick]
└── Task 17: 实现SimpleConfig工具类 [quick]

Wave 4 (After Wave 3 — 补充Predicate类，MAX PARALLEL):
├── Task 18: 实现HasHeaderKey [quick]
├── Task 19: 实现RecordIsTombstone [quick]
└── Task 20: 实现TopicNameMatches [quick]

Wave 5 (After Wave 4 — 补充Transformation类，MAX PARALLEL):
├── Task 21: 实现DropHeaders [quick]
├── Task 22: 实现InsertHeader [quick]
├── Task 23: 实现TimestampRouter [quick]
└── Task 24: 实现ValueToKey [quick]

Wave 6 (After Wave 5 — 补充测试文件，MAX PARALLEL):
├── Task 25: 添加drop_headers_test.rs [quick]
├── Task 26: 添加insert_header_test.rs [quick]
├── Task 27: 添加timestamp_router_test.rs [quick]
├── Task 28: 添加value_to_key_test.rs [quick]
├── Task 29: 添加has_header_key_test.rs [quick]
├── Task 30: 添加record_is_tombstone_test.rs [quick]
├── Task 31: 添加topic_name_matches_test.rs [quick]
├── Task 32: 添加field_syntax_version_test.rs [quick]
├── Task 33: 添加single_field_path_test.rs [quick]
├── Task 34: 添加non_empty_list_validator_test.rs [quick]
├── Task 35: 添加requirements_test.rs [quick]
├── Task 36: 添加schema_util_test.rs [quick]
└── Task 37: 添加simple_config_test.rs [quick]

Wave FINAL (After ALL tasks — 验证):
├── Task F1: 编译验证 (cargo check) [quick]
├── Task F2: 运行所有测试 (cargo test) [quick]
├── Task F3: 完整性验证 (类数量、代码行数对比) [quick]
└── Task F4: 生成最终报告 [quick]
-> Present results -> Get explicit user okay

Critical Path: Task 1 → Task 5-10 → Task 11-17 → Task 18-20 → Task 21-24 → Task 25-37 → F1-F4 → user okay
Parallel Speedup: ~80% faster than sequential
Max Concurrent: 6 (Waves 2, 3, 5, 6)
```

### Dependency Matrix (abbreviated — show ALL tasks in your generated plan)

- **1-4**: — — 5-10, 1
- **5-10**: — — 11-17, 2
- **11-17**: 5-10 — 18-20, 3
- **18-20**: 11-17 — 21-24, 4
- **21-24**: 18-20 — 25-37, 5
- **25-37**: 21-24 — F1-F4, 6
- **F1-F4**: 25-37 — —, 7

> This is abbreviated for reference. YOUR generated plan must include the FULL matrix for ALL tasks.

### Agent Dispatch Summary

- **1**: **4** — T1-T4 → `quick`
- **2**: **6** — T5-T10 → `quick`
- **3**: **7** — T11-T17 → `quick`
- **4**: **3** — T18-T20 → `quick`
- **5**: **4** — T21-T24 → `quick`
- **6**: **13** — T25-T37 → `quick`
- **7**: **4** — F1-F4 → `quick`

---

## TODOs

> **IMPLEMENTATION + TEST = ONE Task. Never separate.**
> **EVERY task MUST have: Recommended Agent Profile + Parallelization info + QA Scenarios.**
> **A task WITHOUT QA Scenarios is INCOMPLETE. No exceptions.**

- [x] 1. 修复hoist_field.rs编译错误

  **What to do**:
  - 定位到connect-rust-new/connect-transforms/src/hoist_field.rs:304
  - 修复缺少的右括号：`self.clone_box(field(field_value)));`
  - 运行cargo check验证编译通过

  **Must NOT do**:
  - 不要修改其他代码
  - 不要添加不必要的变更

  **Recommended Agent Profile**:
  > Select category + skills based on task domain. Justify each choice.
  - **Category**: `quick`
    - Reason: 简单的语法修复，快速完成
  - **Skills**: `[]`
    - No special skills needed for this task
  - **Skills Evaluated but Omitted**:
    - No skills needed

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 2, 3, 4)
  - **Blocks**: Tasks 5-10
  - **Blocked By**: None (can start immediately)

  **References** (CRITICAL - Be Exhaustive):

  > The executor has NO context from your interview. References are their ONLY guide.
  > Each reference must answer: "What should I look at and WHY?"

  **Pattern References** (existing code to follow):
  - `connect-rust-new/connect-transforms/src/hoist_field.rs:295-315` - 错误上下文

  **API/Type References** (contracts to implement against):
  - N/A

  **Test References** (testing patterns to follow):
  - N/A

  **External References** (libraries and frameworks):
  - N/A

  **WHY Each Reference Matters** (explain the relevance):
  - 错误位置的上下文帮助理解需要修复的代码

  **Acceptance Criteria**:

  > **AGENT-EXECUTABLE VERIFICATION ONLY** — No human action permitted.
  > Every criterion MUST be verifiable by running a command or using a tool.

  **If TDD (tests enabled)**:
  - [ ] 修复后cargo check通过

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
  Scenario: 编译验证
    Tool: Bash
    Preconditions: 代码已修复
    Steps:
      1. cd connect-rust-new/connect-transforms
      2. cargo check 2>&1
    Expected Result: 编译成功，无错误
    Failure Indicators: 出现"error:"或"could not compile"
    Evidence: .sisyphus/evidence/task-1-compile.log
  ```

  > **Specificity requirements — every scenario MUST use:**
  > - **Selectors**: Specific CSS selectors (`.login-button`, not "the login button")
  > - **Data**: Concrete test data (`"test@example.com"`, not `"[email]"`)
  > - **Assertions**: Exact values (`text contains "Welcome back"`, not "verify it works")
  > - **Timing**: Wait conditions where relevant (`timeout: 10s`)
  > - **Negative**: At least ONE failure/error scenario per task
  >
  > **Anti-patterns (your scenario is INVALID if it looks like this):**
  > - ❌ "Verify it works correctly" — HOW? What does "correctly" mean?
  > - ❌ "Check the API returns data" — WHAT data? What fields? What values?
  > - ❌ "Test the component renders" — WHERE? What selector? What content?
  > - ❌ Any scenario without an evidence path

  **Evidence to Capture**:
  - [ ] 编译日志: task-1-compile.log

  **Commit**: NO
  - This is a quick fix, will commit with related changes

---

- [x] 2. 完成extract_field.rs的unimplemented

  **What to do**:
  - 定位到connect-rust-new/connect-transforms/src/extract_field.rs:184和188
  - 参考Java的ExtractField.java实现完成这两个unimplemented!()
  - 确保逻辑1:1对应Java实现
  - 运行cargo check验证编译通过

  **Must NOT do**:
  - 不要修改其他未标记为unimplemented的代码
  - 不要跳过任何逻辑细节

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: 完成未实现的部分，逻辑已在Java中定义
  - **Skills**: `[]`
    - No special skills needed

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 1, 3, 4)
  - **Blocks**: Tasks 5-10
  - **Blocked By**: None (can start immediately)

  **References** (CRITICAL - Be Exhaustive):

  **Pattern References** (existing code to follow):
  - `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/ExtractField.java` - Java实现参考
  - `connect-rust-new/connect-transforms/src/extract_field.rs:180-195` - 需要完成的代码上下文

  **API/Type References** (contracts to implement against):
  - `connect-api/src/data.rs` - Schema和Struct类型定义

  **Test References** (testing patterns to follow):
  - `connect/transforms/src/test/java/org/apache/kafka/connect/transforms/ExtractFieldTest.java` - Java测试参考

  **External References** (libraries and frameworks):
  - N/A

  **WHY Each Reference Matters** (explain the relevance):
  - Java实现提供准确的逻辑参考
  - Rust代码上下文帮助理解需要完成的函数签名

  **Acceptance Criteria**:

  **If TDD (tests enabled)**:
  - [ ] 完成后cargo check通过
  - [ ] 无unimplemented!()标记

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  ```
  Scenario: 编译验证
    Tool: Bash
    Preconditions: 代码已完成
    Steps:
      1. cd connect-rust-new/connect-transforms
      2. cargo check 2>&1
    Expected Result: 编译成功，无错误
    Failure Indicators: 出现"error:"或"unimplemented!"
    Evidence: .sisyphus/evidence/task-2-compile.log

  Scenario: 无unimplemented检查
    Tool: Bash
    Preconditions: 代码已完成
    Steps:
      1. cd connect-rust-new/connect-transforms/src
      2. grep -n "unimplemented!" extract_field.rs || echo "No unimplemented found"
    Expected Result: 无unimplemented!()标记
    Failure Indicators: 输出包含"unimplemented!"
    Evidence: .sisyphus/evidence/task-2-no-unimplemented.log
  ```

  **Evidence to Capture**:
  - [ ] 编译日志: task-2-compile.log
  - [ ] unimplemented检查: task-2-no-unimplemented.log

  **Commit**: NO

---

- [x] 3. 完成flatten.rs的unimplemented

  **What to do**:
  - 定位到connect-rust-new/connect-transforms/src/flatten.rs:301和305
  - 参考Java的Flatten.java实现完成这两个unimplemented!()
  - 确保逻辑1:1对应Java实现
  - 运行cargo check验证编译通过

  **Must NOT do**:
  - 不要修改其他未标记为unimplemented的代码

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: 完成未实现的部分，逻辑已在Java中定义
  - **Skills**: `[]`
    - No special skills needed

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 1, 2, 4)
  - **Blocks**: Tasks 5-10
  - **Blocked By**: None (can start immediately)

  **References** (CRITICAL - Be Exhaustive):

  **Pattern References** (existing code to follow):
  - `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/Flatten.java` - Java实现参考
  - `connect-rust-new/connect-transforms/src/flatten.rs:295-310` - 需要完成的代码上下文

  **API/Type References** (contracts to implement against):
  - `connect-api/src/data.rs` - Schema和Struct类型定义

  **Test References** (testing patterns to follow):
  - `connect/transforms/src/test/java/org/apache/kafka/connect/transforms/FlattenTest.java` - Java测试参考

  **External References** (libraries and frameworks):
  - N/A

  **WHY Each Reference Matters** (explain the relevance):
  - Java实现提供准确的逻辑参考

  **Acceptance Criteria**:

  **If TDD (tests enabled)**:
  - [ ] 完成后cargo check通过
  - [ ] 无无implemented!()标记

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  ```
  Scenario: 编译验证
    Tool: Bash
    Preconditions: 代码已完成
    Steps:
      1. cd connect-rust-new/connect-transforms
      2. cargo check 2>&1
    Expected Result: 编译成功，无错误
    Failure Indicators: 出现"error:"或"unimplemented!"
    Evidence: .sisyphus/evidence/task-3-compile.log

  Scenario: 无unimplemented检查
    Tool: Bash
    Preconditions: 代码已完成
    Steps:
      1. cd connect-rust-new/connect-transforms/src
      2. grep -n "unimplemented!" flatten.rs || echo "No unimplemented found"
    Expected Result: 无unimplemented!()标记
    Failure Indicators: 输出包含"unimplemented!"
    Evidence: .sisyphus/evidence/task-3-no-unimplemented.log
  ```

  **Evidence to Capture**:
  - [ ] 编译日志: task-3-compile.log
  - [ ] unimplemented检查: task-3-no-unimplemented.log

  **Commit**: NO

---

- [x] 4. 完成header_from.rs的unimplemented

  **What to do**:
  - 定位到connect-rust-new/connect-transforms/src/header_from.rs:361和365
  - 参考Java的HeaderFrom.java实现完成这两个unimplemented!()
  - 确保逻辑1:1对应Java实现
  - 运行cargo check验证编译通过

  **Must NOT do**:
  - 不要修改其他未标记为unimplemented的代码

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: 完成未实现的部分，逻辑已在Java中定义
  - **Skills**: `[]`
    - No special skills needed

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 1, 2, 3)
  - **Blocks**: Tasks 5-10
  - **Blocked By**: None (can start immediately)

  **References** (CRITICAL - Be Exhaustive):

  **Pattern References** (existing code to follow):
  - `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/HeaderFrom.java` - Java实现参考
  - `connect-rust-new/connect-transforms/src/header_from.rs:355-370` - 需要完成的代码上下文

  **API/Type References** (contracts to implement against):
  - `connect-api/src/data.rs` - Schema和Struct类型定义

  **Test References** (testing patterns to follow):
  - `connect/transforms/src/test/java/org/apache/kafka/connect/transforms/HeaderFromTest.java` - Java测试参考

  **External References** (libraries and frameworks):
  - N/A

  **WHY Each Reference Matters** (explain the relevance):
  - Java实现提供准确的逻辑参考

  **Acceptance Criteria**:

  **If TDD (tests enabled)**:
  - [ ] 完成后cargo check通过
  - [ ] 无unimplemented!()标记

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  ```
  Scenario: 编译验证
    Tool: Bash
    Preconditions: 代码已完成
    Steps:
      1. cd connect-rust-new/connect-transforms
      2. cargo check 2>&1
    Expected Result: 编译成功，无错误
    Failure Indicators: 出现"error:"或"unimplemented!"
    Evidence: .sisyphus/evidence/task-4-compile.log

  Scenario: 无unimplemented检查
    Tool: Bash
    Preconditions: 代码已完成
    Steps:
      1. cd connect-rust-new/connect-transforms/src
      2. grep -n "unimplemented!" header_from.rs || echo "No unimplemented found"
    Expected Result: 无unimplemented!()标记
    Failure Indicators: 输出包含"unimplemented!"
    Evidence: .sisyphus/evidence/task-4-no-unimplemented.log
  ```

  **Evidence to Capture**:
  - [ ] 编译日志: task-4-compile.log
  - [ ] unimplemented检查: task-4-no-unimplemented.log

  **Commit**: YES
  - Message: `fix(transforms): 修复编译错误和完成unimplemented实现`
  - Files: `connect-rust-new/connect-transforms/src/*.rs`
  - Pre-commit: `cargo check`

---

- [x] 5. 实现LRUCache和Cache trait（在kafka-clients-trait中）

  **What to do**:
  - 在kafka-clients-trait/src/cache.rs中实现Cache trait
  - 实现LRUCache<K, V>结构体
  - 实现SynchronizedCache线程安全包装
  - 在kafka-clients-trait/src/lib.rs中导出
  - 在Cargo.toml中添加lru和linked-hash-map依赖

  **Must NOT do**:
  - 不要实现复杂的缓存淘汰策略，使用LRU即可
  - 不要过度优化，先保证功能正确

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: 标准LRU缓存实现
  - **Skills**: `[]`

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2 (with Tasks 6-10)
  - **Blocks**: Tasks 11-17
  - **Blocked By**: Tasks 1-4

  **References** (CRITICAL - Be Exhaustive):

  **Pattern References** (existing code to follow):
  - `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/Cast.java:97-105` - Java中cache的使用方式

  **API/Type References** (contracts to implement against):
  - N/A

  **Test References** (testing patterns to follow):
  - N/A

  **External References** (libraries and frameworks):
  - https://docs.rs/lru/latest/lru/ - LRU缓存crate文档

  **WHY Each Reference Matters** (explain the relevance):
  - Java使用方式帮助理解接口需求

  **Acceptance Criteria**:

  **If TDD (tests enabled)**:
  - [ ] cargo check通过
  - [ ] Cache trait定义完整
  - [ ] LRUCache实现完整

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  ```
  Scenario: 编译验证
    Tool: Bash
    Preconditions: 代码已实现
    Steps:
      1. cd connect-rust-new/kafka-clients-trait
      2. cargo check 2>&1
    Expected Result: 编译成功，无错误
    Failure Indicators: 出现"error:"
    Evidence: .sisyphus/evidence/task-5-compile.log

  Scenario: 基本功能测试
    Tool: Bash
    Preconditions: 代码已实现
    Steps:
      1. cd connect-rust-new/kafka-clients-trait
      2. cargo test cache 2>&1 | head -20
    Expected Result: 测试可以运行（可能失败，但可以编译）
    Failure Indicators: 编译错误
    Evidence: .sisyphus/evidence/task-5-test.log
  ```

  **Evidence to Capture**:
  - [ ] 编译日志: task-5-compile.log
  - [ ] 测试日志: task-5-test.log

  **Commit**: NO

---

- [x] 6. 实现Base64工具类（在kafka-clients-trait中）

  **What to do**:
  - 在kafka-clients-trait/src/base64.rs中实现encode和decode函数
  - 使用base64 crate
  - 在kafka-clients-trait/src/lib.rs中导出
  - 在Cargo.toml中添加base64依赖

  **Must NOT do**:
  - 不要实现复杂的编码选项，使用默认配置即可

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: 简单的Base64封装
  - **Skills**: `[]`

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2 (with Tasks 5, 7-10)
  - **Blocks**: Tasks 11-17
  - **Blocked By**: Tasks 1-4

  **References** (CRITICAL - Be Exhaustive):

  **Pattern References** (existing code to follow):
  - `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/Cast.java:347-355` - Java中Base64的使用

  **API/Type References** (contracts to implement against):
  - N/A

  **Test References** (testing patterns to follow):
  - N/A

  **External References** (libraries and frameworks):
  - https://docs.rs/base64/latest/base64/ - Base64 crate文档

  **WHY Each Reference Matters** (explain the relevance):
  - Java使用方式帮助理解API需求

  **Acceptance Criteria**:

  **If TDD (tests enabled)**:
  - [ ] cargo check通过
  - [ ] encode和decode函数实现

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  ```
  Scenario: 编译验证
    Tool: Bash
    Preconditions: 代码已实现
    Steps:
      1. cd connect-rust-new/kafka-clients-trait
      2. cargo check 2>&1
    Expected Result: 编译成功，无错误
    Failure Indicators: 出现"error:"
    Evidence: .sisyphus/evidence/task-6-compile.log
  ```

  **Evidence to Capture**:
  - [ ] 编译日志: task-6-compile.log

  **Commit**: NO

---

- [x] 7. 实现ByteBuffer工具类（在kafka-clients-trait中）

  **What to do**:
  - 在kafka-clients-trait/src/byte_buffer.rs中实现ByteBuffer结构体
  - 实现allocate、wrap、get、put、array等方法
  - 在kafka-clients-trait/src/lib.rs中导出

  **Must NOT do**:
  - 不要实现所有Java ByteBuffer的方法，只实现transforms模块需要的

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: 简单的字节缓冲区实现
  - **Skills**: `[]`

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2 (with Tasks 5-6, 8-10)
  - **Blocks**: Tasks 11-17
  - **Blocked By**: Tasks 1-4

  **References** (CRITICAL - Be Exhaustive):

  **Pattern References** (existing code to follow):
  - `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/Cast.java:340-345` - Java中ByteBuffer的使用

  **API/Type References** (contracts to implement against):
  - N/A

  **Test References** (testing patterns to follow):
  - N/A

  **External References** (libraries and frameworks):
  - N/A

  **WHY Each Reference Matters** (explain the relevance):
  - Java使用方式帮助理解API需求

  **Acceptance Criteria**:

  **If TDD (tests enabled)**:
  - [ ] cargo check通过
  - [ ] ByteBuffer结构体实现

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  ```
  Scenario: 编译验证
    Tool: Bash
    Preconditions: 代码已实现
    Steps:
      1. cd connect-rust-new/kafka-clients-trait
      2. cargo check 2>&1
    Expected Result: 编译成功，无错误
    Failure Indicators: 出现"error:"
    Evidence: .sisyphus/evidence/task-7-compile.log
  ```

  **Evidence to Capture**:
  - [ ] 编译日志: task-7-compile.log

  **Commit**: NO

---

- [ ] 8. 实现Pattern和Matcher工具类（在kafka-clients-trait中）

  **What to do**:
  - 在kafka-clients-trait/src/regex.rs中实现Pattern和Matcher结构体
  - 使用regex crate
  - 实现compile、matcher、matches、replace_first、replace_all等方法
  - 在kafka-clients-trait/src/lib.rs中导出
  - 在Cargo.toml中添加regex依赖

  **Must NOT do**:
  - 不要实现复杂的正则功能，只实现transforms模块需要的

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: 正则表达式封装
  - **Skills**: `[]`

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2 (with Tasks 5-7, 9-10)
  - **Blocks**: Tasks 11-17
  - **Blocked By**: Tasks 1-4

  **References** (CRITICAL - Be Exhaustive):

  **Pattern References** (existing code to follow):
  - `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/RegexRouter.java:95-105` - Java中Pattern的使用

  **API/Type References** (contracts to implement against):
  - N/A

  **Test References** (testing patterns to follow):
  - N/A

  **External References** (libraries and frameworks):
  - https://docs.rs/regex/latest/regex/ - Regex crate文档

  **WHY Each Reference Matters** (explain the relevance):
  - Java使用方式帮助理解API
  - Regex crate文档提供实现参考

  **Acceptance Criteria**:

  **If TDD (tests enabled)**:
  - [ ] cargo check通过
  - [ ] Pattern和Matcher实现

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  ```
  Scenario: 编译验证
    Tool: Bash
    Preconditions: 代码已实现
    Steps:
      1. cd connect-rust-new/kafka-clients-trait
      2. cargo check 2>&1
    Expected Result: 编译成功，无错误
    Failure Indicators: 出现"error:"
    Evidence: .sisyphus/evidence/task-8-compile.log
  ```

  **Evidence to Capture**:
  - [ ] 编译日志: task-8-compile.log

  **Commit**: NO

---

- [ ] 9. 实现Logger工具类（在kafka-clients-trait中）

  **What to do**:
  - 在kafka-clients-trait/src/logger.rs中实现Logger结构体
  - 使用Rust的log crate封装
  - 实现get_logger、info、debug、warn、error等方法
  - 在kafka-clients-trait/src/lib.rs中导出
  - 在Cargo.toml中添加log依赖

  **Must NOT do**:
  - 不要实现复杂的日志配置，使用默认配置即可

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: 简单的日志封装
  - **Skills**: `[]`

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2 (with Tasks 5-8, 10)
  - **Blocks**: Tasks 11-17
  - **Blocked By**: Tasks 1-4

  **References** (CRITICAL - Be Exhaustive):

  **Pattern References** (existing code to follow):
  - `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/Cast.java:59` - Java中Logger的使用

  **API/Type References** (contracts to implement against):
  - N/A

  **Test References** (testing patterns to follow):
  - N/A

  **External References** (libraries and frameworks):
  - https://docs.rs/log/latest/log/ - Log crate文档

  **WHY Each Reference Matters** (explain the relevance):
  - Java使用方式帮助理解API需求

  **Acceptance Criteria**:

  **If TDD (tests enabled)**:
  - [ ] cargo check通过
  - [ ] Logger实现

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  ```
  Scenario: 编译验证
    Tool: Bash
    Preconditions: 代码已实现
    Steps:
      1. cd connect-rust-new/kafka-clients-trait
      2. cargo check 2>&1
    Expected Result: 编译成功，无错误
    Failure Indicators: 出现"error:"
    Evidence: .sisyphus/evidence/task-9-compile.log
  ```

  **Evidence to Capture**:
  - [ ] 编译日志: task-9-compile.log

  **Commit**: NO

---

- [ ] 10. 实现AppInfoParser工具类（在kafka-clients-trait中）

  **What to do**:
  - 在kafka-clients-trait/src/utils.rs中实现app_info_parser函数
  - 返回固定的Kafka Connect版本字符串
  - 在kafka-clients-trait/src/lib.rs中导出

  **Must NOT do**:
  - 不要实现复杂的版本解析，返回固定字符串即可

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: 非常简单的版本字符串返回
  - **Skills**: `[]`

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2 (with Tasks 5-9)
  - **Blocks**: Tasks 11-17
  - **Blocked By**: Tasks 1-4

  **References** (CRITICAL - Be Exhaustive):

  **Pattern References** (existing code to follow):
  - `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/Cast.java:401-405` - Java中AppInfoParser的使用

  **API/Type References** (contracts to implement against):
  - N/A

  **Test References** (testing patterns to follow):
  - N/A

  **External References** (libraries and frameworks):
  - N/A

  **WHY Each Reference Matters** (explain the relevance):
  - Java使用方式帮助理解需求

  **Acceptance Criteria**:

  **If TDD (tests enabled)**:
  - [ ] cargo check通过
  - [ ] app_info_parser函数实现

  **QA Scenarios (MANDATORY — task is INCOMPLETE without these):**

  ```
  Scenario: 编译验证
    Tool: Bash
    Preconditions: 代码已实现
    Steps:
      1. cd connect-rust-new/kafka-clients-trait
      2. cargo check 2>&1
    Expected Result: 编译成功，无错误
    Failure Indicators: 出现"error:"
    Evidence: .sisyphus/evidence/task-10-compile.log
  ```

  **Evidence to Capture**:
  - [ ] 编译日志: task-10-compile.log

  **Commit**: YES
  - Message: `feat(clients-trait): 实现外部依赖工具类`
  - Files: `kafka-clients-trait/src/*.rs, kafka-clients-trait/Cargo.toml`
  - Pre-commit: `cargo check`

---

- [ ] 11. 实现FieldSyntaxVersion枚举

  **What to do**:
  - 在connect-rust-new/connect-transforms/src/field.rs中实现FieldSyntaxVersion枚举
  - 实现V0, V1, V2枚举值
  - 参考Java: connect/transforms/src/main/java/org/apache/kafka/connect/transforms/field/FieldSyntaxVersion.java

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: `[]`

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 3 (with Tasks 12-17)
  - **Blocks**: Tasks 18-20
  - **Blocked By**: Tasks 5-10

  **References**:
  - `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/field/FieldSyntaxVersion.java` - Java枚举定义

  **Acceptance Criteria**:
  - [ ] cargo check通过
  - [ ] 枚举值完整

  **QA Scenarios**:
  ```
  Scenario: 编译验证
    Tool: Bash
    Steps:
      1. cd connect-rust-new/connect-transforms && cargo check 2>&1
    Expected Result: 编译成功
    Failure Indicators: 出现"error:"
    Evidence: .sisyphus/evidence/task-11-compile.log
  ```

  **Commit**: NO

---

- [ ] 12. 实现SingleFieldPath

  **What to do**:
  - 在connect-rust-new/connect-transforms/src/field.rs中实现SingleFieldPath结构体
  - 实现字段路径解析逻辑
  - 参考Java: connect/transforms/src/main/java/org/apache/kafka/connect/transforms/field/SingleFieldPath.java

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: `[]`

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 3 (with Tasks 11, 13-17)
  - **Blocks**: Tasks 18-20
  - **Blocked By**: Tasks 5-10

  **References**:
  - `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/field/SingleFieldPath.java` - Java实现

  **Acceptance Criteria**:
  - [ ] cargo check通过
  - [ ] 字段路径解析逻辑完整

  **QA Scenarios**:
  ```
  Scenario: 编译验证
    Tool: Bash
    Steps:
      1. cd connect-rust-new/connect-transforms && cargo check 2>&1
    Expected Result: 编译成功
    Failure Indicators: 出现"error:"
    Evidence: .sisyphus/evidence/task-12-compile.log
  ```

  **Commit**: NO

---

- [ ] 13. 实现NonEmptyListValidator

  **What to do**:
  - 在connect-rust-new/connect-transforms/src/util.rs中实现NonEmptyListValidator结构体
  - 实现验证列表非空的逻辑
  - 参考Java: connect/transforms/src/main/java/org/apache/kafka/connect/transforms/util/NonEmptyListValidator.java

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: `[]`

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 3 (with Tasks 11-12, 14-17)
  - **Blocks**: Tasks 18-20
  - **Blocked By**: Tasks 5-10

  **References**:
  - `connect/transforms/src/main/java/org/org/apache/kafka/connect/transforms/util/NonEmptyListValidator.java` - Java实现

  **Acceptance Criteria**:
  - [ ] cargo check通过
  - [ ] 验证逻辑完整

  **QA Scenarios**:
  ```
  Scenario: 编译验证
    Tool: Bash
    Steps:
      1. cd connect-rust-new/connect-transforms && cargo check 2>&1
    Expected Result: 编译成功
    Failure Indicators: 出现"error:"
    Evidence: .sisyphus/evidence/task-13-compile.log
  ```

  **Commit**: NO

---

- [ ] 14. 实现RegexValidator

  **What to do**:
  - 在connect-rust-new/connect-transforms/src/util.rs中实现RegexValidator结构体
  - 实现正则表达式验证逻辑
  - 参考Java: connect/transforms/src/main/java/org/apache/kafka/connect/transforms/util/RegexValidator.java

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: `[]`

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 3 (with Tasks 11-13, 15-17)
  - **Blocks**: Tasks 18-20
  - **Blocked By**: Tasks 5-10

  **References**:
  - `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/util/RegexValidator.java` - Java实现

  **Acceptance Criteria**:
  - [ ] cargo check通过
  - [ ] 正则验证逻辑完整

  **QA Scenarios**:
  ```
  Scenario: 编译验证
    Tool: Bash
    Steps:
      1. cd connect-rust-new/connect-transforms && cargo check 2>&1
    Expected Result: 编译成功
    Failure Indicators: 出现"error:"
    Evidence: .sisyphus/evidence/task-14-compile.log
  ```

  **Commit**: NO

---

- [ ] 15. 实现Requirements工具类

  **What to do**:
  - 在connect-rust-new/connect-transforms/src/util.rs中实现Requirements结构体
  - 实现requireMap, requireStruct等方法
  - 参考Java: connect/transforms/src/main/java/org/apache/kafka/connect/transforms/util/Requirements.java

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: `[]`

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 3 (with Tasks 11-14, 16-17)
  - **Blocks**: Tasks 18-20
  - **Blocked By**: Tasks 5-10

  **References**:
  - `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/util/Requirements.java` - Java实现

  **Acceptance Criteria**:
  - [ ] cargo check通过
  - [ ] 所有require方法实现

  **QA Scenarios**:
  ```
  Scenario: 编译验证
    Tool: Bash
    Steps:
      1. cd connect-rust-new/connect-transforms && cargo check 2>&1
    Expected Result: 编译成功
    Failure Indicators: 出现"error:"
    Evidence: .sisyphus/evidence/task-15-compile.log
  ```

  **Commit**: NO

---

- [ ] 16. 实现SchemaUtil工具类

  **What to do**:
  - 在connect-rust-new/connect-transforms/src/util.rs中实现SchemaUtil结构体
  - 实现Schema复制和更新工具方法
  - 参考Java: connect/transforms/src/main/java/org/apache/kafka/connect/transforms/util/SchemaUtil.java

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: `[]`

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 3 (with Tasks 11-15, 17)
  - **Blocks**: Tasks 18-20
  - **Blocked By**: Tasks 5-10

  **References**:
  - `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/util/SchemaUtil.java` - Java实现

  **Acceptance Criteria**:
  - [ ] cargo check通过
  - [ ] Schema工具方法完整

  **QA Scenarios**:
  ```
  Scenario: 编译验证
    Tool: Bash
    Steps:
      1. cd connect-rust-new/connect-transforms && cargo check 2>&1
    Expected Result: 编译成功
    Failure Indicators: 出现"error:"
    Evidence: .sisyphus/evidence/task-16-compile.log
  ```

  **Commit**: NO

---

- [ ] 17. 实现SimpleConfig工具类

  **What to do**:
  - 在connect-rust-new/connect-transforms/src/util.rs中实现SimpleConfig结构体
  - 实现简单配置封装
  - 参考Java: connect/transforms/src/main/java/org/apache/kafka/connect/transforms/util/SimpleConfig.java

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: `[]`

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 3 (with Tasks 11-16)
  - **Blocks**: Tasks 18-20
  - **Blocked By**: Tasks 5-10

  **References**:
  - `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/util/SimpleConfig.java` - Java实现

  **Acceptance Criteria**:
  - [ ] cargo check通过
  - [ ] 配置封装完整

  **QA Scenarios**:
  ```
  Scenario: 编译验证
    Tool: Bash
    Steps:
      1. cd connect-rust-new/connect-transforms && cargo check 2>&1
    Expected Result: 编译成功
    Failure Indicators: 出现"error:"
    Evidence: .sisyphus/evidence/task-17-compile.log
  ```

  **Commit**: YES
  - Message: `feat(transforms): 补充工具类`
  - Files: `connect-rust-new/connect-transforms/src/util.rs, connect-rust-new/connect-transforms/src/field.rs`
  - Pre-commit: `cargo check`

---

## Wave 4: 补充Predicate类（3个任务）

- [ ] 18. 实现HasHeaderKey

  **What to do**:
  - 在connect-rust-new/connect-transforms/src/predicate.rs中实现HasHeaderKey结构体
  - 实现检查header key是否存在的逻辑
  - 参考Java: connect/transforms/src/main/java/org/apache/kafka/connect/transforms/predicates/HasHeaderKey.java

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: `[]`

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 4 (with Tasks 19-20)
  - **Blocks**: Tasks 21-24
  - **Blocked By**: Tasks 11-17

  **References**:
  - `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/predicates/HasHeaderKey.java` - Java实现

  **Acceptance Criteria**:
  - [ ] cargo check通过
  - [ ] Predicate trait实现完整

  **QA Scenarios**:
  ```
  Scenario: 编译验证
    Tool: Bash
    Steps:
      1. cd connect-rust-new/connect-transforms && cargo check 2>&1
    Expected Result: 编译成功
    Failure Indicators: 出现"error:"
    Evidence: .sisyphus/evidence/task-18-compile.log
  ```

  **Commit**: NO

---

- [ ] 19. 实现RecordIsTombstone

  **What to do**:
  - 在connect-rust-new/connect-transforms/src/predicate.rs中实现RecordIsTombstone结构体
  - 实现检查是否为墓碑记录的逻辑
  - 参考Java: connect/transforms/src/main/java/org/apache/kafka/connect/transforms/predicates/RecordIsTombstone.java

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: `[]`

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 4 (with Tasks 18, 20)
  - **Blocks**: Tasks 21-24
  - **Blocked By**: Tasks 11-17

  **References**:
  - `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/predicates/RecordIsTombstone.java` - Java实现

  **Acceptance Criteria**:
  - [ ] cargo check通过
  - [ ] Predicate trait实现完整

  **QA Scenarios**:
  ```
  Scenario: 编译验证
    Tool: Bash
    Steps:
      1. cd connect-rust-new/connect-transforms && cargo check 2>&1
    Expected Result: 编译成功
    Failure Indicators: 出现"error:"
    Evidence: .sisyphus/evidence/task-19-compile.log
  ```

  **Commit**: NO

---

- [ ] 20. 实现TopicNameMatches

  **What to do**:
  - 在connect-rust-new/connect-transforms/src/predicate.rs中实现TopicNameMatches结构体
  - 实现检查topic名称匹配的逻辑
  - 参考Java: connect/transforms/src/main/java/org/apache/kafka/connect/transforms/predicates/TopicNameMatches.java

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: `[]`

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 4 (with Tasks 18-19)
  - **Blocks**: Tasks 21-24
  - **Blocked By**: Tasks 11-17

  **References**:
  - `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/predicates/TopicNameMatches.java` - Java实现

  **Acceptance Criteria**:
  - [ ] cargo check通过
  - [ ] Predicate trait实现完整

  **QA Scenarios**:
  ```
  Scenario: 编译验证
    Tool: Bash
    Steps:
      1. cd connect-rust-new/connect-transforms && cargo check 2>&1
    Expected Result: 编译成功
    Failure Indicators: 出现"error:"
    Evidence: .sisyphus/evidence/task-20-compile.log
  ```

  **Commit**: YES
  - Message: `feat(transforms): 补充Predicate类`
  - Files: `connect-rust-new/connect-transforms/src/predicate.rs`
  - Pre-commit: `cargo check`

---

## Wave 5: 补充Transformation类（4个任务）

- [ ] 21. 实现DropHeaders

  **What to do**:
  - 创建connect-rust-new/connect-transforms/src/drop_headers.rs
  - 实现删除header转换器
  - 在lib.rs中导出
  - 参考Java: connect/transforms/src/main/java/org/apache/kafka/connect/transforms/DropHeaders.java

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: `[]`

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 5 (with Tasks 22-24)
  - **Blocks**: Tasks 25-37
  - **Blocked By**: Tasks 18-20

  **References**:
  - `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/DropHeaders.java` - Java实现
  - `connect-rust-new/connect-transforms/src/insert_header.rs` - 类似转换器参考

  **Acceptance Criteria**:
  - [ ] cargo check通过
  - [ ] Transformation trait实现完整

  **QA Scenarios**:
  ```
  Scenario: 编译验证
    Tool: Bash
    Steps:
      1. cd connect-rust-new/connect-transforms && cargo check 2>&1
    Expected Result: 编译成功
    Failure Indicators: 出现"error:"
    Evidence: .sisyphus/evidence/task-21-compile.log
  ```

  **Commit**: NO

---

- [ ] 22. 实现InsertHeader

  **What to do**:
  - 创建connect-rust-new/connect-transforms/src/insert_header.rs
  - 实现插入header转换器
  - 在lib.rs中导出
  - 参考Java: connect/transforms/src/main/java/org/apache/kafka/connect/transforms/InsertHeader.java

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: `[]`

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 5 (with Tasks 21, 23-24)
  - **Blocks**: Tasks 25-37
  - **Blocked By**: Tasks 18-20

  **References**:
  - `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/InsertHeader.java` - Java实现
  - `connect-rust-new/connect-transforms/src/insert_field.rs` - 类似转换器参考

  **Acceptance Criteria**:
  - [ ] cargo check通过
  - [ ] Transformation trait实现完整

  **QA Scenarios**:
  ```
  Scenario: 编译验证
    Tool: Bash
    Steps:
      1. cd connect-rust-new/connect-transforms && cargo check 2>&1
    Expected Result: 编译成功
    Failure Indicators: 出现"error:"
    Evidence: .sisyphus/evidence/task-22-compile.log
  ```

  **Commit**: NO

---

- [ ] 23. 实现TimestampRouter

  **What to do**:
  - 创建connect-rust-new/connect-transforms/src/timestamp_router.rs
  - 实现时间戳路由转换器
  - 在lib.rs中导出
  - 参考Java: connect/transforms/src/main/java/org/apache/kafka/connect/transforms/TimestampRouter.java

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: `[]`

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 5 (with Tasks 21-22, 24)
  - **Blocks**: Tasks 25-37
  - **Blocked By**: Tasks 18-20

  **References**:
  - `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/TimestampRouter.java` - Java实现
  - `connect-rust-new/connect-transforms/src/regex_router.rs` - 类似路由器参考

  **Acceptance Criteria**:
  - [ ] cargo check通过
  - [ ] Transformation trait实现完整

  **QA Scenarios**:
  ```
  Scenario: 编译验证
    Tool: Bash
    Steps:
      1. cd connect-rust-new/connect-transforms && cargo check 2>&1
    Expected Result: 编译成功
    Failure Indicators: 出现"error:"
    Evidence: .sisyphus/evidence/task-23-compile.log
  ```

  **Commit**: NO

---

- [ ] 24. 实现ValueToKey

  **What to do**:
  - 创建connect-rust-new/connect-transforms/src/value_to_key.rs
  - 实现值转键转换器
  - 在lib.rs中导出
  - 参考Java: connect/transforms/src/main/java/org/apache/kafka/connect/transforms/ValueToKey.java

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: `[]`

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 5 (with Tasks 21-23)
  - **Blocks**: Tasks 25-37
  - **Blocked By**: Tasks 18-20

  **References**:
  - `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/ValueToKey.java` - Java实现
  - `connect-rust-new/connect-transforms/src/extract_field.rs` - 类似转换器参考

  **Acceptance Criteria**:
  - [ ] cargo check通过
  - [ ] Transformation trait实现完整

  **QA Scenarios**:
  ```
  Scenario: 编译验证
    Tool: Bash
    Steps:
      1. cd connect-rust-new/connect-transforms && cargo check 2>&1
    Expected Result: 编译成功
    Failure Indicators: 出现"error:"
    Evidence: .sisyphus/evidence/task-24-compile.log
  ```

  **Commit**: YES
  - Message: `feat(transforms): 补充Transformation类`
  - Files: `connect-rust-new/connect-transforms/src/*.rs, connect-rust-new/connect-transforms/src/lib.rs`
  - Pre-commit: `cargo check`

---

## Wave 6: 补充测试文件（13个任务）

由于测试文件数量较多，这里使用批量描述。每个测试文件应：
1. 参考对应的Java测试文件
2. 使用cargo test框架
3. 覆盖正常流程和边缘情况
4. 确保测试通过

**测试文件清单**：
- Task 25: drop_headers_test.rs (参考DropHeadersTest.java)
- Task 26: insert_header_test.rs (参考InsertHeaderTest.java)
- Task 27: timestamp_router_test.rs (参考TimestampRouterTest.java)
- Task 28: value_to_key_test.rs (参考ValueToKeyTest.java)
- Task 29: has_header_key_test.rs (参考HasHeaderKeyTest.java)
- Task 30: record_is_tombstone_test.rs (参考RecordIsTombstoneTest.java)
- Task 31: topic_name_matches_test.rs (参考TopicNameMatchesTest.java)
- Task 32: field_syntax_version_test.rs (参考FieldSyntaxVersionTest.java)
- Task 33: single_field_path_test.rs (参考SingleFieldPathTest.java)
- Task 34: non_empty_list_validator_test.rs (参考NonEmptyListValidatorTest.java)
- Task 35: requirements_test.rs (参考RequirementsTest.java)
- Task 36: schema_util_test.rs (参考SchemaUtilTest.java)
- Task 37: simple_config_test.rs (参考SimpleConfigTest.java)

**实现说明**：
- 所有测试文件位于connect-rust-new/connect-transforms/tests/
- 每个测试文件至少包含5个测试用例
- 使用#[cfg(test)]和#[test]属性
- 使用assert!宏进行断言
- 参考已有的测试文件（如simple_test.rs）的格式

**并行执行**：所有13个测试文件可以并行创建

**提交策略**：所有测试文件完成后统一提交

---

## Final Verification Wave ( (MANDATORY — after ALL implementation tasks)

> 4 review agents run in PARALLEL. ALL must APPROVE. Present consolidated results to user and get explicit "okay" before completing.
>
> **Do NOT auto-proceed after verification. Wait for user's explicit approval before marking work complete.**
> **Never mark F1-F4 as checked before getting user's okay.** Rejection or user feedback -> fix -> re-run -> present again -> wait for okay.

- [ ] F1. **编译验证** — `quick`
  运行cargo check验证整个connect-transforms模块编译通过，无错误和警告。
  输出: `Compilation [PASS/FAIL] | Errors [N] | Warnings [N] | VERDICT`

- [ ] F2. **测试验证** — `quick`
  运行cargo test验证所有测试通过。
  输出: `Tests [N pass/N fail] | Coverage [N%] | VERDICT`

- [ ] F3. **完整性验证** — `quick`
  验证Java和Rust的类数量、代码行数对比符合预期。
  检查所有unimplemented!()已移除。
  输出: `Java Classes [26] | Rust Modules [26] | Unimplemented [0] | VERDICT`

- [ ] F4. **生成最终报告** — `quick`
  生成包含完整性和正确性度量的最终报告。
  输出: `Report generated at .sisyphus/reports/final-report.md`
  -> Present results -> Get explicit user okay

---

## Commit Strategy

- **4**: `fix(transforms): 修复编译错误和完成unimplemented实现` — connect-transforms/src/*.rs, cargo check
- **10**: `feat(clients-trait): 实现外部依赖工具类` — kafka-clients-trait/src/*.rs, cargo check
- **17**: `feat(transforms): 补充工具类` — connect-transforms/src/*.rs, cargo check
- **20**: `feat(transforms): 补充Predicate类` — connect-transforms/src/*.rs, cargo check
- **24**: `feat(transforms): 补充Transformation类` — connect-transforms/src/*.rs, cargo check
- **37**: `test(transforms): 补充测试文件` — connect-transforms/tests/*.rs, cargo test

---

## Success Criteria

### Verification Commands
```bash
# 编译验证
cd connect-rust-new/connect-transforms && cargo check

# 测试验证
cd connect-rust-new/connect-transforms && cargo test

# 完整性验证
cd connect-rust-new/connect-transforms/src && ls -1 *.rs | wc -l
cd connect/transforms/src/main/java && find . -name "*.java" | wc -l
```

### Final Checklist
- [ ] 所有"Must Have"已实现
- [ ] 所有"Must NOT Have"已避免
- [ ] 所有测试通过
- [ ] 编译无错误
- [ ] 完整性度量达标
