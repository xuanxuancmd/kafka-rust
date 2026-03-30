# 阶段3：Transforms模块实现

## TL;DR

> **阶段目标**: 实现connect-transforms模块的所有功能
> **依赖**: 阶段1完成（connect-transforms的trait定义已生成）
> **并发度**: 6个任务组可并行
> **预计时间**: 15-25分钟

---

## 一、阶段概述

### 1.1 阶段目标

本阶段的目标是实现connect-transforms模块的所有功能，包括基础设施、字段路径、简单转换器、中等复杂度转换器、复杂转换器、谓词等。

### 1.2 关键输出

1. **完整实现**: 所有Transformation和Predicate的具体实现
2. **编译通过**: `cargo build --release` 成功
3. **测试通过**: `cargo test` 所有单元测试通过

### 1.3 并发策略

本阶段的任务可以**完全并行**执行，无任务间依赖：

```
任务组A（可并行）:
├── 任务3.1：实现基础设施
├── 任务3.2：实现字段路径模块
├── 任务3.3：实现简单转换器
└── 任务3.4：实现中等复杂度转换器

任务组B（可并行）:
├── 任务3.5：实现复杂转换器
└── 任务3.6：实现谓词模块
```

---

## 二、任务清单

### 任务3.1：实现基础设施（可并行）

**目标**: 实现connect-transforms的基础设施

**子任务**:
3.1.1 实现SimpleConfig
3.1.2 实现Requirements工具类
3.1.3 实现SchemaUtil工具类
3.1.4 实现RegexValidator
3.1.5 实现NonEmptyListValidator

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` 工具类测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` 工具类测试通过

**单元测试设计**:
- 测试SimpleConfig
- 测试Requirements验证
- 测试SchemaUtil
- 测试RegexValidator
- 测试NonEmptyListValidator

**预计时间**: 3-5分钟

---

### 任务3.2：实现字段路径模块（可并行）

**目标**: 实现字段路径解析和处理

**子任务**:
3.2.1 实现FieldSyntaxVersion枚举
3.2.2 实现SingleFieldPath

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` 字段路径测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` 字段路径测试通过

**单元测试设计**:
- 测试FieldSyntaxVersion
- 测试SingleFieldPath解析

**预计时间**: 2-3分钟

---

### 任务3.3：实现简单转换器（可并行）

**目标**: 实现简单的数据转换器

**子任务**:
3.3.1 实现Filter
3.3.2 实现InsertHeader
3.3.3 实现DropHeaders
3.3.4 实现RegexRouter
3.3.5 实现TimestampRouter
3.3.6 实现HoistField
3.3.7 实现ValueToKey

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` 简单转换器测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` 简单转换器测试通过

**单元测试设计**:
- 每个转换器都有对应的测试
- 测试转换逻辑
- 测试配置验证

**预计时间**: 5-8分钟

---

### 任务3.4：实现中等复杂度转换器（可并行）

**目标**: 实现中等复杂度的数据转换器

**子任务**:
3.4.1 实现ReplaceField
3.4.2 实现MaskField
3.4.3 实现SetSchemaMetadata
3.4.4 实现InsertField
3.4.5 实现HeaderFrom
3.4.6 实现ExtractField
3.4.7 实现Flatten

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` 中等转换器测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` 中等转换器测试通过

**单元测试设计**:
- 每个转换器都有对应的测试
- 测试转换逻辑
- 测试边界情况

**预计时间**: 8-12分钟

---

### 任务3.5：实现复杂转换器（串行）

**目标**: 实现复杂的数据转换器

**子任务**:
3.5.1 实现Cast（类型转换）
3.5.2 实现TimestampConverter（时间戳转换）

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` 复杂转换器测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` 复杂转换器测试通过

**单元测试设计**:
- 测试所有类型转换
- 测试时间戳格式转换
- 测试边界情况

**预计时间**: 5-8分钟

---

### 任务3.6：实现谓词模块（可并行）

**目标**: 实现记录过滤谓词

**子任务**:
3.6.1 实现RecordIsTombstone
3.6.2 实现HasHeaderKey
3.6.3 实现TopicNameMatches

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` 谓词测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` 谓词测试通过

**单元测试设计**:
- 测试谓词逻辑
- 测试谓词组合

**预计时间**: 2-3分钟

---

## 三、阶段验证

### 3.1 完成标准

- [x] 所有任务都已完成
- [x] 基础设施已实现
- [x] 字段路径模块已实现
- [x] 简单转换器已实现
- [x] 中等复杂度转换器已实现
- [x] 复杂转换器已实现
- [x] 谓词模块已实现

### 3.2 输出验证

- [x] `cargo build --release` 成功
- [x] 所有crate编译通过
- [x] 无编译错误
- [ ] 无编译警告（或只有可接受的警告）

### 3.3 测试验证

- [ ] `cargo test` 所有单元测试通过
- [ ] 测试覆盖率>80%
- [ ] 所有转换器都有测试

### 3.4 质量验证

- [ ] 所有转换器实现与阶段1的trait定义一致
- [ ] 代码行数与Java版本对比（偏差<20%）
- [ ] 文件数量与Java版本一致
- [ ] 无空实现或TODO代码

---

## 四、阶段输出

### 4.1 生成的文件

1. **基础设施**: `connect-transforms/src/util/mod.rs`
2. **字段路径**: `connect-transforms/src/field_path/mod.rs`
3. **简单转换器**: `connect-transforms/src/simple/mod.rs`
4. **中等转换器**: `connect-transforms/src/medium/mod.rs`
5. **复杂转换器**: `connect-transforms/src/complex/mod.rs`
6. **谓词**: `connect-transforms/src/predicates/mod.rs`

### 4.2 下一阶段入口

完成本阶段后，可以进入以下阶段（可并行）：
- 阶段4：connect-runtime-core实现
- 阶段5：connect-runtime-distributed实现
- 阶段6：connect-mirror-client实现
- 阶段7：connect-mirror实现
- 阶段8：connect-basic-auth-extension实现

---

## 五、执行建议

### 5.1 并发执行

本阶段的6个任务组可以完全并行执行：

```bash
# 可以同时启动6个任务组
# 任务3.1 - 任务3.6
```

### 5.2 验证策略

每个任务组完成后立即验证：
- `cargo check` 成功
- `cargo test` 相关测试通过

### 5.3 错误处理

如果某个任务组失败：
- 记录错误信息
- 分析失败原因
- 检查与阶段1的trait定义是否一致

---

## 六、成功标准

### 6.1 必须完成

- [ ] 所有6个任务组都已完成
- [ ] connect-transforms crate实现完成
- [ ] 所有转换器都有实现
- [ ] 所有谓词都有实现

### 6.2 编译标准

- [ ] `cargo build --release` 成功
- [ ] connect-transforms crate编译通过

### 6.3 测试标准

- [ ] `cargo test` 所有单元测试通过
- [ ] 测试覆盖率>80%

### 6.4 质量标准

- [ ] 代码行数与Java版本对比（偏差<20%）
- [ ] 文件数量与Java版本一致
- [ ] 无空实现或TODO代码

---

**阶段3完成标志**: ✅ Transforms模块实现完成
