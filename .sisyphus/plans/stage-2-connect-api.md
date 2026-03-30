# 阶段2：Connect API层实现

## TL;DR

> **阶段目标**: 实现connect-api模块的所有功能
> **依赖**: 阶段1完成（connect-api的trait定义已生成）
> **并发度**: 4个任务组可并行
> **预计时间**: 20-30分钟

---

## 一、阶段概述

### 1.1 阶段目标

本阶段的目标是实现connect-api模块的所有功能，包括错误类型、数据模型、核心trait、配置系统等。

### 1.2 关键输出

1. **完整实现**: 所有trait和类型的具体实现
2. **编译通过**: `cargo build --release` 成功
3. **测试通过**: `cargo test` 所有单元测试通过

### 1.3 并发策略

本阶段的任务可以**完全并行**执行，无任务间依赖：

```
任务组A（可并行）:
├── 任务2.1：实现错误类型系统
├── 任务2.2：实现数据模型
├── 任务2.3：实现核心trait
└── 任务2.4：实现配置系统
```

---

## 二、任务清单

### 任务2.1：实现错误类型系统

**目标**: 实现connect-api的错误类型系统

**子任务**:
2.1.1 定义ConnectError枚举
   - Generic
   - Data
   - AlreadyExists
   - NotFound
   - IllegalWorkerState
   - SchemaBuilder
   - SchemaProjector

2.1.2 定义RetriableError trait
   - `is_retriable()` 方法

2.1.3 使用thiserror实现错误层次

**验证每个子任务**:
- `cargo check` 成功
- 错误类型编译通过

**验证整个任务**:
- `cargo build --release` 成功
- 错误类型编译通过

**单元测试设计**:
- 测试错误类型创建
- 测试RetriableError trait
- 测试错误层次结构

**预计时间**: 3-5分钟

---

### 任务2.2：实现数据模型

**目标**: 实现connect-api的数据模型

**子任务**:
2.2.1 实现Schema trait和ConnectSchema
   - 类型枚举（INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, BOOLEAN, STRING, BYTES, ARRAY, MAP, STRUCT）
   - Schema trait定义
   - ConnectSchema实现

2.2.2 实现Struct和Field
   - Struct结构体
   - Field结构体
   - 类型化访问方法

2.2.3 实现ConnectRecord trait
   - 泛型trait定义
   - 基础字段访问

2.2.4 实现SourceRecord
   - 继承ConnectRecord
   - sourcePartition和sourceOffset字段

2.2.5 实现SinkRecord
   - 继承ConnectRecord
   - kafkaOffset等字段

2.2.6 实现SchemaAndValue
   - Schema和Value的组合

**验证每个子任务**:
- `cargo check` 成功
- 数据模型编译通过

**验证整个任务**:
- `cargo build --release` 成功
- 数据模型编译通过

**单元测试设计**:
- 测试Schema创建和验证
- 测试Struct字段访问
- 测试ConnectRecord字段访问
- 测试SourceRecord和SinkRecord

**预计时间**: 8-12分钟

---

### 任务2.3：实现核心trait

**目标**: 实现connect-api的核心trait

**子任务**:
2.3.1 实现Versioned trait
   - `version()` 方法

2.3.2 实现Connector trait
   - `initialize()`
   - `start()`
   - `stop()`
   - `task_class()`
   - `task_configs()`
   - `validate()`
   - `config()`

2.3.3 实现Task trait
   - `version()`
   - `start()`
   - `stop()`

2.3.4 实现SourceConnector trait
   - 继承Connector
   - `exactly_once_support()`
   - `can_define_transaction_boundaries()`
   - `alter_offsets()`

2.3.5 实现SourceTask trait
   - 继承Task
   - `initialize()`
   - `poll()`
   - `commit()`
   - `commit_record()`

2.3.6 实现SinkConnector trait
   - 继承Connector
   - `alter_offsets()`

2.3.7 实现SinkTask trait
   - 继承Task
   - `initialize()`
   - `put()`
   - `flush()`
   - `pre_commit()`
   - `open()`
   - `close()`

2.3.8 实现Converter trait
   - `configure()`
   - `from_connect_data()`
   - `to_connect_data()`
   - `config()`
   - `close()`

2.3.9 实现HeaderConverter trait
   - `to_connect_header()`
   - `from_connect_header()`
   - `config()`

2.3.10 实现Transformation trait
   - 泛型trait
   - `apply()`
   - `config()`
   - `close()`

**验证每个子任务**:
- `cargo check` 成功
- 所有trait编译通过

**验证整个任务**:
- `cargo build --release` 成功
- 所有trait编译通过

**单元测试设计**:
- 创建trait的mock测试
- 验证trait方法签名正确

**预计时间**: 10-15分钟

---

### 任务2.4：实现配置系统

**目标**: 实现connect-api的配置系统

**子任务**:
2.4.1 实现ConfigDef
   - 配置定义结构

2.4.2 实现ConfigValue
   - 配置值结构

2.4.3 实现Configurable trait
   - `configure()` 方法

**验证每个子任务**:
- `cargo check` 成功
- 配置系统编译通过

**验证整个任务**:
- `cargo build --release` 成功
- 配置系统编译通过

**单元测试设计**:
- 测试配置验证
- 测试配置值转换
- 测试Configurable trait

**预计时间**: 3-5分钟

---

## 三、阶段验证

### 3.1 完成标准

- [x] 所有任务都已完成
- [x] 错误类型系统已实现
- [x] 数据模型已实现
- [x] 核心trait已实现
- [x] 配置系统已实现

### 3.2 输出验证

- [x] `cargo build --release` 成功
- [x] 所有crate编译通过
- [x] 无编译错误
- [ ] 无编译警告（或只有可接受的警告）

### 3.3 测试验证

- [ ] `cargo test` 所有单元测试通过
- [ ] 测试覆盖率>80%
- [ ] 所有核心功能都有测试

### 3.4 质量验证

- [ ] 所有trait实现与阶段1的trait定义一致
- [ ] 所有类型声明完整
- [ ] 代码行数与Java版本对比（偏差<20%）
- [ ] 文件数量与Java版本一致

---

## 四、阶段输出

### 4.1 生成的文件

1. **错误类型**: `connect-api/src/error.rs`
2. **数据模型**: `connect-api/src/data/mod.rs`
3. **核心trait**: `connect-api/src/connector/mod.rs`
4. **配置系统**: `connect-api/src/config/mod.rs`

### 4.2 下一阶段入口

完成本阶段后，可以进入以下阶段（可并行）：
- 阶段3：connect-transforms实现
- 阶段4：connect-runtime-core实现
- 阶段5：connect-runtime-distributed实现
- 阶段6：connect-mirror-client实现
- 阶段7：connect-mirror实现
- 阶段8：connect-basic-auth-extension实现

---

## 五、执行建议

### 5.1 并发执行

本阶段的4个任务组可以完全并行执行：

```bash
# 可以同时启动4个任务组
# 任务2.1 - 任务2.4
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

- [ ] 所有4个任务组都已完成
- [ ] connect-api crate实现完成
- [ ] 所有trait都有实现
- [ ] 所有类型都有实现

### 6.2 编译标准

- [ ] `cargo build --release` 成功
- [ ] connect-api crate编译通过

### 6.3 测试标准

- [ ] `cargo test` 所有单元测试通过
- [ ] 测试覆盖率>80%

###### 6.4 质量标准

- [ ] 代码行数与Java版本对比（偏差<20%）
- [ ] 文件数量与Java版本一致
- [ ] 无空实现或TODO代码

---

**阶段2完成标志**: ✅ Connect API层实现完成
