# 阶段4：Runtime核心实现

## TL;DR

> **阶段目标**: 实现connect-runtime-core模块的所有功能
> **依赖**: 阶段1完成（connect-runtime-core的trait定义已生成）+ 阶段2完成（connect-api实现完成）+ 阶段3完成（connect-transforms实现完成）
> **并发度**: 7个任务组可并行
> **预计时间**: 40-60分钟

---

## 一、阶段概述

### 1.1 阶段目标

本阶段的目标是实现connect-runtime-core模块的所有功能，包括基础配置和状态管理、指标系统、插件隔离层、错误处理层、Worker核心、Herder层、REST API层等。

### 1.2 关键输出

1. **完整实现**: 所有runtime核心功能
2. **编译通过**: `cargo build --release` 成功
3. **测试通过**: `cargo test` 所有单元测试通过

### 1.3 并发策略

本阶段的任务可以**完全并行**执行，无任务间依赖：

```
任务组A（可并行）:
├── 任务4.1：实现基础配置和状态管理
├── 任务4.2：实现指标系统
├── 任务4.3：实现插件隔离层
└── 任务4.4：实现错误处理层

任务组B（可并行）:
├── 任务4.5：实现Worker核心
├── 任务4.6：实现Herder层
└── 任务4.7：实现REST API层
```

---

## 二、任务清单

### 任务4.1：实现基础配置和状态管理（可并行）

**目标**: 实现配置和状态管理组件

**子任务**:
4.1.1 实现WorkerConfig
4.1.2 实现ConnectorConfig
4.1.3 实现TaskConfig
4.1.4 实现StateTracker
4.1.5 实现AbstractStatus
4.1.6 实现ConnectorStatus
4.1.7 实现TaskStatus

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` 配置和状态测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` 配置和状态测试通过

**单元测试设计**:
- 测试配置验证
- 测试状态转换
- 测试状态持久化

**预计时间**: 8-12分钟

---

### 任务4.2：实现指标系统（可并行）

**目标**: 实现Connect指标系统

**子任务**:
4.2.1 实现ConnectMetrics
4.2.2 实现ConnectMetricsRegistry
4.2.3 实现WorkerMetricsGroup

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` 指标测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` 指标测试通过

**单元测试设计**:
- 测试指标创建
- 测试指标注册
- 测试指标报告

**预计时间**: 5-8分钟

---

### 任务4.3：实现插件隔离层（可并行）

**目标**: 实现插件加载和隔离系统

**子任务**:
4.3.1 实现PluginClassLoader
4.3.2 实现DelegatingClassLoader
4.3.3 实现PluginScanner
4.3.4 实现ReflectionScanner
4.3.5 实现ServiceLoaderScanner
4.3.6 实现Plugins
4.3.7 实现PluginUtils

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` 插件系统测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` 插件系统测试通过

**单元测试设计**:
- 测试插件加载
- 测试插件发现
- 测试插件隔离

**预计时间**: 10-15分钟

---

### 任务4.4：实现错误处理层（可并行）

**目标**: 实现错误报告和重试机制

**子任务**:
4.4.1 实现ErrorReporter
4.4.2 实现LogReporter
4.4.3 实现ProcessingContext
4.4.4 实现DeadLetterQueueReporter
4.4.5 实现WorkerErrantRecordReporter
4.4.6 实现RetryWithToleranceOperator
4.4.7 实现ErrorHandlingMetrics

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` 错误处理测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` 错误处理测试通过

**单元测试设计**:
- 测试错误报告
- 测试重试逻辑
- 测试死信队列

**预计时间**: 8-12分钟

---

### 任务4.5：实现Worker核心（串行）

**目标**: 实现Worker核心逻辑

**子任务**:
4.5.1 实现WorkerConnector
4.5.2 实现WorkerTask
4.5.3 实现WorkerTaskContext
4.5.4 实现WorkerSinkTask
4.5.5 实现WorkerSinkTaskContext
4.5.6 实现WorkerSourceTask
4.5.7 实现WorkerSourceTaskContext
4.5.8 实现AbstractWorkerSourceTask
4.5.9 实现ExactlyOnceWorkerSourceTask
4.5.10 实现Worker

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` Worker核心测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` Worker核心测试通过

**单元测试设计**:
- 测试WorkerConnector
- 测试WorkerTask
- 测试WorkerSourceTask
- 测试WorkerSinkTask
- 测试Worker

**预计时间**: 15-20分钟

---

### 任务4.6：实现Herder层（可并行）

**目标**: 实现Herder管理和协调

**子任务**:
4.6.1 实现Herder接口
4.6.2 实现AbstractHerder
4.6.3 实现StandaloneHerder
4.6.4 实现StandaloneConfig
4.6.5 实现HealthCheckThread

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` Herder测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` Herder测试通过

**单元测试设计**:
- 测试AbstractHerder
- 测试StandaloneHerder
- 测试Herder生命周期

**预计时间**: 10-15分钟

---

### 任务4.7：实现REST API层（可并行）

**目标**: 实现REST API服务器

**子任务**:
4.7.1 实现REST实体（19个文件）
4.7.2 实现RestServer
4.7.3 实现RestServerConfig
4.7.4 实现RestClient
4.7.5 实现REST资源（6个文件）
4.7.6 实现REST错误处理

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` REST API测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` REST API测试通过

**单元测试设计**:
- 测试REST实体
- 测试RestServer
- 测试REST资源

**预计时间**: 10-15分钟

---

## 三、阶段验证

### 3.1 完成标准

- [x] 所有任务都已完成
- [x] 基础配置和状态管理已实现
- [x] 指标系统已实现
- [x] 插件隔离层已实现
- [x] 错误处理层已实现
- [x] Worker核心已实现
- [x] Herder层已实现
- [x] REST API层已实现

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

- [ ] 代码行数与Java版本对比（偏差<20%）
- [ ] 文件数量与Java版本一致
- [ ] 无空实现或TODO代码
- [ ] 函数和类名与Java版本1:1对应
- [ ] 代码细节遵从kafka（可靠性细节）

---

## 四、阶段输出

### 4.1 生成的文件

1. **配置和状态**: `connect-runtime-core/src/config/mod.rs`
2. **指标系统**: `connect-runtime-core/src/metrics/mod.rs`
3. **插件隔离**: `connect-runtime-core/src/isolation/mod.rs`
4. **错误处理**: `connect-runtime-core/src/errors/mod.rs`
5. **Worker核心**: `connect-runtime-core/src/worker/mod.rs`
6. **Herder层**: `connect-runtime-core/src/herder/mod.rs`
7. **REST API**: `connect-runtime-core/src/rest/mod.rs`

### 4.2 下一阶段入口

完成本阶段后，可以进入以下阶段（可并行）：
- 阶段5：connect-runtime-distributed实现
- 阶段6：connect-mirror-client实现
- 阶段7：connect-mirror实现
- 阶段8：connect-basic-auth-extension实现

---

## 五、执行建议

### 5.1 并发执行

本阶段的7个任务组可以完全并行执行：

```bash
# 可以同时启动7个任务组
# 任务4.1 - 任务4.7
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

- [ ] 所有7个任务组都已完成
- [ ] connect-runtime-core crate实现完成
- [ ] 所有核心功能都有实现

### 6.2 编译标准

- [ ] `cargo build --release` 成功
- [ ] connect-runtime-core crate编译通过

### 6.3 测试标准

- [ ] `cargo test` 所有单元测试通过
- [ ] 测试覆盖率>80%

### 6.4 质量标准

- [ ] 代码行数与Java版本对比（偏差<20%）
- [ ] 文件数量与Java版本一致
- [ ] 无空实现或TODO代码

---

**阶段4完成标志**: ✅ Runtime核心实现完成
