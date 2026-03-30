# 阶段7：MirrorMaker2实现

## TL;DR

> **阶段目标**: 实现connect-mirror模块的所有功能
> **依赖**: 阶段1完成（connect-mirror的trait定义已生成）+ 阶段4完成（connect-runtime-core实现完成）+ 阶段6完成（connect-mirror-client实现完成）
> **并发度**: 7个任务组可并行
> **预计时间**: 25-40分钟

---

## 一、阶段概述

### 1.1 阶段目标

本阶段的目标是实现connect-mirror模块的所有功能，包括基础工具和配置、Offset同步子系统、Checkpoint子系统、Heartbeat子系统、Source子系统、格式化和REST、MirrorMaker主程序等。

### 1.2 关键输出

1. **完整实现**: 所有MirrorMaker2功能
2. **编译通过**: `cargo build --release` 成功
3. **测试通过**: `cargo test` 所有单元测试通过

### 1.3 并发策略

本阶段的任务可以**完全并行**执行，无任务间依赖：

```
任务组A（可并行）:
├── 任务7.1：实现基础工具和配置
├── 任务7.2：实现Offset同步子系统
├── 任务7.3：实现Checkpoint子系统
└── 任务7.4：实现Heartbeat子系统

任务组B（可并行）:
├── 任务7.5：实现Source子系统
└── 任务7.6：实现格式化和REST

任务组C（串行）:
└── 任务7.7：实现MirrorMaker主程序
```

---

## 二、任务清单

### 任务7.1：实现基础工具和配置（可并行）

**目标**: 实现MirrorMaker的基础工具和配置

**子任务**:
7.1.1 实现MirrorUtils
7.1.2 实现Scheduler
7.1.3 实现MirrorConnectorConfig
7.1.4 实现TopicFilter
7.1.5 实现GroupFilter
7.1.6 实现ConfigPropertyFilter
7.1.7 实现DefaultTopicFilter
7.1.8 实现DefaultGroupFilter
7.1.9 实现DefaultConfigPropertyFilter

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` 工具和配置测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` 工具和配置测试通过

**单元测试设计**:
- 测试MirrorUtils
- 测试Scheduler
- 测试各种Filter

**预计时间**: 5-8分钟

---

### 任务7.2：实现Offset同步子系统（可并行）

**目标**: 实现Offset同步功能

**子任务**:
7.2.1 实现OffsetSync
7.2.2 实现OffsetSyncStore
7.2.3 实现OffsetSyncWriter

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` Offset同步测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` Offset同步测试通过

**单元测试设计**:
- 测试OffsetSync
- 测试OffsetSyncStore
- 测试OffsetSyncWriter

**预计时间**: 5-8分钟

---

### 任务7.3：实现Checkpoint子系统（可并行）

**目标**: 实现Checkpoint功能

**子任务**:
7.3.1 实现CheckpointStore
7.3.2 实现MirrorCheckpointConfig
7.3.3 实现MirrorCheckpointTaskConfig
7.3.4 实现MirrorCheckpointMetrics
7.3.5 实现MirrorCheckpointTask
7.3.6 实现MirrorCheckpointConnector

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` Checkpoint测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` Checkpoint测试通过

**单元测试设计**:
- 测试CheckpointStore
- 测试MirrorCheckpointTask
- 测试MirrorCheckpointConnector

**预计时间**: 5-8分钟

---

### 任务7.4：实现Heartbeat子系统（可并行）

**目标**: 实现Heartbeat功能

**子任务**:
7.4.1 实现MirrorHeartbeatConfig
7.4.2 实现MirrorHeartbeatTask
7.4.3 实现MirrorHeartbeatConnector

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` Heartbeat测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` Heartbeat测试通过

**单元测试设计**:
- 测试MirrorHeartbeatTask
- 测试MirrorHeartbeatConnector

**预计时间**: 3-5分钟

---

### 任务7.5：实现Source子系统（串行）

**目标**: 实现MirrorSource功能

**子任务**:
7.5.1 实现MirrorSourceConfig
7.5.2 实现MirrorSourceTaskConfig
7.5.3 实现MirrorSourceMetrics
7.5.4 实现MirrorSourceTask
7.5.5 实现MirrorSourceConnector

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` Source测试通过

**验证整个整个任务**:
- `cargo build --release` 成功
- `cargo test` Source测试通过

**单元测试设计**:
- 测试MirrorSourceTask
- 测试MirrorSourceConnector
- 测试Source复制逻辑

**预计时间**: 8-12分钟

---

### 任务7.6：实现格式化和REST（可并行）

**目标**: 实现格式化和REST功能

**子任务**:
7.6.1 实现CheckpointFormatter
7.6.2 实现HeartbeatFormatter
7.6.3 实现OffsetSyncFormatter
7.6.4 实现MirrorRestServer
7.6.5 实现InternalMirrorResource

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` 格式化和REST测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` 格式化和REST测试通过

**单元测试设计**:
- 测试各种Formatter
- 测试MirrorRestServer

**预计时间**: 5-8分钟

---

### 任务7.7：实现MirrorMaker主程序（串行）

**目标**: 实现MirrorMaker主入口

**子任务**:
7.7.1 实现MirrorMakerConfig
7.7.2 实现MirrorHerder
7.7.3 实现MirrorMaker

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` MirrorMaker测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` MirrorMaker测试通过

**单元测试设计**:
- 测试MirrorMakerConfig
- 测试MirrorMaker

**预计时间**: 5-8分钟

---

## 三、阶段验证

### 3.1 完成标准

- [x] 所有任务都已完成
- [x] 基础工具和配置已实现
- [x] Offset同步子系统已实现
- [x] Checkpoint子系统已实现
- [x] Heartbeat子系统已实现
- [x] Source子系统已实现
- [x] 格式化和REST已实现
- [x] MirrorMaker主程序已实现

### 3.2 输出验证

- [x] `cargo build --release` 成功
- [x] 所有crate编译通过
- [x] 无编译错误
- [ ] 无编译警告（或只有可接受的警告）

### 3.3 测试验证

- [ ] `cargo test` 所有单元测试通过
- [ ] 测试覆盖率>80%
- [ ] 所有MirrorMaker2功能都有测试

### 3.4 质量验证

- [ ] 代码行数与Java版本对比（偏差<20%）
- [ ] 文件数量与Java版本一致
- [ ] 无空实现或TODO代码
- [ ] 函数和类名与Java版本1:1对应
- [ ] 代码细节遵从kafka（可靠性细节）

---

## 四、阶段输出

### 4.1 生成的文件

1. **工具和配置**: `connect-mirror/src/utils/mod.rs`
2. **Offset同步**: `connect-mirror/src/offset_sync/mod.rs`
3. **Checkpoint**: `connect-mirror/src/checkpoint/mod.rs`
4. **Heartbeat**: `connect-mirror/src/heartbeat/mod.rs`
5. **Source**: `connect-mirror/src/source/mod.rs`
6. **格式化和REST**: `connect-mirror/src/rest/mod.rs`
7. **主程序**: `connect-mirror/src/main.rs`

### 4.2 下一阶段入口

完成本阶段后，可以进入以下阶段：
- 阶段8：connect-basic-auth-extension实现

---

## 五、执行建议

### 5.1 并发执行

本阶段的7个任务组可以完全并行执行：

```bash
# 可以同时启动7个任务组
# 任务7.1 - 任务7.7
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
- [ ] connect-mirror crate实现完成
- [ ] 所有MirrorMaker2功能都有实现

### 6.2 编译标准

- [ ] `cargo build --release` 成功
- [ ] connect-mirror crate编译通过

### 6.3 测试标准

- [ ] `cargo test` 所有单元测试通过
- [ ] 测试覆盖率>80%

### 6.4 质量标准

- [ ] 代码行数与Java版本对比（偏差<20%）
- [ ] 文件数量与Java版本一致
- [ ] 无空实现或TODO代码

---

**阶段7完成标志**: ✅ MirrorMaker2实现完成
