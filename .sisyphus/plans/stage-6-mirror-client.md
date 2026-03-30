# 阶段6：Mirror客户端实现

## TL;DR

> **阶段目标**: 实现connect-mirror-client模块的所有功能
> **依赖**: 阶段1完成（connect-mirror-client的trait定义已生成）
> **并发度**: 1个任务组
> **预计时间**: 10-15分钟

---

## 一、阶段概述

### 1.1 阶段目标

本阶段的目标是实现connect-mirror-client模块的所有功能，包括MirrorClient、ReplicationPolicy、数据模型等。

### 1.2 关键输出

1. **完整实现**: 所有mirror客户端功能
2. **编译通过**: `cargo build --release` 成功
3. **测试通过**: `cargo test` 所有单元测试通过

### 1.3 并发策略

本阶段的任务可以**完全并行**执行：

```
任务组A（可并行）:
└── 任务6.1：实现mirror-client模块
```

---

## 二、任务清单

### 任务6.1：实现mirror-client模块（可并行）

**目标**: 实现mirror客户端的所有功能

**子任务**:
6.1.1 实现ReplicationPolicy接口
6.1.2 实现DefaultReplicationPolicy
6.1.3 实现IdentityReplicationPolicy
6.1.4 实现Heartbeat
6.1.5 实现Checkpoint
6.1.6 实现SourceAndTarget
6.1.7 实现MirrorClientConfig
6.1.8 实现RemoteClusterUtils
6.1.9 实现MirrorClient

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` mirror-client测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` mirror-client测试通过

**单元测试设计**:
-测试ReplicationPolicy
- 测试MirrorClient
- 测试RemoteClusterUtils

**预计时间**: 10-15分钟

---

## 三、阶段验证

### 3.1 完成标准

- [x] 所有任务都已完成
- [x] MirrorClient已实现
- [x] ReplicationPolicy已实现
- [x] 数据模型已实现
- [x] 工具函数已实现

### 3.2 输出验证

- [x] `cargo build --release` 成功
- [x] 所有crate编译通过
- [x] 无编译错误
- [ ] 无编译警告（或只有可接受的警告）

### 3.3 测试验证

- [ ] `cargo test` 所有单元测试通过
- [ ] 测试覆盖率>80%
- [ ] 所有mirror客户端功能都有测试

### 3.4 质量验证

- [ ] 代码行数与Java版本对比（偏差<20%）
- [ ] 文件数量与Java版本一致
- [ ] 无空实现或TODO代码
- [ ] 函数和类名与Java版本1:1对应
- [ ] 代码细节遵从kafka（可靠性细节）

---

## 四、阶段输出

### 4.1 生成的文件

1. **ReplicationPolicy**: `connect-mirror-client/src/policy/mod.rs`
2. **数据模型**: `connect-mirror-client/src/models/mod.rs`
3. **MirrorClient**: `connect-mirror-client/src/client/mod.rs`
4. **工具类**: `connect-mirror-client/src/utils/mod.rs`

### 4.2 下一阶段入口

完成本阶段后，可以进入以下阶段（可并行）：
- 阶段7：connect-mirror实现
- 阶段8：connect-basic-auth-extension实现

---

## 五、执行建议

### 5.1 并发执行

本阶段的任务组可以完全并行执行：

```bash
# 启动任务6.1
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

- [ ] 所有任务组都已完成
- [ ] ] connect-mirror-client crate实现完成
- [ ] 所有mirror客户端功能都有实现

### 6.2 编译标准

- [ ] `cargo build --release` 成功
- [ ] connect-mirror-client crate编译通过

### 6.3 测试标准

- [ ] `cargo test` 所有单元测试通过
- [ ] 测试覆盖率>80%

### 6.4 质量标准

- [ ] 代码行数与Java版本对比（偏差<20%）
- [ ] 文件数量与Java版本一致
- [ ] 无空实现或TODO代码

---

**阶段6完成标志**: ✅ Mirror客户端实现完成
