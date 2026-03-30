# 阶段1：基础设施与API骨架生成

## TL;DR

> **阶段目标**: 创建所有crate结构并生成API契约定义（不包含实现）
> **依赖**: 阶段0完成
> **并发度**: 10个任务完全并行
> **预计时间**: 10-15分钟

---

## 一、阶段概述

### 1.1 阶段目标

本阶段的目标是创建所有crate的项目结构，并基于阶段0生成的API契约文档，生成所有模块的trait定义和类型声明。**不包含实现**，只生成API契约。

### 1.2 关键输出

1. **所有crate的Cargo.toml**: workspace配置和各个crate的依赖配置
2. **所有crate的trait定义**: 基于阶段0的API契约文档生成
3. **所有crate的类型声明**: 数据模型、枚举、结构体
4. **可编译的骨架**: 所有crate都可以独立编译通过

### 1.3 并发策略

本阶段的所有任务可以**完全并行**执行，无任务间依赖：

```
任务1.1 ─┐
任务1.2 ─┤
任务1.3 ─┤
任务1.4 ─┼─ 并行执行（10个任务）
任务1.5 ─┤
任务1.6 ─┤
任务1.7 ─┤
任务1.8 ─┤
任务1.9 ─┤
任务1.10 ─┘
```

---

## 二、任务清单

### 任务1.1：创建workspace和所有crate的Cargo.toml

**目标**: 创建workspace根目录和所有crate的基础结构

**子任务**:
1.1.1 创建workspace根目录和Cargo.toml
1.1.2 创建kafka-clients-trait crate
1.1.3 创建kafka-clients-mock crate
1.1.4 创建connect-api crate
1.1.5 创建connect-transforms crate
1.1.6 创建connect-runtime-core crate
1.1.7 创建connect-runtime-distributed crate
1.1.8 创建connect-mirror-client crate
1.1.9 创建connect-mirror crate
1.1.10 创建connect-basic-auth-extension crate

**验证每个子任务**:
- `cargo check` 成功
- crate结构正确
- 依赖关系正确

**验证整个任务**:
- `cargo build --workspace` 成功
- 所有crate编译通过

**预计时间**: 2-3分钟

---

### 任务1.2：实现kafka-clients-trait crate（trait定义）

**目标**: 定义Kafka客户端核心trait接口

**子任务**:
1.2.1 定义Producer trait
   - `send()` - 发送消息
   - `flush()` - 刷新缓冲区
   - `close()` - 关闭producer

1.2.2 定义Consumer trait
   - `poll()` - 拉取消息
   - `assign()` - 分配分区
   - `seek` - 定位到指定offset
   - `commit()` - 提交offset
   - `close()` - 关闭consumer

1.2.3 定义AdminClient trait
   - `create_topics()` - 创建主题
   - `delete_topics()` - 删除主题
   - `list_topics()` - 列出主题
   - `describe_configs()` - 描述配置

1.2.4 定义ConfigDef trait
   - 配置定义接口

1.2.5 定义序列化接口
   - Serializer trait
   - Deserializer trait

**验证每个子任务**:
- `cargo check` 成功
- trait编译通过
- 代码行数检查（~500行）

**验证整个任务**:
- `cargo build --release` 成功
- 所有trait编译通过

**预计时间**: 3-5分钟

---

### 任务1.3：实现kafka-clients-mock crate（trait实现）

**目标**: 实现基于内存队列的mock实现

**子任务**:
1.3.1 实现MockProducer
   - 内存队列存储消息
   - 支持预制数据

1.3.2 实现MockConsumer
   - 从内存队列消费
   - 支持offset管理

1.3.3 实现MockAdminClient
   - 内存存储主题元数据
   - 支持主题CRUD操作

1.3.4 实现MockConfigDef
   - 配置验证mock

1.3.5 实现序列化mock
   - 简单的序列化/反序列化

**验证每个子任务**:
- `cargo check` 成功
- mock编译通过
- 代码行数检查（~2000行）

**验证整个任务**:
- `cargo build --release` 成功
- 所有mock编译通过

**预计时间**: 5-8分钟

---

### 任务1.4：生成connect-api的trait和类型定义（不包含实现）

**目标**: 生成connect-api的trait定义和类型声明

**子任务**:
1.4.1 定义错误类型系统
   - ConnectError枚举
   - RetriableError trait

1.4.2 定义数据模型
   - Schema trait和ConnectSchema
   - Struct和Field
   - ConnectRecord trait
   - SourceRecord
   - SinkRecord
   - SchemaAndValue

1.4.3 定义核心trait
   - Versioned trait
   - Connector trait
   - Task trait
   - SourceConnector trait
   - SourceTask trait
   - SinkConnector trait
   - SinkTask trait
   - Converter trait
   - HeaderConverter trait
   - Transformation trait

1.4.4 定义配置系统
   - ConfigDef
   - ConfigValue
   - Configurable trait

**验证每个子任务**:
- `cargo check` 成功
- trait和类型定义编译通过

**验证整个任务**:
- `cargo build --release` 成功
- connect-api的trait和类型定义编译通过

**预计时间**: 10-15分钟

---

### 任务1.5：生成connect-transforms的trait定义（不包含实现）

**目标**: 生成connect-transforms的trait定义

**子任务**:
1.5.1 定义Transformation trait
   - `apply()` 方法
   - `config()` 方法
   - `close()` 方法

1.5.2 定义所有内置转换器的trait定义
   - Filter
   - InsertHeader
   - DropHeaders
   - RegexRouter
   - TimestampRouter
   - HoistField
   - ValueToKey
   - ReplaceField
   - MaskField
   - SetSchemaMetadata
   - InsertField
   - HeaderFrom
   - ExtractField
   - Flatten
   - Cast
   - TimestampConverter

**验证每个子任务**:
- `cargo check` 成功
- trait定义编译通过

**验证整个任务**:
- `cargo build --release` 成功
- connect-transforms的trait定义编译通过

**预计时间**: 5-8分钟

---

### 任务1.6：生成connect-runtime-core的trait定义（不包含实现）

**目标**: 生成connect-runtime-core的trait定义

**子任务**:
1.6.1 定义Worker trait
   - `start()` 方法
   - `stop()` 方法
   - `awaitStop()` 方法

1.6.2 定义Herder trait
   - `start()` 方法
   - `stop()` 方法
   - `connect()` 方法
   - `deleteConnector()` 方法

1.6.3 定义StandaloneHerder trait
   - 继承Herder trait

1.6.4 定义DistributedHerder trait
   - 继承Herder trait

1.6.5 定义WorkerConnector trait
   - `start()` 方法
   - `stop()` 方法

1.6.6 定义WorkerTask trait
   - `start()` 方法
   - `stop()` 方法

**验证每个子任务**:
- `cargo check` 成功
- trait定义编译通过

**验证整个任务**:
- `cargo build --release` 成功
- connect-runtime-core的trait定义编译通过

**预计时间**: 8-12分钟

---

### 任务1.7：生成connect-runtime-distributed的trait定义（不包含实现）

**目标**: 生成connect-runtime-distributed的trait定义

**子任务**:
1.7.1 定义ConnectProtocol trait
   - `sendRequest()` 方法
   - `receiveResponse()` 方法

1.7.2 定义WorkerCoordinator trait
   - `join()` 方法
   - `leave()` 方法

1.7.3 定义EagerAssignor trait
   - `assign()` 方法

1.7.4 定义IncrementalCooperativeAssignor trait
   - `assign()` 方法

**验证每个子任务**:
- `cargo check` 成功
- trait定义编译通过

**验证整个任务**:
- `cargo build --release` 成功
- connect-runtime-distributed的trait定义编译通过

**预计时间**: 3-5分钟

---

### 任务1.8：生成connect-mirror-client的trait定义（不包含实现）

**目标**: 生成connect-mirror-client的trait定义

**子任务**:
1.8.1 定义ReplicationPolicy trait
   - `topic()` 方法
   - `partition()` 方法

1.8.2 定义MirrorClient trait
   - `connect()` 方法
   - `disconnect()` 方法

1.8.3 定义Heartbeat trait
   - `send()` 方法

1.8.4 定义Checkpoint trait
   - `record()` 方法

**验证每个子任务**:
- `cargo check` 成功
- trait定义编译通过

**验证整个任务**:
- `cargo build --release` 成功
- connect-mirror-client的trait定义编译通过

**预计时间**: 3-5分钟

---

### 任务1.9：生成connect-mirror的trait定义（不包含实现）

**目标**: 生成connect-mirror的trait定义

**子任务**:
1.9.1 定义MirrorMaker2 trait
   - `start()` 方法
   - `stop()` 方法

1.9.2 定义MirrorSourceTask trait
   - `poll()` 方法

1.9.3 定义MirrorSinkTask trait
   - `put()` 方法

1.9.4 定义MirrorCheckpointTask trait
   - `poll()` 方法

1.9.5 定义MirrorHeartbeatTask trait
   - `poll()` 方法

**验证每个子任务**:
- `cargo check` 成功
- trait定义编译通过

**验证整个任务**:
- `cargo build --release` 成功
- connect-mirror的trait定义编译通过

**预计时间**: 3-5分钟

---

### 任务1.10：生成connect-basic-auth-extension的trait定义（不包含实现）

**目标**: 生成connect-basic-auth-extension的trait定义

**子任务**:
1.10.1 定义BasicAuthExtension trait
   - `authenticate()` 方法

1.10.2 定义RestAuthenticator trait
   - `authenticate()` 方法

**验证每个子任务**:
- `cargo check` 成功
- trait定义编译通过

**验证整个任务**:
- `cargo build --release` 成功
- connect-basic-auth-extension的trait定义编译通过

**预计时间**: 2-3x分钟

---

## 三、阶段验证

### 3.1 完成标准

- [x] 所有10个任务都已完成
- [x] 所有crate的Cargo.toml已生成
- [x] 所有trait定义已生成
- [x] 所有类型声明已生成
- [x] 所有crate都可以独立编译通过

### 3.2 输出验证

- [x] `cargo build --workspace` 成功
- [x] 所有crate编译通过
- [x] 无编译错误
- [x] 无编译警告（或只有可接受的警告）

### 3.3 质量验证

- [x] 所有trait定义与阶段0的API契约一致
- [x] 所有类型声明完整
- [x] 代码行数与Java版本对比（偏差<20%）

---

## 四、阶段输出

### 4.1 生成的文件

1. **workspace配置**: `connect-rust-new/Cargo.toml`
2. **所有crate的Cargo.toml**: 每个crate的依赖配置
3. **所有trait定义**: 基于阶段0的API契约文档
4. **所有类型声明**: 数据模型、枚举、结构体

### 4.2 下一阶段入口

完成本阶段后，进入**阶段2-8：各模块实现（可完全并行）**

---

## 五、执行建议

### 5.1 并发执行

本阶段的所有任务可以完全并行执行，建议使用10个并发任务：

```bash
# 可以同时启动10个任务
# 任务1.1 - 任务1.10
```

### 5.2 验证策略

每个任务完成后立即验证：
- `cargo check` 成功
- trait定义编译通过
- 类型声明编译通过

### 5.3 错误处理

如果某个任务失败：
- 记录编译错误
- 分析失败原因
- 检查与阶段0的API契约是否一致

---

## 六、成功标准

### 6.1 必须完成

- [x] 所有10个任务都已完成
- [x] 所有crate的Cargo.toml已生成
- [x] 所有trait定义已生成
- [x] 所有类型声明已生成

### 6.2 编译标准

- [x] `cargo build --workspace` 成功
- [x] 所有crate编译通过
- [x] 无编译错误

### 6.3 起量标准

- [x] 所有trait定义与阶段0的API契约一致
- [x] 所有类型声明完整
- [x] 代码行数与Java版本对比（偏差<20%）

---

**阶段1完成标志**: ✅ 所有crate骨架和API契约生成完成
