# 阶段6架构决策

## 设计决策

### 1. 使用trait而非抽象类
**决策**: 使用trait定义ReplicationPolicy和MirrorClient
**理由**:
- Rust没有抽象类，trait是自然的替代
- 支持动态分发（Box<dyn Trait>）
- 更灵活的trait对象系统

### 2. 分离Config和Client
**决策**: MirrorClientConfig作为独立结构体
**理由**:
- 配置管理和客户端逻辑分离
- 便于配置验证和解析
- 符合单一职责原则

### 3. 提供BasicMirrorClient实现
**决策**: 提供BasicMirrorClient作为MirrorClient的基本实现
**理由**:
- 允许用户直接使用，无需自己实现
- 提供了完整的功能实现
- 便于测试和扩展

### 4. RemoteClusterUtils作为静态工具
**决策**: 使用struct + impl（无self）实现静态方法
**理由**:
- 与Java的静态方法语义一致
- 提供便捷的入口点
- 自动管理客户端生命周期

## 技术选型

### 1. 配置存储
**选择**: HashMap<String, String>
**理由**:
- 简单直接
- 与Java的Map<String, Object>类似
- 易于序列化和反序列化

### 2. 错误处理
**选择**: Box<dyn Error>
**理由**:
- 支持动态错误类型
- 与trait对象系统兼容
- 便于跨模块错误传播

### 3. 序列化
**选择**: 简单的字符串格式（管道分隔）
**理由**:
- 快速实现
- 便于调试
- 后续可以替换为JSON或Protobuf

## 实现策略

### 1. 占位符实现
**策略**: 对于需要外部依赖的功能，提供占位符实现
**示例**:
- list_topics()返回空集合
- remote_consumer_offsets()返回空映射
**理由**:
- 保证编译通过
- 提供完整的API
- 后续可以集成真正的客户端

### 2. 渐进式开发
**策略**: 先实现核心功能，后优化细节
**步骤**:
1. 实现trait和结构体
2. 实现基本方法
3. 添加工具函数
4. 集成外部依赖（后续阶段）
**理由**:
- 快速验证设计
- 逐步增加复杂度
- 便于问题定位

### 3. 1:1对应Java实现
**策略**: 严格遵循Java版本的方法签名和语义
**执行**:
- 方法名保持一致（snake_case转换）
- 参数和返回类型对应
- 错误处理语义一致
**理由**:
- 确保功能完整
- 便于参考Java文档
- 降低维护成本

## 后续优化

### 1. 集成kafka-clients-trait
**目标**: 使用真正的Admin和Consumer客户端
**影响**:
- BasicMirrorClient的list_topics()实现
- remote_consumer_offsets()实现
- 需要异步支持

### 2. 改进序列化
**目标**: 使用更高效的序列化格式
**选项**:
- JSON（可读性好）
- Protobuf（性能高）
- Bincode（Rust原生）
**影响**:
- Checkpoint和Heartbeat的序列化方法
- 需要添加序列化依赖

### 3. 增加测试覆盖
**目标**: 测试覆盖率>80%
**需要**:
- ReplicationPolicy测试
- MirrorClient测试
- RemoteClusterUtils测试
- 集成测试

### 4. 性能优化
**目标**: 减少内存分配和拷贝
**优化点**:
- 使用&str替代String（可能）
- 使用Arc共享ReplicationPolicy
- 缓存主题名称转换结果
