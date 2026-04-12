```
# 第 1 步：架构分析
opencode "@oracle 分析当前目录下kafka的connect模块的架构，制定connect模块迁移到rust的计划，划分迁移阶段。迁移计划体现“如何保证规格、细节不遗漏，且遵从AGENTS.md的设计与编码要求”，过程中可使用MCP deepwiki工具咨询kakfa的任何问题。"

# 第 2 步：依赖分析（后台任务）
opencode "ulw: @explore 分析所有模块依赖，生成依赖图，识别关键路径模块；@librarian 调研 rust 迁移最佳实践（尤其是原kafka实现过程中用到的java\scala语言特性和组件在rust中如何对等替代）；@oracle 根据依赖分析和调研的结果，刷新迁移计划。注意：1.分析过程中多种方案或其他疑问，应该向我提问。2.模块依赖kafka-client，为方便端到端测试，在严格遵守client的API契约下，你可以提供client的mock代码+预制数据。"

# 第 3 步：分模块迁移
opencode "ulw: @backend-engineer 遵从“迁移计划”，将当前目录下kafka的connect模块迁移到rust，将所有迁移和mock的代码都写到当前根目录的connect-rust下，使用 LSP 工具确保类型安全。迁移过程，应该小步迭代，保证每个模块和子任务迁移后都应该急时通过cargo build和cargo test。"

# 第 4 步：验证与测试
opencode "@explore 最终再一次，针对完整项目，使用 LSP 工具检查类型错误，通过cargo build命令保证编译通过，运行测试套件，生成迁移报告"
```



```
我的原始诉求是完成kafka connect用rust语言重写到connect-rust-new目录下（除connect-file和connect-json外所有均迁移，含测试），当前已经完成了部分，但完整性和正确性较差，现在请具体分析比对connect-rust-new/connect-mirror-client目录和connect/mirror-client目录下的详细差异，完成全部的代码1:1翻译，保证迁移的完整性和正确性。注意：
- 编译过程如果因为依赖connect模块内其他crate，且该crate代码缺失，导致编译不通过，你应该整理缺失内容并展示给我，同时暂停编码。
- 分析该模块中涉及的外部模块依赖，对于依赖在kafka connect之外的，应该考虑：对于struct一并补齐(可存放到kafka-clients-trait中)；对函数进行mock，就像对于kafka-clients的依赖一样，目前已经在保证API兼容情况下实现了mock版的kafka-clients。其他的也参考这条规则梳理外部依赖，并提交我审阅。
- 分析该模块中涉及的java翻译到rust的语言难点，交给我审阅。
- 完整性度量方式为：connect/mirror-client模块中class数量与connect-rust-new/connect-mirror-client中struct+trait数量大致相同，函数个数大致相同，代码量大致相同；函数中不允许标注todo或者其他的空实现。
- 正确性度量方式为：确保connect-rust-new/connect-mirror-client模块代码编译通过，确保核心功能有ut覆盖，且ut覆盖通过；若kafka对应模块有ut也应一并迁移，并保证通过。
- 优先解决完整性确保迁移无遗漏，无todo等实现，再解决正确性问题，即编译和ut的问题。
- 完成完整性和正确性的要求后，请输出最终的报告，包含内容就是证明完成了上述的完整性和正确性，结果让我审阅。
```

