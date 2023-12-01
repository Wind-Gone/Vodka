# Vodka 代码提交规范

## Commit Message Format

```tex
<type>(<scope>) <subject>
```

### type（必选）

- feat：新功能（feature）
- fix：修复bug
- docs：文档（documentation）
- style：格式（不影响代码运行的变动）
- refactor：重构（即不是新增功能，也不是修改bug的代码变动）
- merge：代码合并
- test：增加测试
- revert：回滚到上一个版本
- sync：同步主线或分支的Bug

### scope（可选）

- 注明此次commit影响的代码范围，如只针对同步负载部分修改，则注明sync

### subject（必选）

> subject要求对message进行50字以内的简述，结尾不加句号或其他标点符号
## Example

```ceylon
[fix] 修复读写线程比例不统一问题
[feat(sync)] 实现同步负载的新鲜度指标统计
[test(sync)] 同步负载交叉量控制功能通过基本的单元测试
[style] 所有代码增加Function和Class注释
```

