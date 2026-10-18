# magic-flink

基于 Flink Kubernetes Operator 的自研 YAML 有界批处理服务。用户只编写 YAML，平台统一构建、测试和扫描 fat JAR，规划支持多源采集、SQL Join、前后置检查与验收后发布。

已开始核心逻辑开发：T03 源码核对完成，静态结论为 **GO_TO_SPIKE**；严格 YAML 解析、类型化参数绑定、SQL Planner 校验、规范化对象摘要、迟到回执与文件封存恢复判定已实现。本地 126 个测试通过，尚未实现可提交任务的完整服务或完成集群运行验收。

## 本地核心与验证

- `spec`：严格 YAML、参数绑定和 Flink 1.20.5 本地 SQL 规划，核对表/函数/Join 能力与输出 Schema。
- `protocol`：七类对象的规范化 UTF-8 JSON 和 SHA-256，明确排序、类型、提交身份和恢复字段。
- `control`：逻辑阶段身份、回执接纳和封存恢复的纯判定；不访问 Kubernetes、数据库或对象存储。
- [SQL Planner 与摘要合同](docs/p0/sql-and-digest-contracts.md)：支持范围、字段、黄金向量、保守拒绝条件和构建依赖。
- [核心合同、测试与边界](docs/p0/platform-contracts.md)：本轮实际实现、恢复判定表和剩余集成关口。
- [T03 文件提交源码核对](docs/p0/file-commit-protocol.md)：固定 Flink commit、字段来源和原型路径；GO_TO_SPIKE 不等于运行 PASS。

使用已有 Java 17 和 Maven；缓存齐全时可离线执行：

```sh
JAVA_HOME=/path/to/jdk-17 MAVEN_OPTS="-Xmx256m -XX:ActiveProcessorCount=2" mvn -o package
```

本机已验证的具体命令见核心合同。当前产物为普通核心库 JAR；T05 的单 fat JAR 双入口、扫描和真实依赖贯通尚未实现。环境 NO_GO 限制集群实验，不阻塞这部分本地开发。

## 设计与 P0 文档

- [YAML 批处理服务设计 v0.2](docs/design/flink-yaml-batch-service-design.md)：模块职责、YAML 规范、执行流程、失败恢复协议、P0–P5 实施计划及 33 项验收用例。
- [P0 实施任务与验收计划](docs/superpowers/plans/2026-10-18-p0-baseline-and-file-commit-validation.md)：版本选型、8 个实施任务、文件提交协议的 13 个实验及进入 P1 的门槛；T01/T02 尚未通过任务验收；已完成 15 分钟联合观测，单 Application 资源准入 NO_GO，实验均未运行。
- [T01 环境检查与最小补齐方案](docs/p0/environment-and-fixtures.md)：包含 E01 的 CRD/权限核对及 2026-10-18 14:43:48–14:58:54 联合观测（第 6.2.1 节）；宿主机压力、CPU/load 和两处磁盘未达标，结论为 **NO_GO**。按实测节点峰值重算，单 Application + 外部 S3 总预算约 11.464 GiB，仅余 0.271 GiB；未满足条件见第 6.2.2 节，完整 P0 验收仍未完成。
- [T02 候选版本与制品检查](docs/p0/version-lock.md)：本机版本、制品校验差异、依赖缺口及优先复用方案；版本尚未冻结；附录登记本次本地核心构建所用版本。

设计文档仍是目标合同；已实现范围以核心合同和测试为准。本地 Planner 校验不执行作业；真实 SQL 执行、四源连接器、Flink 提交适配器、数据库事务、首次发布及 F01–F13 仍待运行验收。本轮未安装、部署、提交或推送。
