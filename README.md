# magic-flink

基于 Flink Kubernetes Operator 的自研 YAML 有界批处理服务。用户只编写 YAML，平台统一构建、测试和扫描 fat JAR，规划支持多源采集、SQL Join、前后置检查与验收后发布。

当前处于设计阶段，尚未实现产品代码或完成运行验收。

## 设计文档

- [YAML 批处理服务设计 v0.2](docs/design/flink-yaml-batch-service-design.md)：模块职责、YAML 规范、执行流程、失败恢复协议、P0–P5 实施计划及 33 项验收用例。
- [P0 实施任务与验收计划](docs/superpowers/plans/2026-10-18-p0-baseline-and-file-commit-validation.md)：版本选型、8 个实施任务、文件提交协议的 13 个实验及进入 P1 的门槛；T01/T02 尚未通过任务验收；已完成 15 分钟联合观测，单 Application 资源准入 NO_GO，实验均未运行。
- [T01 环境检查与最小补齐方案](docs/p0/environment-and-fixtures.md)：包含 E01 的 CRD/权限核对及 2026-10-18 14:43:48–14:58:54 联合观测（第 6.2.1 节）；宿主机压力、CPU/load 和两处磁盘未达标，结论为 **NO_GO**。按实测节点峰值重算，单 Application + 外部 S3 总预算约 11.464 GiB，仅余 0.271 GiB；未满足条件见第 6.2.2 节，完整 P0 验收仍未完成。
- [T02 候选版本与制品检查](docs/p0/version-lock.md)：本机版本、制品校验差异、依赖缺口及优先复用方案；版本尚未冻结。

文档中的协议和验收条件属于设计约定，具体实现与运行能力仍需后续验证。
