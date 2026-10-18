# magic-flink

基于 Flink Kubernetes Operator 的自研 YAML 有界批处理服务。用户只编写 YAML，平台统一构建、测试和扫描 fat JAR，规划支持多源采集、SQL Join、前后置检查与验收后发布。

当前处于设计阶段，尚未实现产品代码或完成运行验收。

## 设计文档

- [YAML 批处理服务设计 v0.2](docs/design/flink-yaml-batch-service-design.md)：模块职责、YAML 规范、执行流程、失败恢复协议、P0–P5 实施计划及 33 项验收用例。
- [P0 实施任务与验收计划](docs/superpowers/plans/2026-10-18-p0-baseline-and-file-commit-validation.md)：版本选型、8 个实施任务、文件提交协议的 13 个实验及进入 P1 的门槛；当前仅完成计划，尚未实施。

文档中的协议和验收条件属于设计约定，具体实现与运行能力仍需后续验证。
