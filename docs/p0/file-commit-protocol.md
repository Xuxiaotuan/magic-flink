# T03：文件提交源码核对与原型边界

核对日期：2026-10-18。依据设计 v0.2 第 7、8 节及 P0 T03。

**静态结论：GO_TO_SPIKE。** 固定版本存在委托原生 FileSink 提交并接收成功 committable 的接口路径，可以进入薄封装原型；这不是文件协议运行 PASS。原生 FileSink 直接接一个清单生成器不足以满足合同，需要保留 writer 身份和结束证据的适配层。本轮只完成源码核对，不实现该适配器，不启动集群。T05–T07/F01–F13 仍为 NOT_RUN。

## 1. 固定源码与逐项证据

官方 tag `release-1.20.5` 经 GitHub API 解析到 commit `0980485040c6dbec3a1b32da05c4b655cf01ad2b`。以下链接全部固定到该 commit。本机 Flink 分发包的校验差异仍按版本清单处理；源码核对不替代制品验真或漏洞扫描。

| 问题 | 源码事实与定位 | 对平台的约束 |
|---|---|---|
| 原生委托入口 | [FileSink:132](https://github.com/apache/flink/blob/0980485040c6dbec3a1b32da05c4b655cf01ad2b/flink-connectors/flink-connector-files/src/main/java/org/apache/flink/connector/file/sink/FileSink.java#L132) 提供 createWriter/restoreWriter、writer state serializer、createCommitter、committable serializer 和 pre-commit topology | 包装接口，复用原生提交；禁用 compaction，不修改 Flink 本体 |
| 实际文件路径与长度 | [OutputStreamBasedPartFileWriter:213](https://github.com/apache/flink/blob/0980485040c6dbec3a1b32da05c4b655cf01ad2b/flink-connectors/flink-file-sink-common/src/main/java/org/apache/flink/streaming/api/functions/sink/filesystem/OutputStreamBasedPartFileWriter.java#L213) 的 OutputStreamBasedPendingFileRecoverable 公开 getPath/getSize（getCommitRecoverable 为包可见，不可由应用调用）；[S3Recoverable:76](https://github.com/apache/flink/blob/0980485040c6dbec3a1b32da05c4b655cf01ad2b/flink-filesystems/flink-s3-fs-base/src/main/java/org/apache/flink/fs/s3/common/writer/S3Recoverable.java#L76) 的 getObjectName 提供最终 key | 只识别受支持的 recoverable 类型，使用原生公开 Path 的完整 URI 作为文件位置；path 为 null 或长度未知就拒绝；不能用 bucketId 猜 key，也不能跨 S3 插件类加载器强转私有 recoverable；S3 getObjectName 仅用于此次源码追踪 |
| 成功提交 | [FileCommitter:56](https://github.com/apache/flink/blob/0980485040c6dbec3a1b32da05c4b655cf01ad2b/flink-connectors/flink-connector-files/src/main/java/org/apache/flink/connector/file/sink/committer/FileCommitter.java#L56) 对 pendingFile 执行 commitAfterRecovery；还处理 in-progress/compacted cleanup | 过滤清理消息，不能把每条 committable 当数据文件；一批中间异常不能发布全局索引 |
| 已接受成功消息 | [CommitterOperator:173](https://github.com/apache/flink/blob/0980485040c6dbec3a1b32da05c4b655cf01ad2b/flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/operators/sink/CommitterOperator.java#L173) 先 commit，再向下游发送 getSuccessfulCommittables；[SupportsPostCommitTopology:32](https://github.com/apache/flink/blob/0980485040c6dbec3a1b32da05c4b655cf01ad2b/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/connector/sink2/SupportsPostCommitTopology.java#L32) 提供 post-commit topology 接口 | 成功消息必须来自这个通道，prepareCommit 的返回值仅为待提交集合 |
| 重试与完整接收 | [CheckpointCommittableManagerImpl:137](https://github.com/apache/flink/blob/0980485040c6dbec3a1b32da05c4b655cf01ad2b/flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/operators/sink/committables/CheckpointCommittableManagerImpl.java#L137) 用 received count、预期 subtask 数及请求状态管理提交；失败重试耗尽抛异常 | 适配聚合器仍须独立核对最终 writer 集合与结束标记，不能拿单个成功消息证明全局完整 |
| writer 身份 | [InitContext:70](https://github.com/apache/flink/blob/0980485040c6dbec3a1b32da05c4b655cf01ad2b/flink-core/src/main/java/org/apache/flink/api/connector/sink2/InitContext.java#L70) 的 getTaskInfo 提供 subtask 与 attempt number；[CommittableWithLineage:32](https://github.com/apache/flink/blob/0980485040c6dbec3a1b32da05c4b655cf01ad2b/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/connector/sink2/CommittableWithLineage.java#L32) 原生消息只有 committable、checkpointId、subtaskId；提交后外层 subtask 是 committer 的 subtask | 在 writer 端封装原始身份，随 payload 经过提交通道；禁止把 committer attempt 当 writer attempt |
| 空 writer 与 endInput | [SinkWriterOperator:188](https://github.com/apache/flink/blob/0980485040c6dbec3a1b32da05c4b655cf01ad2b/flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/operators/sink/SinkWriterOperator.java#L188) 在 endInput 时 flush(true) 并 prepareCommit；即使集合为空仍发 CommittableSummary | 原生 summary 不能表达完整原 writer 身份；适配器必须发送携带原身份的显式 WriterEnd，零文件也必须有 |
| S3 恢复提交 | [S3Committer:89](https://github.com/apache/flink/blob/0980485040c6dbec3a1b32da05c4b655cf01ad2b/flink-filesystems/flink-s3-fs-base/src/main/java/org/apache/flink/fs/s3/common/writer/S3Committer.java#L89) 在恢复提交失败后查询对象，检查 ContentLength | 长度相等不是内容相等；封存前仍须逐文件读回 SHA-256、行数及 Schema 核对 |
| 输入完整性 | 上述 FileSink 接口不知道源分片计划及业务控制总数 | 输入完成证据由受控 Source 与固定 StagePlan 提供；不能从文件数或 writer 数倒推 |

另核对 [FileWriter.flush/prepareCommit](https://github.com/apache/flink/blob/0980485040c6dbec3a1b32da05c4b655cf01ad2b/flink-connectors/flink-connector-files/src/main/java/org/apache/flink/connector/file/sink/writer/FileWriter.java#L203)：endOfInput 状态传给各 bucket 的 prepareCommit；[S3RecoverableWriter.recoverForCommit](https://github.com/apache/flink/blob/0980485040c6dbec3a1b32da05c4b655cf01ad2b/flink-filesystems/flink-s3-fs-base/src/main/java/org/apache/flink/fs/s3/common/writer/S3RecoverableWriter.java#L85) 由原生 recoverable 恢复并 closeForCommit，适配器无需直接构造或反射 S3 内部状态。

## 2. 原型的最小适配路径

1. `EvidenceFileSink` 委托固定版本 FileSink 的 writer、state 和 committer。使用单一内部 envelope 包含原生 pending committable、run/stageAttempt、JobID、逻辑 sink operator ID、writer subtask、task attempt number 及本次 writer 文件序号。operator ID 由固定计划赋值，不能只用 JobID + subtask 区分多 sink。
2. `flush(true)` 后最后一次 prepareCommit 加入 WriterEnd（预期数据 committable 数，允许零）。每次重复提交复用稳定身份；清理消息不参与文件数。封装序列化必须保留原生 serializer version。端点/路径只来自固定计划。
3. 包装 committer 将文件 envelope 交给原生 committer，完整转发 CommitRequest 的重试/失败状态。WriterEnd 不代表文件已提交，只有对应所有文件请求成功且无未知请求时才可参与完整性判断。不能在 delegate 循环中直接写成功索引。
4. 利用 SupportsPostCommitTopology 接收原生 CommitterOperator 接纳后的 envelope。单并行度证据聚合器按逻辑 writer 收集成功文件及 WriterEnd；原始 writer 身份在 payload 中，不能依赖提交后外层 lineage。固定并行度、关闭 speculative execution、关闭 compaction，P0 不从其他 stageAttempt 的 savepoint 恢复文件集合。
5. **被接受 attempt 的依据是成功提交通道中的原始身份及本轮完整结束集合，不是 attempt number 最大值。** task 重试、恢复状态中的 pending 文件归属、局部 failover 与 post-commit 重放必须由 F03/F04/F10 证明。发现同一逻辑 writer 的互斥结束身份、跨 stageAttempt、不同内容重复或未决请求时拒绝终结；若真实恢复语义要求同时合法接受多个 attempt 的增量，必须返回协议评审，不能静默取并集。此处只确认接口可携带所需证据，未声称胜出语义已经运行验证。
6. 所有预期 writer 均有唯一合法结束证据、各自文件数匹配、全部逻辑输入分片完成，才按固定排序生成 CommitEvidence。相同文件身份/内容的重放归一；同身份不同路径/长度/摘要冲突。同键最终索引必须原子 create-if-absent；同内容幂等，不同内容拒绝，读取失败维持 UNKNOWN。
7. 聚合器在有限输入结束时写不可变索引，写入失败使作业失败。平台还须保存与 CR UID/JobID 匹配的成功终态；终态及完整索引均存在，才逐文件核验并构建 Manifest。对象存储中存在索引但作业失败时不可封存；失败文件不在下一 attempt 中复用。

这一路径使用 Flink 扩展接口和受控 envelope，不引入通用事务日志。文件内容摘要可在提交后核验阶段计算；索引不可把未知摘要填成空值后宣称已验证。平台原子对象写入能力尚未实现，F08 必须在真实 S3 兼容存储证明。

## 3. 终结与恢复约束

- 原始事实：成功终态证据、完整 CommitEvidence、不可变 Manifest。StageReceipt 可重建，不是唯一事实来源。
- 当前尝试资格与取消状态每次均重新读取；封存和阶段推进必须在同一元数据库事务中检查资格并 CAS。旧 attempt 或取消后的回执仅归档。
- 终态 UNKNOWN 不推断成功；对象 GET 超时不推断不存在。证据未齐先等待；超过证据等待期限且已确认旧执行停止，才能废弃并创建新 attempt。自动重试预算由 RuntimeProfile 另行控制。
- 已有 Manifest 内容冲突、已封存对象损坏或 receipt 摘要冲突必须拒绝并保留现场，不能覆盖修复。
- 先提交文件但没有全局完整证据：不能扫描目录补清单。成功终态加完整证据可重建 Manifest/receipt；缺少其中任何一项都不能补造成功。

## 4. 升级与剩余验证

FileSinkCommittable、FileCommitter、recoverable 与部分 context/runtime 类依赖 Internal/Experimental API。集中隔离于未来 runtime 适配包；升级 Flink、S3 插件、serializer 或更改并行度/compaction/recovery 方式，必须重做 T03、T05–T07，不承诺跨版本二进制兼容。

未验证：适配器编译与序列化、失败 task 隔离、零行及多文件、真实 S3 重复提交和条件写、聚合器崩溃恢复、输入分片证据、终态保存、数据库事务和并发 CAS。F03 不能证明排除失败 attempt，或需要 fork Flink/解析日志/反射私有字段才成立时，该路线转 NO_GO。环境 NO_GO 仅限制这些运行实验；严格 YAML 与纯恢复判定的本地开发可继续。
