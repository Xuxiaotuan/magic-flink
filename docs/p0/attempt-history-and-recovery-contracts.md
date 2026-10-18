# T04 尝试历史、安全切换与下一阶段恢复

2026-10-18，依据设计 v0.2 第 7.2、8.4、11.2、11.3 节，延续 [JDBC 事务合同](transactions-and-publication-contracts.md)。本批在原元数据库事务内补齐历史、停止证据、重试与意图恢复；只使用本地 H2 文件数据库验证，不安装或部署，不连接既有集群或数据库。

## 1. 对象与表职责

| 对象/表 | 职责 |
|---|---|
| [AttemptContracts](../../src/main/java/io/github/xuxiaotuan/magicflink/control/AttemptContracts.java) | AttemptRef、PlannedAttempt、AttemptView、StopEvidence、NextIntent、单事务 RecoverySnapshot |
| [JdbcRunStore](../../src/main/java/io/github/xuxiaotuan/magicflink/connectors/JdbcRunStore.java) | 继续作为公开 JDBC 元数据入口；新方法复用同一 run 锁和 revision |
| [JdbcAttempts](../../src/main/java/io/github/xuxiaotuan/magicflink/connectors/JdbcAttempts.java) | 包内实现，承接历史、停止、安全切换、派发登记和恢复，不调用外部执行器 |
| mf_stage | 每个 `(runId, stageType, inputAlias)` 唯一当前指针；保留身份/完成状态镜像并与历史交叉校验 |
| mf_attempt | 复合尝试键与单调 ordinal；计划/配置/消费/期限、定义摘要、一次绑定的执行身份、执行/接纳状态、派发 ID、停止引用、父尝试和 replacedBy |
| mf_stop_evidence | 按内容摘要追加的完整停止观察；UNKNOWN 也保留，不能覆盖为成功；历史引用缺失或内容不符即报损坏 |
| mf_next_intent | 固定消费清单集合与摘要、首个物化 attemptId、取消墓碑；逻辑键唯一，不以通知到达次数创建任务 |

一次尝试的定义不变；执行状态、停止确认、接纳完成和替换关系按协议更新，并追加 run_event。切换不删除或复用旧尝试，不覆盖旧检查结果，也不把多个尝试的文件拼起来。attemptId 按逻辑阶段键区分，不能强加“不同输入的局部序号必须不同”的限制。

Registration 新增必填的冻结 RetryPolicy 和 TimeLimits，写入 maxStageRetries/backoff/stageTimeout/evidenceWait；调用方须取自该 ResolvedRun 的 RuntimeProfile，不能由用户或回调临时上调。登记摘要升级为 RUN_REGISTRATION_V2 kind，已有发布请求/回执及 canonical-v1/domain-v2 内容不变。普通 Java 调用方必须显式提供新字段，不设置隐藏默认值。测试固定阶段 15 分钟、运行 30 分钟、证据等待 120 秒、退避 5 秒和最多一次阶段重试，不改动既有环境资源档位。

[metadata.sql](../../src/main/resources/db/metadata.sql) 更新为尚未部署的 P0 建表合同。本轮没有执行现有数据库迁移；旧实验表若要保留，后续须显式批准迁移，不能从当前指针猜造不存在的历史或停止证据。

## 2. 计划、执行身份与恢复读取

PlannedAttempt 固定逻辑阶段、attemptId、执行器、已持久化计划的对象键/摘要、ResolvedRunSpec/消费摘要和 deadline。对象键必须属于该 run/阶段/尝试的规范系统路径，不能引用其他运行的计划。实际完整 StagePlan 的生成、领域验证和不可变写入在调用前完成；本批元数据库不重新读取对象内容，也不假定一个传入的摘要就证明对象存在。

FLINK 的 PLANNED/SUBMITTING 状态允许尚无 CR UID/Job ID。取得真实观察后 bindExecution 一次性绑定精确资源，并同时更新历史与当前镜像；相同身份幂等，改变 UID/Job ID、计划或消费数据冲突。同一运行的新尝试不能接管旧尝试的 CR UID。INLINE 没有外部 Flink 资源，其计划身份可提前固定，仍须开始执行后才能接纳报告。

currentStages 是旧的已绑定身份接口：遇到尚未绑定的 FLINK 尝试明确报 MF005，不能返回空列表冒充没有活动尝试。recover 在同一 run 锁事务内返回 run/revision、各逻辑阶段的当前尝试和意图；attemptHistory 按 ordinal 返回完整历史。恢复验证定义摘要、计划载荷、绑定身份、当前镜像及停止引用，不从当前默认配置生成另一份计划。

## 3. 停止证据：观察未知不等于安全停止

recordStop 同时匹配 AttemptRef、计划摘要、已绑定资源和本次 dispatchId；观察时间不得早于该尝试/派发登记，也不能来自未来。确认停止必须同时具备：

- `dispatch=DRAINED`：原派发者已被可靠阻止继续发送，所有在途请求已归结；不是租约刚到期。
- 执行观察为 SUCCEEDED、FAILED、CANCELLED 或确定 NOT_CREATED，且身份对应当前资源；不是单次查询未见 CR。
- `connections=CLOSED`：Writer、远端查询和连接已确认结束；不是本地 future 超时。

缺任何一项，追加 UNKNOWN 证据并返回 WAIT_STOP，不切换当前尝试、不确认取消，也不释放额度。停止已确认后不能被新的未知观察重开。失败/停止原因由可信适配器分类并纳入证据摘要；DTO 和数据库事务无法独立证明物理进程已停。

已绑定的 FLINK 资源不能被报告成“从未创建”；尚未绑定且创建结果不明时，必须取得派发排空与请求确定未创建的联合事实。确定未创建的瞬时失败可以消耗预算创建新尝试，仍使用原消费版本；不把原不明请求当作失败盲目重发。

beginDispatch 先在数据库中登记固定 dispatchId 和 SUBMITTING/RUNNING，再由后续执行器产生副作用。只有首次 APPLIED 表示获得这次派发意图；同 ID 的 ALREADY_APPLIED 只供接管观察，不能解释为许可重发；不同 ID 冲突。确认停止、取消或替换后拒绝重新派发。若开始派发的 commit 确认丢失，恢复读取原 ID，不新造另一请求。

本批没有实现 Operator、线程/进程 fencing 或远端查询取消适配器；DRAINED/CLOSED/终态仍须由未来受控适配器提供真实证据。数据库租约与 CAS 本身不能阻止一台失联机器继续访问外部系统。

## 4. 安全切换与重试预算

retryCurrent 在同一 run 锁和 revision CAS 内完成：

1. 核对旧逻辑阶段仍为当前尝试、phase 合法、没有取消或发布授权、阶段尚未接纳完成。
2. 读取并核验已持久化的完整停止证据；未知返回 WAIT_STOP，不能只看 attempt 状态或 stopDigest 非空。
3. 新尝试必须使用新 attemptId/系统计划路径，逻辑键、ResolvedRunSpec 和 consumedDataDigest 不变。重算前复用原封存输入；不切到最新数据。
4. 检查冻结 maxStageRetries、退避、run deadline 及新尝试不超过 stageTimeout 的 deadline；EVIDENCE_LOST 还须自可靠停止观察起等待完整 evidenceWait，不能只等待重试退避；重复已成功切换的相同请求不重复扣预算或追加第三条历史。
5. 原子追加 ordinal+1 的新尝试、记录旧 replacedBy，并移动唯一当前指针。任何约束/提交错误均回滚整组变更；丢确认后按相同新 ID 读取恢复。

允许的受限原因：瞬时失败、COLLECT/COMPUTE 提交证据无法恢复、INLINE 检查超限。INLINE_LIMIT 只允许检查阶段 INLINE→FLINK；普通重试不能随意切执行器。已记录的必需检查 FAIL、永久错误、无分类原因、预算耗尽、过期、已接纳完成或取消均不能自动重试。

目标检查 ERROR 重试只建立新的当前检查尝试，保留旧 ERROR/PASS 记录；授权只读取新身份对应的全部 PASS，旧 PASS/回执迟到只审计。WRITE 的新 loadId 分配/目标状态协议尚未接入，因此本接口明确拒绝 WRITE 重试，避免错误复用旧 loadId；这部分仍归 P2/P3，而不是用通用重试掩盖目标写入边界。

## 5. 下一阶段意图的持久恢复

sealFile 的事务继续同时接纳 SEALED、完成阶段和写唯一下一逻辑意图，并将消费清单的显式集合与摘要一起保存。recover 会重新核对清单均属于当前成功尝试；缺失、替换或摘要不符报 MF004，不扫描目录、不新采集或重新选最新版本。

materializeNext 用同一事务核对当前 run/revision、phase、取消、原消费集合和计划引用，建立新逻辑阶段的首个 PLANNED 尝试与当前指针，填入 firstAttemptId。相同物化请求幂等，不同计划/ID 冲突。它不执行检查，也不调用 Operator。

崩溃/丢确认后的顺序：读取 RecoverySnapshot → 验证原意图/清单/计划 → 使用当前尝试的计划与派发状态接管观察。firstAttemptId 作为首个物化的历史引用不随重试覆盖；真正的可处理尝试从当前指针读取。旧物化通知再次到达时不能启动已被替换的首个尝试。

旧 Registration 仍是已知执行态的可信登记/导入入口；真实服务首个 COLLECT 的受理、初始计划持久化与 CR 创建尚待 P1 接入，本批不使用假 UID 占位。

本批完成 COLLECT→PRECHECK 与 COMPUTE→RESULT_CHECK 的文件关口意图恢复。检查完成后的全阶段协调、业务规则执行、通用调度循环及真实资源预留/派发仍待后续接入，不把一条持久意图称为已经提交的作业。

## 6. 取消完成

cancel 仍是请求；finishCancellation 在同一 run 锁中再次确认没有 publication_intent，所有已登记尝试均有完整有效的停止证据，才转 CANCELLED 并将未消费意图标成取消墓碑。即使文件阶段已接纳成功，仍不能凭成功字段推断派发者/连接已排空。任何 UNKNOWN 继续 CANCEL_REQUESTED/WAIT_STOP。

发布已授权时仍返回 CANCEL_TOO_LATE；本批不撤销授权、不删除产物，也没有自动释放全局/连接额度。真实资源释放需要后续 resource_reservation 协议。

## 7. 验证范围

测试继续使用现有 Java 17/Maven/H2 缓存与专属临时文件数据库。验证历史重开、事务回滚/丢确认、当前指针 CAS、双连接竞争、检查重试、派发/绑定约束、停止未知与取消完成、源清单缺失的拒绝；实际测试数量和最终构建见下文记录。

这证明本地数据库协议按给定可信事实处理，不证明真实 Operator 派发被 fencing、Flink/INLINE/Writer 已停止，或 PostgreSQL 锁/进程崩溃/网络故障已验收。F01–F13 的真实运行要求、T04/P0 总体 evidence_incomplete 不变；不依赖集群的实现继续推进，运行准入单独处理。


### 本轮构建记录

2026-10-18 18:26:17（Asia/Shanghai），使用既有 Java 17.0.14/Maven 3.9.16 和固定堆限制执行 `mvn -o package`，退出码 0，**227 tests / 0 failures / 0 errors / 0 skipped，BUILD SUCCESS**。新增 [JdbcAttemptRecoveryTest](../../src/test/java/io/github/xuxiaotuan/magicflink/protocol/JdbcAttemptRecoveryTest.java) 26 项，原有 201 项全量回归；日志在忽略目录 `target/t04-attempt-final-package.log`。普通应用 JAR 含 134 个应用 class，不含 Flink/Calcite/H2 或测试类，仍不是 T05 fat JAR。

新增用例覆盖：完整停止/未知派发/连接未关闭、退避与预算耗尽、原输入/配置不变、永久错误/必需 FAIL 拒绝、INLINE_LIMIT、ERROR 新检查、证据等待和阶段期限；同时验证切换回滚/丢确认/双连接竞争、派发/绑定/物化丢确认、旧 UID 和停止派发拒绝、取消与物化竞争、取消完成、停止/清单/意图缺失及运行身份不一致的拒绝。

初始缺 API 的编译失败不作为行为证明。七项追加反例取得真实断言失败后修正：已停止派发被同 ID 再接管、旧 CR UID 绑定到新尝试、恢复忽略缺失停止证据、确定未创建的瞬时失败无法安全重试、提交证据等待窗口未到仍切换、新尝试超阶段预算、旧当前身份读取接口遗漏运行 spec 核对。另修正多输入历史表上多余的 run+attemptId 唯一约束，保留正确的复合阶段键。

Markdown 本地链接、所有修改/新增文件空白检查及 `git diff --check` 通过；设计 v0.2 SHA-256 仍为 `fff41e1fbe958dbec7c1490c2a76633dc72d60ea9077957771c4c042f8818291`。既有 Calcite 可选注解和 SLF4J NOP 警告仍在，发行版依赖/日志装配留给 T05。本批未安装、部署、暂存、提交或推送。


后续实现：首个 COLLECT 的完整计划受理、实际不可变对象回读、协调器单步接管与本地资源预留/归还已进入[第六批合同](admission-coordinator-and-reservations.md)。本页保留第五批历史边界；完整后续阶段、真实派发者 fencing、停止来源及 PostgreSQL 仍独立验收。
