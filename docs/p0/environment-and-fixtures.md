# T01 环境盘点、实验边界与最小补齐方案

**检查日期：** 2026-10-18。**状态：** T01/T02 盘点与 E01 专项只读核对已完成；OrbStack 复用有静态证据支持，资源预算尚未确定，T01/E01 整体验收仍为 `evidence_incomplete`。

本文保留 T01/T02 初次盘点（项目基准 `76f7739`），并补充随后基于 `1225e2a` 的 E01 专项核对。E01 证据来自显式指定 OrbStack context 的资源/权限查询、官方 Chart 对比、离线渲染及节点统计；资源快照约为 2026-10-18 14:14（Asia/Shanghai）。本轮仅整理已有检查结果，未重跑环境检查，未安装、启动、部署、修改环境或读取业务表。运行状态会变化，实施补齐前须重新核对。

关联文档：[P0 实施计划](../superpowers/plans/2026-10-18-p0-baseline-and-file-commit-validation.md)、[T02 候选版本与制品核验](version-lock.md)、[设计 v0.2](../design/flink-yaml-batch-service-design.md)。本文保留脱敏后的关键结果与复查入口；完整命令输出未作为仓库附件保存，不能替代后续实验的证据索引。

## 1. 已具备的条件与实际缺口

| 对象 | 实际检查结果 | 对 P0 的意义 |
|---|---|---|
| 主机 | macOS 26.6.2，arm64，8 个逻辑 CPU，24 GiB 内存；初查文件系统约 42 GiB 可用，E01 复查约 40 GiB，使用率 91% | 可以继续静态研究；镜像、构建缓存和实验数据需控制空间，尚未证明实验容量充足 |
| 容器运行时 | Docker CLI/Engine 29.4.0，context 为 `orbstack`，服务端 linux/arm64；分配 8 CPU、约 11.7 GiB 内存 | 可复用运行时；分配量不等于空闲量，不代表两个 Application 并发实验已具备资源 |
| 本地 Kubernetes | 显式查询 `orbstack`，服务端 `v1.35.6+orb1`，节点 Ready、linux/arm64 | 集群存在；尚无本项目 namespace、配额、ServiceAccount 或权限验证 |
| kubectl / Helm | kubectl 1.33.9，Helm 4.0.1；只读查询成功 | kubectl 比服务端低两个 minor，需补兼容客户端；查询成功不等于版本受支持 |
| Apache Flink Operator | 旧 Helm release 声称 deployed，但对应 Deployment 为 NotFound；E01 已核对共享 CRD、旧 release 范围和现有 ZIO 权限 | 可保留 CRD、在新 namespace 规划独立 Operator；控制器运行能力仍未验证，详见第 2 节 |
| JDK / Maven / Flink | 已有 Java 17、Maven 3.9 系列、本地 Flink 1.20.5 目录 | 可减少后续工具安装，精确版本与制品身份缺口见 T02 |
| PostgreSQL | 本地已有声明为 17.11 的镜像；另一个项目的容器已停止；旧 Kubernetes PostgreSQL StatefulSet 副本数为 0 | 可考虑复用镜像层；尚无本项目可访问的实验库，未连接或核验数据库内容 |
| S3 兼容存储 | 检查范围内未发现运行中的 MinIO/RustFS 服务；已有 `mc` 客户端与 `minio/mc` 镜像 | 客户端不是存储服务；本项目 endpoint、bucket、权限与服务端版本均未取得 |
| 固定数据 | 未创建或查询本项目 fixture，未登记实际批次、权限和保留期 | 100 行 / 5050.00 目前是计划预期，不是已有数据的核验结果 |
| 扫描能力 | PATH 与已检查镜像中未找到 Trivy、Syft、Grype、Cosign 等相关工具；未取得扫描报告 | 不等于组织内没有扫描平台；优先确认既有获准平台，详见 T02 |

默认 Kubernetes context 指向另一个集群，本次没有查询该集群或切换默认 context。所有集群盘点均显式使用 `--context orbstack`。后续同样必须指定目标，不能依赖终端当前 context。

## 2. E01 专项结果：保留共享 CRD，限定新 Operator 范围

### 2.1 现有对象的管理来源与有效权限

| 对象 | 实际证据 | 归属与边界判断 |
|---|---|---|
| 旧 Apache release | `flink-lineage-test/flink-operator`，revision 2，Chart/app `1.17-SNAPSHOT`；保存的 manifest 引用 `flink-operator-local:38a9f1974650`，实际 Deployment 不存在 | Helm deployed 不代表控制器存活；保留 release，不恢复或接管 |
| 旧 release 的配置/RBAC | Helm values 的 `watchNamespaces` 与现存 ConfigMap 的 `kubernetes.operator.watched.namespaces` 均为 `flink-lineage-test`；相关 Role、RoleBinding、ServiceAccount、ConfigMap 有旧 release 注解 | 这些对象的 Helm 归属明确，配置范围不是整个集群；不复用或修改 |
| 四个 Flink CRD | 均为 Namespaced，served/storage/storedVersion 为 `v1beta1`，conversion 为 None；managedFields 显示 `helm` 在 2026-09-06 写入；无 release 归属注解、ownerReference | 可确认 Helm 管理痕迹，但不能唯一反推 release 或人工责任人；不将缺注解解释成“无主可删” |
| ZIO 服务 | Deployment `flink-lineage-test/zio-flink-operator`，两个 Pod Ready；镜像 `zio-flink-operator-lab:local`；`FLINK_NAMESPACE=flink-lineage-test` | 它是另一个项目的服务，不是待安装的 Apache Operator 1.16.1 |
| ZIO 权限 | ServiceAccount、Role/RoleBinding 均为 `zio-flink-operator`；Role 允许原 namespace 内三类 Flink CR 的读写/watch；未发现额外 Flink 跨 namespace 授权 | 默认 namespace 不是安全边界，实际隔离依靠 RBAC；系统组继承的权限仅涉及发现与自查等能力，未增加 Flink 访问范围 |
| CR 实例与 webhook | 四类 Flink CR 当前实例数均为 0；现存 admission webhook 只有 cert-manager 的相关配置，无 Flink webhook | 当前没有 Flink admission 拦截冲突；实例为空不代表 CRD 没有使用者 |

使用管理员的 impersonation 能力，携带 ZIO ServiceAccount 身份及对应系统组，只执行 FlinkDeployment 的 list 请求，未读取 token：

| 查询 namespace | 实际返回 | 证据边界 |
|---|---|---|
| `flink-lineage-test` | 成功，空列表 | 原范围访问可用 |
| `default` | Forbidden | 已有 namespace 的跨范围读取被拒绝 |
| `magic-flink-p0` | Forbidden | 当前权限未授予拟建 namespace；该 namespace 尚不存在，创建及绑定后还需重验 |

本地 ZIO 源码 `5075855` 辅助显示：轮询默认使用 `FLINK_NAMESPACE`，watch 使用 namespaced API，HTTP 请求可以另传 namespace。因此不能将环境变量当作不可绕过的限制。该源码未与运行镜像建立制品对应关系，结论以实际 Pod 配置、RBAC 和上述访问结果为准，不宣称已追踪每条运行中的 watch 连接。

### 2.2 四份 CRD 与候选 Chart 的对比

已读取[官方 1.16.1 Chart](https://archive.apache.org/dist/flink/flink-kubernetes-operator-1.16.1/flink-kubernetes-operator-1.16.1-helm.tgz)，大小 67952 字节，SHA-256 为 `e3e107e7bf0a9e9762d699f81d782991a974d3193532e4953a4d4a8e3e9d8579`，与[官方索引](https://archive.apache.org/dist/flink/flink-kubernetes-operator-1.16.1/index.yaml)的 digest 相符。此检查未替代签名、扫描或准入。

| CRD（均属 `flink.apache.org`） | OpenAPI schema | 原始 spec 差异 |
|---|---|---|
| `flinkdeployments` | 一致 | 当前对象显式包含 conversion=None、listKind；候选两处打印列显式包含 priority=0 |
| `flinksessionjobs` | 一致 | 同上，两处 priority=0 |
| `flinkstatesnapshots` | 一致 | 同上，三处 priority=0 |
| `flinkbluegreendeployments` | 一致 | 同上，一处 priority=0 |

仅补齐这些默认表示后，四份完整 spec 均一致；没有忽略字段 schema、served/storage、子资源等行为配置。由此支持保留当前 CRD、后续安装跳过 CRD 的方案，不需要因旧 Chart 标为 1.17-SNAPSHOT 就执行降级。此结论是结构对比，不是 1.16.1 控制器在 Kubernetes 1.35.6 上运行成功的证据。

### 2.3 离线渲染与安全复用结论

使用现有 Helm 4.0.1 对上述 Chart 做本地 `helm template`，release/namespace 为 `magic-flink-p0`，Kubernetes 版本参数为 `1.35.6`，`watchNamespaces[0]=magic-flink-p0`，并指定 `--skip-crds`。没有使用服务端 dry-run 或提交资源；临时 Chart 文件已清除。

结果为 14 个渲染对象：无 CRD、ClusterRole、ClusterRoleBinding；namespace 级资源全部位于 `magic-flink-p0`；Operator 配置只监听该 namespace。两个集群级 webhook 的名称均为 `flink-operator-magic-flink-p0-webhook-configuration`（kind 不同），namespaceSelector 都只匹配 `kubernetes.io/metadata.name=magic-flink-p0`，failurePolicy 为 Fail。Certificate/Issuer 也仅在新 namespace，复用既有 cert-manager，不升级它。渲染出来的 Secret 等对象只是文本，没有创建到集群。

**结论：复用 OrbStack 的方案有静态证据支持，当前未发现必须另建集群的冲突；E01 尚未整体验收。** 保留共享 CRD、旧 release、ZIO 服务及其权限；新 Operator 使用独立 release、namespace 和 ServiceAccount。必须显式限定监听范围，不能使用空 `watchNamespaces` 的默认配置。

本次没有确立共享 CRD 的人工维护责任人，也没有授权接管其生命周期。后续 CRD 升级或删除需跨项目重新核对。安装前还须对最终配置重新渲染并检查对象重名、跨 namespace 权限、webhook 选择器与当前资源是否变化；静态对比不能替代第 6 节的容量与运行验收。如果最终方案需要覆盖共享对象或扩大现有服务权限，应暂停并重新评审，不自动改用独立集群或清理旧 release。

## 3. 缺失项的最小补齐顺序

下表列出剩余补齐动作；E01 的所有权线索、权限与 CRD 对比已完成，详见第 2 节。其余未取得实际证据的内容仍为待实施，不表示已创建资源。

| 编号 / 责任角色 | 最小方案，优先复用 | 影响边界与完成证据 |
|---|---|---|
| E01 平台工程：确定集群边界 | 共享对象与作用范围的只读核对已完成；下一步按第 6 节补齐代表性负载基线、资源预算、实验时段与责任人 | 不切 context、不升级集群、不改已有工作负载；最终清单复核及资源预算通过后，才具备申请新 namespace 与受限权限的条件 |
| E02 平台工程：兼容客户端与 Operator | 保留现有 Helm；后续取得与 Kubernetes 1.35 兼容的 kubectl 精确版本，放项目专用工具目录并显式调用；Operator 1.16.1 仍按 T02 核验 | 不替换全局 kubectl；Operator 部署以 E01 通过为前提。验收包括真实 Deployment/Pod、限定监听范围、RBAC 和最小 Application 行为，不能只看 Helm 状态 |
| E03 数据库负责人：实验库 | 优先复用已有 PostgreSQL 17.11 镜像层；后续新建本项目实例与新数据卷，用不同 schema/角色隔离元数据、固定源、目标 | 不启动旧项目容器、不扩容旧 StatefulSet、不挂载旧 PVC。记录实际 `SELECT version()`、访问权限、TLS 适用约束和固定数据核验结果；主机无需先装 psql，可使用专属实例的客户端 |
| E04 存储负责人：专属对象范围 | 先确认是否有已获准、可访问的测试 S3 服务；若有，仅申请专属 bucket/prefix 与受限凭据引用。若没有，后续再选一个获准产品建立独立本地实例和新卷 | 目前没有已确认可复用 endpoint，不扫描或尝试其他项目凭据。记录服务端版本、权限、TLS、保留期；本地预实验不能代替银行目标存储验收，原子写入与 multipart 等能力仍由 T06 实测 |
| E05 平台/数据负责人：固定批次 | E03/E04 就绪后，仅创建第 4 节的合成数据与独立控制账本 | 确认版本身份、冻结权限和保留期后再读取；预期值必须与实际查询证据分列，不能把生成规则当作核验结果 |
| E06 平台工程：资源与清理范围 | 后续只给本项目资源加所有者标识、设配额和保留期；先完成单 Application 小数据预检，再进入计划规定的并行和竞争实验 | 不停止其他项目腾资源，不清理共享镜像/PVC。资源不足就暂停相关实验；不以降低 F01–F13 验收要求换取通过 |

kubectl 支持范围依据 [Kubernetes 官方版本偏差策略](https://kubernetes.io/releases/version-skew-policy/#kubectl)：与 API server 相差不超过一个 minor。此处建议优先选择 1.35 系列的获准补丁版本，实际版本与摘要取得后登记到 T02，当前不虚填。

## 4. 固定数据与访问边界：待创建、待核验

沿用 P0 计划，两个 JDBC 源使用同一独立 PostgreSQL 实验实例，不增加 HTTP/BQ/ClickHouse 实现范围：

- `orders_jdbc`：100 笔订单，`order_id` 为 1…100，金额为 1.00…100.00，采用精确十进制定点类型。
- `customers_jdbc`：10 个唯一客户；订单使用固定客户映射且全部匹配。映射明细在创建 fixture 时登记，当前未生成。
- Join 预期：100 行、金额总计 5050.00；控制数来自独立 fixture 账本，不从本轮采集计数器反推。
- 空输入与过滤后空输出使用独立批次，分别验证，不能混成同一种情况。
- 批次置为 ready 前核验内容和控制数；置为 ready 后，实验读取角色没有修改权限。保留版本、摘要、创建者、冻结时间及保留截止时间；这些字段当前尚无实际值。

建议资源命名为 namespace `magic-flink-p0`、容器 `magic-flink-p0-postgres`、数据卷 `magic-flink-p0-*`、对象前缀 `magic-flink/p0/{executionId}/`，统一标识 `app.kubernetes.io/part-of=magic-flink`。这些名称仅是待核准约定，未创建资源；不能凭名称前缀批量删除，清理需基于本次创建资源的明确清单。

仍需取得：实际资源所有者、凭据引用、网络/TLS 方式、源只读权限、元数据与目标角色、对象创建/读取/废弃权限、保留期，以及故障注入可操作的准确对象范围。公开仓库只保存脱敏配置说明，不保存凭据或内部端点。

## 5. 必须保护的已有环境

以下对象只作为保护清单，不作为待接管资源：

- `springboot-inventory-tutorial-postgres-1` 已停止的容器及其数据；只考虑复用已存在的镜像层。
- `paimon-gateway` 中副本数为 0 的 PostgreSQL StatefulSet 与 20 GiB PVC。
- `cube-ha-remediation`、`cube-operator-demo` 中原 MinIO 数据 PVC；PVC 为 Bound 不代表有可用 S3 服务，也不证明其数据可供本项目使用。
- 原 Flink namespace、Helm release、集群级 CRD，以及运行中的 `zio-flink-operator-lab`、Meteor、Spark Operator、cert-manager 等。
- 现有 Flink 安装目录、全局 Java/Maven 配置及其他项目的仓库认证信息。

共享主机和集群仍会共享 CPU、内存及磁盘，不能承诺仅靠 namespace 就“零影响”。本项目新增负载必须以余量检查、限额和受控实验时段为前提；无法证明满足边界时，暂停部署。

## 6. 剩余资源预算与分阶段验收条件

### 6.1 已取得的容量快照

2026-10-18 14:14 左右，节点 Ready，MemoryPressure/DiskPressure/PIDPressure 均为 False。`kubectl top` 返回 Metrics API 不可用，随后通过只读 kubelet stats summary 取得统计，未安装 metrics-server。

| 口径 | 实际观察 | 不可推导的结论 |
|---|---|---|
| Kubernetes allocatable | 8 CPU、约 11.7 GiB 内存 | 不是全部可供本项目独占 |
| 已有 Pod requests / limits | CPU 1.3 / 2 核；内存 2630 / 3412 MiB | 部分工作负载未设 requests/limits，不能用差值当真实空闲预算 |
| kubelet 瞬时统计 | CPU 约 0.30 核；memory.availableBytes=9311125504，约 8.67 GiB；working set 约 3.06 GiB | 不是高峰值、持续余量或两个 Application 的承载证明 |
| 主机可用磁盘 | `df -h` 约 40 GiB，使用率 91% | 与虚拟机/容器文件系统的可用量不能相加，也不能按 PVC 声明容量判断空间充足 |
| 配额 | 集群未发现 ResourceQuota/LimitRange；`magic-flink-p0` 不存在 | 当前没有本项目已生效的容量保护 |

### 6.2 必须确定的预算

以下为预算登记要求，数值尚未确定，不是部署配置或已批准额度。平台工程汇总，数据库/存储负责人确认各自占用，现有项目维护者确认实验时段；具体责任人尚待登记。

| 组成 | 必须登记的数值与口径 | 通过条件 |
|---|---|---|
| Operator 与 webhook | 实际副本数、各容器 CPU/内存 requests/limits、临时存储及日志增长上限 | 纳入总预算，不能只计算 Flink JM/TM |
| Service/验证进程 | 所在主机或集群、进程数、CPU/内存及文件核验缓冲上限 | 即使跑在宿主机或 Docker 中，也计入同一物理资源预算；不以 Kubernetes quota 代替这些限额 |
| Flink Applications | 同时运行数量、每个 JM/TM 的 requests/limits、TM 数、每 TM slot 数、失败期间可能重叠的 Pod | 保留计划的最多 2 个 Application 与并行度 1/4 场景；逐场景核对 slot 和峰值，不能把一个作业的 4 slot 重复计给两个同时运行的作业 |
| PostgreSQL | 实例内存/CPU、连接预算 8（探测最多 2）、数据/WAL/日志增长 | 保持新实例/新卷边界；不占用旧项目数据库容量 |
| 对象存储 | 实际采用内部 endpoint 还是本地实例；本地实例资源、输入/输出/失败尝试/multipart 数据与保留空间 | 远端服务计入本机客户端缓冲；本地服务需计入服务端开销；保留至少 7 天的空间纳入预算 |
| 构建与镜像 | 镜像拉取、构建缓存、临时解压与日志的磁盘峰值 | 避免与运行高峰叠加；不以清理其他项目文件或镜像作为预算前提 |
| 共享余量 | 代表性时段的既有负载基线、CPU/内存安全余量、磁盘最低剩余量、停止新增实验的阈值 | 用测量和协商结果填写明确数值；不能直接采用这次瞬时余量作为可分配上限 |

预算需同时通过调度与实际使用两种检查：已有 requests 加新增 requests 不超可调度资源；代表性既有使用量加本项目并发峰值预算及预留量不超可用容量。磁盘还需覆盖构建峰值、数据保留及失败残留。不能对同一占用重复相加，也不能因已有工作负载没有 limit 就把其预算算为零。

项目 namespace 的 ResourceQuota、LimitRange 与各容器限额应由该预算推导，当前不虚填数值。小数据单 Application 预检只能作为后续运行的第一步，不能替代双 Application、并行度 4 及故障重叠场景。预算不足时暂停相关实验并调整本项目执行时段或配置，不缩容其他项目，也不降低原验收要求。

### 6.3 E01 结束条件与后续运行验收的区别

| 阶段 | 当前状态 | 必须取得的证据 |
|---|---|---|
| E01 只读范围核对 | 已完成 | 第 2 节的管理来源、当前 RBAC 拒绝结果、四份 CRD 对比、离线渲染结果 |
| E01 剩余准备条件 | 未完成 | 责任人/实验时段、代表性负载与明确资源预算、停止阈值、最终渲染清单的资源归属和作用范围复查；不得包含旧资源改写 |
| E02 环境隔离验收 | `NOT_RUN` | 后续获准部署后，新旧 ServiceAccount 对各自范围的访问与跨范围拒绝；新 Operator 只监听专属 namespace；webhook 的实际选择器、证书与就绪状态；共享 CRD spec 和旧项目配置保持不变 |
| T05/T06 运行与容量验收 | `NOT_RUN` | 同一制品的真实 Application、规定的并发/slot 场景、运行高峰资源与停止阈值证据，以及实验前后旧项目健康基线对照；不能以 Pod Running 替代作业和文件验收 |

E01 准备条件可以在部署前完成，不以尚未授权的运行实验作为完成只读核对的前提；反过来，E01 通过也不会自动使 E02、T05/T06 或 P0 GO 通过。静态隔离无法保证共享硬件上的绝对零影响，运行验收必须保留这个边界。

## 7. T01 验收状态与下次复查

| T01 要求 | 当前状态 | 尚缺证据 |
|---|---|---|
| 环境身份、CRD 与 Operator 范围 | 静态核对完成，整体验收未通过 | 责任人、资源预算、实验时段、最终配置复查及后续实际隔离验收 |
| PostgreSQL / S3 及受限访问 | `external_blocked`：尚无本项目可用环境 | 实际连接、权限、版本、TLS、保留与清理边界 |
| 固定数据及不可变性 | `NOT_RUN / evidence_incomplete` | 实际批次、控制账本、查询结果与写权限隔离 |
| 故障注入隔离 | 已给出约束，未验收 | 专属资源清单、权限验证及实验前复核 |

可复查入口为 `docker context show`、`docker version`、`docker ps -a`、镜像 inspect、`kubectl --context orbstack --request-timeout=8s` 下的节点/工作负载/CRD/RBAC/webhook 只读查询，以及 `helm --kube-context orbstack list -A`、旧 release 的 values/manifest。CRD 管理来源需显式使用 `--show-managed-fields=true`；容量可读取 `/api/v1/nodes/orbstack/proxy/stats/summary`，核对返回时间。离线 Chart 渲染应使用上述精确版本和摘要。避免直接导出 Secrets、环境变量全文或包含凭据的连接配置。

T01 尚未通过；T05–T07 的运行前提不满足。仍可继续只读版本、源码与边界研究，但不得因此勾选实际环境和故障恢复验收。
