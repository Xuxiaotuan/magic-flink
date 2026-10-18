# T01 环境盘点、实验边界与最小补齐方案

**检查日期：** 2026-10-18。**状态：** T01/T02 盘点、E01 范围核对与短时负载检查已完成，已形成资源预算草案。OrbStack 复用有静态证据支持，但本次宿主机负载不满足新增实验的保守准入条件；T01/E01 整体验收仍为 `evidence_incomplete`。

本文保留 T01/T02 初次盘点（项目基准 `76f7739`）、基于 `1225e2a` 的 E01 范围核对，以及基于 `3b97ecb` 的资源预算检查。范围核对快照约为 2026-10-18 14:14；后续负载采样为 14:28–14:30（均为 Asia/Shanghai），详见第 6 节。本轮仅整理这些已有检查结果，未重跑环境检查，未安装、启动、部署、修改环境或读取业务表。运行状态会变化，实施补齐前须重新核对。

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
| E01 平台工程：确定集群边界 | 共享对象核对、短时负载检查和预算草案已完成；下一步按第 6 节取得代表性稳定窗口，确认预算、外部存储、实验时段与责任人 | 不切 context、不升级集群、不改已有工作负载；本轮新增负载准入不通过。最终清单和预算确认后，才具备申请专属资源的准备条件 |
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

### 6.2 资源预算专项采样：虚拟机有余量，宿主机尚不适合增加负载

2026-10-18 14:28:03–14:30:05，通过 kubelet stats summary 读取 9 次节点统计，共 7 个不同的 CPU 统计时间戳；重复返回不算独立样本。宿主机同时读取 `vm_stat`、`sysctl vm.swapusage`、压力值及一次 `top`。没有压测、启动服务或修改资源限额。

| 对象 / 口径 | 实际结果 | 解释边界 |
|---|---|---|
| 节点可调度资源 | 8 CPU、11.735 GiB 内存 | 不是为本项目预留的物理资源 |
| 节点 CPU | 约 0.19–1.23 核 | 两分钟短样本，不是代表性高峰 |
| 节点内存 | 工作集最高约 3.60 GiB，可用内存最低约 8.13 GiB | 后续预算以既有负载 4 GiB 计入；仍须复查 |
| 既有 requests / limits | CPU 1.3 / 2 核，内存 2630 / 3412 MiB | Meteor、cert-manager 等部分容器未设限，不能把请求量当实际负载上界 |
| Docker | 20 个运行容器，检查中均带 Kubernetes Pod 归属标签 | 不把容器统计与 kubelet 用量再次相加；也不说明宿主机没有其他进程负载 |
| 宿主机内存 | 24 GiB；一分钟样本中 swap 已用约 13.8–14.0 GiB，压缩内存约 10.0–11.5 GiB，压力 sysctl 连续 5 次为 2 | 保留原始压力值，不将其与内核内部枚举混用；仅凭历史 swap 存量不能断言压力持续恶化 |
| 宿主机换页增量 | 约一分钟 swap-in 增加 168.78 MiB、swap-out 增加 0、pageout 增加 35.81 MiB | 存在换入活动，但没有观察到 swap-out 增长；计数增量不能反推具体项目的因果关系 |
| 宿主机 CPU / load | 一次 `top`：CPU 忙碌约 69%，load average 为 29.56 / 37.32 / 47.52 | load 包含调度等待等影响，不等价于 CPU 使用百分比，也未测得持续 CPU 峰值 |
| 磁盘 | 主机约 40 GiB 可用；虚拟机文件系统约 37.8 GiB 可用 | 两者共享底层容量，不能相加；同样不能将 `docker system df` 的可回收量当已释放空间 |

本次仍无 ResourceQuota/LimitRange，旧项目资源状态未改。OrbStack 的内存上限允许其增长，但不代表宿主机已经为其保留空闲物理内存，见 [OrbStack 资源说明](https://docs.orbstack.dev/settings)。结合宿主机负载、压缩和换页观测，采取保守判定：**本轮不具备新增实验负载的准入证据，先等待既有重负载自然结束，再复查拟定实验窗口。** 这不推翻第 2 节的 CRD/权限隔离结论。

### 6.3 单 Application 最小预检配置草案

适用范围为计划中的 100 行合成数据、单 Application 功能预检。以下是待验证的配置起点，不是已测得的最低运行要求、发行冻结值或部署授权。内存 request 与 limit 设为相同值，避免通过少报内存请求制造可调度假象。

| 组件 | 数量 | CPU request / limit | 内存 request / limit |
|---|---|---|---|
| Operator 主容器 | 1 | 250m / 500m | 768 / 768 MiB |
| Webhook 容器 | 1 | 50m / 250m | 384 / 384 MiB |
| Service / 验证进程 | 1 | 100m / 500m | 768 / 768 MiB |
| Flink JobManager | 1 | 250m / 500m | 1024 / 1024 MiB |
| Flink TaskManager | 1 | 500m / 1000m | 1536 / 1536 MiB |
| PostgreSQL | 1 | 100m / 500m | 512 / 512 MiB |
| **合计，不含本地 S3** | | **1.25 / 3.25 核** | **4992 MiB，4.875 GiB** |
| 本地 S3，若后续采用 | 1 | 暂按 100m / 500m | 暂留 1024 MiB，产品选定后重核 |

- Operator 与 webhook 是两个 JVM；已读取 1.16.1 Chart，资源入口分别为 `operatorPod.resources`、`operatorPod.webhook.resources`，必须同时限额。不能把 Pod 总预算只配置给主容器。
- JM/TM 分别以 `jobmanager.memory.process.size=1024m`、`taskmanager.memory.process.size=1536m` 作为候选，保留堆、堆外、Metaspace 和 JVM overhead 的空间，不把全部预算设为 `-Xmx`。后续核对实际生成 Pod 的内存 request/limit，防止配置覆盖或 limit-factor 造成预算漂移。依据见 [Flink 1.20 内存说明](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/memory/mem_tuning/)。
- 单 TM 先使用 1 slot；并行度 4 的小数据对照可评估同一 TM 的 4 slot，但必须显式设置 CPU。Flink 默认 TM CPU 可能按 slot 数派生，4 slot 不是 4 个独占 CPU。单 TM 对照不能替代需要多 TM 或故障重叠的场景，见 [Flink Kubernetes 配置](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/config/)。
- PostgreSQL 候选为 `shared_buffers=64MB`、`work_mem=2MB`、`maintenance_work_mem=32MB`、`max_connections=16`。平台连接总预算仍为 8，其中探测最多 2；剩余连接用于管理和其他必要连接。`work_mem` 会随查询操作和会话叠加，不是全局内存上限，见 [PostgreSQL 17 资源配置](https://www.postgresql.org/docs/17/runtime-config-resource.html)。
- 首轮容量计算假定使用已获准的外部 S3，客户端缓冲包含在应用内存内；目前尚未确认该 endpoint。不能将“优先复用”写成“已有可用服务”。本地 S3 的 1 GiB 只是预算占位，不证明任意存储产品能在此限额内运行。
- Service 即使暂时运行在宿主机，也保留在项目总预算中，并单独验证其限额；namespace quota 不约束宿主进程。本表只计算一份 Service/验证进程，不能额外启动同等进程却不计入预算。

若启动、Planner 加载、JDBC/Parquet/S3 或实际作业不能在候选配置下稳定运行，记录失败并重新核算；不静默提高限额、依靠无限重试或声称“最低配置已通过”。

### 6.4 共享预留与并发容量判定

暂按既有节点负载 **4 GiB**（本次最高 3.60 GiB 向上取整）加 **2 GiB 机动空间**计算。两者不是新创建的资源预留，也不能保证其他项目不增长；准入时还必须单独满足宿主机条件。新增项目不能以侵占这 2 GiB 来容纳已知并发或故障重叠。

| 场景 | 项目内存预算 | 加既有负载与机动空间 | 对比 allocatable 11.735 GiB |
|---|---|---|---|
| 单 Application + 外部 S3 | 4.875 GiB | 10.875 GiB | 容量计算可容纳；宿主机、外部 S3 与准入仍未满足 |
| 单 Application + 本地 S3 | 5.875 GiB | 11.875 GiB | 不通过，且存储产品占用未实测 |
| 双 Application + 外部 S3 | 7.375 GiB | 13.375 GiB | 不通过；每个作业各计一组 JM/TM |
| 单 Application + 一个重叠替换 TM | 6.375 GiB | 12.375 GiB | 不通过；已知替换开销不能藏进机动空间 |

首轮单 Application 可考虑 `requests.memory=limits.memory=5Gi`、`requests.cpu=1250m`、`limits.cpu=3250m` 的 namespace quota 草案，并为所有容器显式配置表中资源。它只覆盖稳态预检，不包含额外 sidecar、init 容器、滚动更新或故障替换重叠；后续最终 Pod 清单含这些对象时必须重新核算，不能只靠配额拒绝来证明恢复安全。quota 和 LimitRange 当前均未创建。

已有 requests 加新增 requests 必须能调度，但调度检查不替代实际使用检查。既有实际峰值、项目总预算及机动空间应共同满足节点容量；宿主 CPU、内存和磁盘另设准入条件。CPU limit 也不代表物理 CPU 隔离或性能保证。

### 6.5 磁盘增长预算与保留空间

| 用途 | 新增增长预算 | 计入内容 |
|---|---|---|
| 项目镜像、依赖及构建临时空间 | 6 GiB | 包括拉取/解压/构建峰值；只是预算上限，尚未取得最终镜像大小 |
| PostgreSQL | 2 GiB | 数据、WAL 及数据库侧临时开销；不是仅计算源表大小 |
| Flink 临时空间 | 2 GiB | spill、shuffle 与运行临时文件 |
| 日志及本地证据 | 1 GiB | 覆盖实验前后记录及至少 7 天保留，实际测量后调整 |
| **合计，不含本地 S3 对象数据** | **11 GiB** | 同一占用不在多个项目重复计入 |

主机和实际承载数据的文件系统均保留至少 **25 GiB**。从空项目开始，需满足可用量至少为 **剩余增长预算 11 GiB + 保留量 25 GiB = 36 GiB**；已存在的本项目文件反映在磁盘可用量中，后续只计算尚未发生的增长，避免重复扣减。按本次主机约 40 GiB、虚拟机约 37.8 GiB 的快照仅有小幅余量，不能据此提前批准持续构建或更多数据。

两处可用量共享底层空间，必须分别满足要求，不能相加；也不能用 Docker 报告的可回收量抵扣。若引入本地 S3，还需另算输入、候选输出、失败尝试及未完成 multipart 的保留空间，本方案尚未覆盖，不能套用 11 GiB 总额。

现有 StorageClass 为 `local-path`。PVC 声明大小、`max_wal_size` 或表中预算不自动等于已验证的硬写入上限；后续须核实实际容量约束、日志轮转与增长观测。磁盘不足时停止本项目新增实验，不删除旧 PVC、共享镜像、其他项目缓存或仍需保留的本项目证据。

### 6.6 准入、停止和恢复条件

以下为本项目拟采用的保护阈值，不是官方通用性能标准；尚未实现监控或自动停止逻辑。平台工程与现有项目维护者须确认责任人、实验窗口及实际观测方式，数据库/存储负责人确认对应增长预算。

| 信号 | 准入 / 禁止新增实验 | 停止本项目当前实验 |
|---|---|---|
| 宿主机内存与 CPU | 至少连续 15 分钟代表性稳定窗口：内存压力正常、无持续新增 swap-out；CPU 忙碌低于 60%、1 分钟 load 低于 6。记录换入/换出增量，不能用累计 swap 存量单独决定是否通过 | critical 内存压力立即停止；warning 持续 60 秒或 CPU 忙碌超过 85% 持续 60 秒，停止当前实验。压力判断须映射本机接口语义，不能把内部枚举值直接当 sysctl 返回值 |
| 节点内存 | 可用内存覆盖剩余新增进程预算 + 2 GiB；单 Application 从空环境启动建议至少 7 GiB。既有负载超出 4 GiB 基线时重算 | 可用内存低于 1 GiB，或 MemoryPressure=True，立即停止 |
| 节点 CPU | 总使用超过 6 核持续 60 秒，禁止新增工作 | 持续不恢复则结束本项目窗口；拟以总计 180 秒作为退出上限，实施前确认 |
| 磁盘 | 任一相关文件系统低于 25 GiB，或不足以覆盖剩余增长预算 + 25 GiB，禁止新增实验 | 低于 20 GiB 或 DiskPressure=True，立即停止本项目写入实验 |
| 本项目容器 | 内存超过限额 85% 持续一分钟，暂停增加并发并评估；未确认限额不得启动 | OOMKilled、Evicted 或超过既定数据增长预算，当前预检判失败，不以自动重试掩盖 |
| 其他项目健康 | 新增异常重启或 Ready 下降，暂停新增实验并调查；只比较本窗口增量，不把历史重启数当新故障 | 持续异常时保守结束本项目实验，不等待证明因果关系后才停止，也不操作其他项目 |
| 观测失效 | 指标时间戳过期超过 60 秒或关键指标不可读，禁止新增实验；重复返回不算新样本 | 无法及时恢复观测则结束当前窗口；结果不可判定时保持 UNKNOWN，不宣称成功 |

“停止”是停止提交和自动重试，按已有取消协议结束本项目当前尝试，保留已有证据并追踪尚未确认的外部副作用。对于对象写入结果或作业终态无法确认的情况，保持 UNKNOWN；不得直接删除数据来制造完成状态。预算触发的取消不计为故障恢复实验 PASS。本轮未执行上述任何停止动作。

恢复前重新取得至少 15 分钟稳定基线、确认资源和磁盘剩余量、原尝试状态及实验负责人允许恢复；不自动循环重启。不能通过扩大 OrbStack 全局内存、缩容其他项目、删除旧资源或压低 JVM 内存来绕过准入。

### 6.7 单 Application 预检与完整 P0 验收

| 维度 | 单 Application 预检 | 完整 P0 验收 |
|---|---|---|
| 目的 | 验证候选制品能在受限资源中启动、加载依赖并完成小数据基本链路 | 验证固定发行组合、文件集合证明、封存/发布协议与失败恢复 |
| 并发 / slot | 一组 JM/TM，先 1 slot；通过后才评估单作业并行度 4 对照 | 保留计划最多 2 个 Application、并行度 1/4 及各规定场景的资源要求；不能复用同一份 slot 预算供两个并发作业 |
| 故障和重叠 | 5 GiB 草案不覆盖替换 TM、滚动更新或其他 Pod 重叠 | 按 F01–F13 的真实故障窗口核算峰值并运行，不能为适应预检限额改变原故障语义 |
| 存储 | 假定获准外部 S3；当前 endpoint 未确认 | 必须在登记的实际存储版本和配置上通过文件、不可变写入与恢复验证 |
| 成功证据 | 仅支持对应预检范围；Pod Running、进程退出 0 都不足以证明完整链路 | 原 G1–G6 全部满足，F01–F13 必需子场景有真实证据，失败与阻塞不能折算通过 |
| 当前状态 | `NOT_RUN`，新增负载准入不通过 | `NOT_RUN / evidence_incomplete`；双 Application 和故障重叠容量未满足，存储等环境仍缺 |

单 Application 预检通过，不能勾选整个 T05/T06，更不能宣布 P0 GO。若以后需要调整容量方案，应先重新核算和评审，不降低 F01–F13、G1–G6 或 v0.2 的验收条件。

### 6.8 E01 剩余准备与后续验收

| 阶段 | 当前状态 | 下一项可判定证据 |
|---|---|---|
| E01 范围核对与预算草案 | 已完成只读检查和方案整理 | 第 2 节静态隔离证据、第 6.2 节短时采样与上述预算；不等于准入通过 |
| E01 剩余准备条件 | 未完成 | 代表性稳定窗口、具体责任人/实验时段、外部 S3 可用身份、获确认的预算与阈值、最终资源清单复查；完整 P0 还需解决并发和故障峰值容量 |
| E02 环境隔离验收 | `NOT_RUN` | 后续获准部署后，新旧 ServiceAccount 的范围内访问与跨范围拒绝；新 Operator 的限定监听、webhook 选择器/证书/就绪；共享 CRD 与旧项目配置保持不变 |
| T05–T07 运行验收 | `NOT_RUN` | 同一制品的真实 Application、规定并发和故障场景、峰值与停止阈值证据、实验前后旧项目健康对照，以及原文件和恢复协议验收 |

E01 的准备条件可以在部署前完成；E01 通过不自动授权部署，也不替代 E02、T05–T07 或 P0 GO。共享硬件不能仅凭 namespace、限额或本次短样本承诺绝对零影响。

## 7. T01 验收状态与下次复查

| T01 要求 | 当前状态 | 尚缺证据 |
|---|---|---|
| 环境身份、CRD 与 Operator 范围 | 静态核对及预算草案完成，整体验收未通过 | 责任人、稳定窗口、预算与阈值确认、完整 P0 容量、最终配置复查及实际隔离验收 |
| PostgreSQL / S3 及受限访问 | `external_blocked`：尚无本项目可用环境 | 实际连接、权限、版本、TLS、保留与清理边界 |
| 固定数据及不可变性 | `NOT_RUN / evidence_incomplete` | 实际批次、控制账本、查询结果与写权限隔离 |
| 故障注入隔离 | 已给出约束，未验收 | 专属资源清单、权限验证及实验前复核 |

可复查入口为 `docker context show`、`docker version`、`docker ps -a`、镜像 inspect、`kubectl --context orbstack --request-timeout=8s` 下的节点/工作负载/CRD/RBAC/webhook 只读查询，以及 `helm --kube-context orbstack list -A`、旧 release 的 values/manifest。CRD 管理来源需显式使用 `--show-managed-fields=true`；容量可读取 `/api/v1/nodes/orbstack/proxy/stats/summary`，核对返回时间。离线 Chart 渲染应使用上述精确版本和摘要。避免直接导出 Secrets、环境变量全文或包含凭据的连接配置。

T01 尚未通过；T05–T07 的运行前提不满足。仍可继续只读版本、源码与边界研究，但不得因此勾选实际环境和故障恢复验收。
