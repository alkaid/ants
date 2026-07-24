# PoolWithID 迁移指南

状态：适用于内部版本 `v2.12.1-ak-3`，2026-07-24

> 本文描述 `v2.12.1-ak-3` 已实现的 API 和运行语义。该版本仅用于
> `github.com/alkaid/ants/v2` 内部镜像；本地 tag 不代表公开 release，也不会
> 建立 fork 的公开发布或 CI 身份。

本文覆盖两条迁移路径:

1. 从 upstream `github.com/panjf2000/ants/v2` 迁移到内部 fork `github.com/alkaid/ants/v2`。
2. 从 fork 的旧实现迁移到 `v2.12.1-ak-3`。

## 1. 行为变化摘要

| 项目 | 升级前实现或开发基线 | `v2.12.1-ak-3` | 迁移动作 |
|---|---|---|---|
| PoolWithID idle expiry | 零值默认为 1 秒 | 零值默认为 30 秒, 只负责 idle owner | 对回收时延敏感时显式配置 |
| running-task escape | 与 `ExpiryDuration` 共用阈值, 零值约 1 秒 | 独立 `RunningTaskTimeout`, 零值 5 分钟 | 根据最长正常任务显式配置或禁用 |
| escape 数量 | global 和 per-ID 都无硬上限 | per-ID 默认 1; global 默认由容量计算, 最大 64 | 为阻塞任务压测并设置告警 |
| `TaskBuffer` | 零值 10, 物理 channel 20; 上限只防整数溢出 | 零值 100, 物理 channel 200; 最大 64K | 检查内存和排队时延 |
| blocking 配额 | 只覆盖新 ID 的 capacity wait | 覆盖 capacity、reservation 和已有 ID queue wait | 重新评估 `MaxBlockingTasks` |
| `Waiting()` | 不含已有 ID queue wait | 只统计当前真实阻塞的全部 Submit | 更新监控阈值 |
| `ReleaseContext` / `ReleaseTimeout` | 只等待 managed drain | 保持 managed-only, 契约更明确 | 不要把返回成功解释为 escaped task 已退出 |
| `Reboot()` | 不等待 escaped worker | 保持 managed-only; escape permit 和计数跨 Reboot | 不要用 Reboot 绕过 budget 或清理僵尸任务 |
| escape 日志 | 默认同步调用 `Logger.Printf` | 删除默认同步日志, 以 event/snapshot 为准 | 先迁移日志告警 |
| 最低 Go 版本 | 开发基线曾临时声明高于实际需要的 Go 版本 | 恢复真实最低版本 Go 1.19 | 在 Go 1.19 和组织当前版本各构建一次 |
| Tune | 存在扩容丢唤醒和缩容不退休 idle owner 的风险 | 修复并增加确定性交错测试 | 升级前不要依赖 Tune 规避当前问题 |

## 2. 从 upstream 迁移

### 2.1 import path

将 import 从 upstream 路径改为内部 fork 路径:

```go
import ants "github.com/alkaid/ants/v2"
```

在 `go.mod` 中固定组织批准的内部版本。不要仅使用浮动分支或无版本约束的最新提交。

两个 module 中同名的公开类型仍是不同的 Go 类型。切换路径后, 依赖 upstream 类型的接口、封装包、mock 和跨包配置都必须一起迁移, 不能把它当作原 module 的普通小版本升级。

普通 `Pool`、`PoolWithFunc` 和 generic pool 的目标是保持 upstream 行为。`PoolWithID` 是 fork 扩展, upstream 没有对应的兼容承诺。

### 2.2 Options 源码兼容

fork 在 upstream 的公开 `Options` 末尾增加了 PoolWithID 字段，本版本还增加了 running timeout 和 escape budget 字段。使用 unkeyed literal 的代码会编译失败，字段以后变化时也可能再次失败。

不要继续使用:

```go
opts := ants.Options{
	time.Minute,
	false,
	0,
	false,
	nil,
	logger,
	false,
}
```

改为 keyed literal:

```go
opts := ants.Options{
	ExpiryDuration: 30 * time.Second,
	Logger:         logger,
}
```

更推荐直接使用 Option 函数:

```go
pool, err := ants.NewPool(
	128,
	ants.WithExpiryDuration(30*time.Second),
	ants.WithLogger(logger),
)
```

迁移检查可以先查找 `Options{`。所有跨 module 边界传递 `ants.Options` 的配置封装也要一起检查, 不只检查直接调用构造函数的位置。

`WithOptions` 会覆盖完整 `Options` struct, 不会把非零字段与其他 Option 自动合并。它与其他 Option 混用时以调用顺序为准。优先使用单项 Option, 或在 keyed literal 中明确所有依赖的值。

`WithPreAlloc(true)` 对 PoolWithID 仍是 no-op, 不会预分配 ID worker 或 task channel。不要把它纳入 PoolWithID 的内存容量保证。

## 3. 从 fork 旧实现迁移

### 3.1 固定默认值和边界

下表中的公开名称均已在 `v2.12.1-ak-3` 实现。

| 配置 | 零值 | 正值 | 负值/超限 |
|---|---|---|---|
| PoolWithID `ExpiryDuration` | 30 秒 | idle 回收周期 | 构造返回配置错误 |
| `RunningTaskTimeout` | 5 分钟 | running escape 阈值 | 构造返回配置错误 |
| `TaskBuffer` | `DefaultTaskBuffer=100` | 允许 1 到 `MaxTaskBuffer=64*1024` | 小于 0 或大于 64K 返回 `ErrInvalidPoolWithIDTaskBuffer` |
| `MaxEscapedWorkers` | 有限池为 `min(64, max(1, Cap()/4))`, 无限池为 64 | 固定 global budget | 构造返回配置错误 |
| `MaxEscapedWorkersPerID` | 1 | 固定 per-ID budget | 构造返回配置错误 |
| `MaxBlockingTasks` | 0 表示不限制 | 全池真实阻塞 Submit 上限 | 负值不属于支持配置, 迁移时必须改为 0 或正值 |

`MinTaskBuffer` 保留值 10 以避免不必要的源码破坏, 但会标记 deprecated。它不再表示默认值。新代码应使用 `DefaultTaskBuffer` 或显式传入业务值。

默认 global escape budget 随有限池 `Cap()` 动态变化。例如容量 1、2、3 的默认值都是 1, 容量 100 的默认值是 25, 容量 1000 的默认值是 64。显式配置的正值不随 `Tune` 改变。

per-ID 显式预算可以大于 global, 构造函数不做交叉拒绝。实际 replacement 数始终同时受两个预算约束, 等价于取当前可用额度的较小值。

`Tune` 降容不会停止已经 escaped 的 worker。如果当前 escaped 数超过新的默认 budget, pool 会等计数自然回落到预算以下后才允许新的 replacement。

### 3.2 推荐显式配置

下面代码使用当前 API：

```go
pool, err := ants.NewPoolWithID(
	128,
	ants.WithExpiryDuration(30*time.Second),
	ants.WithRunningTaskTimeout(5*time.Minute),
	ants.WithTaskBuffer(100),
	ants.WithMaxEscapedWorkers(32),
	ants.WithMaxEscapedWorkersPerID(1),
	ants.WithMaxBlockingTasks(1000),
)
if err != nil {
	return err
}
```

升级时建议先显式写出所有影响生产资源的值, 即使它们等于默认值。这样后续库默认值调整不会静默改变运行行为。

### 3.3 严格串行业务

running escape 只能放弃旧 owner 的管理权, 不能停止旧任务。发生 escape 后, 后续同 ID 任务可能与旧任务并发, 旧任务也可能晚到写入数据库或外部系统。

必须保证同 ID 永不重叠时, 使用:

```go
ants.WithDisablePurgeRunning(true)
```

代价是永久阻塞的任务会永久阻塞该 ID。任务本身仍应接收应用 context, 设置下游超时并实现协作式退出。

为兼容当前 fork, `WithDisablePurge(true)` 保持组合语义, 同时禁用 idle purge 和 running escape。

开关矩阵为:

| `DisablePurge` | `DisablePurgeRunning` | idle owner 回收 | running escape |
|---|---|---|---|
| false | false | 开启 | 开启 |
| false | true | 开启 | 关闭 |
| true | false | 关闭 | 关闭, 兼容当前组合语义 |
| true | true | 关闭 | 关闭 |

### 3.4 需要保留旧的快速恢复阈值

旧实现通常在任务运行约 1 到 2 秒时 replacement。本版本默认延长到约 5 分钟到 5 分 30 秒。确实需要秒级恢复的调用方必须显式设置 `RunningTaskTimeout`，不能再依赖 `ExpiryDuration` 的副作用：

```go
ants.WithRunningTaskTimeout(time.Second)
```

秒级阈值很容易把正常慢任务误判为 escaped task。上线前必须验证任务时延分布，并配置足够小但非零的 global/per-ID budget。本版本不提供无界 escape 模式。

## 4. 背压与提交结果

本版本统一以下三类等待：

1. 新 ID 等待 owner capacity。
2. 同一个新 ID 的并发提交等待 reservation allocator。
3. 已有 ID 的物理 task channel 满后等待 queue space。

`MaxBlockingTasks` 对三类等待使用同一个全池配额。`Waiting()` 只统计调用时刻真实阻塞的 Submit, 一个 Submit 在相邻等待阶段间不会重复计数。

升级后, 原来能够无限堆积在已有 ID queue send 上的 goroutine 可能得到 `ErrPoolOverload`。调用方必须把该错误纳入现有过载处理, 例如上游限流、短暂退避或持久队列, 不应立即无界重试。

escape budget 耗尽与 `Submit` 返回值不是同一件事。任务可能已经成功入队并得到 `nil`, 随后因为当前 owner 永久阻塞且 replacement budget 已满而不能继续执行。budget-exhausted event 表示队列恢复被限制, 不能解释为某次 Submit 失败。

blocking 模式下, 任务递归向自己的已满 ID queue 提交仍可能自锁。统一 waiter 配额能限制等待 goroutine 数, 不能让这种业务模式自动取得进展。

## 5. TaskBuffer 与内存

`TaskBuffer=N` 仍表示每 ID 的接纳水位，物理 channel 容量为 `2*N`。默认值从 10 调整为 100，因而每 ID 的默认物理 channel 从 20 个槽位变为 200 个槽位。

`MaxTaskBuffer=64*1024` 只防止单个 ID 请求荒谬的大 channel。按当前 amd64 估算, 上限对应每个活跃 ID 约 1 MiB channel backing storage。容量 1000 且所有 ID 都使用上限时, 仅槽位就可能接近 1 GiB, 还不包括 entry、worker、goroutine stack、closure 捕获对象和 GC 成本。

上线前至少计算:

```text
active_ID_upper_bound * 2 * TaskBuffer * slot_size
```

本版本不包含 pool 级 aggregate queue budget。需要巨大 backlog 时应使用业务队列或持久队列，不要把它全部放进 PoolWithID channel。

## 6. Release、Reboot 与资源关闭

本版本保持 managed-only 生命周期：

- `Release()` 发起关闭后立即返回。
- `ReleaseContext` 和 `ReleaseTimeout` 等待当前 generation 的 admission、已经接受的 queue、managed owner 和后台循环。
- escaped worker 不在等待范围内。
- `Reboot()` 等 managed close 完成后创建新 registry, 不等待 escaped worker。
- escaped 计数、permit、snapshot、dropped-event 计数和 event stream 跨 Release/Reboot 保留。
- pool 进入 CLOSING 后, Release 前已经接纳的 running owner 仍可 timeout escape, 让 managed drain 在任务永久阻塞时完成。
- `ReleaseContext` 成功返回后, 旧 generation 不再产生新的 escape-start transition。旧 escaped worker 退出只归还自己的 permit 并更新跨 generation 的观测状态。

因此 `ReleaseContext` 返回 `nil` 或 pool 进入 CLOSED, 都不表示所有任务 goroutine 已退出。escaped task 仍可持有数据库连接、文件句柄或调用方对象, 也可能与 Reboot 后同 ID 的新任务重叠。

旧 worker 的内部 bookkeeping 不得修改新 registry 或新 generation 的 `Running()`。但 task 函数是用户代码, 它如果在 Reboot 后继续持有 pool 并主动调用 `Submit`, 该调用会作为新 generation 的普通提交处理。不要把内部 generation 隔离误解为业务代码无法访问新池。

严格资源关闭必须由应用负责:

1. 停止新流量。
2. 取消传给任务的应用 context。
3. 等待应用自己的任务完成信号或超时策略。
4. 再释放任务依赖的数据库、客户端和文件资源。

不要把重复 `Reboot()` 当作清除 escaped worker 或重置 budget 的手段。

## 7. 观测与日志迁移

旧实现会在 worker escaped 和 escaped worker exited 时同步调用 `Logger.Printf`，文本前缀为 `pool_with_id_escape`。阻塞 Logger 会拖住 purge 或退出路径。

本版本删除默认同步 escape 日志，正式观测面为：

- `EscapeEvents()` 的非阻塞 best-effort event stream。
- `EscapeSnapshot()` 的权威对账快照。
- O(1) 的 `Escaped()`、`TotalWorkers()`、`EscapeBudgetStatus(id)` 和 `DroppedEscapeEvents()`。
- global/per-ID budget-exhausted reason 和跨 generation 归因。

公开观测契约如下:

- `PoolWithIDEscapeEventType` 包含 `PoolWithIDEscapeBudgetExhausted`。
- bitmask `PoolWithIDEscapeBudgetReason` 的值为 `PoolWithIDEscapeGlobalBudgetExhausted` 和 `PoolWithIDEscapePerIDBudgetExhausted`；零值表示未耗尽。
- event 包含 `Generation`、`BudgetReason`、`GlobalBudget` 和 `PerIDBudget`；`Total` / `ByID` 表示 transition 后的 escaped 数。
- snapshot 包含 `GlobalBudget`、`PerIDBudget` 和 caller-owned `ExhaustedByID` map。
- `EscapeBudgetStatus(id)` 返回 `PoolWithIDEscapeBudgetStatus`, 字段为 `GlobalUsed`、`GlobalLimit`、`PerIDUsed`、`PerIDLimit` 和 `Reason`。
- exhausted event 在对应 ID 新增 reason bit 时发布一次, quota 恢复或 entry drained 后清除。event 允许丢失, snapshot/status 才是权威状态。

`Running()` 和 `Free()` 继续只反映 managed owner, 不包含 escaped worker。`Escaped()` 只返回当前仍存活的 escaped worker 总数, `TotalWorkers()` 返回 managed owner 加 escaped worker。完整 snapshot 的 `ByID` 用于按 ID 对账, budget 值表示允许的上限, 不是当前 worker 数。

event stream 跨 Release/Reboot 保持打开。event 携带 generation 便于归因；旧 generation 的 exit event 可能晚于新 generation 的 escape event 到达，消费方不能假设事件按 generation 分组。

event channel 满时允许丢事件, 所以告警不能只依赖逐条 event。监控应周期性读取权威 totals/snapshot, 并对 dropped event 增量报警。完整 `ByID` snapshot 是 O(K) 复制, 只适合低频诊断; 高频指标应使用 O(1) totals。

若生产告警正在匹配日志文本，请在升级前完成以下迁移：

1. 接入 event consumer, 用应用 context 控制其生命周期。
2. 增加 totals、budget exhausted、dropped events 和 `Waiting()` 指标。
3. 对比一段时间内日志与新指标。
4. 确认告警规则不再依赖日志文本后再升级。

本项只涉及 PoolWithID 的 escape 状态日志。普通 task panic 的 `PanicHandler` 和默认 panic 日志不在删除范围内。

## 8. 灰度、验收与回滚

### 升级前

- 查找并改写所有 unkeyed `ants.Options` literal。
- 记录当前显式和隐式的 expiry、TaskBuffer、MaxBlockingTasks 配置。
- 统计任务 p50、p99、最大正常运行时间和永久阻塞案例。
- 计算活跃 ID 上界和 queue 内存上界。
- 确认任务有应用 context, 不依赖 Release 强杀 goroutine。
- 在 Go 1.19 和组织当前 Go 版本各完成一次构建和测试。

### 灰度期间

- 先在少量实例显式配置新默认值, 不依赖零值。
- 观察 `Running()`、`Escaped()`、`TotalWorkers()`、`Waiting()`、budget exhausted、dropped events、RSS、GC pause 和队列时延。
- 注入永久阻塞任务, 验证 per-ID/global budget 都不能被 Reboot 绕过。
- 填满已有 ID queue, 验证 `MaxBlockingTasks` 和 `ErrPoolOverload` 处理不会形成重试风暴。
- 交错执行 Tune、Release 和 Reboot, 验证无永久 waiter、无 stale reservation 污染新 generation。

### 回滚

- 使用明确的旧内部 tag/commit 回滚, 因为 module path 仍是 `/v2`, 没有新的 major path 隔离语义。
- 回滚前停止新流量并取消应用任务。Release 不会停止 escaped zombie; 必要时只能通过进程退出获得严格隔离。
- 恢复旧版本时显式恢复旧配置, 不要假设两个版本的零值相同。
- 新版本写出的外部副作用不能通过库版本回滚撤销, 任务本身必须保持幂等或有业务补偿。

## 9. 实现边界

`v2.12.1-ak-3` 保持以下实现边界：

1. `WithDisablePurge(true)` 保持组合语义, 同时禁用 idle purge 和 running escape。
2. 本轮不增加 pool 级 aggregate queue budget, 大容量生产配置必须先完成内存评估。
3. 删除默认同步 escape 日志。如果生产依赖日志告警, 先完成第 7 节迁移。
4. 公开 budget 字段使用 `MaxEscapedWorkers` 和 `MaxEscapedWorkersPerID`, 对应 Option 使用 `WithMaxEscapedWorkers` 和 `WithMaxEscapedWorkersPerID`。

内部镜像保留 Go 1.19 与当前 stable 的 test CI、CodeQL 和必要的 PR 检查, 但不运行 Release Drafter, 不保留 upstream Funding 元数据, 也不向外部 Codecov 项目上传覆盖率。

本文是内部迁移与运维材料，不是公开 release announcement。版本交付使用本地轻量 tag，不 push，也不创建 GitHub release。
