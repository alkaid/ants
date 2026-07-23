<p align="center">
<img src="https://raw.githubusercontent.com/panjf2000/logos/master/ants/logo.png" />
<b>A goroutine pool for Go</b>
<br/><br/>
<a title="Build Status" target="_blank" href="https://github.com/panjf2000/ants/actions?query=workflow%3ATests"><img src="https://img.shields.io/github/actions/workflow/status/panjf2000/ants/test.yml?branch=master&style=flat-square&logo=github-actions" /></a>
<a title="Codecov" target="_blank" href="https://codecov.io/gh/panjf2000/ants"><img src="https://img.shields.io/codecov/c/github/panjf2000/ants?style=flat-square&logo=codecov" /></a>
<a title="Release" target="_blank" href="https://github.com/panjf2000/ants/releases"><img src="https://img.shields.io/github/v/release/panjf2000/ants.svg?color=161823&style=flat-square&logo=smartthings" /></a>
<a title="Tag" target="_blank" href="https://github.com/panjf2000/ants/tags"><img src="https://img.shields.io/github/v/tag/panjf2000/ants?color=%23ff8936&logo=fitbit&style=flat-square" /></a>
<br/>
<a title="Minimum Go Version" target="_blank" href="https://github.com/panjf2000/gnet"><img src="https://img.shields.io/badge/go-%3E%3D1.26.5-30dff3?style=flat-square&logo=go" /></a>
<a title="Go Report Card" target="_blank" href="https://goreportcard.com/report/github.com/panjf2000/ants"><img src="https://goreportcard.com/badge/github.com/panjf2000/ants?style=flat-square" /></a>
<a title="Doc for ants" target="_blank" href="https://pkg.go.dev/github.com/alkaid/ants/v2?tab=doc"><img src="https://img.shields.io/badge/go.dev-doc-007d9c?style=flat-square&logo=read-the-docs" /></a>
<a title="Mentioned in Awesome Go" target="_blank" href="https://github.com/avelino/awesome-go#goroutines"><img src="https://awesome.re/mentioned-badge-flat.svg" /></a>
</p>

English | [中文](README_ZH.md)

## 📖 Introduction

Library `ants` implements a goroutine pool with fixed capacity, managing and recycling a massive number of goroutines, allowing developers to limit the number of goroutines in your concurrent programs.

## 🚀 Features:

- Managing and recycling a massive number of goroutines automatically
- Purging overdue goroutines periodically
- Abundant APIs: submitting tasks, getting the number of running goroutines, tuning the capacity of the pool dynamically, releasing the pool, rebooting the pool, etc.
- Handle panic gracefully to prevent programs from crash
- Efficient in memory usage and it may even achieve ***higher performance*** than unlimited goroutines in Go
- Nonblocking mechanism
- Preallocated memory (ring buffer, optional)

## 💡 How `ants` works

### Flow Diagram

<p align="center">
<img width="1011" alt="ants-flowchart-en" src="https://user-images.githubusercontent.com/7496278/66396509-7b42e700-ea0c-11e9-8612-b71a4b734683.png">
</p>

### Activity Diagrams

![](https://raw.githubusercontent.com/panjf2000/illustrations/master/go/ants-pool-1.png)

![](https://raw.githubusercontent.com/panjf2000/illustrations/master/go/ants-pool-2.png)

![](https://raw.githubusercontent.com/panjf2000/illustrations/master/go/ants-pool-3.png)

![](https://raw.githubusercontent.com/panjf2000/illustrations/master/go/ants-pool-4.png)

## 🧰 How to install

### For `ants` v1

``` powershell
go get -u github.com/panjf2000/ants
```

### For `ants` v2 (with GO111MODULE=on)

```powershell
go get -u github.com/alkaid/ants/v2
```

## 🛠 How to use
Check out [the examples](https://pkg.go.dev/github.com/alkaid/ants/v2#pkg-examples) for basic usage.

### Functional options for pool

`ants.Options`contains all optional configurations of the ants pool, which allows you to customize the goroutine pool by invoking option functions to set up each configuration in `NewPool`/`NewPoolWithFunc`/`NewPoolWithFuncGeneric` method.

Check out [ants.Options](https://pkg.go.dev/github.com/alkaid/ants/v2#Options) and [ants.Option](https://pkg.go.dev/github.com/alkaid/ants/v2#Option) for more details.

### Customize pool capacity

`ants` supports customizing the capacity of the pool. You can call the `NewPool` method to instantiate a `Pool` with a given capacity, as follows:

``` go
p, _ := ants.NewPool(10000)
```

### Submit tasks
Tasks can be submitted by calling `ants.Submit`
```go
ants.Submit(func(){})
```

### Tune pool capacity at runtime
You can tune the capacity of `ants` pool at runtime with `ants.Tune`:

``` go
pool.Tune(1000) // Tune its capacity to 1000
pool.Tune(100000) // Tune its capacity to 100000
```

Don't worry about the contention problems in this case, the method here is thread-safe (or should be called goroutine-safe).

### Pre-malloc goroutine queue in pool

`ants` allows you to pre-allocate the memory of the goroutine queue in the pool, which may get a performance enhancement under some special certain circumstances such as the scenario that requires a pool with ultra-large capacity, meanwhile, each task in goroutine lasts for a long time, in this case, pre-mallocing will reduce a lot of memory allocation in goroutine queue.

```go
// ants will pre-malloc the whole capacity of pool when calling ants.NewPool.
p, _ := ants.NewPool(100000, ants.WithPreAlloc(true))
```

### Release pool

```go
pool.Release()
```

or

```go
pool.ReleaseTimeout(time.Second * 3)
```

### Reboot pool

```go
// A pool that has been released can be still used after calling the Reboot().
pool.Reboot()
```

### `PoolWithID` contract

`NewPoolWithID(size, ...Option)` uses the same public `Option` type as the
other constructors. Direct options, expanded `[]Option` slices, and
`WithOptions` remain supported. This fork adds `TaskBuffer` and
`DisablePurgeRunning` to `Options`, so it does not promise compatibility with
unkeyed `Options` literals written for the upstream field count. Use option
functions or keyed literals. `WithPreAlloc(true)` is accepted by `PoolWithID`
but intentionally has no effect: ID workers are neither preallocated nor
reused.

For each ID, successful non-concurrent submissions start in FIFO order and run
serially during normal operation. Concurrent calls to `Submit` have no defined
order. `ExpiryDuration` is measured from the start of task execution, not from
`Submit`. When a running task reaches it, the old owner escapes and a replacement
owner can run later tasks for the same ID. This restores scheduling but allows
the old and new tasks to overlap; Go cannot forcibly stop the old task, which
may retain resources or produce late side effects. Timeout recovery does not
guarantee completion order.

`TaskBuffer` is the per-ID admission limit, not the channel capacity. A positive
value `N` creates a physical channel of `2*N`; zero selects the default limit 10
and capacity 20. Negative values and values that overflow when doubled make the
constructor return `ErrInvalidPoolWithIDTaskBuffer`.

| Submission path | Behavior |
|---|---|
| New ID, no owner capacity, `Nonblocking=true` | Returns `ErrPoolOverload` immediately. |
| Existing ID, `Nonblocking=true` | Rejects when the observed queue length reaches `TaskBuffer`; a final nonblocking send also rejects if the physical channel is full. |
| New ID, `Nonblocking=false` | Waits for owner capacity and is subject to `MaxBlockingTasks`. |
| Existing ID, `Nonblocking=false` | May use the full `2*TaskBuffer` channel, then waits for queue space or pool closure. `MaxBlockingTasks` does not limit this wait. |

The nonblocking admission check and send are not serialized. Concurrent callers
may therefore enter the reserved half between `TaskBuffer` and `2*TaskBuffer`;
the bounded channel and final nonblocking send are what guarantee that `Submit`
does not wait. A task that uses blocking mode to recursively submit to its own
full ID queue is not guaranteed to make progress.

`WithDisablePurgeRunning(true)` prevents a long-running owner from escaping.
`WithDisablePurge(true)` stops the purge loop and therefore disables this
recovery as well. Either option lets a permanently blocked task block its ID
indefinitely.

`Release` stops admission and starts draining accepted tasks; use
`ReleaseTimeout` or `ReleaseContext` to wait for the managed drain. Escaped
workers are not included in that wait, so `CLOSED` does not mean every task
goroutine has exited. `Reboot` waits for the managed drain, opens an empty ID
registry, and also does not wait for escaped workers. An old escaped task cannot
change the new scheduler state, but it may overlap same-ID work after reboot and
still produce late side effects.

`EscapeEvents` is a best-effort notification channel with a fixed capacity of
64. Publishing never blocks; full-channel notifications are counted in
`EscapeSnapshot().DroppedEvents`. The channel has one direct application
consumer, remains open across `Release` and `Reboot`, and must be stopped with an
application-owned context. `EscapeSnapshot` is the authoritative current state.
`Running()+EscapeSnapshot().Total` estimates the pool's live worker goroutines.
Do not automatically retry a task because it escaped: the old task can still
complete and duplicate side effects.

The following external-package example is compiled as part of the test suite.
It keeps per-ID state out of metric labels and exports only a low-cardinality
total gauge:

```go
package monitoring

import (
	"context"
	"log"
	"time"

	ants "github.com/alkaid/ants/v2"
)

func MonitorPoolWithID(
	ctx context.Context,
	pool *ants.PoolWithID,
	recordByID func(id, escaped int),
	setEscapedGauge func(total int),
) {
	knownIDs := make(map[int]struct{})
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case event := <-pool.EscapeEvents():
			recordByID(event.ID, event.ByID)
			if event.ByID == 0 {
				delete(knownIDs, event.ID)
			} else {
				knownIDs[event.ID] = struct{}{}
			}
			log.Printf("pool escape type=%d id=%d by_id=%d total=%d",
				event.Type, event.ID, event.ByID, event.Total)
		case <-ticker.C:
			// Events notify promptly; the snapshot repairs missed notifications.
			snapshot := pool.EscapeSnapshot()
			for id := range knownIDs {
				if snapshot.ByID[id] == 0 {
					recordByID(id, 0)
					delete(knownIDs, id)
				}
			}
			for id, count := range snapshot.ByID {
				recordByID(id, count)
				knownIDs[id] = struct{}{}
			}
			setEscapedGauge(snapshot.Total)
			if snapshot.DroppedEvents != 0 {
				log.Printf("pool escape notifications dropped=%d", snapshot.DroppedEvents)
			}
		case <-ctx.Done():
			return
		}
	}
}
```

## ⚙️ About sequence

All tasks submitted to `ants` pool will not be guaranteed to be addressed in order, because those tasks scatter among a series of concurrent workers, thus those tasks would be executed concurrently.

## 👏 Contributors

Please read our [Contributing Guidelines](CONTRIBUTING.md) before opening a PR and thank you to all the developers who already made contributions to `ants`!

<a href="https://github.com/panjf2000/ants/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=panjf2000/ants" />
</a>

## 📄 License

The source code in `ants` is available under the [MIT License](/LICENSE).

## 📚 Relevant Articles

-  [Goroutine 并发调度模型深度解析之手撸一个高性能 goroutine 池](https://taohuawu.club/high-performance-implementation-of-goroutine-pool)
-  [Visually Understanding Worker Pool](https://medium.com/coinmonks/visually-understanding-worker-pool-48a83b7fc1f5)
-  [The Case For A Go Worker Pool](https://brandur.org/go-worker-pool)
-  [Go Concurrency - GoRoutines, Worker Pools and Throttling Made Simple](https://twin.sh/articles/39/go-concurrency-goroutines-worker-pools-and-throttling-made-simple)

## 🖥 Use cases

### business corporations & open-source organizations

Trusted by the following corporations/organizations.

<table>
  <tbody>
    <tr>
      <td align="center" valign="middle">
        <a href="https://www.tencent.com/">
          <img src="https://res.strikefreedom.top/static_res/logos/tencent_logo.png" width="250" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://www.bytedance.com/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/ByteDance_Logo.png" width="250" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://tieba.baidu.com/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/baidu-tieba-logo.png" width="300" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://weibo.com/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/weibo-logo.png" width="300" />
        </a>
      </td>
    </tr>
    <tr>
      <td align="center" valign="middle">
        <a href="https://www.tencentmusic.com/en-us/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/tencent-music-logo.png" width="250" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://www.futuhk.com/en/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/futu-logo.png" width="250" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://www.shopify.com/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/shopify-logo.png" width="250" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://www.wechat.com/en/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/wechat-logo.png" width="250" />
        </a>
      </td>
    </tr>
    <tr>
      <td align="center" valign="middle">
        <a href="https://www.baidu.com/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/baidu-mobile-logo.png" width="250" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://www.360.com/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/360-logo.png" width="250" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://www.huaweicloud.com/intl/en-us/" target="_blank">
          <img src="https://res-static.hc-cdn.cn/cloudbu-site/china/zh-cn/%E7%BB%84%E4%BB%B6%E9%AA%8C%E8%AF%81/pep-common-header/logo-en.png" width="250" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://www.matrixorigin.io/" target="_blank">
          <img src="https://www.matrixorigin.io/_next/static/media/logo-light-en.b8e29d17.svg" width="250" />
        </a>
      </td>
    </tr>
    <tr>
      <td align="center" valign="middle">
        <a href="https://adguard-dns.io/" target="_blank">
          <img src="https://cdn.adtidy.org/website/images/AdGuardDNS_black.svg" width="250" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://bk.tencent.com/" target="_blank">
          <img src="https://static.apiseven.com/2022/11/14/6371adab14119.png" width="250" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://www.alibabacloud.com/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/aliyun-intl-logo.png" width="250" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://www.zuoyebang.com/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/zuoyebang-logo.jpeg" width="300" />
        </a>
      </td>
    </tr>
    <tr>
      <td align="center" valign="middle">
        <a href="https://www.antgroup.com/en/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/ant-group-logo.png" width="250" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://zilliz.com/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/zilliz-logo.png" width="250" />
        </a>
      </td>
      </td>
      <td align="center" valign="middle">
        <a href="https://amap.com/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/amap-logo.png" width="250" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://www.apache.org/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/asf-estd-1999-logo.jpg" width="250" />
        </a>
      </td>
    </tr>
    <tr>
      <td align="center" valign="middle">
        <a href="https://www.coze.com/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/coze-logo.png" width="250" />
        </a>
      </td>
    </tr>
  </tbody>
</table>

If you're also using `ants` in production, please help us enrich this list by opening a pull request.

### open-source software

The open-source projects below do concurrent programming with the help of `ants`.

- [gnet](https://github.com/panjf2000/gnet):  A high-performance, lightweight, non-blocking, event-driven networking framework written in pure Go.
- [milvus](https://github.com/milvus-io/milvus): An open-source vector database for scalable similarity search and AI applications.
- [nps](https://github.com/ehang-io/nps): A lightweight, high-performance, powerful intranet penetration proxy server, with a powerful web management terminal.
- [TDengine](https://github.com/taosdata/TDengine): TDengine is an open source, high-performance, cloud native time-series database optimized for Internet of Things (IoT), Connected Cars, and Industrial IoT.
- [siyuan](https://github.com/siyuan-note/siyuan): SiYuan is a local-first personal knowledge management system that supports complete offline use, as well as end-to-end encrypted synchronization.
- [BillionMail](https://github.com/aaPanel/BillionMail): A future open-source Mail server, Email marketing platform designed to help businesses and individuals manage their email campaigns with ease.
- [WeKnora](https://github.com/Tencent/WeKnora): An LLM-powered framework designed for deep document understanding and semantic retrieval, especially for handling complex, heterogeneous documents.
- [coze-loop](https://github.com/coze-dev/coze-loop): A developer-oriented, platform-level solution focused on the development and operation of AI agents.
- [osmedeus](https://github.com/j3ssie/osmedeus): A Workflow Engine for Offensive Security.
- [jitsu](https://github.com/jitsucom/jitsu/tree/master): An open-source Segment alternative. Fully-scriptable data ingestion engine for modern data teams. Set-up a real-time data pipeline in minutes, not days.
- [triangula](https://github.com/RH12503/triangula): Generate high-quality triangulated and polygonal art from images.
- [teler](https://github.com/kitabisa/teler): Real-time HTTP Intrusion Detection.
- [bsc](https://github.com/binance-chain/bsc): A Binance Smart Chain client based on the go-ethereum fork.
- [jaeles](https://github.com/jaeles-project/jaeles): The Swiss Army knife for automated Web Application Testing.
- [devlake](https://github.com/apache/incubator-devlake): The open-source dev data platform & dashboard for your DevOps tools.
- [matrixone](https://github.com/matrixorigin/matrixone): MatrixOne is a future-oriented hyper-converged cloud and edge native DBMS that supports transactional, analytical, and streaming workloads with a simplified and distributed database engine, across multiple data centers, clouds, edges and other heterogeneous infrastructures.
- [bk-bcs](https://github.com/TencentBlueKing/bk-bcs): BlueKing Container Service (BCS, same below) is a container management and orchestration platform for the micro-services under the BlueKing ecosystem.
- [trueblocks-core](https://github.com/TrueBlocks/trueblocks-core): TrueBlocks improves access to blockchain data for any EVM-compatible chain (particularly Ethereum mainnet) while remaining entirely local.
- [openGemini](https://github.com/openGemini/openGemini): openGemini is an open-source,cloud-native time-series database(TSDB) that can be widely used in IoT, Internet of Vehicles(IoV), O&M monitoring, and industrial Internet scenarios.
- [AdGuardDNS](https://github.com/AdguardTeam/AdGuardDNS): AdGuard DNS is an alternative solution for tracker blocking, privacy protection, and parental control.
- [WatchAD2.0](https://github.com/Qihoo360/WatchAD2.0): WatchAD2.0 是 360 信息安全中心开发的一款针对域安全的日志分析与监控系统，它可以收集所有域控上的事件日志、网络流量，通过特征匹配、协议分析、历史行为、敏感操作和蜜罐账户等方式来检测各种已知与未知威胁，功能覆盖了大部分目前的常见内网域渗透手法。
- [vanus](https://github.com/vanus-labs/vanus): Vanus is a Serverless, event streaming system with processing capabilities. It easily connects SaaS, Cloud Services, and Databases to help users build next-gen Event-driven Applications.
- [trpc-go](https://github.com/trpc-group/trpc-go): A pluggable, high-performance RPC framework written in Golang.
- [motan-go](https://github.com/weibocom/motan-go): Motan is a cross-language remote procedure call(RPC) framework for rapid development of high performance distributed services. motan-go is the golang implementation of Motan.

#### All use cases:

- [Repositories that depend on ants/v2](https://github.com/panjf2000/ants/network/dependents?package_id=UGFja2FnZS0yMjY2ODgxMjg2)

- [Repositories that depend on ants/v1](https://github.com/panjf2000/ants/network/dependents?package_id=UGFja2FnZS0yMjY0ODMzNjEw)

If you have `ants` integrated into projects, feel free to open a pull request refreshing this list of use cases.

## 🔋 JetBrains OS licenses

`ants` has been being developed with GoLand under the **free JetBrains Open Source license(s)** granted by JetBrains s.r.o., hence I would like to express my thanks here.

<a href="https://www.jetbrains.com/?from=ants" target="_blank"><img src="https://resources.jetbrains.com/storage/products/company/brand/logos/jetbrains.svg" alt="JetBrains logo."></a>

## ☕️ Buy me a coffee

> Please be sure to leave your name, GitHub account, or other social media accounts when you donate by the following means so that I can add it to the list of donors as a token of my appreciation.

<table>
  <tbody>
    <tr>
      <td align="center" valign="middle">
        <a target="_blank" href="https://buymeacoffee.com/panjf2000">
          <img src="https://res.strikefreedom.top/static_res/logos/bmc_qr.png" width="250" alt="Buy me a coffee" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a target="_blank" href="https://www.patreon.com/panjf2000">
          <img src="https://res.strikefreedom.top/static_res/logos/patreon_logo.png" width="250" alt="Patreon" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a target="_blank" href="https://opencollective.com/panjf2000">
          <img src="https://res.strikefreedom.top/static_res/logos/open-collective-logo.png" width="250" alt="OpenCollective" />
        </a>
      </td>
    </tr>
  </tbody>
</table>

## 🔋 Sponsorship

[![DigitalOcean Referral Badge](https://web-platforms.sfo2.cdn.digitaloceanspaces.com/WWW/Badge%203.svg)](https://www.digitalocean.com/?refcode=5d8774f42124&utm_campaign=Referral_Invite&utm_medium=Referral_Program&utm_source=badge)
