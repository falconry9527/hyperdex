# 整体架构

 一套围绕 Hyperliquid 行情数据的采集 / 查询 / 推送平台。仓库内三个 Go 服务
（[api](api/)、[collector](collector/)、[stream](stream/)）加一个静态前端
（[web](web/)），共享一套 PostgreSQL（TimescaleDB）和 Redis。

```
                  ┌────────────────────┐
                  │  Hyperliquid (HL)  │
                  └──┬──────────────┬──┘
                     │              │
         WS 行情订阅 │              │ REST /exchange + 私有 WS
         (公共数据)  │              │ (账户/下单 · 浏览器直连)
                     ▼              │
              ┌─────────────┐       │
              │  collector  │       │
              │ (常驻+回补) │       │
              └──┬───────┬──┘       │
       finalized │       │ every    │
       via COPY  ▼       ▼ tick     │
           ┌──────────┐ ┌──────────┐ │
           │ Postgres │ │  Redis   │ │
           │ TimescDB │ │ pub/sub  │ │
           └─────┬────┘ └────┬─────┘ │
            只读 │           │ 订阅   │
                 ▼           ▼       │
           ┌──────────┐ ┌──────────┐ │
           │   api    │ │  stream  │ │
           │  :8080   │ │  :8090   │ │
           └─────┬────┘ └────┬─────┘ │
            REST │           │ WS    │
                 └─────┬─────┘       │
                       ▼             │
                 ┌──────────┐        │
                 │   web    │◄───────┘
                 │ (浏览器)  │
                 └──────────┘
```

数据走向是单向的：**collector 写，api / stream 只读**。collector 是 schema 的 owner
（[collector/migrations/schema.sql](collector/migrations/schema.sql)），其余两个服务不
做 DDL，也不写库。

---

## collector — 上游采集

**职责**：连 Hyperliquid 的 WS / REST，把原始行情落到 Postgres，同时实时转发到 Redis。

**入口**：[collector/cmd/collector/main.go](collector/cmd/collector/main.go) 启动一个常驻
进程，复用单条 HL WS 连接，并发拉起 5 个子模块；另外暴露 `:9090` 的 `/healthz` 与
`/metrics`（Prometheus）。

| 子模块 | 路径 | 落 PG | 写 Redis |
|---|---|---|---|
| kline | [internal/ingest/kline](collector/internal/ingest/kline/) | ✅ 按 interval 分 hypertable | ✅ 每根 bar |
| l2book | [internal/ingest/l2book](collector/internal/ingest/l2book/) | ❌ | ✅ 实时快照 |
| ticker | [internal/ingest/ticker](collector/internal/ingest/ticker/) | ❌ | ✅ 实时 |
| trades | [internal/ingest/trades](collector/internal/ingest/trades/) | ❌ | ✅ 实时 |
| retention janitor | [internal/retention](collector/internal/retention/) | DELETE 清理 | — |

**配置**：[collector/configs/config.example.toml](collector/configs/config.example.toml)
。HL endpoint、PG 连接池、Redis、每个 ingester 的开关、coin 列表、批量大小（kline
默认 500 行 / 500ms 一刷）。

**写 PG**：只有 finalized 的 K 线（HL 推送 `s.closed = true`）会通过 `pgx COPY` 批量
upsert 到 `klines_1m / 5m / 15m / 1h / 4h / 1d / 1mo` 七张 hypertable。存储测算见
[collector/docs/storage.md](collector/docs/storage.md)。

**写 Redis**：每根 bar（含未收盘）都会
- `SET kline:last:<coin>:<interval>`（TTL 5min）
- `PUBLISH kline.<coin>.<interval>`

l2book / ticker / trades 同理，channel / key 命名集中在
[collector/internal/db/redis.go](collector/internal/db/redis.go)，stream 端复用同一组
helper（[stream/internal/db/redis.go](stream/internal/db/redis.go)）保持对齐。

**Backfill 工具**（独立二进制，给 cron 调用）：
- [cmd/collector --backfill-days N](collector/cmd/collector/main.go) — 全 interval 全量回补
- [cmd/backfill-intraday](collector/cmd/backfill-intraday/) — 1m–4h，默认 30 天，建议每小时
- [cmd/backfill-daily](collector/cmd/backfill-daily/) — 1d / 1mo，默认 5 年，建议每天
- [cmd/sync-fills](collector/cmd/sync-fills/) — 按地址同步 `user_fills`，按需触发

---

## api — 历史查询 REST

**职责**：对外提供历史 K 线、资产元信息、用户成交查询。**只读 Postgres**，不接 HL WS。

**入口**：[api/cmd/api/main.go](api/cmd/api/main.go)。Gin 路由，监听 `:8080`，分层为
`handler → service → repository`。

| 路由 | 处理 | 后端 |
|---|---|---|
| `GET /klines?coin=&interval=&from=&to=&limit=` | [kline_handler.go](api/internal/api/handlers/kline_handler.go) | PG `klines_<interval>` |
| `GET /meta` | [meta_handler.go](api/internal/api/handlers/meta_handler.go) | HL REST 透传 + 进程内缓存 300s |
| `GET /myfills?addr=&limit=&offset=` | [myfills_handler.go](api/internal/api/handlers/myfills_handler.go) | PG `user_fills` |
| `GET /healthz` | — | — |

**响应封套**：统一 `{code, data, msg}`，定义在
[internal/api/msg](api/internal/api/msg/)。详细参数和错误码见
[api/docs/api.md](api/docs/api.md)。

**字段对齐**：K 线响应字段 `t/T/s/i/o/h/l/c/v/n` 与 HL wire 格式一致，也与 stream 推送
的帧一致 —— 前端拿同一种结构既能渲历史也能渲实时。

**配置**：[api/configs/config.example.toml](api/configs/config.example.toml)。复用同
一个 `marketdata` 库，建议生产用只读账号。limit / max_limit 在 service 层兜底。

---

## stream — 实时推送 WebSocket

**职责**：把 collector 写到 Redis pub/sub 的实时数据扇出给 WS 客户端。**只读 Redis**，
不依赖 PG。

**入口**：[stream/cmd/stream/main.go](stream/cmd/stream/main.go)。Gin + gorilla
websocket，监听 `:8090`。

| 路由 | 处理 |
|---|---|
| `GET /ws` | WebSocket 升级，单连接多路复用 |
| `GET /ws/status` | 调试，返回各 channel 的订阅人数 |
| `GET /healthz` | — |

**协议**（详见 [stream/docs/protocol.md](stream/docs/protocol.md)）：客户端按 JSON 帧
sub / unsub，比如：

```json
{"action":"sub","channel":"kline","coin":"BTC","interval":"1m"}
```

**Hub 设计**（[internal/stream/ws/hub.go](stream/internal/stream/ws/hub.go)）：
- 每个 `(channel, coin[, interval])` 维护一个订阅者集合。
- **第一个**客户端订阅时才真正去 Redis `SUBSCRIBE` 那个频道；最后一个走时再
  `UNSUBSCRIBE`，避免空订阅打满 Redis 连接。
- 新订阅者立刻拿一帧 `kline:last:<coin>:<interval>` 缓存值，避免"等下一 tick 才出图"
  的体感空白。
- 帧体直接是 collector 发来的原始 JSON，**不套响应封套**，最低开销。

**横向扩展**：状态全在 Redis（pub/sub 自带广播），多实例横排即可，前面 LB 不需要
sticky session。

---

## web — 前端

[web/](web/) 是无构建步骤的纯静态页面，TradingView Lightweight Charts。开发时
`python3 -m http.server 5173` 即可，先调 [api](api/) 拉历史 K 线铺图，再连
[stream](stream/) 把最右一根 bar 实时替换。

---

## 集成约定

**共享存储一份**：所有服务连同一个 PG 实例（`marketdata` 库）和同一个 Redis 实例。
跨服务的耦合都收敛到这两个数据源 —— 没有引入 Kafka / RabbitMQ 之类的额外消息中间件。

**Redis channel / key 命名**（见 [collector/internal/db/redis.go](collector/internal/db/redis.go)
和 [stream/internal/db/redis.go](stream/internal/db/redis.go)，两边必须一致）：

| 数据 | pub/sub channel | last cache key |
|---|---|---|
| K 线 | `kline.<coin>.<interval>` | `kline:last:<coin>:<interval>` |
| L2 盘口 | `l2book.<coin>` | `l2book:last:<coin>` |
| Ticker | `ticker.<coin>` | `ticker:last:<coin>` |
| Trades | `trades.<coin>` | `trades:last:<coin>` |

**字段对齐**：K 线在 collector 落库、api 返回、stream 推送三处都用 HL wire 格式
（`t/T/s/i/o/h/l/c/v/n`），前端不需要任何 adapter。

**部署边界**：collector 是有状态写者，重启会有短暂的实时缺口（依赖 backfill 兜底）；
api / stream 都是无状态只读，可以随意横扩 / 滚动重启。
