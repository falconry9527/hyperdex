# Stream WebSocket 协议文档

实时推送服务，用 WebSocket 把 collector 写入 Redis 的 K 线 tick / L2 盘口快照转发给前端。
历史查询走 [api 服务](../../api/docs/api.md) 的 REST（仅 K 线，L2 不落盘）。

- WebSocket URL: `ws://localhost:8090/ws`（dev）
- 子频道协议：JSON 帧，单连接多路复用（multiplex）
- 鉴权：暂无（dev）
- 一条 ws 连接可订阅任意多个 channel
- 当前支持的 channel：`kline`（按 coin+interval）、`l2book` / `ticker` / `trades`（按 coin）
- **K 线帧字段对齐 HL wire 格式**（`t/T/s/i/o/h/l/c/v/n`），跟 api REST 返回的 Kline 一致
- **L2 盘口帧字段对齐 HL wire 格式**（`coin/time/levels`），透传 HL 原生快照

---

## 连接握手

标准 WebSocket Upgrade。Origin 检查在 dev 是关闭的（生产收紧）。

```js
const ws = new WebSocket('ws://localhost:8090/ws')
ws.onopen = () => console.log('connected')
ws.onclose = () => console.log('disconnected')
```

> 服务端不接受任何 `Sec-WebSocket-Protocol` subprotocol。直接连即可。

---

## 客户端发送的命令

每条命令是一行 JSON。两种 action：

### `sub` — 订阅一个 channel

K 线：
```json
{"action":"sub","channel":"kline","coin":"BTC","interval":"1m"}
```

L2 盘口 / Ticker / Trades（都按 coin 订阅，不要 interval）：
```json
{"action":"sub","channel":"l2book","coin":"BTC"}
{"action":"sub","channel":"ticker","coin":"BTC"}
{"action":"sub","channel":"trades","coin":"BTC"}
```

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `action` | string | ✅ | `"sub"` / `"unsub"` |
| `channel` | string | ✅ | `"kline"` / `"l2book"` / `"ticker"` / `"trades"` |
| `coin` | string | ✅ | 币种，例如 `"BTC"` |
| `interval` | string | `kline` 必填 | 周期：`1m / 5m / 15m / 1h / 4h / 1d / 1mo`；其它 channel 忽略 |

> 命令字段是**客户端 → 服务端**协议；服务端只用来识别"你要订哪条流"。
> 跟服务端推下来的**数据帧**字段是两件事——K 线帧用 `s/i`，L2 帧用 `coin/time`。

成功后服务端会**立刻**推一帧 LastLive 缓存（如果 Redis 里有），之后每次 collector 推送都会转发到这条连接。

订阅同一 channel 多次是无副作用的（重复 sub 会被忽略）。

### `unsub` — 取消订阅

```json
{"action":"unsub","channel":"kline","coin":"BTC","interval":"1m"}
{"action":"unsub","channel":"l2book","coin":"BTC"}
{"action":"unsub","channel":"ticker","coin":"BTC"}
{"action":"unsub","channel":"trades","coin":"BTC"}
```

跟 `sub` 字段一致。无返回帧，静默生效。

不发 `unsub` 直接断开连接也安全——服务端会在断连时清理所有订阅。

---

## 服务端推送的帧

服务端的帧分三类，用**字段是否存在**区分：

### 1. K 线数据帧（最多）

每次 collector 写 Redis（`kline:last:*` SET + PUBLISH）就会转发：

```ts
interface KlineFrame {
  t: number   // open time, unix milliseconds
  T: number   // close time, unix milliseconds
  s: string   // symbol, e.g. "BTC"
  i: string   // interval, e.g. "1m"
  o: string   // open
  h: string   // high
  l: string   // low
  c: string   // close
  v: string   // volume
  n: number   // trades count
  closed: boolean   // ★ 唯一非 HL-原生字段：true = 已收盘；false = 进行中
}
```

实例：

```json
{
  "t":1746201600000, "T":1746201659999,
  "s":"BTC", "i":"1m",
  "o":"79708.0", "h":"79723.0", "l":"79708.0", "c":"79710.0",
  "v":"6.0107", "n":161,
  "closed":false
}
```

> 前 10 个字段跟 [HL 自身的 wire 格式](https://hyperliquid.gitbook.io)和我们 [api REST 返回的 Kline](../../api/docs/api.md#成功响应) **完全一致**——同名同类型同精度。

#### `closed` 字段语义

- `false`（绝大多数）：bar 还在进行中，价格/量持续在变。前端应**覆盖**最右那根蜡烛。
- `true`：HL 那侧的 closeTime 已过，这是该 bar 的**最终值**。前端可以把它"压栈"成历史的一部分，下一根新 bar 会以新 `t` 出现。

> 这个字段是 collector 自己加的（`closed = now > T`），HL 原始 wire 里没有。文档里特意标了 ★，省得跟 HL 直连时看不到这个字段感到意外。

### 2. L2 盘口数据帧

每次 collector 收到 HL `l2Book` 推送（约 5–10 Hz/币）就会透传转发。**完全是 HL 原生 wire 格式**，没有任何字段改动：

```ts
interface L2BookFrame {
  coin: string         // 例如 "BTC"
  time: number         // unix milliseconds
  levels: [
    Array<Level>,      // [0] = bids，按价格降序
    Array<Level>,      // [1] = asks，按价格升序
  ]
}

interface Level {
  px: string           // 价格（HL 用 string 保精度）
  sz: string           // 该价位聚合数量
  n: number            // 该价位订单数
}
```

实例（截取 top-2 档；实际推送 top-20 档/边）：

```json
{
  "coin": "BTC",
  "time": 1746201600123,
  "levels": [
    [{"px":"79708.0","sz":"1.234","n":3},{"px":"79707.5","sz":"0.500","n":1}],
    [{"px":"79710.0","sz":"0.567","n":2},{"px":"79710.5","sz":"2.100","n":4}]
  ]
}
```

#### 区别于 K 线帧

| 特性 | K 线 | L2 盘口 |
|---|---|---|
| 字段命名 | `s/i/t/T/...` | `coin/time/levels` |
| 数值类型 | string（保精度） | `px/sz` string，`n` int |
| 是否落盘 | 是（Postgres + REST 历史） | **否**（live-only） |
| 频率 | 1~5 Hz/币 | 5~10 Hz/币 |
| 体量 | 小 | **大**（每帧 40 档 × ~7Hz） |

#### 前端处理建议

- **整体覆盖，不要增量合并** — HL 推的就是当前完整 top-N 快照，每次直接替换本地簿即可。
- **stale 判定**：服务端 Redis 缓存 TTL 30s，断开后重连若拿不到 LastLive 帧，说明源头掉线，前端应显示 loading 而非陈旧簿。
- **截断显示**：如果只展示 top-10，自己 slice 即可；服务端透传 HL 默认的 top-20。

### 3. Ticker 数据帧（HL `activeAssetCtx`）

每秒约 1 帧，**HL 原生格式**（永续/现货共用一个 schema，永续多 funding/openInterest/oraclePx）：

```ts
interface TickerFrame {
  coin: string
  ctx: {
    dayNtlVlm: string    // 24h notional volume（USD 计量）
    prevDayPx: string    // 24h 前开盘价（用来算涨跌幅）
    markPx:    string    // 标记价
    midPx:     string

    // 仅永续
    funding?:      string   // 资金费率（每小时）
    openInterest?: string
    oraclePx?:     string

    // 仅现货
    circulatingSupply?: string
  }
}
```

实例：

```json
{
  "coin": "BTC",
  "ctx": {
    "dayNtlVlm":"5023987.123",
    "prevDayPx":"79123.5",
    "markPx":"79708.0",
    "midPx":"79709.0",
    "funding":"0.0000125",
    "openInterest":"1234.567",
    "oraclePx":"79705.0"
  }
}
```

前端基于此渲染顶部 bar：last/24h%/24h vol/funding 倒计时。

### 4. Trades 数据帧（HL `trades`）

每次 collector 收到 HL `trades` 推送（逐笔成交）就会转发。**帧是数组**，可能 1 笔也可能 N 笔：

```ts
interface TradeFrame extends Array<Trade> {}

interface Trade {
  coin:  string
  side:  "B" | "A"     // B = buy（taker 买）, A = sell（taker 卖）
  px:    string
  sz:    string
  time:  number        // unix ms
  hash:  string        // L1 tx hash
  tid:   number        // 交易 ID（HL 全局递增）
  users: [string, string]   // [maker, taker]，双方地址
}
```

实例：

```json
[
  {"coin":"BTC","side":"B","px":"79710","sz":"0.054","time":1746201600123,"hash":"0xabc…","tid":98765,"users":["0x…","0x…"]}
]
```

#### 订阅时的"history catch-up"

服务端缓存最近 **50 笔**（Redis LIST，LPUSH+LTRIM）。订阅成功立刻收到一帧"历史数组"，之后每次 HL 推送转发新批次。两种帧形状相同（都是 Trade 数组），前端**统一处理**：合并、按 `tid` / `time` 去重，保留最新 N 条即可。

### 5. 错误帧

```json
{"error":"invalid json"}
{"error":"unknown action"}
{"error":"unknown channel"}
```

| 触发 | error |
|---|---|
| 客户端发的 frame 不是合法 JSON | `invalid json` |
| `action` 不是 `sub` / `unsub` | `unknown action` |
| `channel` 不在白名单（目前仅 `kline`） | `unknown channel` |

错误帧不会断开连接，可以继续发后续命令。

### 6. （没有其它）

服务端不会主动发心跳/握手帧。WebSocket 标准 ping/pong 由协议层处理（`coder/websocket` 默认开启）。

---

## 跟 REST `/klines` 的字段差异

REST 的 `Kline` 跟 WS 的 `KlineFrame` **数据字段完全一样**——都对齐到 HL wire `t/T/s/i/o/h/l/c/v/n`。差别只在外层包装和数据时效。

| 维度 | REST `/klines` | WS `/ws` |
|---|---|---|
| 外层壳 | `{code, data, msg}` envelope | 裸帧（出错才有 `{error: ...}`） |
| K 线字段 | 10 个：`t/T/s/i/o/h/l/c/v/n` | **多 1 个**：`closed` |
| 时效 | 只含已收盘的 bar | 每 tick 一帧（含进行中） |
| 范围 | 完整历史 | 只有最右那根 bar 在变 |

### 前端写代码的注意点

1. **REST 解一层 envelope，WS 直接用**——
   ```ts
   // REST
   if (j.code !== 200) throw new Error(j.msg)
   const bars: Kline[] = j.data
   
   // WS
   const f = JSON.parse(e.data)
   if ('error' in f) console.warn(f.error)
   else handleBar(f)
   ```

2. **数值统一用 string 解析**——`o/h/l/c/v` 两边都是字符串。展示用 `parseFloat`，金融计算用 `bignumber.js` 之类。

3. **用图表库就别自己合并**——`lightweight-charts` 的 `series.update({time, o, h, l, c})` 同 `time` 自动 in-place 更新，新 `time` 自动 append。前端不用看 `closed` 字段。
   - 如果你自己维护 `bars[]`，那就要 `closed=true` 时 `bars.push(...)`，否则 `bars[bars.length-1] = ...`。

4. **REST 与 WS 别互查 sanity**——REST 落库可能比 WS 当下推的那根落后 1 分钟（取决于 collector 的入库节奏）。两个系统的 source of truth 不重叠：
   - **WS = 当下**（包括未收盘）
   - **REST = 已收盘历史**
   - WS `closed=true` 的最终一帧和 REST 后续查到同 `t` 的那一行**应该等价**——观察到不一致是 bug 信号。

---

## 多路复用

一条 ws 连接可以同时订阅多个 (coin, interval)。后端按**Hub 共享订阅**模型：

```
50 个浏览器都订 BTC.1m
    │
    └─► api 进程的 Hub 在 Redis 上**只开 1 条** SUBSCRIBE，
        在内存里 fan-out 给 50 条 ws

切到 ETH.5m：
    └─► Hub 自动复用已有 Redis 连接（如果其它客户端已订），
        否则新开一条 Redis SUBSCRIBE
```

意味着：**前端不用为不同 channel 开多条 ws**。一条连接 + 多个 sub 才是正确用法。

---

## 完整前端集成示例

```ts
const WS_URL = 'ws://localhost:8090/ws'

interface KlineFrame {
  t: number; T: number          // open / close time (ms)
  s: string; i: string          // symbol / interval
  o: string; h: string; l: string; c: string; v: string
  n: number
  closed: boolean
}

interface ErrorFrame { error: string }

type Frame = KlineFrame | ErrorFrame

class StreamClient {
  private ws: WebSocket | null = null
  private currentSub: { coin: string; interval: string } | null = null

  constructor(private onFrame: (f: KlineFrame) => void) {}

  async connect() {
    return new Promise<void>((resolve, reject) => {
      this.ws = new WebSocket(WS_URL)
      this.ws.onopen = () => resolve()
      this.ws.onerror = reject
      this.ws.onclose = () => { this.ws = null }
      this.ws.onmessage = (e) => {
        const f: Frame = JSON.parse(e.data)
        if ('error' in f) {
          console.warn('stream error:', f.error)
          return
        }
        // ignore stale frames during channel switch race
        if (this.currentSub
            && (f.s !== this.currentSub.coin || f.i !== this.currentSub.interval)) {
          return
        }
        this.onFrame(f)
      }
    })
  }

  /** Switch the (coin, interval) being subscribed. Auto-unsub the previous. */
  switchTo(coin: string, interval: string) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return
    if (this.currentSub) {
      this.send('unsub', this.currentSub.coin, this.currentSub.interval)
    }
    this.currentSub = { coin, interval }
    this.send('sub', coin, interval)
  }

  close() {
    this.ws?.close()
    this.ws = null
    this.currentSub = null
  }

  private send(action: 'sub' | 'unsub', coin: string, interval: string) {
    this.ws!.send(JSON.stringify({ action, channel: 'kline', coin, interval }))
  }
}

// 使用：
const client = new StreamClient((bar) => {
  // bar 是 KlineFrame，喂给图表
  series.update({
    time: Math.floor(bar.t / 1000),    // ms → seconds
    open: parseFloat(bar.o), high: parseFloat(bar.h),
    low:  parseFloat(bar.l), close: parseFloat(bar.c),
  })
})
await client.connect()
client.switchTo('BTC', '1m')
```

---

## REST 调试接口

### `GET /healthz`

```bash
curl http://localhost:8090/healthz
# ok
```

### `GET /ws/status`

返回 Hub 当前每个 channel 的订阅者数量（**裸 JSON，无 envelope**）。仅供调试。

```bash
curl http://localhost:8090/ws/status
# {"kline.BTC.1m": 3, "kline.ETH.1h": 1, "l2book.BTC": 5}
```

无活跃订阅时返回 `{}`。

---

## 重连与可靠性

- **Stream 是可恢复的**：所有状态都在内存 + Redis，断开重连不会丢历史，最多丢正在播的几个 tick。
- 推荐前端在 `onclose` 后做指数退避重连，重连成功后**重新发一遍 sub 命令**。
- 不保证 frame 顺序与 collector 写入完全一致（中间走了 Redis pubsub），但同一 (coin, interval) 内 `t` 不会倒退。

---

## 性能/容量备注

- 一条 ws 连接的内存开销 ≈ subscribers map 一项 + websocket 协议状态 ≈ 几 KB
- Redis 端订阅数 = 不同 channel 数（≠ ws 连接数）
- 推送频率：K 线 1~5 帧/秒/币，L2 盘口 5~10 帧/秒/币（HL ws 行情速率）
- L2 是带宽大头：每帧 ≈ 1–2 KB（top-20 双边），三币订阅时 ws 出带宽峰值约 30–60 KB/s/连接
- 切 channel 不会影响其它客户端

---

## 变更日志

| 日期 | 变更 |
|---|---|
| 2026-05-04 | **新增 `ticker` / `trades` channel**：HL 原生 `activeAssetCtx` / `trades` 透传；trades 服务端缓存最近 50 笔为 catch-up |
| 2026-05-04 | **新增 `l2book` channel**：L2 盘口快照实时推送，HL 原生 `coin/time/levels` 格式，live-only 不落盘 |
| 2026-05-04 | **wire 格式重构**：K 线帧字段全部对齐 HL 的 `t/T/s/i/o/h/l/c/v/n`；时间从 RFC3339 改为 unix ms；REST 与 WS 现在仅相差一个 `closed` 字段 |
| 2026-05-04 | 从 api 拆出独立服务（端口 :8090） |
| 2026-05-04 | `/ws/status` 改为裸 JSON（无 envelope） |
| 2026-05-04 | 支持所有周期（1m/5m/15m/1h/4h/1d）实时推送，月线 1mo 不在 live 范围（HL 不主动推），仅 REST 可查 |
