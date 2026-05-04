# API 接口文档

REST 服务，提供 Hyperliquid 历史 K 线查询。实时推送在 [stream 服务](../../stream/docs/protocol.md)。

- Base URL: `http://localhost:8080`（dev）
- Content-Type: `application/json`
- 鉴权：暂无（dev），生产部署请加网关层
- CORS：dev 全开（`*`），生产收紧到具体源
- **K 线字段对齐 HL wire 格式**（`t/T/s/i/o/h/l/c/v/n`），跟 stream 的 WS 帧字段一致

---

## 通用响应格式

所有 JSON 响应统一封套：

```ts
interface Response<T> {
  code: number  // 200 = 成功；其它见"错误码表"
  data: T       // 失败时为 null
  msg:  string  // 失败时为可读错误描述；成功时为 "SUCCESS"
}
```

成功示例：

```json
{ "code": 200, "data": [...], "msg": "SUCCESS" }
```

失败示例：

```json
{ "code": 504, "data": null, "msg": "invalid interval" }
```

## 错误码

| code | 含义 | 何时触发 |
|---:|---|---|
| 200 | success | 请求成功 |
| 400 | error  | 通用错误（消息见 `msg`） |
| 401 | not found | 资源不存在 |
| 500 | internal error | 服务端异常 |
| 504 | param error | 参数缺失或非法（消息见 `msg`） |

> 注：HTTP 状态码统一为 200，**业务码看 `code`**。这是为了让前端只解一层 JSON 就能分发。

---

## Endpoints

### `GET /healthz`

存活探针。返回 `200 OK`，body 为字符串 `ok`（**不带 envelope**）。

```bash
curl http://localhost:8080/healthz
# ok
```

---

### `GET /klines` — 查询历史 K 线

```
GET /klines?coin={coin}&interval={interval}&from={ms}&to={ms}&limit={n}
```

#### Query 参数

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `coin` | string | ✅ | — | 币种符号，例如 `BTC` / `ETH` / `SOL` |
| `interval` | string | ✅ | — | K 线周期，见下表 |
| `limit` | int | ❌ | 1500 | 返回条数上限；最大 5000 |
| `from` | int64 | ❌ | `to - limit × interval` | 起始时间（unix 毫秒，含） |
| `to` | int64 | ❌ | now | 结束时间（unix 毫秒，不含） |

#### 支持的 interval

`1m` / `5m` / `15m` / `1h` / `4h` / `1d` / `1mo`

> `1mo` 是月线（30 天近似默认窗口；真实月份边界由 DB 端计算）。

#### 成功响应

```ts
interface Kline {
  t: number   // open time, unix milliseconds
  T: number   // close time, unix milliseconds (= t + intervalMs)
  s: string   // symbol, e.g. "BTC"
  i: string   // interval, e.g. "1m"
  o: string   // open
  h: string   // high
  l: string   // low
  c: string   // close
  v: string   // volume
  n: number   // trades count
}

interface Response<Kline[]> { code: 200, data: Kline[], msg: "SUCCESS" }
```

实例：

```json
{
  "code": 200,
  "data": [
    {"t":1746057600000,"T":1746057660000,"s":"BTC","i":"1m",
     "o":"79708.0","h":"79723.0","l":"79708.0","c":"79710.0","v":"6.0107","n":161}
  ],
  "msg": "SUCCESS"
}
```

**为什么是这个 schema**：跟 [HL 自身的 candleSnapshot wire 格式](https://hyperliquid.gitbook.io)一致——同样的字段名、同样的 unix ms 时间戳、同样的紧凑数值字符串。前端只学一套字段名，REST 和 WS 帧也是同款。

**字段为字符串的原因**（`o/h/l/c/v`）：避免 JS 浮点精度损失。前端在做计算前用 `parseFloat` 或 `bignumber.js` 自取所需。

#### 时间顺序

`data` **升序**（最早 → 最新）。直接喂给图表库（lightweight-charts、TradingView 等）就是正确顺序。

#### 错误情况

| 触发 | code | msg |
|---|---:|---|
| 缺 `coin` | 504 | `coin is required` |
| 缺 `interval` | 504 | `interval is required` |
| `interval` 不在白名单 | 504 | `invalid interval` |
| `limit` 不是正整数 | 504 | `invalid limit` |
| `from` / `to` 不是合法整数 | 504 | `invalid from` / `invalid to` |
| DB 错误等服务端问题 | 400 | （具体异常信息） |

#### 示例

最常用——拉最近 500 根 1 分钟 BTC 蜡烛：

```bash
curl 'http://localhost:8080/klines?coin=BTC&interval=1m&limit=500'
```

指定窗口（2026-04-01 到 2026-05-01 的 BTC 日线）：

```bash
curl 'http://localhost:8080/klines?coin=BTC&interval=1d&from=1743465600000&to=1746057600000'
```

#### TypeScript 集成示例

```ts
const API_URL = 'http://localhost:8080'

export interface Kline {
  t: number; T: number          // open / close time (ms)
  s: string; i: string          // symbol / interval
  o: string; h: string; l: string; c: string; v: string
  n: number
}

export async function fetchKlines(
  coin: string,
  interval: '1m' | '5m' | '15m' | '1h' | '4h' | '1d' | '1mo',
  limit = 500,
  to?: number,
): Promise<Kline[]> {
  const qs = new URLSearchParams({ coin, interval, limit: String(limit) })
  if (to) qs.set('to', String(to))
  const r = await fetch(`${API_URL}/klines?${qs}`)
  const j = await r.json()
  if (j.code !== 200) throw new Error(`[${j.code}] ${j.msg}`)
  return j.data
}

// 转换给 lightweight-charts (它要的是 unix 秒)
export function toChartBar(k: Kline) {
  return {
    time:  Math.floor(k.t / 1000),
    open:  parseFloat(k.o),
    high:  parseFloat(k.h),
    low:   parseFloat(k.l),
    close: parseFloat(k.c),
  }
}
```

---

## 配套实时推送

历史拉完之后，订阅 `stream` 服务的 WS（`ws://localhost:8090/ws`）拿"最右那根蜡烛"的实时变化。详见 [stream 协议文档](../../stream/docs/protocol.md)。

典型前端流程：

```
1. fetch GET /klines → setData(bars)            // api（这里）
2. ws connect /ws → sub kline.<coin>.<interval>  // stream
3. 收到 LastLive 一帧 → 立刻覆盖最右蜡烛
4. 后续 ws 帧持续 update 最右蜡烛
```

### 与 WS 帧的关系

REST 的 `Kline` 和 WS 的 `KlineFrame` **字段名 + 类型完全相同**——都对齐到 HL wire 的 `t/T/s/i/o/h/l/c/v/n`。WS 帧仅多一个 `closed: boolean` 我们自己加的扩展字段（用来告诉前端"这根 bar 是不是已经收盘"）。详细对照见
[stream 文档的 "跟 REST /klines 的字段差异"](../../stream/docs/protocol.md#跟-rest-klines-的字段差异)。

---

## 变更日志

| 日期 | 变更 |
|---|---|
| 2026-05-04 | **wire 格式重构**：REST/WS 字段全部对齐 HL 的 `t/T/s/i/o/h/l/c/v/n`；时间字段从 RFC3339 改为 unix ms |
| 2026-05-04 | WS 拆出独立服务（`stream` :8090），api 仅保留 REST |
| 2026-05-04 | 新增 `1mo` 月线支持 |
| 2026-05-04 | 切换到 per-interval hypertable，每个 interval 独立 backfill |
