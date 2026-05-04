# Storage sizing — 全交易对全 interval

测算 collector 把 Hyperliquid 上**所有**交易对 × **所有 7 个 interval** 全开同步时，
PostgreSQL（TimescaleDB）的存储量级、以及部署在 AWS 时的月成本。

只有 `kline` 路径会写 PG（[internal/db/kline.go](../internal/db/kline.go)）；
`l2book` / `ticker` / `trades` 全部走 Redis，不进 PG。`user_fills` 默认关闭，
启用后体量取决于地址数量，单独估算。

> **关键变化（v?）**：collector 现在内置 retention janitor
> （[internal/retention/retention.go](../internal/retention/retention.go)），
> 每小时一轮按表按 `keep_days` 批量 `DELETE` 老行。这意味着存储**不再随时间线性增长**，
> 而是收敛到稳态上限。下面所有"1 年""3 年"的旧外推已被稳态估算替代，详见 §2/§3。

---

## 1. 假设

| 参数 | 取值 | 说明 |
|---|---|---|
| 交易对数量 | **300** | HL 永续 ~200 + Spot HIP ~100。实际请用 `/info meta` 与 `/info spotMeta` 校准 |
| Intervals | 7 | `1m, 5m, 15m, 1h, 4h, 1d, 1mo`（[migrations/schema.sql](../migrations/schema.sql)） |
| 行物理大小（含索引） | **~200 B/行** | tuple header (~27B) + coin (~12B) + ts (8B) + 5×numeric(38,18) (~80B) + trades (4B) + padding；**加上 PK 与一条二级索引**摊销 |
| 压缩比 | **10×** | Timescale 列存 + `segmentby=coin`，OHLCV 数据经验区间 6–15× |
| 压缩策略 | 见 schema | 1m/5m/15m: 30d；1h: 90d；4h: 180d；1d: 365d；1mo: 不压缩 |
| 保留策略 | 见 §2 | 1m/5m/15m: 30d；1h/4h: 90d；1d: 730d；1mo: 1095d。**老于此窗口的行被定时删除** |

> 200 B/行是含两条索引（PK + `(coin, ts DESC)`）的"实占盘"估算。
> `klines_1m` 的 `coin_ts_desc_idx` 与 PK `(coin, ts)` 实质冗余，删掉可省约 50 B/行。

---

## 2. 保留策略（定时删除）

进程内 retention janitor（[internal/retention/retention.go](../internal/retention/retention.go)，
配置 [configs/config.toml](../configs/config.toml) 的 `[retention]` 段）在 collector 启动时
拉起，常驻 goroutine。

- **触发**：启动时立刻扫一轮，之后每 `interval_ms`（默认 **1h**）扫一次。
- **删除方式**：行级 `DELETE`（不是 `drop_chunks`），每表每轮分批 `batch_size=1000` 行，
  最多 `max_rounds=200` → 单表单 sweep 上限 **20 万行**。这样做是为了把"轰炸面"控住，
  代价是落到压缩 chunk 上的 DELETE 比 chunk 级 drop 慢得多。
- **保留窗口**（每表 `keep_days`）：

| 表 | keep_days | 备注 |
|---|---:|---|
| `klines_1m`  | 30  | 等于压缩点，删除点≈压缩点，几乎不存在压缩段 |
| `klines_5m`  | 30  | 同上 |
| `klines_15m` | 30  | 同上 |
| `klines_1h`  | 90  | 等于压缩点 |
| `klines_4h`  | 90  | 压缩点 180d，**永远删除在压缩之前发生 → 全部为未压缩段** |
| `klines_1d`  | 730 | 压缩点 365d → 365d 未压缩 + 365d 压缩段 |
| `klines_1mo` | 1095| 不压缩 |

`user_fills` **没有 retention policy**，永久保留。

> 影响：所有"1 年/3 年"的线性外推不再成立。系统在 ~30 d / ~90 d / ~730 d 后分别对各表
> 进入稳态，存储不再增长（除非新增交易对）。下面的估算改用稳态。

---

## 3. 行数与存储量（300 交易对）

### 每 interval 的速率与稳态

把"产生速率"和"DB 实际稳态库存"（retention + 压缩 policy 都生效后）放在同一张表：

| Interval | 行/天/coin | keep_days | 稳态行数（300 coins） | 未压缩段 | 压缩段（10×） |
|---|---:|---:|---:|---:|---:|
| 1m   | 1,440  | 30   | 1.30 × 10⁷ | **2.6 GB**  | ≈ 0 |
| 5m   | 288    | 30   | 2.60 × 10⁶ | **0.52 GB** | ≈ 0 |
| 15m  | 96     | 30   | 8.6 × 10⁵  | **0.17 GB** | ≈ 0 |
| 1h   | 24     | 90   | 6.5 × 10⁵  | **0.13 GB** | ≈ 0 |
| 4h   | 6      | 90   | 1.6 × 10⁵  | **33 MB**   | 0（先删后压） |
| 1d   | 1      | 730  | 2.2 × 10⁵  | 22 MB       | ~2 MB |
| 1mo  | ≈0.033 | 1095 | 1.1 × 10⁴  | ~2 MB       | 0（不压缩） |
| **合计** | **1,855** | | **~1.65 × 10⁷** | **~3.5 GB** | **<10 MB** |

retention 与压缩 policy 的相对位置决定了是否存在压缩段：

- 1m / 5m / 15m / 1h：retention 删除点 ≤ 压缩点 → 几乎不存在压缩段。
- 4h：压缩点 180d，retention 90d → **永远先删后压**。
- 1d：压缩点 365d，retention 730d → 365d 未压缩 + 365d 压缩段，唯一有意义的压缩段。
- 1mo：不压缩。

### 时间维度汇总（含 WAL/碎片余量约 1.3×）

| 时段 | 总行数 | 估算落盘 | 取整规划 | 说明 |
|---|---:|---:|---:|---|
| 1 天   | ~5.6 × 10⁵  | ~110 MB | 200 MB | retention 未触发 |
| 30 天  | ~1.67 × 10⁷ | ~3.3 GB | 5 GB | 1m/5m/15m 刚到 retention 边界 |
| 1 年   | ~1.74 × 10⁷ | ~4.4 GB | — | 1h/4h 已稳态；1d 累积 365d 进入压缩 |
| **稳态 ≥1095 d** | **~1.75 × 10⁷** | **~4.5 GB** | **10 GB**（~1× 余量） | 全表稳态 |

各档窗口达稳态的时间点：第 30 天 1m/5m/15m；第 90 天 1h/4h；第 365 天 1d 开始压缩；
第 730 天 1d retention 介入；第 1095 天 1mo 进入稳态。**30 天到 1095 天之间总行数只从
1.67 × 10⁷ 涨到 1.75 × 10⁷** —— 1m 占 75% 的体量，而它在 30d 就被锁定在上限。

— 对比 retention 上线前的线性外推（1 年 2.03 × 10⁸ 行 / 7-11 GB；3 年 ~30 GB），
**稳态收敛到 ~4.5 GB**。除非新增交易对或扩大 keep_days，磁盘规划可以一次到位。

---

## 4. user_fills（如启用）

每条 fill 约 200 B/行（同样含索引）。一个高频交易地址日均几十到几百笔；
按每地址 200 fills/天估：

| 地址数 | 1 天 | 30 天 | 1 年 |
|---|---:|---:|---:|
| 1   | ~40 KB  | ~1.2 MB | ~15 MB |
| 10  | ~400 KB | ~12 MB  | ~150 MB |
| 100 | ~4 MB   | ~120 MB | ~1.5 GB |

`user_fills` 既没有压缩 policy 也没有 retention policy。地址多且活跃时记得加。

---

## 5. AWS 月成本

> **重要：AWS RDS for PostgreSQL 不支持 TimescaleDB 扩展**（Aurora 也不支持）。
> 项目重度依赖 Timescale 的 hypertable + 列存压缩，落到 AWS 上的现实选择是：
> A. **EC2 自建** PostgreSQL+Timescale；
> B. **Timescale Cloud**（通过 AWS Marketplace，托管在 AWS 上，但供应商不是 AWS 本身）。
>
> 价格按 us-east-1 on-demand list price 计，区域差异 ±20%。

### 方案 A：EC2 自建（推荐起步）

按稳态数据量 ~4.5 GB、写入率 < 100 行/s 估算，**资源需求很小**。
工作集（最近 30 天未压缩段，~3.3 GB）能完整放进 8 GB 内存。retention 让磁盘
不再随时间膨胀，可以从一开始就用最小卷。

| 项目 | 规格 | 月成本 |
|---|---|---:|
| EC2 `t4g.large` | 2 vCPU / 8 GB RAM (ARM Graviton) | ~$49 |
| EBS gp3 | 100 GB（含 3000 IOPS / 125 MB/s） | ~$8 |
| EBS 快照 | ~100 GB 滚动（去重后约 30–50 GB 计费） | ~$2–3 |
| 数据出网 | VPC 内通信，假设 ~10 GB/月 | ~$1 |
| **小计 on-demand** | | **~$60/月** |
| **1 年 RI（约 -30%）** | | **~$42/月** |
| **3 年 RI（约 -60%）** | | **~$25/月** |

如果想加 standby（DR）：再 ×2，或买较小的 ReadReplica 实例 + Postgres 物理流复制。

**何时升 `t4g.xlarge`（4 vCPU / 16 GB，~$98/月）**：
- 交易对扩到 1000+；或
- 监控显示 buffer hit rate < 95%（说明 RAM 不够）；或
- 同时启用 `user_fills` 多地址回填。

### 方案 B：Timescale Cloud（托管在 AWS）

最小可用配置参考价（按 cloud.timescale.com 公开报价）：

| 项目 | 规格 | 月成本 |
|---|---|---:|
| Compute | 0.5 CPU / 2 GB RAM | ~$50 |
| Storage（已含压缩定价） | 25 GB | ~$5 |
| 备份 | 默认包含 | $0 |
| **小计** | | **~$55/月** |

写入量再大一档（1 CPU / 4 GB / 50 GB）≈ **$120/月**。
省去了运维 + 备份策略的工作，但同等资源比自建贵 1.5–2×。

### 方案 C：RDS PostgreSQL（**只在放弃 Timescale 时**）

如果接受失去列存压缩、改用普通分区表，RDS 也能跑。需要先重写 schema：

| 项目 | 规格 | 月成本 |
|---|---|---:|
| RDS `db.t4g.medium` | 2 vCPU / 4 GB | ~$53 |
| gp3 存储 100 GB | | ~$11.5 |
| 自动备份 | 7 天 retention 内免费 | $0 |
| **Single-AZ 小计** | | **~$65/月** |
| **Multi-AZ（×2 计算）** | | **~$120/月** |

但**没有列存压缩**意味着 1 年存储从 ~10 GB 涨回 ~40–50 GB，3 年到 150 GB+，
且 PG 原生分区在 hot path 写入与跨分区查询上要自己手工调优，得不偿失。

### 推荐

- **当前规模（300 coins）**：EC2 `t4g.large` + 100 GB gp3，**~$60/月**（on-demand）或 ~$42/月（1 年 RI）。
- **要省心**：Timescale Cloud 起步价 ~$55/月。
- **长期**：retention 让磁盘稳定在 ~4.5 GB，100 GB 卷已经留了 ~20× 余量；只要交易对数量
  不暴涨，实例和磁盘都不需要再扩。新增交易对时再按 §6 的"交易对数 ×N"线性放缩。

---

## 6. 压力影响因素（敏感性）

按比例放缩各项假设：

| 因素 | 影响 |
|---|---|
| 交易对数 ×N | 稳态行数与存储 ×N（线性） |
| 行宽 200B → 250B | 全部数字 ×1.25（如 numeric 实际占用偏大） |
| 加 user_fills（多地址） | 见 §4，单独累加（注：user_fills 没有 retention） |
| 删 1m 的冗余索引 | 1m 稳态节省约 25%（≈ 0.6 GB） |
| 关闭 retention（`enabled=false`） | 回到原始线性增长：1y ~7-11 GB / 3y ~20-30 GB |
| 1m keep_days 30 → 7 | 稳态 1m 段从 2.6 GB 降到 0.6 GB（总稳态 ≈ 2.5 GB） |
| 1d keep_days 730 → 365 | 1d 段砍半（~22 MB），且不再有压缩段 |
| 压缩比 6× → 15× | 影响很小：稳态下压缩段 < 10 MB（retention 已先删大部分） |
| retention `interval_ms` 拉长 | 短期内可见的"超期未删"行增加；不影响稳态上限 |

---

## 7. PG 之外的存储

完整一览（不在本文档主体内估算，仅列出位置）：

- **Redis**：l2book 最近快照 + ticker + 最近 50 笔 trades + live kline pub/sub。
  Hot working set，`maxmemory` 256 MB 足够 300 coins。
- **collector 进程内存**：channel buf 与各 ingester state，全开 300 coins 估 < 200 MB。
- **日志**：`/tmp/logs/collector.log`，按部署机本地处置。
