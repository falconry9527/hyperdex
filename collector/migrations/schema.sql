-- Final consolidated schema (merged from 0001_init + 0002_klines_real_tables).
-- Each k-line interval is an independent hypertable, written directly by the
-- collector (driven by cfg.Kline.Intervals).

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Reference tables -----------------------------------------------------------

CREATE TABLE IF NOT EXISTS coins (
  coin       text        PRIMARY KEY,
  asset_id   int         NOT NULL,
  is_active  bool        NOT NULL DEFAULT true,
  added_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS coverage (
  domain     text        NOT NULL,
  coin       text        NOT NULL,
  earliest   timestamptz,
  latest     timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (domain, coin)
);

-- K-line hypertables ---------------------------------------------------------

CREATE TABLE IF NOT EXISTS klines_1m (
  coin    text           NOT NULL,
  ts      timestamptz    NOT NULL,
  open    numeric(38,18) NOT NULL,
  high    numeric(38,18) NOT NULL,
  low     numeric(38,18) NOT NULL,
  close   numeric(38,18) NOT NULL,
  volume  numeric(38,18) NOT NULL,
  trades  int            NOT NULL,
  PRIMARY KEY (coin, ts)
);
SELECT create_hypertable('klines_1m', 'ts',
  chunk_time_interval => INTERVAL '7 days',
  if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS klines_1m_coin_ts_desc_idx ON klines_1m (coin, ts DESC);

CREATE TABLE IF NOT EXISTS klines_5m (
  coin    text           NOT NULL,
  ts      timestamptz    NOT NULL,
  open    numeric(38,18) NOT NULL,
  high    numeric(38,18) NOT NULL,
  low     numeric(38,18) NOT NULL,
  close   numeric(38,18) NOT NULL,
  volume  numeric(38,18) NOT NULL,
  trades  int            NOT NULL,
  PRIMARY KEY (coin, ts)
);
SELECT create_hypertable('klines_5m', 'ts',
  chunk_time_interval => INTERVAL '30 days',
  if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS klines_5m_coin_ts_desc_idx ON klines_5m (coin, ts DESC);
CREATE INDEX IF NOT EXISTS klines_5m_ts_idx           ON klines_5m (ts DESC);

CREATE TABLE IF NOT EXISTS klines_15m (
  coin    text           NOT NULL,
  ts      timestamptz    NOT NULL,
  open    numeric(38,18) NOT NULL,
  high    numeric(38,18) NOT NULL,
  low     numeric(38,18) NOT NULL,
  close   numeric(38,18) NOT NULL,
  volume  numeric(38,18) NOT NULL,
  trades  int            NOT NULL,
  PRIMARY KEY (coin, ts)
);
SELECT create_hypertable('klines_15m', 'ts',
  chunk_time_interval => INTERVAL '30 days',
  if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS klines_15m_coin_ts_desc_idx ON klines_15m (coin, ts DESC);
CREATE INDEX IF NOT EXISTS klines_15m_ts_idx           ON klines_15m (ts DESC);

CREATE TABLE IF NOT EXISTS klines_1h (
  coin    text           NOT NULL,
  ts      timestamptz    NOT NULL,
  open    numeric(38,18) NOT NULL,
  high    numeric(38,18) NOT NULL,
  low     numeric(38,18) NOT NULL,
  close   numeric(38,18) NOT NULL,
  volume  numeric(38,18) NOT NULL,
  trades  int            NOT NULL,
  PRIMARY KEY (coin, ts)
);
SELECT create_hypertable('klines_1h', 'ts',
  chunk_time_interval => INTERVAL '90 days',
  if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS klines_1h_coin_ts_desc_idx ON klines_1h (coin, ts DESC);
CREATE INDEX IF NOT EXISTS klines_1h_ts_idx           ON klines_1h (ts DESC);

CREATE TABLE IF NOT EXISTS klines_4h (
  coin    text           NOT NULL,
  ts      timestamptz    NOT NULL,
  open    numeric(38,18) NOT NULL,
  high    numeric(38,18) NOT NULL,
  low     numeric(38,18) NOT NULL,
  close   numeric(38,18) NOT NULL,
  volume  numeric(38,18) NOT NULL,
  trades  int            NOT NULL,
  PRIMARY KEY (coin, ts)
);
SELECT create_hypertable('klines_4h', 'ts',
  chunk_time_interval => INTERVAL '365 days',
  if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS klines_4h_coin_ts_desc_idx ON klines_4h (coin, ts DESC);
CREATE INDEX IF NOT EXISTS klines_4h_ts_idx           ON klines_4h (ts DESC);

CREATE TABLE IF NOT EXISTS klines_1d (
  coin    text           NOT NULL,
  ts      timestamptz    NOT NULL,
  open    numeric(38,18) NOT NULL,
  high    numeric(38,18) NOT NULL,
  low     numeric(38,18) NOT NULL,
  close   numeric(38,18) NOT NULL,
  volume  numeric(38,18) NOT NULL,
  trades  int            NOT NULL,
  PRIMARY KEY (coin, ts)
);
SELECT create_hypertable('klines_1d', 'ts',
  chunk_time_interval => INTERVAL '365 days',
  if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS klines_1d_coin_ts_desc_idx ON klines_1d (coin, ts DESC);
CREATE INDEX IF NOT EXISTS klines_1d_ts_idx           ON klines_1d (ts DESC);

CREATE TABLE IF NOT EXISTS klines_1mo (
  coin    text           NOT NULL,
  ts      timestamptz    NOT NULL,
  open    numeric(38,18) NOT NULL,
  high    numeric(38,18) NOT NULL,
  low     numeric(38,18) NOT NULL,
  close   numeric(38,18) NOT NULL,
  volume  numeric(38,18) NOT NULL,
  trades  int            NOT NULL,
  PRIMARY KEY (coin, ts)
);
SELECT create_hypertable('klines_1mo', 'ts',
  chunk_time_interval => INTERVAL '1825 days',
  if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS klines_1mo_coin_ts_desc_idx ON klines_1mo (coin, ts DESC);
CREATE INDEX IF NOT EXISTS klines_1mo_ts_idx           ON klines_1mo (ts DESC);

-- Compression: rows older than the threshold move to columnar storage --------

ALTER TABLE klines_1m SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'coin',
  timescaledb.compress_orderby   = 'ts DESC'
);
SELECT add_compression_policy('klines_1m', INTERVAL '30 days');

ALTER TABLE klines_5m SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'coin',
  timescaledb.compress_orderby   = 'ts DESC'
);
SELECT add_compression_policy('klines_5m', INTERVAL '30 days');

ALTER TABLE klines_15m SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'coin',
  timescaledb.compress_orderby   = 'ts DESC'
);
SELECT add_compression_policy('klines_15m', INTERVAL '30 days');

ALTER TABLE klines_1h SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'coin',
  timescaledb.compress_orderby   = 'ts DESC'
);
SELECT add_compression_policy('klines_1h', INTERVAL '90 days');

ALTER TABLE klines_4h SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'coin',
  timescaledb.compress_orderby   = 'ts DESC'
);
SELECT add_compression_policy('klines_4h', INTERVAL '180 days');

ALTER TABLE klines_1d SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'coin',
  timescaledb.compress_orderby   = 'ts DESC'
);
SELECT add_compression_policy('klines_1d', INTERVAL '365 days');

-- ─── User fills (private trade history per address) ────────────────────────
--
-- Indexed by collector's `userfills` ingester from HL's REST userFillsByTime
-- endpoint. HL only retains a few thousand recent fills server-side, so this
-- table is the long-term source of truth for analytics: PnL curves, win rate,
-- tax exports, etc.
--
-- (user_addr, tid) is the natural key — `tid` is HL's globally unique trade
-- id which identifies one fill (a single market order can cause multiple
-- fills, each with its own tid).

CREATE TABLE IF NOT EXISTS user_fills (
  user_addr     text           NOT NULL,
  tid           bigint         NOT NULL,
  time          timestamptz    NOT NULL,
  coin          text           NOT NULL,
  side          char(1)        NOT NULL,    -- 'B' (buy) or 'A' (sell)
  px            numeric(38,18) NOT NULL,
  sz            numeric(38,18) NOT NULL,
  fee           numeric(38,18) NOT NULL,
  closed_pnl    numeric(38,18) NOT NULL,
  start_pos     numeric(38,18),
  dir           text,                       -- "Open Long" / "Close Short" / etc
  oid           bigint,
  hash          text,
  crossed       boolean,
  fee_token     text,
  PRIMARY KEY (user_addr, tid)
);

-- Per-user time-range scan is the dominant query pattern (UI scrollback,
-- pnl-by-day). DESC index supports `ORDER BY time DESC` without a sort.
CREATE INDEX IF NOT EXISTS user_fills_addr_time_desc_idx
  ON user_fills (user_addr, time DESC);
