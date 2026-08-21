"""Gold layer: consumption-ready tables built from floorsheet ticks.

Everything here is derived and fully rebuildable from silver, so these are
overwrite-by-partition rather than upsert - reprocessing a day is just a rerun.
"""

from __future__ import annotations

import logging
from datetime import date

import duckdb

from ..config import CONFIG, Config
from ..storage import connect

log = logging.getLogger(__name__)

GOLD_SCHEMA = """
CREATE TABLE IF NOT EXISTS bars_1m (
    business_date DATE NOT NULL,
    symbol        VARCHAR NOT NULL,
    bar_ts        TIMESTAMP NOT NULL,
    open          DECIMAL(18,4),
    high          DECIMAL(18,4),
    low           DECIMAL(18,4),
    close         DECIMAL(18,4),
    volume        BIGINT,
    turnover      DECIMAL(22,4),
    trades        BIGINT,
    vwap          DECIMAL(18,6),
    PRIMARY KEY (symbol, bar_ts)
);

CREATE TABLE IF NOT EXISTS broker_flow_daily (
    business_date DATE NOT NULL,
    symbol        VARCHAR NOT NULL,
    broker_id     VARCHAR NOT NULL,
    buy_qty       BIGINT,
    sell_qty      BIGINT,
    net_qty       BIGINT,
    buy_value     DECIMAL(22,4),
    sell_value    DECIMAL(22,4),
    net_value     DECIMAL(22,4),
    PRIMARY KEY (business_date, symbol, broker_id)
);

CREATE TABLE IF NOT EXISTS daily_stats (
    business_date DATE NOT NULL,
    symbol        VARCHAR NOT NULL,
    vwap          DECIMAL(18,6),
    volume        BIGINT,
    turnover      DECIMAL(22,4),
    trades        BIGINT,
    first_trade   TIMESTAMP,
    last_trade    TIMESTAMP,
    PRIMARY KEY (business_date, symbol)
);
"""

BARS_SQL = """
INSERT OR REPLACE INTO bars_1m
SELECT
    business_date,
    stock_symbol AS symbol,
    time_bucket(INTERVAL '1 minute', trade_time) AS bar_ts,
    first(contract_rate ORDER BY contract_id)    AS open,
    max(contract_rate)                           AS high,
    min(contract_rate)                           AS low,
    last(contract_rate  ORDER BY contract_id)    AS close,
    sum(contract_quantity)                       AS volume,
    sum(contract_amount)                         AS turnover,
    count(*)                                     AS trades,
    sum(contract_amount) / nullif(sum(contract_quantity), 0) AS vwap
FROM floorsheet
WHERE business_date = ?
GROUP BY business_date, stock_symbol, bar_ts;
"""

# One row per (symbol, broker) with both sides netted. The buyer/seller split is
# what makes NEPSE unusual - most exchanges never expose broker identity.
FLOW_SQL = """
INSERT OR REPLACE INTO broker_flow_daily
WITH sides AS (
    SELECT business_date, stock_symbol, buyer_member_id  AS broker_id,
           contract_quantity AS qty, contract_amount AS val, 'B' AS side
    FROM floorsheet WHERE business_date = ?
    UNION ALL
    SELECT business_date, stock_symbol, seller_member_id AS broker_id,
           contract_quantity, contract_amount, 'S'
    FROM floorsheet WHERE business_date = ?
)
SELECT
    business_date,
    stock_symbol AS symbol,
    broker_id,
    sum(CASE WHEN side='B' THEN qty ELSE 0 END) AS buy_qty,
    sum(CASE WHEN side='S' THEN qty ELSE 0 END) AS sell_qty,
    sum(CASE WHEN side='B' THEN qty ELSE -qty END) AS net_qty,
    sum(CASE WHEN side='B' THEN val ELSE 0 END) AS buy_value,
    sum(CASE WHEN side='S' THEN val ELSE 0 END) AS sell_value,
    sum(CASE WHEN side='B' THEN val ELSE -val END) AS net_value
FROM sides
GROUP BY business_date, stock_symbol, broker_id;
"""

STATS_SQL = """
INSERT OR REPLACE INTO daily_stats
SELECT
    business_date,
    stock_symbol AS symbol,
    sum(contract_amount) / nullif(sum(contract_quantity), 0) AS vwap,
    sum(contract_quantity) AS volume,
    sum(contract_amount)   AS turnover,
    count(*)               AS trades,
    min(trade_time)        AS first_trade,
    max(trade_time)        AS last_trade
FROM floorsheet
WHERE business_date = ?
GROUP BY business_date, stock_symbol;
"""


def build(business_date: date, cfg: Config = CONFIG) -> dict:
    with connect(cfg) as con:
        con.execute(GOLD_SCHEMA)
        con.execute("DELETE FROM bars_1m           WHERE business_date = ?", [business_date])
        con.execute("DELETE FROM broker_flow_daily WHERE business_date = ?", [business_date])
        con.execute("DELETE FROM daily_stats       WHERE business_date = ?", [business_date])

        con.execute(BARS_SQL, [business_date])
        con.execute(FLOW_SQL, [business_date, business_date])
        con.execute(STATS_SQL, [business_date])

        counts = {
            t: con.execute(
                f"SELECT count(*) FROM {t} WHERE business_date = ?", [business_date]
            ).fetchone()[0]
            for t in ("bars_1m", "broker_flow_daily", "daily_stats")
        }
        _reconcile(con, business_date)
        for t in ("bars_1m", "broker_flow_daily", "daily_stats"):
            _export(con, t, business_date, cfg)

    log.info("gold_built date=%s %s", business_date, counts)
    return counts


def _reconcile(con: duckdb.DuckDBPyConnection, d: date) -> None:
    """Gold must conserve volume and turnover from the ticks it was built from."""
    src = con.execute(
        "SELECT sum(contract_quantity), sum(contract_amount) FROM floorsheet WHERE business_date = ?",
        [d],
    ).fetchone()
    for table in ("bars_1m", "daily_stats"):
        got = con.execute(
            f"SELECT sum(volume), sum(turnover) FROM {table} WHERE business_date = ?", [d]
        ).fetchone()
        if src[0] and got[0] != src[0]:
            raise AssertionError(f"{table}: volume {got[0]} != floorsheet {src[0]}")
        if src[1] and abs(float(got[1]) - float(src[1])) > 1.0:
            raise AssertionError(f"{table}: turnover {got[1]} != floorsheet {src[1]}")
    log.info("gold_reconciled date=%s volume=%s turnover=%s", d, src[0], src[1])


def _export(con: duckdb.DuckDBPyConnection, table: str, d: date, cfg: Config) -> None:
    out_dir = cfg.gold / table / f"business_date={d.isoformat()}"
    out_dir.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY (SELECT * FROM {table} WHERE business_date = ?) "
        f"TO '{out_dir / 'part-0.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD)",
        [d],
    )
