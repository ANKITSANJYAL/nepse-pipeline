"""DuckDB warehouse + Parquet export.

DuckDB because the whole dataset is ~12 GB/year: single-node columnar handles
it with room to spare. Spark/Kafka here would be cost with no benefit.

Writes are upserts on natural keys, so re-running any job over the same input
converges to the same table. Backfill and incremental share one code path.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Iterable, Iterator, Sequence

import duckdb

from .config import CONFIG, Config
from .schemas import EodQuote, FloorsheetContract

log = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS floorsheet (
    contract_id        BIGINT PRIMARY KEY,
    business_date      DATE       NOT NULL,
    trade_time         TIMESTAMP  NOT NULL,
    stock_symbol       VARCHAR    NOT NULL,
    stock_id           INTEGER    NOT NULL,
    security_name      VARCHAR,
    buyer_member_id    VARCHAR    NOT NULL,
    seller_member_id   VARCHAR    NOT NULL,
    buyer_broker_name  VARCHAR,
    seller_broker_name VARCHAR,
    contract_quantity  BIGINT     NOT NULL,
    contract_rate      DECIMAL(18,4) NOT NULL,
    contract_amount    DECIMAL(20,4) NOT NULL,
    trade_book_id      BIGINT,
    ingested_at        TIMESTAMP  DEFAULT current_timestamp
);
CREATE INDEX IF NOT EXISTS floorsheet_sym_date ON floorsheet (stock_symbol, business_date);
CREATE INDEX IF NOT EXISTS floorsheet_date     ON floorsheet (business_date);

CREATE TABLE IF NOT EXISTS eod_price (
    business_date            DATE    NOT NULL,
    symbol                   VARCHAR NOT NULL,
    security_id              INTEGER NOT NULL,
    security_name            VARCHAR,
    open_price               DECIMAL(18,4),
    high_price               DECIMAL(18,4),
    low_price                DECIMAL(18,4),
    close_price              DECIMAL(18,4),
    total_traded_quantity    BIGINT,
    total_traded_value       DECIMAL(22,4),
    previous_day_close_price DECIMAL(18,4),
    fifty_two_week_high      DECIMAL(18,4),
    fifty_two_week_low       DECIMAL(18,4),
    last_updated_time        TIMESTAMP,
    last_updated_price       DECIMAL(18,4),
    total_trades             BIGINT,
    average_traded_price     DECIMAL(18,4),
    market_capitalization    DECIMAL(22,4),
    quality_flags            VARCHAR,
    ingested_at              TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (business_date, symbol)
);

ALTER TABLE eod_price ADD COLUMN IF NOT EXISTS quality_flags VARCHAR;

CREATE TABLE IF NOT EXISTS index_intraday (
    index_id   INTEGER   NOT NULL,
    ts         TIMESTAMP NOT NULL,
    value      DECIMAL(18,4) NOT NULL,
    PRIMARY KEY (index_id, ts)
);

-- Second-source daily history (ShareSansar), kept SEPARATE from the NEPSE
-- tables on purpose: it reaches back to 2011 but is not authoritative, and
-- merging it blindly would make the two indistinguishable forever.
CREATE TABLE IF NOT EXISTS eod_history_ext (
    source          VARCHAR NOT NULL,
    symbol          VARCHAR NOT NULL,
    business_date   DATE    NOT NULL,
    open_price      DECIMAL(18,4),
    high_price      DECIMAL(18,4),
    low_price       DECIMAL(18,4),
    close_price     DECIMAL(18,4),
    percent_change  DECIMAL(12,4),
    traded_quantity BIGINT,
    traded_amount   DECIMAL(22,4),
    ingested_at     TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (source, symbol, business_date)
);

CREATE TABLE IF NOT EXISTS ext_symbol_state (
    source     VARCHAR NOT NULL,
    symbol     VARCHAR NOT NULL,
    status     VARCHAR NOT NULL,
    row_count  INTEGER,
    oldest     DATE,
    newest     DATE,
    note       VARCHAR,
    updated_at TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (source, symbol)
);

-- Watermarks make every job resumable after a crash without rescanning.
CREATE TABLE IF NOT EXISTS ingest_watermark (
    job           VARCHAR PRIMARY KEY,
    business_date DATE,
    cursor_value  BIGINT,
    updated_at    TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS backfill_state (
    business_date DATE PRIMARY KEY,
    status        VARCHAR NOT NULL,   -- done | empty | unavailable | failed
    row_count     INTEGER,
    note          VARCHAR,
    updated_at    TIMESTAMP DEFAULT current_timestamp
);
"""

FLOORSHEET_COLS = [
    "contract_id", "business_date", "trade_time", "stock_symbol", "stock_id",
    "security_name", "buyer_member_id", "seller_member_id", "buyer_broker_name",
    "seller_broker_name", "contract_quantity", "contract_rate", "contract_amount",
    "trade_book_id",
]

EOD_COLS = [
    "business_date", "symbol", "security_id", "security_name", "open_price",
    "high_price", "low_price", "close_price", "total_traded_quantity",
    "total_traded_value", "previous_day_close_price", "fifty_two_week_high",
    "fifty_two_week_low", "last_updated_time", "last_updated_price",
    "total_trades", "average_traded_price", "market_capitalization", "quality_flags",
]


@contextmanager
def connect(cfg: Config = CONFIG, read_only: bool = False) -> Iterator[duckdb.DuckDBPyConnection]:
    cfg.ensure_dirs()
    con = duckdb.connect(str(cfg.db_path), read_only=read_only)
    try:
        if not read_only:
            con.execute(SCHEMA_SQL)
        yield con
    finally:
        con.close()


def _upsert(
    con: duckdb.DuckDBPyConnection,
    table: str,
    cols: Sequence[str],
    rows: list[tuple],
    key: Sequence[str],
) -> int:
    if not rows:
        return 0
    placeholders = ", ".join("?" for _ in cols)
    collist = ", ".join(cols)
    updates = ", ".join(f"{c} = excluded.{c}" for c in cols if c not in key)
    sql = (
        f"INSERT INTO {table} ({collist}) VALUES ({placeholders}) "
        f"ON CONFLICT ({', '.join(key)}) DO UPDATE SET {updates}"
    )
    con.executemany(sql, rows)
    return len(rows)


def upsert_floorsheet(con: duckdb.DuckDBPyConnection, records: Iterable[FloorsheetContract]) -> int:
    rows = [tuple(getattr(r, c) for c in FLOORSHEET_COLS) for r in records]
    return _upsert(con, "floorsheet", FLOORSHEET_COLS, rows, ["contract_id"])


def upsert_eod(con: duckdb.DuckDBPyConnection, records: Iterable[EodQuote]) -> int:
    rows = [tuple(getattr(r, c) for c in EOD_COLS) for r in records]
    return _upsert(con, "eod_price", EOD_COLS, rows, ["business_date", "symbol"])


def upsert_index(con: duckdb.DuckDBPyConnection, index_id: int, points: Iterable[tuple]) -> int:
    rows = [(index_id, ts, val) for ts, val in points]
    return _upsert(con, "index_intraday", ["index_id", "ts", "value"], rows, ["index_id", "ts"])


EXT_COLS = ["source", "symbol", "business_date", "open_price", "high_price",
            "low_price", "close_price", "percent_change", "traded_quantity",
            "traded_amount"]


def upsert_ext_history(con: duckdb.DuckDBPyConnection, records) -> int:
    rows = [tuple(getattr(r, c) for c in EXT_COLS) for r in records]
    return _upsert(con, "eod_history_ext", EXT_COLS, rows, ["source", "symbol", "business_date"])


def mark_ext_symbol(con, source, symbol, status, row_count=0, oldest=None, newest=None, note=""):
    con.execute(
        "INSERT INTO ext_symbol_state (source, symbol, status, row_count, oldest, newest, note, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, now()) "
        "ON CONFLICT (source, symbol) DO UPDATE SET status = excluded.status, "
        "row_count = excluded.row_count, oldest = excluded.oldest, newest = excluded.newest, "
        "note = excluded.note, updated_at = now()",
        [source, symbol, status, row_count, oldest, newest, note[:400]],
    )


def ext_done_symbols(con, source: str) -> set[str]:
    return {r[0] for r in con.execute(
        "SELECT symbol FROM ext_symbol_state WHERE source = ? AND status = 'done'", [source]
    ).fetchall()}


def get_watermark(con: duckdb.DuckDBPyConnection, job: str, business_date: date) -> int | None:
    row = con.execute(
        "SELECT cursor_value FROM ingest_watermark WHERE job = ? AND business_date = ?",
        [job, business_date],
    ).fetchone()
    return row[0] if row else None


def set_watermark(con: duckdb.DuckDBPyConnection, job: str, business_date: date, cursor: int) -> None:
    con.execute(
        "INSERT INTO ingest_watermark (job, business_date, cursor_value, updated_at) "
        "VALUES (?, ?, ?, now()) "
        "ON CONFLICT (job) DO UPDATE SET business_date = excluded.business_date, "
        "cursor_value = excluded.cursor_value, updated_at = now()",
        [job, business_date, cursor],
    )


def mark_backfill(
    con: duckdb.DuckDBPyConnection, d: date, status: str, row_count: int = 0, note: str = ""
) -> None:
    con.execute(
        "INSERT INTO backfill_state (business_date, status, row_count, note, updated_at) "
        "VALUES (?, ?, ?, ?, now()) "
        "ON CONFLICT (business_date) DO UPDATE SET status = excluded.status, "
        "row_count = excluded.row_count, note = excluded.note, updated_at = now()",
        [d, status, row_count, note],
    )


def backfill_done_dates(con: duckdb.DuckDBPyConnection) -> set[date]:
    return {
        r[0]
        for r in con.execute(
            "SELECT business_date FROM backfill_state WHERE status IN ('done','empty','unavailable')"
        ).fetchall()
    }


def export_parquet(con: duckdb.DuckDBPyConnection, table: str, d: date, cfg: Config = CONFIG) -> Path | None:
    """Silver: one Parquet file per table per trading day.

    One file per day, not one per poll - 2,880 tiny files a day would make the
    small-files problem the dominant query cost.
    """
    out_dir = cfg.silver / table / f"business_date={d.isoformat()}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "part-0.parquet"
    n = con.execute(f"SELECT count(*) FROM {table} WHERE business_date = ?", [d]).fetchone()[0]
    if n == 0:
        return None
    con.execute(
        f"COPY (SELECT * FROM {table} WHERE business_date = ? ORDER BY 1) "
        f"TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)",
        [d],
    )
    log.info("silver_export table=%s date=%s rows=%d path=%s", table, d, n, out)
    return out


def export_ext_history(con: duckdb.DuckDBPyConnection, source: str, cfg: Config = CONFIG) -> Path:
    """Silver: the whole second-source history as ONE Parquet file.

    Deliberately not partitioned by date. The table spans ~31 years but is only
    tens of MB; a business_date partition would make ~7,000 files of a few KB
    each and the small-files overhead would dominate every read. Re-exported
    whole because the source republishes the full series on each fetch anyway.
    """
    out_dir = cfg.silver / "eod_history_ext"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{source}.parquet"
    con.execute(
        f"COPY (SELECT * FROM eod_history_ext WHERE source = ? ORDER BY symbol, business_date) "
        f"TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)",
        [source],
    )
    log.info("silver_export_ext source=%s path=%s", source, out)
    return out
