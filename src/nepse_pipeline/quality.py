"""Data-quality gates. A breach fails the job rather than publishing quietly-wrong data."""

from __future__ import annotations

import gzip
import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

import duckdb

from .config import CONFIG, NPT, Config
from .parsers import Rejected

log = logging.getLogger(__name__)


class DataQualityError(RuntimeError):
    pass


@dataclass
class Check:
    name: str
    passed: bool
    detail: str


@dataclass
class QualityReport:
    business_date: date
    checks: list[Check] = field(default_factory=list)

    def add(self, name: str, passed: bool, detail: str = "") -> None:
        self.checks.append(Check(name, passed, detail))

    @property
    def failures(self) -> list[Check]:
        return [c for c in self.checks if not c.passed]

    def raise_if_failed(self) -> None:
        if self.failures:
            lines = "\n".join(f"  FAIL {c.name}: {c.detail}" for c in self.failures)
            raise DataQualityError(f"quality gate failed for {self.business_date}:\n{lines}")

    def render(self) -> str:
        return "\n".join(
            f"  {'PASS' if c.passed else 'FAIL'} {c.name:28} {c.detail}" for c in self.checks
        )


def quarantine(dataset: str, rejected: list[Rejected], cfg: Config = CONFIG) -> Path | None:
    """Bad rows are preserved with their reason - never dropped, never passed through."""
    if not rejected:
        return None
    cfg.quarantine.mkdir(parents=True, exist_ok=True)
    now = datetime.now(NPT)
    path = cfg.quarantine / f"{dataset}-{int(now.timestamp()*1000)}.jsonl.gz"
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for r in rejected:
            fh.write(json.dumps({"reason": r.reason, "raw": r.raw}, default=str) + "\n")
    log.warning("quarantined dataset=%s count=%d path=%s", dataset, len(rejected), path)
    return path


def check_floorsheet(con: duckdb.DuckDBPyConnection, d: date, expected_total: int | None = None) -> QualityReport:
    rep = QualityReport(d)
    q = lambda sql: con.execute(sql, [d]).fetchone()[0]  # noqa: E731

    n = q("SELECT count(*) FROM floorsheet WHERE business_date = ?")
    rep.add("row_count_nonzero", n > 0, f"{n} contracts")

    if expected_total is not None:
        # Upstream keeps trading while we paginate, so we may legitimately hold
        # a few more than the total observed at the start of the sweep.
        ok = n >= expected_total * 0.999
        rep.add("completeness_vs_upstream", ok, f"have {n}, upstream reported {expected_total}")

    dupes = q("SELECT count(*) - count(DISTINCT contract_id) FROM floorsheet WHERE business_date = ?")
    rep.add("contract_id_unique", dupes == 0, f"{dupes} duplicates")

    nulls = q(
        "SELECT count(*) FROM floorsheet WHERE business_date = ? AND "
        "(stock_symbol IS NULL OR contract_rate IS NULL OR contract_quantity IS NULL)"
    )
    rep.add("required_not_null", nulls == 0, f"{nulls} null in required cols")

    bad_amt = q(
        "SELECT count(*) FROM floorsheet WHERE business_date = ? "
        "AND abs(contract_rate * contract_quantity - contract_amount) > 1.0"
    )
    rep.add("amount_reconciles", bad_amt == 0, f"{bad_amt} rows fail qty*rate=amount")

    nonpos = q(
        "SELECT count(*) FROM floorsheet WHERE business_date = ? "
        "AND (contract_rate <= 0 OR contract_quantity <= 0)"
    )
    rep.add("prices_positive", nonpos == 0, f"{nonpos} non-positive")

    off_date = q(
        "SELECT count(*) FROM floorsheet WHERE business_date = ? AND CAST(trade_time AS DATE) <> business_date"
    )
    rep.add("trade_time_matches_date", off_date == 0, f"{off_date} mismatched")

    syms = q("SELECT count(DISTINCT stock_symbol) FROM floorsheet WHERE business_date = ?")
    rep.add("symbol_breadth", syms > 20, f"{syms} distinct symbols")
    return rep


def check_eod(con: duckdb.DuckDBPyConnection, d: date) -> QualityReport:
    rep = QualityReport(d)
    q = lambda sql: con.execute(sql, [d]).fetchone()[0]  # noqa: E731

    n = q("SELECT count(*) FROM eod_price WHERE business_date = ?")
    rep.add("row_count_nonzero", n > 0, f"{n} securities")

    dupes = q(
        "SELECT count(*) - count(DISTINCT symbol) FROM eod_price WHERE business_date = ?"
    )
    rep.add("symbol_unique", dupes == 0, f"{dupes} duplicates")

    ohlc = q(
        "SELECT count(*) FROM eod_price WHERE business_date = ? "
        "AND high_price IS NOT NULL AND low_price IS NOT NULL AND high_price < low_price"
    )
    rep.add("high_ge_low", ohlc == 0, f"{ohlc} bars with high < low")

    flagged = q(
        "SELECT count(*) FROM eod_price WHERE business_date = ? AND quality_flags IS NOT NULL"
    )
    # A handful of debentures is normal; a surge means something changed upstream.
    rep.add("flagged_rows_bounded", flagged <= max(10, n * 0.05), f"{flagged} rows flagged")

    # Row-count drift against recent history catches a partial fetch that would
    # otherwise look like a perfectly healthy small day.
    hist = con.execute(
        "SELECT median(c) FROM (SELECT count(*) c FROM eod_price WHERE business_date < ? "
        "GROUP BY business_date ORDER BY business_date DESC LIMIT 10)",
        [d],
    ).fetchone()[0]
    if hist:
        drift = abs(n - hist) / hist
        rep.add("rowcount_vs_trailing_median", drift < 0.25, f"{n} vs median {hist:.0f} ({drift:.0%})")
    return rep


def freshness(con: duckdb.DuckDBPyConnection) -> str:
    row = con.execute(
        "SELECT max(business_date), max(ingested_at) FROM floorsheet"
    ).fetchone()
    return f"latest floorsheet date={row[0]} ingested_at={row[1]}"
