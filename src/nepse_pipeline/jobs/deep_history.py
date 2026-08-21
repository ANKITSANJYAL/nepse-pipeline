"""Deep daily history from ShareSansar (back to 2011), in two independent phases.

Phase 1 (fetch) writes raw JSON to bronze and touches no database. Phase 2 (load)
parses bronze into DuckDB. They are split because the live poller holds the
DuckDB write lock intermittently, and a 20-minute fetch should not contend with
it - and because a re-parse must never require re-fetching 478 symbols.

Resume is by bronze file existence, so an interrupted run costs nothing.
"""

from __future__ import annotations

import gzip
import json
import logging
from datetime import date, datetime
from pathlib import Path
from urllib.parse import quote

from ..config import CONFIG, NPT, Config
from ..parsers import Rejected
from ..quality import quarantine
from ..sources.sharesansar import (
    SOURCE,
    ShareSansarClient,
    ShareSansarError,
    parse_history_payload,
)
from ..storage import connect, export_ext_history, mark_ext_symbol, upsert_ext_history

log = logging.getLogger(__name__)


def _bronze_dir(cfg: Config) -> Path:
    return cfg.bronze / f"{SOURCE}_history"


def _bronze_name(symbol: str) -> str:
    """Debenture symbols contain a slash (GBILD84/85), which is a path separator.
    Percent-encoding keeps the filename flat and one-to-one with the symbol; the
    authoritative symbol is stored inside the envelope regardless."""
    return quote(symbol.upper(), safe="")


def symbols_from_nepse(cfg: Config = CONFIG) -> list[str]:
    """Universe = every symbol NEPSE has shown us. Traded-recently first."""
    with connect(cfg, read_only=True) as con:
        return [
            r[0]
            for r in con.execute(
                "SELECT symbol FROM eod_price GROUP BY symbol ORDER BY max(business_date) DESC, symbol"
            ).fetchall()
        ]


def universe(cfg: Config = CONFIG, include_listed: bool = True) -> list[str]:
    """NEPSE's traded symbols first, then anything ShareSansar lists that NEPSE
    has never shown us.

    `eod_price` only contains symbols that traded inside NEPSE's rolling 365-day
    window, so a universe drawn from it alone is survivorship-biased: suspended,
    delisted and never-traded names simply vanish from the archive.
    """
    nepse = symbols_from_nepse(cfg)
    if not include_listed:
        return nepse
    known = {s.upper() for s in nepse}
    with ShareSansarClient() as client:
        extra = [s for s in client.listed_symbols() if s not in known]
    log.info("deep_universe nepse=%d sharesansar_only=%d", len(nepse), len(extra))
    return nepse + extra


def fetch_all(
    symbols: list[str] | None = None,
    *,
    resume: bool = True,
    include_listed: bool = True,
    min_gap_s: float = 1.2,
    cfg: Config = CONFIG,
) -> dict:
    """Phase 1: raw JSON to bronze. No DB writes."""
    out_dir = _bronze_dir(cfg)
    out_dir.mkdir(parents=True, exist_ok=True)
    symbols = symbols or universe(cfg, include_listed=include_listed)

    todo = [s for s in symbols if not (resume and (out_dir / f"{_bronze_name(s)}.json.gz").exists())]
    log.info("deep_fetch_start total=%d todo=%d skipped=%d", len(symbols), len(todo), len(symbols) - len(todo))

    counts = {"ok": 0, "failed": 0, "skipped": len(symbols) - len(todo)}
    failures: list[tuple[str, str]] = []

    with ShareSansarClient(min_gap_s=min_gap_s) as client:
        for i, sym in enumerate(todo, 1):
            try:
                payload = client.price_history(sym)
                envelope = {
                    "source": SOURCE,
                    "symbol": sym.upper(),
                    "fetched_at": datetime.now(NPT).isoformat(),
                    "payload": payload,
                }
                tmp = out_dir / f"{_bronze_name(sym)}.tmp"
                with gzip.open(tmp, "wt", encoding="utf-8") as fh:
                    json.dump(envelope, fh, separators=(",", ":"))
                tmp.replace(out_dir / f"{_bronze_name(sym)}.json.gz")
                counts["ok"] += 1
            except ShareSansarError as exc:
                # Delisted or renamed symbols legitimately 404. Record and move on.
                counts["failed"] += 1
                failures.append((sym, str(exc)[:120]))
                log.warning("deep_fetch_failed symbol=%s %s", sym, str(exc)[:120])
            except Exception as exc:  # noqa: BLE001
                counts["failed"] += 1
                failures.append((sym, f"{type(exc).__name__}: {exc}"[:120]))
                log.exception("deep_fetch_error symbol=%s", sym)

            if i % 25 == 0:
                log.info("deep_fetch_progress %d/%d %s", i, len(todo), counts)

    log.info("deep_fetch_done %s", counts)
    return {"counts": counts, "failures": failures}


def load(cfg: Config = CONFIG, symbols: set[str] | None = None) -> dict:
    """Phase 2: bronze -> DuckDB. Re-runnable; upserts on (source, symbol, date).

    `symbols` restricts the load to a subset, which matters because a full
    reload is ~500k single-row upserts and takes minutes of write lock the
    live poller also wants.
    """
    out_dir = _bronze_dir(cfg)
    files = sorted(out_dir.glob("*.json.gz"))
    if symbols is not None:
        wanted = {_bronze_name(s) for s in symbols}
        files = [f for f in files if f.name[: -len(".json.gz")] in wanted]
    totals = {"symbols": 0, "rows": 0, "rejected": 0}
    rejected: list[Rejected] = []

    with connect(cfg) as con:
        for path in files:
            with gzip.open(path, "rt", encoding="utf-8") as fh:
                env = json.load(fh)
            sym = env["symbol"]
            try:
                good, bad = parse_history_payload(sym, env["payload"])
            except ShareSansarError as exc:
                mark_ext_symbol(con, SOURCE, sym, "failed", 0, None, None, str(exc))
                continue

            n = upsert_ext_history(con, good)
            dates = [r.business_date for r in good]
            mark_ext_symbol(
                con, SOURCE, sym, "done" if good else "empty", n,
                min(dates) if dates else None, max(dates) if dates else None,
                f"rejected={len(bad)}",
            )
            totals["symbols"] += 1
            totals["rows"] += n
            totals["rejected"] += len(bad)
            rejected.extend(
                Rejected(raw={"symbol": sym, **b["raw"]}, reason=b["reason"]) for b in bad
            )

    # Row-level rejects were previously counted and thrown away; a rejected row
    # is evidence about the source, not noise.
    quarantine(f"{SOURCE}_history", rejected, cfg=cfg)

    with connect(cfg) as con:
        totals["parquet"] = str(export_ext_history(con, SOURCE, cfg))
    log.info("deep_load_done %s", totals)
    return totals


def reconcile(cfg: Config = CONFIG) -> dict:
    """Measure ShareSansar against NEPSE on the window where both exist.

    This is the only way to earn trust in the pre-2025 portion, which has no
    authoritative source to check against.
    """
    with connect(cfg, read_only=True) as con:
        row = con.execute("""
            SELECT count(*) AS overlap_rows,
                   sum(CASE WHEN e.close_price = x.close_price THEN 1 ELSE 0 END) AS close_exact,
                   sum(CASE WHEN abs(coalesce(e.close_price,0) - coalesce(x.close_price,0)) <= 0.01
                            THEN 1 ELSE 0 END) AS close_within_1p,
                   sum(CASE WHEN e.total_traded_quantity = x.traded_quantity THEN 1 ELSE 0 END) AS vol_exact,
                   count(DISTINCT e.symbol) AS symbols,
                   min(e.business_date), max(e.business_date)
            FROM eod_price e
            JOIN eod_history_ext x
              ON x.symbol = e.symbol AND x.business_date = e.business_date AND x.source = ?
        """, [SOURCE]).fetchone()
    keys = ["overlap_rows", "close_exact", "close_within_1_paisa", "volume_exact",
            "symbols", "from", "to"]
    return dict(zip(keys, row))
