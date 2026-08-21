"""End-of-day OHLCV: today's close and the 365-day backfill.

NEPSE clamps every history endpoint to a rolling 365 days (measured: 2025-08-19
returns data, 2025-08-15 404s, and the per-security route silently truncates a
2010 start date to exactly one year). So this job has a hard floor, and the
window slides - a date that works today is gone in a year. Backfill early.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

from .. import bronze
from ..config import CONFIG, Config
from ..parsers import parse_eod_page
from ..quality import check_eod, quarantine
from ..session import NepseError, NepseSession
from ..storage import (
    backfill_done_dates,
    connect,
    export_parquet,
    mark_backfill,
    upsert_eod,
)

log = logging.getLogger(__name__)


def fetch_day(session: NepseSession, d: date, cfg: Config = CONFIG) -> dict:
    """Fetch one business date's full EOD board. Returns a status dict, never raises on 'no data'."""
    iso = d.isoformat()
    page, all_good, all_rejected, total = 0, [], [], 0

    while True:
        try:
            payload = session.eod_prices(iso, page=page)
        except NepseError as exc:
            # Outside the rolling window NEPSE 404s. That is a boundary, not a bug.
            if "404" in str(exc):
                return {"status": "unavailable", "date": d, "rows": 0, "note": "outside 365d window"}
            raise

        bronze.write_raw("eod_price", payload, business_date=d, suffix=f"p{page}", cfg=cfg)
        total = int(payload.get("totalElements", 0))
        res = parse_eod_page(payload)
        all_good.extend(res.good)
        all_rejected.extend(res.rejected)

        if payload.get("last", True) or not res.good:
            break
        page += 1

    quarantine("eod_price", all_rejected, cfg=cfg)

    if not all_good:
        return {"status": "empty", "date": d, "rows": 0, "note": "no trading (holiday/weekend)"}

    with connect(cfg) as con:
        n = upsert_eod(con, all_good)
        mark_backfill(con, d, "done", n, f"upstream_total={total}")
    return {"status": "done", "date": d, "rows": n, "upstream_total": total}


def backfill(
    session: NepseSession,
    *,
    days: int | None = None,
    end: date | None = None,
    resume: bool = True,
    cfg: Config = CONFIG,
) -> dict:
    """Walk backwards from `end` up to `days`. Resumable - already-done dates are skipped."""
    end = end or date.today()
    days = days or cfg.history_window_days

    with connect(cfg) as con:
        done = backfill_done_dates(con) if resume else set()

    targets = [end - timedelta(days=i) for i in range(days + 1)]
    # Only Saturday is safe to skip.
    #
    # NEPSE traded SUNDAY-THURSDAY until the Chaitra 2082 schedule change
    # (~2026-04-06), then switched to Monday-Friday. Verified: 2025-11-16 (Sun)
    # returned 337 securities and 2026-03-15 (Sun) returned 350, while Fridays
    # before the change are empty. A weekday()<5 filter therefore silently drops
    # every pre-change Sunday - about 33 trading days of real data.
    #
    # Saturday is the weekly holiday in Nepal under both regimes, so it is the
    # one day that is never a trading day. Everything else gets probed; an
    # actual holiday costs one cheap request and is recorded as 'empty'.
    targets = [d for d in targets if d.weekday() != 5 and d not in done]

    log.info("backfill_start end=%s days=%d todo=%d skipped=%d", end, days, len(targets), len(done))
    counts = {"done": 0, "empty": 0, "unavailable": 0, "failed": 0}
    rows_total = 0

    for i, d in enumerate(targets, 1):
        try:
            r = fetch_day(session, d, cfg=cfg)
        except Exception as exc:  # one bad day must not abort a 250-day backfill
            log.exception("backfill_day_failed date=%s", d)
            with connect(cfg) as con:
                mark_backfill(con, d, "failed", 0, f"{type(exc).__name__}: {exc}"[:400])
            counts["failed"] += 1
            continue

        status = r["status"]
        counts[status] = counts.get(status, 0) + 1
        rows_total += r["rows"]
        if status in ("empty", "unavailable"):
            with connect(cfg) as con:
                mark_backfill(con, d, status, 0, r.get("note", ""))
        if status == "unavailable":
            log.info("backfill_hit_floor date=%s - stopping", d)
            break
        if i % 10 == 0:
            log.info("backfill_progress %d/%d rows=%d %s", i, len(targets), rows_total, counts)

    log.info("backfill_done %s rows=%d", counts, rows_total)
    return {"counts": counts, "rows": rows_total}


def finalize(business_date: date, cfg: Config = CONFIG) -> None:
    with connect(cfg) as con:
        rep = check_eod(con, business_date)
        print(rep.render())
        rep.raise_if_failed()
        export_parquet(con, "eod_price", business_date, cfg=cfg)
