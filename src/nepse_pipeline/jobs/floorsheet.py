"""Floorsheet ingestion - the tick record, and the only dataset with no second chance.

NEPSE serves the current session only and ignores businessDate on this endpoint
(verified: every historical date returns today's rows), so anything missed today
cannot be fetched tomorrow from anywhere.

Ordering caveat that drives the whole design: `sort=contractId,desc` is NOT
honoured across pages - page 10 was observed starting at ...03013919 and ending
at ...04009466. Ordering is only approximately descending, so a "stop when I
reach a known contract_id" watermark would silently skip trades. Instead:

  * during the session, sweep the first N pages (cheap, near-real-time),
  * at close, sweep every page and assert the row count against upstream's
    totalElements before the day is published.

contract_id is the primary key, so re-reading pages is free and every sweep is
idempotent.
"""

from __future__ import annotations

import logging
import math
from datetime import date

from .. import bronze
from ..config import CONFIG, Config
from ..parsers import (
    SchemaDriftError,
    TransientEmptyPage,
    floorsheet_page_meta,
    parse_floorsheet_page,
)
from ..quality import check_floorsheet, quarantine
from ..session import NepseSession
from ..storage import connect, export_parquet, mark_backfill, upsert_floorsheet

log = logging.getLogger(__name__)
JOB = "floorsheet"

# How many extra pages beyond the strict minimum to read during an incremental
# sweep. Ordering is fuzzy, so new contracts can land a few pages deep.
PAGE_SAFETY_FACTOR = 3
MIN_INCREMENTAL_PAGES = 2

# NEPSE has been observed serving a floorsheet page where *every* row carries a
# null buyerMemberId, then serving the same page clean seconds later. Taking the
# first response at face value raises SchemaDriftError and aborts the sweep - and
# an aborted floorsheet sweep can mean a permanently lost session, because the
# endpoint only ever serves the current day. Refetch before believing it.
DRIFT_REFETCH_ATTEMPTS = 3


def _fetch_page(session: NepseSession, page: int, bd, cfg: Config):
    """Fetch, persist to bronze, and parse one page - refetching on apparent drift.

    Returns (raw_payload, ParseResult). Raises SchemaDriftError only if the shape
    is still wrong after refetching, which is the signal that the endpoint really
    did change rather than hiccupped.
    """
    last: Exception | None = None
    for attempt in range(DRIFT_REFETCH_ATTEMPTS):
        payload = session.floorsheet_page(page=page)
        bronze.write_raw(
            "floorsheet", payload, business_date=bd, suffix=f"p{page}", cfg=cfg
        )
        try:
            return payload, parse_floorsheet_page(payload)
        except SchemaDriftError as exc:
            last = exc
            log.warning(
                "floorsheet_page_drift page=%d attempt=%d - refetching: %s",
                page, attempt, str(exc)[:180],
            )
    raise last



def _rows_held(con, bd: date) -> int:
    return con.execute(
        "SELECT count(*) FROM floorsheet WHERE business_date = ?", [bd]
    ).fetchone()[0]


def sweep(
    session: NepseSession,
    *,
    full: bool = False,
    max_pages: int = 400,
    cfg: Config = CONFIG,
) -> dict:
    """Fetch contracts and upsert. Idempotent; safe to run any number of times."""
    with connect(cfg) as con:
        try:
            first, res = _fetch_page(session, 0, None, cfg)
        except TransientEmptyPage:
            log.warning("floorsheet page 0 empty after retries - nothing to do")
            return {"status": "empty", "fetched": 0, "upstream_total": 0}

        total, pages = floorsheet_page_meta(first)
        if not res.good:
            return {"status": "empty", "fetched": 0, "upstream_total": total}

        bd: date = res.good[0].business_date
        held = _rows_held(con, bd)

        if full:
            want_pages = min(pages, max_pages)
        else:
            missing = max(total - held, 0)
            want_pages = max(
                MIN_INCREMENTAL_PAGES,
                math.ceil(missing / cfg.page_size) * PAGE_SAFETY_FACTOR,
            )
            want_pages = min(want_pages, pages, max_pages)

        log.info(
            "floorsheet_sweep date=%s upstream=%d held=%d pages=%d/%d mode=%s",
            bd, total, held, want_pages, pages, "full" if full else "incremental",
        )

        good, rejected = list(res.good), list(res.rejected)
        empty_pages: list[int] = []

        for page in range(1, want_pages):
            try:
                _, pres = _fetch_page(session, page, bd, cfg)
            except TransientEmptyPage:
                # Recorded, not fatal: the completeness gate below decides
                # whether the gap actually cost us anything.
                log.warning("floorsheet_page_empty page=%d", page)
                empty_pages.append(page)
                continue
            good.extend(pres.good)
            rejected.extend(pres.rejected)

        quarantine("floorsheet", rejected, cfg=cfg)
        written = upsert_floorsheet(con, good)
        now_held = _rows_held(con, bd)

        log.info(
            "floorsheet_done date=%s pages=%d parsed=%d held=%d/%d rejected=%d empty_pages=%s",
            bd, want_pages, written, now_held, total, len(rejected), empty_pages or "none",
        )
        return {
            "status": "ok",
            "business_date": bd,
            "pages_read": want_pages,
            "parsed": written,
            "held": now_held,
            "upstream_total": total,
            "rejected": len(rejected),
            "empty_pages": empty_pages,
            "complete": now_held >= total,
        }


def sweep_until_complete(
    session: NepseSession, *, attempts: int = 3, cfg: Config = CONFIG
) -> dict:
    """Full sweeps until held == upstream total. Guards against transient empty pages."""
    r: dict = {}
    for i in range(attempts):
        r = sweep(session, full=True, cfg=cfg)
        if r.get("status") != "ok":
            return r
        if r["complete"]:
            log.info("floorsheet_complete after %d full sweep(s)", i + 1)
            return r
        log.warning(
            "floorsheet_incomplete attempt=%d held=%d upstream=%d - resweeping",
            i + 1, r["held"], r["upstream_total"],
        )
    return r


def finalize(business_date: date, upstream_total: int | None = None, cfg: Config = CONFIG) -> None:
    """Session close: quality gate, then publish silver. Raises if the gate fails."""
    with connect(cfg) as con:
        rep = check_floorsheet(con, business_date, expected_total=upstream_total)
        print(rep.render())
        rep.raise_if_failed()
        out = export_parquet(con, "floorsheet", business_date, cfg=cfg)
        n = _rows_held(con, business_date)
        mark_backfill(con, business_date, "done", n, "floorsheet captured")
        log.info("floorsheet_finalized date=%s rows=%d parquet=%s", business_date, n, out)
