"""Rebuild silver from bronze without touching the network.

This is the whole reason bronze exists. When a parser turns out to be wrong -
and it will - the fix is to correct the parser and replay, not to refetch. For
floorsheet, refetching is not even possible: NEPSE serves only the current
session, so bronze is the sole surviving copy of every past day's ticks.
"""

from __future__ import annotations

import logging
from datetime import date

from .. import bronze
from ..config import CONFIG, Config
from ..parsers import TransientEmptyPage, parse_eod_page, parse_floorsheet_page
from ..quality import quarantine
from ..storage import connect, export_parquet, upsert_eod, upsert_floorsheet

log = logging.getLogger(__name__)


def replay(business_date: date | None = None, cfg: Config = CONFIG) -> dict:
    counts = {"floorsheet_files": 0, "floorsheet_rows": 0, "eod_files": 0, "eod_rows": 0,
              "skipped_empty": 0, "rejected": 0}
    dates_seen: set[date] = set()

    with connect(cfg) as con:
        for path, env in bronze.iter_raw("floorsheet", business_date, cfg):
            counts["floorsheet_files"] += 1
            try:
                res = parse_floorsheet_page(env["payload"])
            except TransientEmptyPage:
                counts["skipped_empty"] += 1
                continue
            quarantine("floorsheet_replay", res.rejected, cfg=cfg)
            counts["rejected"] += len(res.rejected)
            counts["floorsheet_rows"] += upsert_floorsheet(con, res.good)
            dates_seen.update(r.business_date for r in res.good)

        for path, env in bronze.iter_raw("eod_price", business_date, cfg):
            counts["eod_files"] += 1
            res = parse_eod_page(env["payload"])
            quarantine("eod_replay", res.rejected, cfg=cfg)
            counts["rejected"] += len(res.rejected)
            counts["eod_rows"] += upsert_eod(con, res.good)
            dates_seen.update(r.business_date for r in res.good)

        for d in sorted(dates_seen):
            export_parquet(con, "floorsheet", d, cfg=cfg)
            export_parquet(con, "eod_price", d, cfg=cfg)

    log.info("replay_done dates=%d %s", len(dates_seen), counts)
    return {**counts, "dates": sorted(dates_seen)}
