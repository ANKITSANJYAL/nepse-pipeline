"""Command line entry point. Every job is small, independently runnable, and retryable."""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date, datetime

from .config import CONFIG, NPT
from .jobs import eod as eod_job
from .jobs import floorsheet as fs_job
from .jobs import gold as gold_job
from .jobs import reprocess as reprocess_job
from .jobs import deep_history as deep_job
from .parsers import parse_index_series, parse_market_status
from .quality import freshness
from .session import NepseSession
from .storage import connect, upsert_index

log = logging.getLogger("nepse")


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
        stream=sys.stdout,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)


def cmd_status(_: argparse.Namespace) -> int:
    with NepseSession() as s:
        st = parse_market_status(s.market_status())
        print(f"market   : {st.is_open}  asOf={st.as_of}  id={st.id}")
        print(f"npt now  : {datetime.now(NPT):%Y-%m-%d %H:%M:%S %a}")
    with connect(read_only=False) as con:
        print(f"freshness: {freshness(con)}")
        for t in ("floorsheet", "eod_price", "index_intraday"):
            n = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
            d = con.execute(f"SELECT min(business_date), max(business_date) FROM {t}").fetchone() \
                if t != "index_intraday" else (None, None)
            print(f"  {t:15} rows={n:>9,}  range={d[0]}..{d[1]}")
    return 0


def cmd_floorsheet(args: argparse.Namespace) -> int:
    with NepseSession() as s:
        r = fs_job.sweep_until_complete(s) if args.full else fs_job.sweep(s)
        print(r)
        if args.finalize and r.get("business_date"):
            fs_job.finalize(r["business_date"], r.get("upstream_total"))
    return 0


def cmd_eod(args: argparse.Namespace) -> int:
    d = date.fromisoformat(args.date) if args.date else datetime.now(NPT).date()
    with NepseSession() as s:
        print(eod_job.fetch_day(s, d))
    if args.finalize:
        eod_job.finalize(d)
    return 0


def cmd_backfill(args: argparse.Namespace) -> int:
    with NepseSession() as s:
        print(eod_job.backfill(s, days=args.days, resume=not args.no_resume))
    return 0


def cmd_gold(args: argparse.Namespace) -> int:
    d = date.fromisoformat(args.date) if args.date else datetime.now(NPT).date()
    print(gold_job.build(d))
    return 0


def cmd_reprocess(args: argparse.Namespace) -> int:
    d = date.fromisoformat(args.date) if args.date else None
    r = reprocess_job.replay(d)
    print({k: v for k, v in r.items() if k != "dates"})
    print(f"dates rebuilt: {len(r['dates'])}")
    return 0


def cmd_deep(args: argparse.Namespace) -> int:
    if args.phase in ("fetch", "all"):
        r = deep_job.fetch_all(resume=not args.no_resume, include_listed=not args.nepse_only)
        print(r["counts"])
        for sym, why in r["failures"][:15]:
            print(f"  FAIL {sym}: {why}")
    if args.phase in ("load", "all"):
        print(deep_job.load())
    if args.phase in ("reconcile", "all"):
        for k, v in deep_job.reconcile().items():
            print(f"  {k:22} {v}")
    return 0


def cmd_index(_: argparse.Namespace) -> int:
    with NepseSession() as s:
        raw = s.index_intraday(58)
        pts = parse_index_series(raw)
    with connect() as con:
        n = upsert_index(con, 58, pts)
    print(f"index points upserted: {n}  span={pts[0][0]}..{pts[-1][0]}" if pts else "no points")
    return 0


def _capture_index(s: NepseSession, index_id: int = 58) -> None:
    """Idempotent: the endpoint returns the whole day each call, upserted by ts."""
    try:
        pts = parse_index_series(s.index_intraday(index_id))
        if pts:
            with connect() as con:
                upsert_index(con, index_id, pts)
            log.info("index_captured points=%d span=%s..%s", len(pts), pts[0][0], pts[-1][0])
    except Exception:
        log.exception("index_capture_failed")


def cmd_run(args: argparse.Namespace) -> int:
    """Live session loop. Polls only while NEPSE reports OPEN, then finalizes once."""
    cfg = CONFIG
    finalized_for: date | None = None

    with NepseSession() as s:
        while True:
            try:
                st = parse_market_status(s.market_status())
                today = datetime.now(NPT).date()

                if st.open:
                    r = fs_job.sweep(s)
                    log.info("poll parsed=%s held=%s total=%s",
                             r.get("parsed"), r.get("held"), r.get("upstream_total"))
                    _capture_index(s)
                    finalized_for = None
                else:
                    # One post-close sweep catches the tail of the session, then
                    # the day is sealed: gate, parquet, done.
                    if finalized_for != today:
                        log.info("market closed - final sweep + finalize")
                        # The intraday index series is wiped a few hours after
                        # close (610 points at 19:30 NPT, zero by 20:25), so it
                        # must be grabbed before sealing the day.
                        _capture_index(s)
                        r = fs_job.sweep_until_complete(s)
                        bd = r.get("business_date")
                        if bd:
                            try:
                                fs_job.finalize(bd, r.get("upstream_total"))
                            except Exception:
                                log.exception("finalize_failed date=%s", bd)
                            try:
                                eod_job.fetch_day(s, bd)
                                eod_job.finalize(bd)
                            except Exception:
                                log.exception("eod_finalize_failed date=%s", bd)
                            try:
                                gold_job.build(bd)
                            except Exception:
                                log.exception("gold_build_failed date=%s", bd)
                            finalized_for = today
                        else:
                            # Sealing on an empty sweep is how a whole session
                            # gets lost: the loop boots pre-open (06:28 NPT is
                            # still "today" in NPT), finds nothing, marks the day
                            # done, and then never sweeps again - even if the
                            # host was asleep through the entire session and
                            # NEPSE is still serving it hours after close.
                            # Leave the day unsealed and retry.
                            log.warning("post-close sweep empty date=%s - "
                                        "not sealing, will retry", today)
                    if args.once:
                        return 0
                    time.sleep(min(300.0, cfg.poll_interval_s * 15))
                    continue

                if args.once:
                    return 0
                time.sleep(cfg.poll_interval_s)

            except KeyboardInterrupt:
                log.info("interrupted - watermark is durable, safe to restart")
                return 0
            except Exception:
                # A poller that dies at 11:04 loses the whole session, and the
                # floorsheet cannot be refetched. Log and keep going.
                log.exception("poll_iteration_failed")
                time.sleep(cfg.poll_interval_s * 2)
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="nepse", description="NEPSE ingestion pipeline")
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("status", help="market + warehouse state").set_defaults(fn=cmd_status)

    f = sub.add_parser("floorsheet", help="sweep floorsheet once")
    f.add_argument("--full", action="store_true", help="ignore watermark, re-read all pages")
    f.add_argument("--finalize", action="store_true")
    f.set_defaults(fn=cmd_floorsheet)

    e = sub.add_parser("eod", help="fetch one day's EOD board")
    e.add_argument("--date")
    e.add_argument("--finalize", action="store_true")
    e.set_defaults(fn=cmd_eod)

    b = sub.add_parser("backfill", help="walk back through the 365-day window")
    b.add_argument("--days", type=int, default=None)
    b.add_argument("--no-resume", action="store_true")
    b.set_defaults(fn=cmd_backfill)

    g = sub.add_parser("gold", help="build bars/broker-flow/daily stats")
    g.add_argument("--date")
    g.set_defaults(fn=cmd_gold)

    rp = sub.add_parser("reprocess", help="rebuild silver from bronze, no network")
    rp.add_argument("--date")
    rp.set_defaults(fn=cmd_reprocess)

    dh = sub.add_parser("deep-history", help="ShareSansar daily history back to 2011")
    dh.add_argument("phase", choices=["fetch", "load", "reconcile", "all"], default="all", nargs="?")
    dh.add_argument("--no-resume", action="store_true")
    dh.add_argument("--nepse-only", action="store_true",
                    help="skip symbols NEPSE has not traded in 365 days")
    dh.set_defaults(fn=cmd_deep)

    sub.add_parser("index", help="capture intraday index series").set_defaults(fn=cmd_index)

    r = sub.add_parser("run", help="live poll loop (use this under launchd)")
    r.add_argument("--once", action="store_true")
    r.set_defaults(fn=cmd_run)

    args = p.parse_args(argv)
    _setup_logging(args.verbose)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
