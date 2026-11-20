"""Regression tests for the 2026-08-25 outage and the gaps it exposed. No network."""

from __future__ import annotations

import io
import tarfile
from datetime import date, datetime, timedelta

import pytest

from nepse_pipeline.config import NPT, Config
from nepse_pipeline.jobs import eod as eod_job
from nepse_pipeline.jobs import sync as sync_job
from nepse_pipeline.session import (
    NepseDateOutOfRange,
    NepseHostError,
    classify_failure,
)
from nepse_pipeline.storage import (
    connect,
    export_index_intraday,
    export_parquet,
    upsert_index,
)

# Captured verbatim from www.nepalstock.com.np on 2026-11-20.
NGINX_404 = "<html>\r\n<head><title>404 Not Found</title></head>\r\n<body>\r\n<center><h1>404 Not Found</h1></center>"


@pytest.fixture
def cfg(tmp_path) -> Config:
    return Config(data_root=tmp_path)


# ---- response classification -------------------------------------------

def test_html_404_on_api_route_is_host_error():
    err = classify_failure(404, "text/html", NGINX_404, "https://x", "/api/nots/nepse-data/floorsheet")
    assert isinstance(err, NepseHostError)
    assert "NEPSE_BASE_URL" in str(err)


def test_out_of_window_date_is_its_own_signal():
    # Measured 2026-11-20 for businessDate=2025-11-10.
    err = classify_failure(500, "application/json", "Searched Date is not valid.", "https://x",
                           "/api/nots/nepse-data/today-price")
    assert isinstance(err, NepseDateOutOfRange)


@pytest.mark.parametrize("status,ctype,body", [
    (500, "application/json", '{"error":"boom"}'),   # ordinary 5xx: retry policy
    (404, "application/json", '{"message":"nope"}'),  # API's own 404: not a host fault
    (401, "text/html", "<html>"),                      # token aged out: retry policy
])
def test_other_failures_fall_through_to_retry_policy(status, ctype, body):
    assert classify_failure(status, ctype, body, "https://x", "/api/nots/a") is None


# ---- EOD backfill must not mistake a dead host for the history floor ------

class _FakeSession:
    def __init__(self, exc: Exception):
        self.exc = exc
        self.calls = 0

    def eod_prices(self, *_a, **_k):
        self.calls += 1
        raise self.exc


def test_out_of_window_maps_to_unavailable(cfg):
    r = eod_job.fetch_day(_FakeSession(NepseDateOutOfRange("x", 500)), date(2025, 1, 1), cfg=cfg)
    assert r["status"] == "unavailable"


def test_dead_host_aborts_backfill_instead_of_marking_days(cfg):
    s = _FakeSession(NepseHostError("dead", 404))
    with pytest.raises(NepseHostError):
        eod_job.backfill(s, days=10, end=date(2026, 11, 19), cfg=cfg)
    assert s.calls == 1, "must stop at the first day, not record ten 'failed' rows"
    with connect(cfg) as con:
        assert con.execute("SELECT count(*) FROM backfill_state").fetchone()[0] == 0


class _ByDateSession:
    """Out-of-range for `bad` dates, an empty (holiday) board otherwise."""

    def __init__(self, bad: set[date]):
        self.bad = bad
        self.seen: list[str] = []

    def eod_prices(self, iso, page=0, **_k):
        self.seen.append(iso)
        if date.fromisoformat(iso) in self.bad:
            raise NepseDateOutOfRange("x", 500)
        return {"content": [], "totalElements": 0, "last": True}


def test_unpublished_today_is_skipped_not_recorded_as_floor(cfg):
    # Measured 2026-11-20 09:16 NPT: today's date, pre-open, answers
    # "Searched Date is not valid" - the same answer as past the floor.
    end = date(2026, 11, 19)  # Thursday
    s = _ByDateSession({end})
    r = eod_job.backfill(s, days=3, end=end, cfg=cfg)
    assert r["counts"]["not_yet_published"] == 1
    assert len(s.seen) == 4, "must continue past today instead of stopping"
    with connect(cfg) as con:
        assert con.execute("SELECT count(*) FROM backfill_state WHERE business_date = ?",
                           [end]).fetchone()[0] == 0


def test_out_of_range_near_floor_stops_backfill(cfg):
    end = date(2026, 11, 19)
    floor_day = end - timedelta(days=364)
    s = _ByDateSession({floor_day})
    r = eod_job.backfill(s, days=365, end=end, cfg=cfg)
    assert r["counts"]["unavailable"] == 1
    assert s.seen[-1] == floor_day.isoformat()


# ---- index_intraday is exported and survives a rebuild from Parquet -------

def test_index_export_roundtrips_through_hydrate(cfg):
    d = date(2026, 11, 19)
    pts = [(datetime(2026, 11, 19, 11, 0, 0) + timedelta(seconds=24 * i), 2600.0 + i) for i in range(5)]
    with connect(cfg) as con:
        upsert_index(con, 58, pts)
        out = export_index_intraday(con, d, cfg=cfg)
        assert export_index_intraday(con, date(2026, 11, 18), cfg=cfg) is None
    assert out is not None and out.exists()

    counts = sync_job.hydrate(cfg)  # drops the DB and rebuilds from Parquet only
    assert counts["index_intraday"] == 5
    with connect(cfg) as con:
        assert con.execute("SELECT max(value) FROM index_intraday").fetchone()[0] == 2604


def test_empty_day_leaves_no_partition_directory(cfg):
    with connect(cfg) as con:
        assert export_parquet(con, "floorsheet", date(2025, 8, 19), cfg=cfg) is None
    assert not (cfg.silver / "floorsheet" / "business_date=2025-08-19").exists()


# ---- bronze bundling ------------------------------------------------------

def test_bronze_bundles_one_tar_per_dataset_day_within_window(cfg):
    today = datetime.now(NPT).date()
    fresh = cfg.bronze / "floorsheet" / f"date={today.isoformat()}"
    stale = cfg.bronze / "floorsheet" / f"date={(today - timedelta(days=30)).isoformat()}"
    for p in (fresh, stale):
        p.mkdir(parents=True)
        for i in range(3):
            (p / f"{1000 + i}-p{i}.json.gz").write_bytes(b"x")

    bundles = sync_job.bundle_bronze(cfg)
    assert len(bundles) == 1, "stale partitions must not be re-uploaded every run"
    path, blob = bundles[0]
    assert path.startswith(f"floorsheet/date={today.isoformat()}/bundle-") and path.endswith(".tar")
    with tarfile.open(fileobj=io.BytesIO(blob)) as tar:
        assert sorted(tar.getnames()) == ["1000-p0.json.gz", "1001-p1.json.gz", "1002-p2.json.gz"]


# ---- the actual root cause: library headers leaking into our requests -----

def test_only_authorization_is_taken_from_the_token_library():
    """The library's header set carries `Host: www.nepalstock.com`; sent to the
    .com.np server, that is an nginx 404 (the 2026-08-25 outage)."""
    import threading

    from nepse_pipeline.session import _TokenSource

    class _Lib:
        def getAuthorizationHeaders(self):
            return {"Authorization": "Salter t", "Host": "www.nepalstock.com",
                    "User-Agent": "Firefox/89.0", "Referer": "www.nepalstock.com"}

    ts = object.__new__(_TokenSource)
    ts._n, ts._lock = _Lib(), threading.Lock()
    assert ts.auth_headers() == {"Authorization": "Salter t"}
