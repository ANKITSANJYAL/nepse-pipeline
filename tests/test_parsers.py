"""Parser tests run against real captured responses - no network, no mocks."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from nepse_pipeline.parsers import (
    SchemaDriftError,
    TransientEmptyPage,
    floorsheet_page_meta,
    parse_eod_page,
    parse_floorsheet_page,
    parse_index_series,
)

FIX = Path(__file__).parent / "fixtures"


def load(name: str):
    return json.loads((FIX / name).read_text())


def test_floorsheet_parses_real_page():
    res = parse_floorsheet_page(load("floorsheet_page.json"))
    assert len(res.good) == 25
    assert not res.rejected
    c = res.good[0]
    assert c.contract_quantity > 0
    assert c.contract_rate > 0
    assert abs(c.contract_rate * c.contract_quantity - c.contract_amount) <= Decimal("1.0")
    assert c.trade_time.date() == c.business_date


def test_floorsheet_meta():
    total, pages = floorsheet_page_meta(load("floorsheet_page.json"))
    assert total > 0 and pages > 0


def test_bare_list_is_transient_not_drift():
    # NEPSE intermittently answers a valid page with []. Must not be read as EOF.
    with pytest.raises(TransientEmptyPage):
        parse_floorsheet_page([])


def test_renamed_envelope_raises_drift():
    payload = load("floorsheet_page.json")
    payload["floorsheetsV2"] = payload.pop("floorsheets")
    with pytest.raises(SchemaDriftError):
        parse_floorsheet_page(payload)


def test_mass_row_failure_raises_drift():
    payload = load("floorsheet_page.json")
    for row in payload["floorsheets"]["content"]:
        row["contractQuantity"] = -1  # violates gt=0
    with pytest.raises(SchemaDriftError, match="failed validation"):
        parse_floorsheet_page(payload)


def test_single_bad_row_is_quarantined_not_fatal():
    payload = load("floorsheet_page.json")
    payload["floorsheets"]["content"][0]["contractAmount"] = 1.0  # breaks qty*rate
    res = parse_floorsheet_page(payload)
    assert len(res.rejected) == 1
    assert len(res.good) == 24
    assert "amount mismatch" in res.rejected[0].reason


def test_new_upstream_field_is_caught():
    payload = load("floorsheet_page.json")
    for row in payload["floorsheets"]["content"]:
        row["settlementId"] = 1
    with pytest.raises(SchemaDriftError):
        parse_floorsheet_page(payload)


def test_eod_parses_and_flags_debentures():
    res = parse_eod_page(load("eod_page.json"))
    assert res.good
    for q in res.good:
        if q.high_price is not None and q.low_price is not None:
            assert q.high_price >= q.low_price


def test_eod_close_outside_range_is_flagged_not_dropped():
    payload = load("eod_page.json")
    row = payload["content"][0]
    row["openPrice"] = row["highPrice"] = row["lowPrice"] = 100.0
    row["closePrice"] = 108.0
    res = parse_eod_page(payload)
    assert not res.rejected, "flagged rows must survive, not be quarantined"
    assert res.good[0].quality_flags == "close_outside_range"


def test_eod_high_below_low_is_rejected():
    payload = load("eod_page.json")
    payload["content"][0]["highPrice"] = 1.0
    payload["content"][0]["lowPrice"] = 99.0
    res = parse_eod_page(payload)
    assert len(res.rejected) == 1
    assert "high" in res.rejected[0].reason


def test_index_series():
    pts = parse_index_series([[1787115900, 2622.84], [1787115930, 2623.0]])
    assert len(pts) == 2 and pts[0][1] == 2622.84
    with pytest.raises(SchemaDriftError):
        parse_index_series([[1787115900]])


def test_bronze_name_flattens_slash_symbols():
    """Debenture symbols contain a path separator; GBILD84/85 must not become
    a directory. Eight symbols silently failed to persist before this."""
    from nepse_pipeline.jobs.deep_history import _bronze_name

    assert _bronze_name("GBILD84/85") == "GBILD84%2F85"
    assert "/" not in _bronze_name("nicad85/86")
    assert _bronze_name("nabil") == "NABIL"
