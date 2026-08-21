"""Pure parsing. No I/O, no network - takes JSON, returns models.

Every parser returns (good, rejected). Bad rows are never dropped and never
let through: they go to quarantine with the reason attached.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pydantic import ValidationError

from .config import NPT
from .schemas import EodQuote, FloorsheetContract, MarketStatus

log = logging.getLogger(__name__)


class SchemaDriftError(RuntimeError):
    """Raised when the response shape itself changed - not just one bad row."""


@dataclass
class Rejected:
    raw: dict[str, Any]
    reason: str


@dataclass
class ParseResult:
    good: list[Any]
    rejected: list[Rejected]

    @property
    def total(self) -> int:
        return len(self.good) + len(self.rejected)

    def reject_rate(self) -> float:
        return len(self.rejected) / self.total if self.total else 0.0


def _parse_rows(rows: list[dict[str, Any]], model: type, ctx: str) -> ParseResult:
    good, rejected = [], []
    for raw in rows:
        try:
            good.append(model.model_validate(raw))
        except ValidationError as exc:
            rejected.append(Rejected(raw=raw, reason=str(exc)))

    res = ParseResult(good=good, rejected=rejected)

    # A handful of bad rows is bad data. Most rows failing is a schema change,
    # and writing the survivors would quietly corrupt the table.
    if res.total and res.reject_rate() > 0.10:
        sample = rejected[0].reason.splitlines()[:4] if rejected else []
        raise SchemaDriftError(
            f"{ctx}: {len(rejected)}/{res.total} rows failed validation "
            f"({res.reject_rate():.0%}). Sample: {' | '.join(sample)}"
        )
    return res


class TransientEmptyPage(RuntimeError):
    """NEPSE intermittently answers a valid page request with a bare `[]`.

    Observed mid-sweep on a page that returns 500 rows on retry, so it is a
    server hiccup, not the end of the result set. Treating it as the end would
    silently truncate the day's trades.
    """


def parse_floorsheet_page(payload: dict[str, Any]) -> ParseResult:
    if isinstance(payload, list):
        raise TransientEmptyPage(f"bare list of len {len(payload)}")
    if not isinstance(payload, dict) or "floorsheets" not in payload:
        raise SchemaDriftError(
            f"floorsheet: expected 'floorsheets' key, got {list(payload)[:8] if isinstance(payload, dict) else type(payload)}"
        )
    content = payload["floorsheets"].get("content")
    if content is None:
        raise SchemaDriftError("floorsheet: missing floorsheets.content")
    return _parse_rows(content, FloorsheetContract, "floorsheet")


def floorsheet_page_meta(payload: dict[str, Any]) -> tuple[int, int]:
    fl = payload["floorsheets"]
    return int(fl.get("totalElements", 0)), int(fl.get("totalPages", 0))


def parse_eod_page(payload: dict[str, Any]) -> ParseResult:
    if not isinstance(payload, dict) or "content" not in payload:
        raise SchemaDriftError(
            f"eod: expected 'content' key, got {list(payload)[:8] if isinstance(payload, dict) else type(payload)}"
        )
    return _parse_rows(payload["content"], EodQuote, "eod")


def parse_market_status(payload: dict[str, Any]) -> MarketStatus:
    return MarketStatus.model_validate(payload)


def parse_index_series(payload: list[list[float]]) -> list[tuple[datetime, float]]:
    if not isinstance(payload, list):
        raise SchemaDriftError(f"index: expected list, got {type(payload)}")
    out = []
    for pt in payload:
        if not (isinstance(pt, (list, tuple)) and len(pt) >= 2):
            raise SchemaDriftError(f"index: malformed point {pt!r}")
        out.append((datetime.fromtimestamp(pt[0], NPT).replace(tzinfo=None), float(pt[1])))
    return out
