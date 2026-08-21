"""ShareSansar: daily OHLCV going back to 1995, far beyond NEPSE's 365-day floor.

NEPSE clamps every history endpoint to a rolling year. ShareSansar publishes the
same series back to 1995-07-20 (measured across 470 symbols on 2026-08-19), which
is the only practical route to a multi-year history.

Their robots.txt is `User-agent: * / Disallow:` - an empty Disallow, permitting
everything. (NepseAlpha by contrast explicitly disallows /api/* and /rest/v1/*,
so it is deliberately not used here.)

This data is SECOND-SOURCE and is stored tagged with its origin, never merged
blindly into the NEPSE tables. The 225-day overlap where both exist is used to
measure agreement before the older, unverifiable portion is trusted.
"""

from __future__ import annotations

import logging
import random
import re
import time
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation

import httpx

from ..config import BROWSER_UA

log = logging.getLogger(__name__)

BASE = "https://www.sharesansar.com"
SOURCE = "sharesansar"

_TOKEN_RE = re.compile(r'name="_token"\s+content="([^"]+)"')
_LIST_SYM_RE = re.compile(r">([^<]+)</a>")

# company-list returns an empty DataTable unless a sector is named, so the
# universe has to be assembled sector by sector. IDs are not contiguous.
SECTOR_IDS = (1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 15, 16, 17, 18, 19, 20)
_CID_RE = re.compile(r'id="companyid"[^>]*>(\d+)<')


class ShareSansarError(RuntimeError):
    pass


@dataclass(frozen=True)
class HistoryRow:
    source: str
    symbol: str
    business_date: date
    open_price: Decimal | None
    high_price: Decimal | None
    low_price: Decimal | None
    close_price: Decimal | None
    percent_change: Decimal | None
    traded_quantity: int | None
    traded_amount: Decimal | None


def _dec(v) -> Decimal | None:
    if v in (None, "", "-", "N/A"):
        return None
    try:
        return Decimal(str(v).replace(",", "").strip())
    except (InvalidOperation, ValueError):
        return None


def parse_history_payload(symbol: str, payload: dict) -> tuple[list[HistoryRow], list[dict]]:
    """Pure: DataTables JSON -> rows. Returns (good, rejected)."""
    if not isinstance(payload, dict) or "data" not in payload:
        raise ShareSansarError(f"{symbol}: unexpected payload keys {list(payload)[:6]}")

    good: list[HistoryRow] = []
    bad: list[dict] = []
    for raw in payload["data"]:
        try:
            d = date.fromisoformat(raw["published_date"])
            o, h, l, c = (_dec(raw.get(k)) for k in ("open", "high", "low", "close"))
            if h is not None and l is not None and h < l:
                raise ValueError(f"high {h} < low {l}")
            qty = raw.get("traded_quantity")
            good.append(
                HistoryRow(
                    source=SOURCE,
                    symbol=symbol.upper(),
                    business_date=d,
                    open_price=o, high_price=h, low_price=l, close_price=c,
                    percent_change=_dec(raw.get("per_change")),
                    traded_quantity=int(float(str(qty).replace(",", ""))) if qty not in (None, "") else None,
                    traded_amount=_dec(raw.get("traded_amount")),
                )
            )
        except Exception as exc:  # noqa: BLE001 - row-level, recorded not raised
            bad.append({"raw": raw, "reason": f"{type(exc).__name__}: {exc}"})

    if payload["data"] and len(bad) / len(payload["data"]) > 0.10:
        raise ShareSansarError(
            f"{symbol}: {len(bad)}/{len(payload['data'])} rows failed - schema likely changed"
        )
    return good, bad


class ShareSansarClient:
    def __init__(self, min_gap_s: float = 1.0, timeout_s: float = 60.0) -> None:
        self.min_gap = min_gap_s
        self._last = 0.0
        self._client = httpx.Client(
            headers={
                "User-Agent": BROWSER_UA,
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=timeout_s,
            follow_redirects=True,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "ShareSansarClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _throttle(self) -> None:
        wait = self.min_gap - (time.monotonic() - self._last)
        if wait > 0:
            time.sleep(wait)
        self._last = time.monotonic()

    def _get(self, url: str, retries: int = 4, **kw) -> httpx.Response:
        for attempt in range(retries):
            self._throttle()
            try:
                r = self._client.get(url, **kw)
                if r.status_code == 200:
                    return r
                if r.status_code == 404:
                    raise ShareSansarError(f"404 {url}")
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(min(2**attempt, 30) * (0.5 + random.random() / 2))
                    continue
                raise ShareSansarError(f"{r.status_code} {url}")
            except httpx.HTTPError as exc:
                if attempt == retries - 1:
                    raise ShareSansarError(f"transport {url}: {exc}") from exc
                time.sleep(min(2**attempt, 30))
        raise ShareSansarError(f"exhausted retries {url}")

    def company_context(self, symbol: str) -> tuple[str, str]:
        """Returns (company_id, csrf_token). Slug is the lowercased symbol."""
        slug = symbol.lower()
        r = self._get(f"{BASE}/company/{slug}")
        tok = _TOKEN_RE.search(r.text)
        cid = _CID_RE.search(r.text)
        if not tok or not cid:
            raise ShareSansarError(f"{symbol}: could not locate token/companyid on /company/{slug}")
        return cid.group(1), tok.group(1)

    def price_history(self, symbol: str, length: int = -1, retries: int = 4) -> dict:
        """length=-1 is the DataTables "all rows" convention and returns the
        complete history in one request (~730 KB / 3,515 rows for NABIL).
        Intermediate values (500..10000) are rejected with a 202 + empty data,
        so do not "chunk" this - it silently yields nothing."""
        cid, tok = self.company_context(symbol)
        for attempt in range(retries):
            self._throttle()
            try:
                r = self._client.post(
                    f"{BASE}/company-price-history",
                    headers={
                        "X-CSRF-Token": tok,
                        "X-Requested-With": "XMLHttpRequest",
                        "Referer": f"{BASE}/company/{symbol.lower()}",
                    },
                    data={"company": cid, "draw": 1, "start": 0, "length": length},
                )
            except httpx.HTTPError as exc:
                # A read timeout or reset here used to lose the symbol outright:
                # the GET path retried, the POST path did not. Measured 3 of 183.
                if attempt == retries - 1:
                    raise ShareSansarError(f"{symbol}: transport {exc}") from exc
                time.sleep(min(2**attempt, 30) * (0.5 + random.random() / 2))
                continue

            if r.status_code == 202:
                # Deterministic server-side guard on the page size, not a fault.
                raise ShareSansarError(
                    f"{symbol}: 202 guard hit (length={length}); use length=-1"
                )
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504) and attempt < retries - 1:
                time.sleep(min(2**attempt, 30) * (0.5 + random.random() / 2))
                continue
            raise ShareSansarError(f"{symbol}: history {r.status_code}")
        raise ShareSansarError(f"{symbol}: exhausted retries")

    def listed_symbols(self) -> list[str]:
        """Every symbol ShareSansar lists, across all 17 sectors.

        NEPSE's own `eod_price` only knows symbols that traded in the last 365
        days, so this is how suspended and illiquid names stay in the archive.
        """
        out: set[str] = set()
        for sid in SECTOR_IDS:
            r = self._get(
                f"{BASE}/company-list",
                params={"draw": 1, "start": 0, "length": -1, "sector": sid},
                headers={"X-Requested-With": "XMLHttpRequest",
                         "Referer": f"{BASE}/company-list"},
            )
            rows = r.json().get("data", [])
            if not rows:
                log.warning("sharesansar_sector_empty sector=%s", sid)
            for row in rows:
                m = _LIST_SYM_RE.search(row.get("symbol", ""))
                if m:
                    out.add(m.group(1).strip().upper())
            log.info("sharesansar_sector sector=%s rows=%d cumulative=%d", sid, len(rows), len(out))

        # 651 measured on 2026-08-19. A collapse to a handful means the markup or
        # the sector IDs moved; silently fetching a short universe would look fine.
        if len(out) < 400:
            raise ShareSansarError(
                f"company-list yielded only {len(out)} symbols - layout likely changed"
            )
        return sorted(out)
