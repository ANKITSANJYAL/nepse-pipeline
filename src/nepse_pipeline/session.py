"""HTTP + auth against NEPSE.

The volatile part - the WASM-derived `Salter` token and the POST payload id -
is delegated to the `nepse` package, which executes NEPSE's own css.wasm rather
than hardcoding extracted constants. That is the one dependency that will break
when NEPSE rotates the module; it is deliberately isolated behind `_TokenSource`
so it can be swapped for a Playwright fallback without touching callers.
"""

from __future__ import annotations

import logging
import random
import threading
import time
from typing import Any, Callable

import httpx

from .config import CONFIG, BROWSER_UA, Config

log = logging.getLogger(__name__)

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class NepseError(RuntimeError):
    pass


class NepseAuthError(NepseError):
    pass


class _TokenSource:
    """Adapter over the `nepse` package. Owns nothing but token + payload ids."""

    def __init__(self) -> None:
        from nepse import Nepse  # imported lazily: heavy (pywasm) and optional

        self._n = Nepse()
        self._n.setTLSVerification(False)  # NEPSE's chain has been intermittently broken
        self._lock = threading.Lock()

    def auth_headers(self) -> dict[str, str]:
        with self._lock:
            return dict(self._n.getAuthorizationHeaders())

    def floorsheet_payload_id(self) -> int:
        with self._lock:
            return self._n.getPOSTPayloadIDForFloorSheet()

    def payload_id(self) -> int:
        with self._lock:
            return self._n.getPOSTPayloadID()

    def market_status_raw(self) -> dict[str, Any]:
        with self._lock:
            return self._n.getMarketStatus()

    def security_id_map(self) -> dict[str, int]:
        with self._lock:
            return self._n.getSecurityIDKeyMap()


class NepseSession:
    """Rate-limited, retrying client. Returns raw JSON - parsing lives elsewhere."""

    def __init__(self, cfg: Config = CONFIG) -> None:
        self.cfg = cfg
        self._tokens = _TokenSource()
        self._last_request_at = 0.0
        self._gap_lock = threading.Lock()
        self._client = httpx.Client(
            base_url=cfg.base_url,
            timeout=cfg.request_timeout_s,
            verify=False,
            follow_redirects=True,
            headers={
                "User-Agent": BROWSER_UA,
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": f"{cfg.base_url}/",
                "Cache-Control": "no-cache",
            },
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "NepseSession":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _throttle(self) -> None:
        with self._gap_lock:
            wait = self.cfg.min_request_gap_s - (time.monotonic() - self._last_request_at)
            if wait > 0:
                time.sleep(wait)
            self._last_request_at = time.monotonic()

    def request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        body_factory: Callable[[], dict[str, Any]] | None = None,
        params: dict[str, Any] | None = None,
        auth: bool = True,
    ) -> Any:
        """`body_factory` is re-evaluated on every attempt.

        The POST `id` is derived from the same salts that mint the access token,
        and the token lives 45s. A body computed once before the loop goes stale
        the moment the token rotates, so every retry 401s forever. Headers are
        refreshed first so the id is always computed against the live salts.
        """
        last_exc: Exception | None = None

        for attempt in range(self.cfg.max_retries):
            headers: dict[str, str] = {}
            if auth:
                headers.update(self._tokens.auth_headers())

            # Order matters: auth_headers() refreshes the token, and the id must
            # be derived from the salts that refresh produced.
            body = body_factory() if body_factory is not None else json_body
            if body is not None:
                headers["Content-Type"] = "application/json"

            self._throttle()
            t0 = time.perf_counter()
            try:
                r = self._client.request(method, path, headers=headers, json=body, params=params)
            except (httpx.TransportError, httpx.HTTPError) as exc:
                last_exc = exc
                log.warning("transport_error path=%s attempt=%d err=%s", path, attempt, exc)
                self._sleep_backoff(attempt)
                continue

            dt_ms = (time.perf_counter() - t0) * 1000
            log.info(
                "http method=%s path=%s status=%d bytes=%d ms=%.0f attempt=%d",
                method, path, r.status_code, len(r.content), dt_ms, attempt,
            )

            if r.status_code == 200:
                return r.json()

            # 401 = our 45s token aged out mid-flight. Refresh and retry immediately.
            if r.status_code == 401:
                last_exc = NepseAuthError(f"401 on {path}")
                self._sleep_backoff(attempt, cap=2.0)
                continue

            if r.status_code in RETRYABLE_STATUS:
                last_exc = NepseError(f"{r.status_code} on {path}")
                self._sleep_backoff(attempt)
                continue

            # Any other 4xx is a bug in our request, not a transient fault.
            # Retrying it just burns quota and hides the defect.
            raise NepseError(f"non-retryable {r.status_code} on {path}: {r.text[:200]}")

        raise NepseError(f"exhausted {self.cfg.max_retries} attempts on {path}") from last_exc

    def _sleep_backoff(self, attempt: int, cap: float = 60.0) -> None:
        delay = min(self.cfg.backoff_base_s * (2**attempt), cap)
        time.sleep(delay * (0.5 + random.random() / 2))  # jitter: avoid lockstep retries

    # ---- endpoint wrappers (raw JSON in, raw JSON out) ----

    def market_status(self) -> dict[str, Any]:
        return self._tokens.market_status_raw()

    # Server rejects size > 500 with a bare `[]`. Measured, not documented.
    MAX_PAGE_SIZE = 500

    def floorsheet_page(self, page: int, size: int | None = None, retries: int = 3) -> dict[str, Any]:
        size = min(size or self.cfg.page_size, self.MAX_PAGE_SIZE)
        for attempt in range(retries):
            payload = self.request(
                "POST",
                "/api/nots/nepse-data/floorsheet",
                params={"size": size, "page": page, "sort": "contractId,desc"},
                body_factory=lambda: {"id": self._tokens.floorsheet_payload_id()},
            )
            if isinstance(payload, dict):
                return payload
            log.warning("floorsheet_bare_list page=%d attempt=%d - retrying", page, attempt)
            self._sleep_backoff(attempt, cap=8.0)
        return payload

    def eod_prices(self, business_date: str, page: int = 0, size: int | None = None) -> dict[str, Any]:
        # today-price is keyed off the *floorsheet* payload id, not the scrips
        # one - they are computed differently and the wrong one yields 401.
        size = min(size or self.cfg.page_size, self.MAX_PAGE_SIZE)
        return self.request(
            "POST",
            "/api/nots/nepse-data/today-price",
            params={"size": size, "page": page, "businessDate": business_date},
            body_factory=lambda: {"id": self._tokens.floorsheet_payload_id()},
        )

    def index_intraday(self, index_id: int = 58) -> list[list[float]]:
        return self.request("GET", f"/api/nots/graph/index/{index_id}")

    def security_id_map(self) -> dict[str, int]:
        return self._tokens.security_id_map()
