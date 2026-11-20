"""Configuration. Everything tunable lives here or in the environment."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import timedelta, timezone
from pathlib import Path

NPT = timezone(timedelta(hours=5, minutes=45))

# NEPSE hangs forever without a browser UA. Not a 403 - a silent stall.
BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)


def _env_path(key: str, default: str) -> Path:
    return Path(os.environ.get(key, default)).expanduser().resolve()


@dataclass(frozen=True)
class Config:
    # Both `.com` and `.com.np` serve the API (separate servers). `.com` is the
    # `nepse` library's own default, so token and data share a host unless
    # overridden - and the session pins the library to whatever is set here.
    # See session._TokenSource.auth_headers for the 2026-08-25 outage.
    base_url: str = os.environ.get("NEPSE_BASE_URL", "https://www.nepalstock.com")

    data_root: Path = field(default_factory=lambda: _env_path("NEPSE_DATA_ROOT", "./data"))

    # Backend refreshes roughly every 24s (measured: 610 index points per 4h session).
    # Polling faster than that buys nothing but ban risk.
    poll_interval_s: float = float(os.environ.get("NEPSE_POLL_INTERVAL", "20"))
    request_timeout_s: float = float(os.environ.get("NEPSE_TIMEOUT", "45"))
    max_retries: int = int(os.environ.get("NEPSE_MAX_RETRIES", "5"))
    backoff_base_s: float = float(os.environ.get("NEPSE_BACKOFF_BASE", "1.5"))
    min_request_gap_s: float = float(os.environ.get("NEPSE_MIN_GAP", "0.7"))

    page_size: int = int(os.environ.get("NEPSE_PAGE_SIZE", "500"))

    # NEPSE clamps every history endpoint to a rolling 365 days. Measured, not assumed.
    history_window_days: int = 365

    @property
    def bronze(self) -> Path:
        return self.data_root / "bronze"

    @property
    def silver(self) -> Path:
        return self.data_root / "silver"

    @property
    def gold(self) -> Path:
        return self.data_root / "gold"

    @property
    def quarantine(self) -> Path:
        return self.data_root / "quarantine"

    @property
    def db_path(self) -> Path:
        return self.data_root / "nepse.duckdb"

    def ensure_dirs(self) -> None:
        for p in (self.bronze, self.silver, self.gold, self.quarantine):
            p.mkdir(parents=True, exist_ok=True)


CONFIG = Config()
