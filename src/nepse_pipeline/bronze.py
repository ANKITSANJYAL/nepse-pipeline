"""Bronze layer: raw responses to disk, before anything touches them.

Non-negotiable because floorsheet history does not exist upstream. NEPSE serves
only the current session and ignores businessDate on that endpoint - a trade not
captured on the day is gone permanently. Parsing can be redone; fetching cannot.
"""

from __future__ import annotations

import gzip
import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any

from .config import CONFIG, NPT, Config

log = logging.getLogger(__name__)


def write_raw(
    dataset: str,
    payload: Any,
    *,
    business_date: date | None = None,
    suffix: str = "",
    cfg: Config = CONFIG,
) -> Path:
    """Persist one raw response. Returns the path written."""
    now = datetime.now(NPT)
    part = (business_date or now.date()).isoformat()
    out_dir = cfg.bronze / dataset / f"date={part}"
    out_dir.mkdir(parents=True, exist_ok=True)

    name = f"{int(now.timestamp() * 1000)}{('-' + suffix) if suffix else ''}.json.gz"
    path = out_dir / name

    envelope = {
        "dataset": dataset,
        "fetched_at": now.isoformat(),
        "business_date": part,
        "payload": payload,
    }
    # Written atomically: a crash mid-write must not leave a half-file that
    # later reprocessing would silently treat as complete.
    tmp = path.with_suffix(".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as fh:
        json.dump(envelope, fh, separators=(",", ":"), default=str)
    tmp.replace(path)

    log.debug("bronze_write dataset=%s path=%s", dataset, path)
    return path


def read_raw(path: Path) -> dict[str, Any]:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return json.load(fh)


def iter_raw(dataset: str, business_date: date | None = None, cfg: Config = CONFIG):
    """Replay bronze for reprocessing. Sorted by filename == fetch order."""
    root = cfg.bronze / dataset
    globs = f"date={business_date.isoformat()}/*.json.gz" if business_date else "date=*/*.json.gz"
    for path in sorted(root.glob(globs)):
        yield path, read_raw(path)
