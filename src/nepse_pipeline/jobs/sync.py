"""Move the warehouse between a Hugging Face dataset repo and local disk.

The inversion that makes hosted capture possible: **Parquet in the dataset repo
is the source of truth, and the DuckDB file is a derived artifact** rebuilt from
it on demand. Nothing durable then lives on the machine that ran the job, so the
pipeline can run on an ephemeral CI runner that is destroyed 15 minutes later.

Only NEPSE-sourced tables are published. `eod_history_ext` is deliberately
excluded: it is ShareSansar's compiled series, and republishing someone else's
compilation is a heavier act than republishing the exchange's own prints.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path

from ..config import CONFIG, Config
from ..storage import connect

log = logging.getLogger(__name__)

# A partition this recent is re-uploaded even if the repo already has it,
# because re-finalizing a session rewrites its Parquet.
REWRITE_WINDOW_DAYS = 5

# (table, layer) - layer is the directory under data/ the partitions live in.
PUBLISHED: tuple[tuple[str, str], ...] = (
    ("floorsheet", "silver"),
    ("eod_price", "silver"),
    ("bars_1m", "gold"),
    ("broker_flow_daily", "gold"),
    ("daily_stats", "gold"),
)

DATASET_CARD = """---
license: other
language: [en]
tags: [finance, stock-market, nepal, nepse, order-flow, ohlcv]
pretty_name: NEPSE Market Data
---

# NEPSE Market Data

Daily and tick-level data from the Nepal Stock Exchange, captured by an
open-source pipeline and validated at every layer boundary.

## Tables

| Path | Grain | Notes |
|---|---|---|
| `floorsheet/` | one executed contract | microsecond `trade_time`, **both** counterparty broker IDs |
| `eod_price/` | symbol x day | the official end-of-day board |
| `bars_1m/` | symbol x minute | derived from floorsheet, reconciles to it exactly |
| `broker_flow_daily/` | symbol x broker x day | signed participation per broker |
| `daily_stats/` | symbol x day | traded VWAP and session boundaries |

All are Parquet, partitioned by `business_date`.

## Load it

```python
import polars as pl
df = pl.read_parquet("hf://datasets/{repo_id}/floorsheet/**/*.parquet")
```

```python
import duckdb
duckdb.sql("SELECT * FROM 'hf://datasets/{repo_id}/eod_price/**/*.parquet' LIMIT 5")
```

## What to know before modelling

- **Corporate actions are not adjusted.** Bonus shares and rights issues are
  common on NEPSE and leave artificial gaps in any raw price series.
- **Tick history starts when the pipeline did.** NEPSE's floorsheet endpoint
  serves only the current session and ignores the date requested, so tick data
  cannot be backfilled by anyone, ever. It accumulates one session at a time.
- **The trading week changed.** NEPSE traded Sunday-Thursday until roughly
  2026-04-06, then Monday-Friday. A naive weekday filter drops real sessions.
- Every published session passed a completeness gate against upstream's own
  row count, plus uniqueness, null, and `qty * rate = amount` checks.

## Provenance

Captured from NEPSE's public web API. NEPSE publishes this data free to any
browser; this dataset is provided for research and educational use. Commercial
redistribution may require a licence from NEPSE.

Pipeline source: {source_url}
"""


def _partitions(cfg: Config) -> list[tuple[str, str, Path]]:
    out = []
    for table, layer in PUBLISHED:
        root = (cfg.data_root / layer / table)
        if root.exists():
            for part in sorted(root.glob("business_date=*")):
                out.append((table, part.name, part))
    return out


def hydrate(cfg: Config = CONFIG, *, rebuild: bool = True) -> dict:
    """Rebuild the DuckDB warehouse from the Parquet on disk.

    Idempotent by construction: the DB is dropped and refilled, so a corrupt or
    half-written DB from a killed job never survives into the next run.
    """
    if rebuild and cfg.db_path.exists():
        cfg.db_path.unlink()
        wal = cfg.db_path.with_suffix(cfg.db_path.suffix + ".wal")
        if wal.exists():
            wal.unlink()

    counts: dict[str, int] = {}
    with connect(cfg) as con:  # connect() applies SCHEMA_SQL
        # Gold DDL lives with the gold job, not in the core schema.
        from .gold import GOLD_SCHEMA

        con.execute(GOLD_SCHEMA)
        for table, layer in PUBLISHED:
            root = cfg.data_root / layer / table
            glob = str(root / "business_date=*" / "*.parquet")
            if not list(root.glob("business_date=*/*.parquet")):
                counts[table] = 0
                continue
            # Column order is fixed by export (SELECT * FROM table), so a
            # positional insert is safe and avoids naming 15 columns here.
            con.execute(f"INSERT INTO {table} BY NAME SELECT * FROM read_parquet('{glob}')")
            counts[table] = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    log.info("hydrate_done %s", counts)
    return counts


def push(
    repo_id: str,
    cfg: Config = CONFIG,
    *,
    token: str | None = None,
    source_url: str = "https://github.com/",
) -> dict:
    """Upload published Parquet partitions to a Hugging Face dataset repo."""
    from huggingface_hub import HfApi

    api = HfApi(token=token)
    api.create_repo(repo_id, repo_type="dataset", exist_ok=True)

    parts = _partitions(cfg)
    if not parts:
        log.warning("push_nothing_to_do")
        return {"uploaded": 0}

    # Re-uploading every partition on every session would mean ~230 files a day
    # for data that has not changed. Upload what the repo lacks, plus a recent
    # window: a day can legitimately be rewritten when it is re-finalized.
    try:
        existing = set(api.list_repo_files(repo_id, repo_type="dataset"))
    except Exception:  # noqa: BLE001 - first run, repo has no files yet
        existing = set()
    cutoff = (date.today() - timedelta(days=REWRITE_WINDOW_DAYS)).isoformat()

    # One commit for the whole run, so the repo history has one entry per
    # session rather than one per table.
    from huggingface_hub import CommitOperationAdd

    ops = []
    skipped = 0
    for table, part_name, part_dir in parts:
        part_date = part_name.split("=", 1)[-1]
        for f in sorted(part_dir.glob("*.parquet")):
            path_in_repo = f"{table}/{part_name}/{f.name}"
            if path_in_repo in existing and part_date < cutoff:
                skipped += 1
                continue
            ops.append(
                CommitOperationAdd(path_in_repo=path_in_repo, path_or_fileobj=str(f))
            )

    card = DATASET_CARD.replace("{repo_id}", repo_id).replace("{source_url}", source_url)
    ops.append(CommitOperationAdd(path_in_repo="README.md", path_or_fileobj=card.encode()))

    api.create_commit(
        repo_id=repo_id,
        repo_type="dataset",
        operations=ops,
        commit_message=f"session update: {len(ops) - 1} files",
    )
    log.info("push_done repo=%s uploaded=%d skipped=%d", repo_id, len(ops) - 1, skipped)
    return {"uploaded": len(ops) - 1, "skipped": skipped, "repo": repo_id}


def pull(repo_id: str, cfg: Config = CONFIG, *, token: str | None = None) -> dict:
    """Download published Parquet from the dataset repo into the local layout.

    Missing repo is not an error - the first ever run has nothing to pull.
    """
    from huggingface_hub import snapshot_download
    from huggingface_hub.errors import RepositoryNotFoundError

    try:
        local = snapshot_download(
            repo_id, repo_type="dataset", token=token,
            allow_patterns=["*.parquet"],
        )
    except RepositoryNotFoundError:
        log.warning("pull_repo_absent repo=%s - starting from empty", repo_id)
        return {"downloaded": 0}

    n = 0
    for table, layer in PUBLISHED:
        src = Path(local) / table
        if not src.exists():
            continue
        dst = cfg.data_root / layer / table
        dst.mkdir(parents=True, exist_ok=True)
        for part in src.glob("business_date=*"):
            out = dst / part.name
            out.mkdir(parents=True, exist_ok=True)
            for f in part.glob("*.parquet"):
                target = out / f.name
                if not target.exists():
                    target.write_bytes(f.read_bytes())
                    n += 1
    log.info("pull_done repo=%s files=%d", repo_id, n)
    return {"downloaded": n}
