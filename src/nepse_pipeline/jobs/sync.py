"""Move the warehouse between a Hugging Face dataset repo and local disk.

The inversion that makes hosted capture possible: **Parquet in the dataset repo
is the source of truth, and the DuckDB file is a derived artifact** rebuilt from
it on demand. Nothing durable then lives on the machine that ran the job, so the
pipeline can run on an ephemeral CI runner that is destroyed 15 minutes later.

Both NEPSE-sourced and second-source tables are published, but never merged.
`eod_history_ext` keeps its `source` column, so every row states its own
provenance and a consumer who wants exchange-authoritative data only can filter
it out. Merging the two into one series would make that distinction
unrecoverable, which is why it is not done here.

NEPSE's own API cannot supply pre-2025 data at all - verified 2026-08-21, an
explicit `startDate=2010-01-01` on the per-security history route returns
exactly 365 days - so the deep history has no authoritative alternative.
"""

from __future__ import annotations

import io
import logging
import tarfile
from datetime import date, datetime, timedelta
from pathlib import Path

from ..config import CONFIG, NPT, Config
from ..storage import connect

log = logging.getLogger(__name__)

# A partition this recent is re-uploaded even if the repo already has it,
# because re-finalizing a session rewrites its Parquet.
REWRITE_WINDOW_DAYS = 5

# (table, layer) - layer is the directory under data/ the partitions live in.
PUBLISHED: tuple[tuple[str, str], ...] = (
    ("floorsheet", "silver"),
    ("eod_price", "silver"),
    # Was missing until 2026-11: captured on the runner, then destroyed with it.
    ("index_intraday", "silver"),
    ("bars_1m", "gold"),
    ("broker_flow_daily", "gold"),
    ("daily_stats", "gold"),
)

# Not partitioned by date: 31 years is ~10 MB, and a business_date partition
# would mean ~7,000 files of a few KB each. Published as one file per source.
WHOLE_FILE_TABLES: tuple[tuple[str, str], ...] = (
    ("eod_history_ext", "silver"),
)

DATASET_CARD = """---
license: other
language: [en]
tags: [finance, stock-market, nepal, nepse, order-flow, ohlcv, time-series]
pretty_name: NEPSE Market Data
---

# NEPSE Market Data

Daily and tick-level data from the Nepal Stock Exchange, captured by an
open-source pipeline and validated at every layer boundary.

**Daily prices reach back to 1995-07-20. Tick data starts 2026-08-19.** That gap
is a property of the source, not a backlog - see *Coverage* below.

## Tables

| Path | Grain | Source | Coverage |
|---|---|---|---|
| `eod_history_ext/` | symbol x day | **ShareSansar** (second source) | 1995-07-20 -> present, 653 symbols |
| `eod_price/` | symbol x day | NEPSE (authoritative) | rolling 365 days, richer columns |
| `floorsheet/` | one executed contract | NEPSE (authoritative) | from 2026-08-19, accumulating; gap 2026-08-26 -> 2026-11-18 |
| `index_intraday/` | NEPSE index x ~24s | NEPSE (authoritative) | from 2026-11, only sessions captured within ~4h of close |
| `bars_1m/` | symbol x minute | derived from floorsheet | from 2026-08-19 |
| `broker_flow_daily/` | symbol x broker x day | derived from floorsheet | from 2026-08-19 |
| `daily_stats/` | symbol x day | derived from floorsheet | from 2026-08-19 |

`floorsheet` names **both** counterparty brokers on every trade with a
microsecond timestamp - something most exchanges never expose.

## Worked examples

Three analyses with committed outputs — read them without running anything:

| Notebook | Finding |
|---|---|
| [Who is actually trading this stock?]({source_url}/blob/main/notebooks/01_broker_network.ipynb) | Large broker positions repeat direction **71.8%** of the time next session |
| [The corporate actions trap]({source_url}/blob/main/notebooks/02_corporate_actions.ipynb) | NEPSE's ~10% circuit limit doubles as a corporate-action detector; **747 candidates** found from prices alone |
| [The symbols that disappeared]({source_url}/blob/main/notebooks/03_survivorship.ipynb) | **36%** of symbols are delisted; survivorship bias measured at **+0.74 pts/year** |

## Load it

```python
import duckdb

# 31 years of daily bars
duckdb.sql("SELECT * FROM 'hf://datasets/{repo_id}/eod_history_ext/*.parquet'")

# tick-level contracts
duckdb.sql("SELECT * FROM 'hf://datasets/{repo_id}/floorsheet/**/*.parquet'")
```

## Provenance - read this before combining tables

The two daily tables are **deliberately not merged**, and `eod_history_ext`
carries a `source` column on every row so it always identifies itself.

- `eod_price` is NEPSE's own end-of-day board. Authoritative, but NEPSE's API
  clamps every history endpoint to a rolling 365 days. Verified 2026-08-21: an
  explicit `startDate=2010-01-01` on the per-security route returns exactly 365
  days, `totalPages=1`. There is no deeper authoritative source.
- `eod_history_ext` is compiled by [ShareSansar](https://www.sharesansar.com/)
  and is the only route to pre-2025 data. Credit for that compilation is theirs.

**How far to trust the second source.** On the 226-day window where both exist:

| Check | Result |
|---|---|
| Overlapping rows | 75,671 (every NEPSE row matched) |
| Traded volume identical | **75,671 / 75,671 - 100.00%** |
| Close price identical | 75,657 / 75,671 - 99.98% |

The 14 close disagreements are 4 debentures already flagged
`close_outside_range` plus 10 single-session differences under 1%. Volume
agreeing on every row is the strong signal: both sources describe the same
trades. This validates the *mechanism*; nothing independently verifies the
pre-2025 portion.

## What will bite you when modelling

- **Corporate actions are not adjusted.** Bonus shares and rights issues are
  common on NEPSE and leave artificial overnight gaps in any raw price series.
  Returns computed on the raw close will contain those artefacts.
- **Tick history cannot be backfilled by anyone.** NEPSE's floorsheet endpoint
  serves only the current session and ignores the date requested. It accumulates
  one session at a time and a missed day is gone permanently.
- **The trading week changed.** NEPSE traded Sunday-Thursday until roughly
  2026-04-06, then Monday-Friday. A naive weekday filter drops real sessions.
- **The early years are thin.** 1995-2002 is a single symbol. The market only
  becomes broadly observable from 2011 (93 symbols); 125 symbols carry 10+ years.
- Every published session passed a completeness gate against upstream's own row
  count, plus uniqueness, null, and `qty * rate = amount` checks.

## Licence and use

NEPSE publishes its data free to any browser and ShareSansar's `robots.txt`
permits crawling. This dataset is provided for research and educational use.
Commercial redistribution may require a licence from NEPSE. If you are
ShareSansar or NEPSE and want this changed, open a discussion on this repo.

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
            # BY NAME, with hive partitioning off: every file carries its own
            # columns, and index_intraday has no business_date column, so the
            # directory name must not be injected as one.
            con.execute(
                f"INSERT INTO {table} BY NAME "
                f"SELECT * FROM read_parquet('{glob}', hive_partitioning = false)"
            )
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
    cutoff = (datetime.now(NPT).date() - timedelta(days=REWRITE_WINDOW_DAYS)).isoformat()

    # One commit for the whole run, so the repo history has one entry per
    # session rather than one per table.
    from huggingface_hub import CommitOperationAdd

    ops = []
    skipped = 0
    for table, layer in WHOLE_FILE_TABLES:
        for f in sorted((cfg.data_root / layer / table).glob("*.parquet")):
            ops.append(
                CommitOperationAdd(
                    path_in_repo=f"{table}/{f.name}", path_or_fileobj=str(f)
                )
            )
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


BRONZE_DATASETS = ("floorsheet", "eod_price", "index_intraday")


def bundle_bronze(cfg: Config = CONFIG, *, window_days: int = REWRITE_WINDOW_DAYS) -> list[tuple[str, bytes]]:
    """Pack recent bronze into one uncompressed tar per (dataset, date).

    One tar instead of ~90 files per session: a year of per-response files
    would be ~50k objects in the repo, which is the small-files problem in
    another costume. Members are already gzipped, so tar adds no compression.
    Returns (path_in_repo, tar_bytes); the name carries the bundle time so a
    second run the same day adds a bundle instead of replacing the first -
    bronze is append-only.
    """
    stamp = int(datetime.now(NPT).timestamp() * 1000)
    cutoff = datetime.now(NPT).date() - timedelta(days=window_days)
    out: list[tuple[str, bytes]] = []
    for ds in BRONZE_DATASETS:
        for part in sorted((cfg.bronze / ds).glob("date=*")):
            try:
                part_date = date.fromisoformat(part.name.split("=", 1)[1])
            except ValueError:
                continue
            files = sorted(part.glob("*.json.gz"))
            if part_date < cutoff or not files:
                continue
            buf = io.BytesIO()
            with tarfile.open(fileobj=buf, mode="w") as tar:
                for f in files:
                    tar.add(f, arcname=f.name)
            out.append((f"{ds}/{part.name}/bundle-{stamp}.tar", buf.getvalue()))
    return out


def push_bronze(repo_id: str, cfg: Config = CONFIG, *, token: str | None = None) -> dict:
    """Archive raw responses to a PRIVATE dataset repo.

    Private because a raw mirror of NEPSE's API responses is a step closer to
    redistribution than the validated tables already published; it exists so
    a parser bug found later can be fixed by replay, which the hosted path
    could not do at all because the runner's disk is discarded.
    """
    from huggingface_hub import CommitOperationAdd, HfApi

    bundles = bundle_bronze(cfg)
    if not bundles:
        log.warning("push_bronze_nothing_to_do")
        return {"uploaded": 0}
    api = HfApi(token=token)
    api.create_repo(repo_id, repo_type="dataset", private=True, exist_ok=True)
    if not api.repo_info(repo_id, repo_type="dataset").private:
        # create_repo(exist_ok=True) does not change an existing repo's
        # visibility. Refuse rather than publish raw data by accident.
        raise RuntimeError(f"{repo_id} exists and is PUBLIC - refusing to push bronze")
    ops = [CommitOperationAdd(path_in_repo=p, path_or_fileobj=b) for p, b in bundles]
    api.create_commit(repo_id=repo_id, repo_type="dataset", operations=ops,
                      commit_message=f"bronze: {len(ops)} bundles")
    size = sum(len(b) for _, b in bundles)
    log.info("push_bronze_done repo=%s bundles=%d bytes=%d", repo_id, len(ops), size)
    return {"uploaded": len(ops), "bytes": size, "repo": repo_id}


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
    for table, layer in WHOLE_FILE_TABLES:
        src = Path(local) / table
        if not src.exists():
            continue
        dst = cfg.data_root / layer / table
        dst.mkdir(parents=True, exist_ok=True)
        for f in src.glob("*.parquet"):
            target = dst / f.name
            if not target.exists():
                target.write_bytes(f.read_bytes())
                n += 1

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
