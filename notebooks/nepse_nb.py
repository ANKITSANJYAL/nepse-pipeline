"""Shared setup for the NEPSE tutorial notebooks.

Keeps the three notebooks visually and methodologically consistent, and puts the
data-loading decision in one place: snapshot the dataset once, then query the
local Parquet with DuckDB.

Why not query `hf://` directly? It works, but each glob re-lists every partition
directory over the API, and 229 of them will earn you an HTTP 429. Snapshotting
is one call, is cached between runs, and works offline afterwards.
"""

from __future__ import annotations

import os
import warnings

# Progress bars and the ipywidgets nag render as noise in a committed notebook.
os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
warnings.filterwarnings("ignore", message=".*IProgress not found.*")

import duckdb
import matplotlib as mpl
import matplotlib.pyplot as plt
from huggingface_hub import snapshot_download

REPO = "AnkitSanjyal/nepse-market-data"

# Pinned so these notebooks keep reporting the numbers they narrate. The dataset
# grows every weekday; without a pin, the prose and the output would silently
# drift apart. Set REVISION = None to run against the latest data instead.
REVISION = "5d534de4fc0f3eb7f3b80a2e18411c19e6b86f33"

# --- palette ---------------------------------------------------------------
# Validated for colour-vision deficiency and against both light and dark
# backgrounds. Used in fixed order, never cycled.
INK = "#17141a"
MUTED = "#6f6774"
GRID = "#e7e2e5"
BLUE = "#2a78d6"   # series 1
AQUA = "#1baf7a"   # series 2
ORANGE = "#eb6834"  # series 3
CRIMSON = "#c8102e"  # emphasis only, never a data series


def setup_plots() -> None:
    mpl.rcParams.update({
        "figure.figsize": (9, 4.6),
        "figure.dpi": 130,
        "savefig.dpi": 130,
        "font.size": 10.5,
        "axes.titlesize": 12.5,
        "axes.titleweight": "bold",
        "axes.titlelocation": "left",
        "axes.titlepad": 12,
        "axes.labelsize": 10,
        "axes.labelcolor": MUTED,
        "axes.edgecolor": GRID,
        "axes.linewidth": 1.0,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "axes.axisbelow": True,
        "grid.color": GRID,
        "grid.linewidth": 0.9,
        "xtick.color": MUTED,
        "ytick.color": MUTED,
        "xtick.labelsize": 9.5,
        "ytick.labelsize": 9.5,
        "text.color": INK,
        "legend.frameon": False,
        "legend.fontsize": 9.5,
        "figure.facecolor": "white",
        "axes.facecolor": "white",
    })


def annotate(ax, text: str, y: float = -0.30) -> None:
    """One line under a chart saying what to take from it."""
    ax.annotate(text, xy=(0, y), xycoords="axes fraction",
                fontsize=9.2, color=MUTED, va="top", wrap=True)


def connect() -> duckdb.DuckDBPyConnection:
    """Download (or reuse) the dataset and register a view per table."""
    path = snapshot_download(
        REPO, repo_type="dataset", revision=REVISION, allow_patterns=["*.parquet"]
    )
    con = duckdb.connect()
    partitioned = ["floorsheet", "eod_price", "bars_1m", "broker_flow_daily", "daily_stats"]
    for t in partitioned:
        con.execute(
            f"CREATE OR REPLACE VIEW {t} AS "
            f"SELECT * FROM read_parquet('{path}/{t}/**/*.parquet')"
        )
    con.execute(
        "CREATE OR REPLACE VIEW eod_history_ext AS "
        f"SELECT * FROM read_parquet('{path}/eod_history_ext/*.parquet')"
    )
    return con


def npr(x: float) -> str:
    """Nepali rupees at a readable magnitude."""
    for unit, div in (("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if abs(x) >= div:
            return f"{x/div:,.2f}{unit}"
    return f"{x:,.0f}"
