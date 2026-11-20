# NEPSE Data Pipeline

Live and historical market data from the Nepal Stock Exchange, landed in DuckDB +
Parquet with validation at every layer boundary.

## What you can do with it

Three worked analyses, committed with their outputs — see [notebooks/](notebooks/):

| | Finding |
|---|---|
| [Who is actually trading this stock?](notebooks/01_broker_network.ipynb) | Large broker positions repeat direction **71.8%** of the time next session; no next-day price edge detectable yet |
| [The corporate actions trap](notebooks/02_corporate_actions.ipynb) | NEPSE's ~10% circuit limit doubles as a corporate-action detector; **747 candidates** recovered from prices alone |
| [The symbols that disappeared](notebooks/03_survivorship.ipynb) | **36%** of NEPSE symbols are delisted; survivorship bias measured at **+0.74 pts/year** |

## Quick start

```bash
make install          # venv + deps
make status           # market state + warehouse contents
make backfill         # pull NEPSE's full 365-day history (resumable, ~18 min)
make daemon           # install the launchd agent that captures each session
```

## What NEPSE actually gives you

Every line below was measured against the live API on 2026-08-19, not taken from
documentation. Several are load-bearing design constraints.

| Fact | Consequence |
|---|---|
| **`.com` and `.com.np` are different servers** (110.44.116.20 / 182.93.68.4), both serving the API. Since 2026-08-25 the `.com.np` server answers a bare nginx 404 to any request whose `Host` header names `.com`. The `nepse` library's `getAuthorizationHeaders()` returns its whole header set, `Host: www.nepalstock.com` included, and we merged it into every request - which is what broke capture for 46 sessions. Verified 2026-11-20: same token, `Host` stripped -> 200, left in -> 404. | Only `Authorization` is taken from the library; our client sends its own Host/UA/Referer. An HTML 404/403 on an API route raises `NepseHostError`. |
| **No streaming feed exists.** The site polls REST; there is no websocket. | "Live" means polling. |
| **Backend refreshes every ~24s.** The index graph returns 610 points per 4h session. | Polling faster than ~20s buys nothing. |
| **A browser `User-Agent` is mandatory.** Without one the connection completes TLS and then hangs forever — no 403, no error. | Every client must set it or die silently. |
| **Auth is a WASM-derived token.** `/api/authenticate/prove` returns 5 salts; NEPSE's own `css.wasm` turns them into character offsets that strip 5 chars from the token. Header is `Authorization: Salter <token>`, TTL **45 seconds**. | Isolated in `session.py::_TokenSource`. |
| **POST bodies carry a computed `id`** derived from the *same salts* as the live token. | Re-derive per attempt or every retry 401s forever. |
| **`size` caps at 500.** `size=1000` returns a bare `[]`. | Page size pinned. |
| **`sort=contractId,desc` is not honoured across pages.** Page 10 was observed starting at `...03013919` and ending at `...04009466`. | A watermark "stop at known id" would **silently skip trades**. Not used. |
| **Pages intermittently return a bare `[]`** and yield 500 rows on retry. | Treated as transient, never as end-of-data. |
| **All history endpoints clamp to a rolling 365 days.** `businessDate=2025-08-19` worked and `2025-08-15` 404'd on 2026-08-19; since then the refusal is a **500 `Searched Date is not valid.`**, which NEPSE also returns for *today* before the board is published (2026-11-20). Asking the per-security route for a 2010 start silently truncates to exactly one year. | Hard floor on backfill. Out-of-range counts as the floor only within 7 days of it; otherwise the date is skipped and not recorded. |
| **The intraday index series is wiped a few hours after close** — 610 points at 19:30 NPT, zero by 20:25. | Must be captured during or right after the session. |
| **NEPSE traded Sunday–Thursday until ~2026-04-06**, then Monday–Friday. Verified: 2025-11-16 (Sun) returned 337 securities; Fridays before the change are empty. | A `weekday() < 5` weekend filter silently drops ~33 real trading days. Only Saturday is safe to skip. |
| **Floorsheet ignores `businessDate` entirely** — every historical date returns the current session. Per-company floorsheet is nginx-403'd. | **Tick history cannot be backfilled. Ever.** |

### The one thing that matters most

Floorsheet is the only dataset with **no second chance**. NEPSE serves the
current session and nothing else, so a day the poller misses is gone permanently
— not from NEPSE, not from a vendor, not from anywhere. Everything else in this
repo can be rebuilt; this cannot. That is why bronze is written before parsing
and why the launchd agent has `KeepAlive`.

It is also the most valuable dataset: **42,709 contracts on a normal day**, each
with microsecond `tradeTime` and both buyer and seller broker IDs. Most exchanges
never expose broker identity.

## Layers

```
bronze/   raw gzipped JSON, one file per response, immutable
          -> data/bronze/{dataset}/date=YYYY-MM-DD/{epoch_ms}-p{page}.json.gz
silver/   validated, typed, deduplicated Parquet, one file per table per day
gold/     bars_1m, broker_flow_daily, daily_stats
```

`eod_history_ext` is exported whole to `silver/eod_history_ext/sharesansar.parquet`
rather than partitioned by date: 31 years is only ~10 MB, and a date partition
would mean ~7,000 files of a few KB each.

DuckDB (`data/nepse.duckdb`) is the working warehouse; Parquet is the durable
export. At ~12 GB/year the whole thing fits on one machine — Spark or Kafka here
would be cost with no benefit.

## Validation

Bad data fails loudly; it is never dropped and never let through.

- **Pydantic contracts** with `extra="forbid"` — a *new* upstream field raises,
  so schema drift surfaces immediately instead of as a column of nulls.
- **Row-level tripwires**: `qty × rate == amount` within 1 rupee, positive
  prices, `trade_time` date matching `business_date`, `high >= low`.
- **Drift detection**: >10% of rows failing on a page raises `SchemaDriftError`
  rather than writing the survivors.
- **Quarantine**: rejected rows go to `data/quarantine/*.jsonl.gz` with the
  reason attached.
- **Flag, don't drop**: some debentures legitimately report a `closePrice`
  outside the day's traded range. Those rows are stored with
  `quality_flags='close_outside_range'` — dropping them would lose real
  securities, accepting them silently would hide a real anomaly.
- **Completeness gate**: a session is only published once the row count matches
  upstream's `totalElements`.
- **Reconciliation**: gold must conserve floorsheet volume and turnover exactly,
  or the build raises.

## Commands

| Command | Purpose |
|---|---|
| `./nepse status` | market state + row counts + freshness |
| `./nepse run` | live loop; polls while open, finalizes at close |
| `./nepse floorsheet --full --finalize` | complete sweep + gate + silver |
| `./nepse eod [--date YYYY-MM-DD] --finalize` | EOD board for a day |
| `./nepse backfill [--days N]` | walk the 365-day window, resumable |
| `./nepse gold [--date ...]` | build bars / broker flow / stats |
| `./nepse index` | capture the intraday index series |
| `./nepse deep-history [fetch\|load\|reconcile\|all]` | ShareSansar history back to 1995 |

## Deep history (second source)

NEPSE's 365-day floor is hard. ShareSansar publishes the same daily series much
further back, so `deep-history` fetches it per symbol and lands it in
`eod_history_ext`, tagged `source='sharesansar'` and never merged into the NEPSE
tables.

Measured on 2026-08-20:

| Fact | Value |
|---|---|
| Rows | **516,620** |
| Symbols | **653** |
| Range | **1995-07-20 -> 2026-08-19** (BBC is the oldest series, 2,346 sessions) |
| Parquet | 10.3 MB, single file |
| Rejected rows | 131, quarantined with reasons |

The universe is deliberately wider than NEPSE's. `eod_price` only contains
symbols that traded inside the rolling 365-day window, so a universe drawn from
it alone is survivorship-biased: **175 symbols** exist only in ShareSansar —
delisted mutual funds, converted promoter shares, long-suspended names such as
KRBL (2,583 sessions ending 2024-12-24). They would otherwise be invisible.

### How much can this be trusted?

The 225-day window where NEPSE and ShareSansar overlap is the only place the
second source can be checked against an authoritative one. `./nepse deep-history
reconcile` measures it:

| Check | Result |
|---|---|
| Overlapping rows | 75,671 (every NEPSE row has a match) |
| Volume exact | **75,671 / 75,671 — 100.00%** |
| Close exact | 75,657 / 75,671 — **99.98%** |

Fourteen closes disagree. Four are debentures already carrying
`quality_flags='close_outside_range'`; the rest differ by under 1% on a single
session and look like last-trade-vs-close convention, not corruption. Volume
agreeing exactly on every single row is the stronger signal: the two sources are
reporting the same trades.

That earns confidence in the *mechanism*, not a guarantee about 1998. Nothing
verifies the pre-2025 portion, and corporate actions are still unadjusted there.

## Hosted capture (free)

The capture runs on GitHub Actions and publishes to Hugging Face. Both are free:
public repositories get unmetered Actions minutes, and dataset repos cost nothing.

**Published dataset:** https://huggingface.co/datasets/AnkitSanjyal/nepse-market-data

```python
import duckdb
duckdb.sql("SELECT * FROM 'hf://datasets/AnkitSanjyal/nepse-market-data/floorsheet/**/*.parquet'")
```

### Why post-close instead of a live poller

NEPSE keeps serving the current session's floorsheet for hours after the 15:00
NPT close - measured complete at +2h and again at +5.5h, wiped by +5.9h on
2026-08-20, but complete at +18h (09:01 NPT the next morning, 40,837/40,837) on
2026-11-20. Treat the window as variable, not as a constant. A single
short run after close therefore captures the same contracts a four-hour poller
would, at a twentieth of the compute. The intraday index is the exception: it is
gone about 4h after close, which is what sets the deadline.

The workflow runs twice (09:45 and 11:15 UTC, Mon-Fri) because GitHub delays
scheduled jobs by 10-30 minutes routinely and occasionally drops one. Every write
is an upsert, so the second run is a no-op when the first succeeded.

### Parquet is the source of truth

`./nepse warehouse pull` downloads the published Parquet and rebuilds DuckDB from
it; `push` uploads new and recently-rewritten partitions. The DuckDB file is a
derived artifact, which is what lets the pipeline run on a CI runner that is
destroyed minutes later. Verified lossless on 2026-08-21: a warehouse rebuilt
from Parquet alone reproduced all five tables exactly, floorsheet turnover
included, to the paisa.

### Setup

| Where | Name | Value |
|---|---|---|
| GitHub repo secret | `HF_TOKEN` | a Hugging Face **write** token |
| GitHub repo variable | `HF_DATASET_REPO` | `AnkitSanjyal/nepse-market-data` |
| GitHub repo variable (optional) | `HF_BRONZE_REPO` | a **private** dataset repo for raw responses, e.g. `AnkitSanjyal/nepse-bronze`. Unset = raw not archived. Refuses to push to a public repo. |

The heartbeat commit (`STATUS.txt`) runs on every outcome, failures included:
when it ran only on success, the outage stopped the commits and GitHub disabled
the schedule for inactivity. Any failure, or a newest floorsheet older than 7
days, opens or comments on a single `capture-session is failing` issue.

Run the `probe-reachability` workflow first. NEPSE's failure mode is a silent
hang rather than a 403, so whether it answers a datacenter IP has to be measured
before the capture job is trusted - the probe reports HUNG, REFUSED, HTTP_4XX or
OK rather than collapsing them into "it broke".

## Known gaps

- **Floorsheet gap: 46 sessions, 2026-08-26 -> 2026-11-18** (~2.36M trades by
  NEPSE's own `totalTrades`). Every request to `.com.np` carried the library's
  `Host: www.nepalstock.com` header, which that server began rejecting; nothing alerted, and the schedule was then
  disabled for inactivity. EOD for those days is backfilled. The ticks cannot
  be recovered from NEPSE.

- **Corporate actions are not adjusted.** Bonus shares and rights issues are
  common on NEPSE and will leave fake gaps in any price series. An adjustment
  table is required before this data is fit for modelling.
- **History beyond 365 days is second-source, not NEPSE.** Re-verified 2026-08-21
  against the per-security route with an explicit `startDate=2010-01-01`: it
  returns exactly 365 days with `totalPages=1`, so the clamp is server-side and
  there is no deeper authoritative source to prefer. `eod_history_ext`
  comes from ShareSansar and is tagged with its origin; it is never merged into
  the NEPSE tables. Trust it as far as the reconciliation below justifies.
- **Index intraday history before today cannot be recovered** — same capture-or-lose-it property as floorsheet. It was also never published until 2026-11 (it lived only in the runner's DuckDB); it is now exported to `index_intraday/` and taken *first* in `capture`, but only lands when a run starts within ~4h of close.
- **Market depth / order book is not collected.** It costs one request per
  symbol and was deliberately deferred.
- **A sleeping laptop loses the session.** (Superseded by hosted capture above,
  but recorded because it cost three sessions before it was fixed.) NEPSE trades 11:00-15:00 NPT, which
  is 01:15-05:15 US Eastern; a MacBook on battery is in Deep Idle sleep for all
  of it and `KeepAlive` does not wake a sleeping host. Measured on 2026-08-20:
  the agent was alive the whole time and captured nothing. Recovered by hand
  two hours after close, because floorsheet is still served for some hours.
  The durable fix is an always-on host; `sudo pmset repeat wakeorpoweron MTWRF
  01:10:00` plus mains power is the local workaround.
- **The WASM token will break** when NEPSE rotates the module. The failure is
  loud (401s). A Playwright fallback is the intended remedy and is not built yet.
- **`.pth` files do not work under this Python 3.14.4 venv**, so `pip install -e`
  produces nothing importable. The `./nepse` wrapper sets `PYTHONPATH` instead.

## Legal

NEPSE publishes this data free to any browser. Personal research use is the
posture here; commercial redistribution requires a licence from NEPSE. The
`Salter` token scheme is a technical access control and working around it is a
grey area, stated plainly rather than glossed. No CAPTCHA is bypassed, no login
is used, and no personal data is collected. Requests are rate-limited to one
per 0.7s by default.
