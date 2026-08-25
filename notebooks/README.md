# What you can do with NEPSE data

Three worked analyses on the [NEPSE market dataset](https://huggingface.co/datasets/AnkitSanjyal/nepse-market-data).
Every notebook is committed **with its outputs**, so you can read the findings
without running anything.

Each one leads with a question, reports what the data actually said, and states
where the answer is weak. None of them are "how to load a dataframe".

---

### 1. [Who is actually trading this stock?](01_broker_network.ipynb)
[![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/ANKITSANJYAL/nepse-pipeline/blob/main/notebooks/01_broker_network.ipynb)

NEPSE names **both counterparty brokers on every executed trade** — something
most exchanges never expose. Three findings:

- Nepali brokerage is **competitive, not captured**: HHI of 240 across 91 brokers,
  no single firm above 8% of turnover.
- **Large broker positions persist.** Positions over 1M NPR repeat direction the
  next session **71.8%** of the time, against a 50% baseline. Small positions
  barely do — the persistence lives exactly where brokers act deliberately.
- **No next-day predictive power** in this sample. Same-day correlation is +0.32;
  next-day is −0.03 on a coin-flip hit rate. With five sessions the test could
  only have found a very large effect, so the honest answer is *not yet
  measurable*, not *no signal*.

---

### 2. [The corporate actions trap](02_corporate_actions.ipynb)
[![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/ANKITSANJYAL/nepse-pipeline/blob/main/notebooks/02_corporate_actions.ipynb)

Every price series in this dataset is quietly wrong, and this notebook finds the
damage using nothing but the prices.

- NEPSE enforces a **~10% daily price limit**, sharply visible in 516,000
  observations: 7,564 moves pile up at 10%, then a cliff to 164 at 11%.
- That rule doubles as a **corporate-action detector** — no external feed needed,
  and none exists free for Nepal.
- **747 candidate events** cluster at factors of 1.1, 1.2 and 1.3: the 10%, 20%
  and 30% bonus issues Nepali banks declare year after year. That clustering is
  the validation.
- Screening the breaches out **halves measured volatility**, from ~81% to ~41%
  annualised.

Produces `candidate_corporate_actions.csv` — 747 rows you can apply yourself.

---

### 3. [The symbols that disappeared](03_survivorship.ipynb)
[![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/ANKITSANJYAL/nepse-pipeline/blob/main/notebooks/03_survivorship.ipynb)

Survivorship bias measured rather than warned about, using the 237 delisted
symbols this dataset deliberately kept.

- **36% of symbols that ever traded on NEPSE are gone.** A universe built from
  today's board silently excludes all of them.
- Dead companies returned **9.7%** annualised against survivors' **21.5%** — but
  the aggregate bias is only **+0.74 points a year**, because dead names are a
  few percent of observations.
- That gap between the two numbers is the real lesson: bias magnitude depends on
  the *weight* of the excluded population, not just how badly it did.

---

## Running them

```bash
pip install duckdb pandas matplotlib huggingface_hub pyarrow
cd notebooks && jupyter lab
```

The data downloads itself on first run (~10 MB, cached afterwards). No API key,
no credentials, nothing to configure.

**The dataset revision is pinned** in `nepse_nb.py`. It grows every trading day,
so without a pin the prose and the outputs would drift apart. Set
`REVISION = None` to run against the latest data — expect the numbers to differ
from the narrative.

## A note on method

The findings here were produced before the narrative was written. Where the data
refused to support an interesting claim — notably the next-day prediction test in
notebook 1 — the notebook says so and quantifies why the test was too weak to
conclude anything. A tutorial that only ever confirms its own hypothesis is not
worth much.
