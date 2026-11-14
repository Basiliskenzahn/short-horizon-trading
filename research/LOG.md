# Research log

Every feature, parameter set, horizon or strategy variant that gets evaluated goes here, including the
ones that failed. The count matters: it is the number of trials for the deflated Sharpe ratio, and it is
the honest answer to "how many things did you try before this worked?".

Holdout: **not set yet** (set `HOLDOUT_START` in the notebook once there are ~2 weeks of data, then
record the date here and never move it).

| # | Date | Data window | Coins | What was tested | Result | Decision |
|---|------|-------------|-------|-----------------|--------|----------|
| 1 | 2026-11-14 | ~40 min (pipeline test) | BTC ETH SOL HYPE | Notebook 01, all features, 1/5/30 s | Pipeline works; sample far too short for conclusions | Re-run on ≥ 1 week |
