# short-horizon-trading

Research project on short-horizon (seconds to minutes) trading on [Hyperliquid](https://hyperliquid.xyz),
an exchange with a fully on-chain central limit order book.

The goal is not latency HFT — a solo setup cannot win that race — but signals and passive market-making
strategies on horizons where a careful, well-validated approach can still compete:

1. **Data** — record my own tick-level dataset (`hl_recorder`), Hyperliquid + Binance futures ✅
2. **Signals** — microprice, order-book imbalance, trade-flow imbalance, Binance lead-lag;
   predictive power vs. costs (`research/01_signal_research.ipynb`) 🔄
3. **Backtester** — event-driven, conservative fill model (passive fills only when price trades through), latency and fees
4. **Validation** — walk-forward splits, untouched holdout, deflated Sharpe ratio, cross-coin robustness
5. **Live** — testnet, then tiny size; compare live fills and PnL against the backtest

## Why record my own data?

Hyperliquid's free S3 archive only has **hourly** L2 snapshots, which is useless at this horizon.
The public websocket is much richer, but it has to be captured continuously. What the public feeds deliver
(measured Nov 2026):

| Channel    | Subscription            | Content                         | Update rate         |
|------------|-------------------------|---------------------------------|---------------------|
| `bbo`      | `bbo`                   | best bid/ask with size          | ~every 50–100 ms    |
| `l2Book`   | `l2Book`, `fast: true`  | top **5** levels per side       | ~every 0.5 s        |
| `l2Book20` | `l2Book` (default)      | top **20** levels per side      | ~every 6 s          |
| `trades`   | `trades`                | every trade incl. buyer/seller  | every trade         |

The server can't tell two `l2Book` subscriptions for the same coin apart, so the 20-level book runs on a
second connection.

**Binance USD-M futures** is recorded alongside, for lead-lag research (adapted from the spot collector in
[Polymarket-bot](https://github.com/Basiliskenzahn/Polymarket-bot)):

| Channel     | Stream              | Content                     | Rate                         |
|-------------|---------------------|-----------------------------|------------------------------|
| `bn_book5`  | `depth5@100ms`      | top 5 levels per side       | every 100 ms                 |
| `bn_trades` | `aggTrade`          | aggregated trades           | every trade                  |
| `bn_bbo`    | `bookTicker` (opt-in) | every best bid/ask change | hundreds–thousands msg/s     |

Gotcha found while building this: Binance futures serves `aggTrade` only on the `/market/stream` path. On the
old `/stream` path the subscription is accepted and then **nothing is ever sent**. Book streams are the other
way round, so Binance runs on two connections, and the recorder has a silent-stream watchdog that logs any
subscribed stream that goes quiet while its connection is up.

## Design

- **Crash-safe storage.** The current hour is written as plain JSONL (a crash can't corrupt it); closed
  hours are gzipped in a background thread. Leftover files from a crash are recovered on startup.
- **Raw first.** Every line stores the untouched exchange payload plus the local receive time in ns
  (`{"r": recv_ns, "d": data}`), so nothing is lost if parsing decisions change later.
- **No duplicate trades.** On (re)subscribe the server replays recent trades. These are deduplicated by
  trade id, and the dedupe window is rebuilt from disk after a crash (tested with `SIGKILL`).
- **Self-healing connections.** App-level pings, stale detection (no message for 30 s → reconnect),
  exponential backoff with jitter.
- **Gaps are recorded, not hidden.** Every connect/disconnect goes to `events.jsonl`; `stats` reports
  outages and the longest silence per hour, so later analysis can exclude bad windows.
- **Silent streams are flagged.** A stream that delivers nothing for 3 minutes while its connection is up
  is logged as `silent_stream` (and shows up in `stats`).
- **Typed research data.** `compact` converts closed hours to zstd-compressed Parquet; `load()` returns
  a pandas DataFrame.

## Usage

```bash
pip install -e .            # or: pip install -r requirements.txt

python -m hl_recorder record                          # BTC ETH SOL HYPE, Hyperliquid + Binance, into ./data
python -m hl_recorder record --coins BTC ETH --channels bbo trades --binance   # no Binance
python -m hl_recorder compact                         # closed hours -> data/parquet/
python -m hl_recorder compact --include-current       # also the hour still being recorded
python -m hl_recorder stats                           # coverage + gap report
```

```python
from hl_recorder import load
bbo = load("data", "bbo", "ETH", start="2026-11-14", end="2026-11-20")
bbo["mid"] = (bbo.bid_px + bbo.ask_px) / 2
```

### Data layout

```
data/
  events.jsonl                                   connect / disconnect / backoff / shutdown
  raw/<channel>/<coin>/<YYYY-MM-DD>/<HH>.jsonl   hour being written (UTC)
  raw/<channel>/<coin>/<YYYY-MM-DD>/<HH>.jsonl.gz
  parquet/<channel>/<coin>/<YYYY-MM-DD>/<HH>.parquet
```

Parquet schemas: `recv_ns` (local receive time, ns) and `time_ms` (exchange time, ms) on every table.
`bbo`: `bid_px, bid_sz, bid_n, ask_px, ask_sz, ask_n`. `l2Book`/`l2Book20`: the same as lists, level 0 = best.
`trades`: `side` (`B` = buyer aggressed, `A` = seller aggressed), `px, sz, tid, hash, buyer, seller`.

Binance tables (`coin` = Hyperliquid name, e.g. `BTC` for BTCUSDT): `bn_book5` and `bn_bbo` like above plus
`event_ms`; `bn_trades`: `side, px, sz, agg_id` with the same `B`/`A` aggressor convention.

Volume: roughly 0.2–0.3 GB/day compressed for 4 coins on Hyperliquid; Binance trades add about as much again.

## Running 24/7 on a VPS

Any small Linux VPS works (1 vCPU, 2 GB RAM, 40 GB disk is plenty for months).

```bash
sudo useradd -m hl && sudo -iu hl
git clone https://github.com/Basiliskenzahn/short-horizon-trading.git && cd short-horizon-trading
python3 -m venv .venv && .venv/bin/pip install -e .
exit
sudo cp deploy/hl-recorder.service /etc/systemd/system/
sudo systemctl enable --now hl-recorder
journalctl -u hl-recorder -f                         # logs: one throughput line per minute
```

`deploy/compact.cron` runs compaction hourly. Note the receive-minus-exchange latency in the data
(`recv_ns/1e6 - time_ms`); it depends on where the VPS is and matters for any backtest latency assumption.

## Research

```bash
pip install -e ".[research]"
jupyter lab research/01_signal_research.ipynb
```

`research/signals.py` holds the tested building blocks: features sampled on a regular grid with backward
as-of joins (no lookahead), forward returns, decile tables, information coefficients, lead-lag on the
receive clock *and* the exchange clock (to separate price lead from feed latency), and a holdout split.
Every variant tried goes into `research/LOG.md`.

## Tests

```bash
pytest -q
```

Covers hourly rotation, crash recovery, gzip append, trade dedupe (including after `SIGKILL`),
compaction round-trip on real captured messages (both venues), reconnects after a stale or dropped
connection against a fake local exchange server, the silent-stream watchdog, and the research helpers
(no-lookahead as-of joins, replayed-trade filtering, lead-lag recovery on synthetic data).
