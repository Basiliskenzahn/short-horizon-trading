"""Convert closed raw hours (*.jsonl.gz) into typed Parquet files for research.

<root>/raw/<channel>/<coin>/<date>/<HH>.jsonl.gz  ->  <root>/parquet/<channel>/<coin>/<date>/<HH>.parquet

Schemas (prices/sizes as float64; times: recv_ns = local receive time in ns,
time_ms = exchange timestamp in ms):
  bbo     recv_ns, time_ms, bid_px, bid_sz, bid_n, ask_px, ask_sz, ask_n
  l2Book / l2Book20
          recv_ns, time_ms, bid_px[], bid_sz[], bid_n[], ask_px[], ask_sz[], ask_n[]  (level 0 = best)
  trades  recv_ns, time_ms, side ('B' = buyer aggressed, 'A' = seller aggressed),
          px, sz, tid, hash, buyer, seller
Binance (coin = Hyperliquid name, e.g. BTC for BTCUSDT; time_ms = Binance transaction time):
  bn_book5   recv_ns, time_ms, event_ms, bid_px[], bid_sz[], ask_px[], ask_sz[]
  bn_bbo     recv_ns, time_ms, event_ms, bid_px, bid_sz, ask_px, ask_sz
  bn_trades  recv_ns, time_ms, side, px, sz, agg_id
"""

from __future__ import annotations

import gzip
import logging
from pathlib import Path

import orjson
import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger(__name__)

NAN = float("nan")


def _f(x) -> float:
    return float(x) if x is not None else NAN


def _side(levels: list[dict]) -> tuple[list[float], list[float], list[int]]:
    return [float(l["px"]) for l in levels], [float(l["sz"]) for l in levels], [int(l["n"]) for l in levels]


def read_raw(path: Path):
    """Rows of a raw file (.jsonl.gz, or the plain .jsonl still being written).
    A partially written last line of a live file is skipped."""
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rb") as fh:
        for line in fh:
            if not line.strip():
                continue
            try:
                yield orjson.loads(line)
            except orjson.JSONDecodeError:
                if path.suffix == ".gz":
                    raise


def bbo_table(rows) -> pa.Table:
    cols = {k: [] for k in ("recv_ns", "time_ms", "bid_px", "bid_sz", "bid_n", "ask_px", "ask_sz", "ask_n")}
    for row in rows:
        d = row["d"]
        bid, ask = (d.get("bbo") or [None, None])[:2]
        cols["recv_ns"].append(row["r"])
        cols["time_ms"].append(d["time"])
        for prefix, lvl in (("bid", bid), ("ask", ask)):
            cols[f"{prefix}_px"].append(_f(lvl and lvl["px"]))
            cols[f"{prefix}_sz"].append(_f(lvl and lvl["sz"]))
            cols[f"{prefix}_n"].append(int(lvl["n"]) if lvl else 0)
    return pa.table(cols, schema=pa.schema([
        ("recv_ns", pa.int64()), ("time_ms", pa.int64()),
        ("bid_px", pa.float64()), ("bid_sz", pa.float64()), ("bid_n", pa.int32()),
        ("ask_px", pa.float64()), ("ask_sz", pa.float64()), ("ask_n", pa.int32()),
    ]))


def book_table(rows) -> pa.Table:
    cols = {k: [] for k in ("recv_ns", "time_ms", "bid_px", "bid_sz", "bid_n", "ask_px", "ask_sz", "ask_n")}
    for row in rows:
        d = row["d"]
        bids, asks = d["levels"][0], d["levels"][1]
        cols["recv_ns"].append(row["r"])
        cols["time_ms"].append(d["time"])
        for prefix, lv in (("bid", bids), ("ask", asks)):
            px, sz, n = _side(lv)
            cols[f"{prefix}_px"].append(px)
            cols[f"{prefix}_sz"].append(sz)
            cols[f"{prefix}_n"].append(n)
    lf, li = pa.list_(pa.float64()), pa.list_(pa.int32())
    return pa.table(cols, schema=pa.schema([
        ("recv_ns", pa.int64()), ("time_ms", pa.int64()),
        ("bid_px", lf), ("bid_sz", lf), ("bid_n", li),
        ("ask_px", lf), ("ask_sz", lf), ("ask_n", li),
    ]))


def trades_table(rows) -> pa.Table:
    cols = {k: [] for k in ("recv_ns", "time_ms", "side", "px", "sz", "tid", "hash", "buyer", "seller")}
    seen = set()  # safety net: drop duplicate trade ids (e.g. replay after a crash)
    for row in rows:
        for t in row["d"]:
            if t["tid"] in seen:
                continue
            seen.add(t["tid"])
            users = t.get("users") or [None, None]
            cols["recv_ns"].append(row["r"])
            cols["time_ms"].append(t["time"])
            cols["side"].append(t["side"])
            cols["px"].append(float(t["px"]))
            cols["sz"].append(float(t["sz"]))
            cols["tid"].append(int(t["tid"]))
            cols["hash"].append(t.get("hash"))
            cols["buyer"].append(users[0])   # Hyperliquid docs: users = [buyer, seller]
            cols["seller"].append(users[1])
    return pa.table(cols, schema=pa.schema([
        ("recv_ns", pa.int64()), ("time_ms", pa.int64()), ("side", pa.string()),
        ("px", pa.float64()), ("sz", pa.float64()), ("tid", pa.int64()),
        ("hash", pa.string()), ("buyer", pa.string()), ("seller", pa.string()),
    ]))


def bn_book_table(rows) -> pa.Table:
    """Binance depth5@100ms. time_ms = transaction time T, event_ms = E."""
    cols = {k: [] for k in ("recv_ns", "time_ms", "event_ms", "bid_px", "bid_sz", "ask_px", "ask_sz")}
    for row in rows:
        d = row["d"]
        cols["recv_ns"].append(row["r"])
        cols["time_ms"].append(d.get("T", d.get("E")))
        cols["event_ms"].append(d.get("E"))
        for prefix, key in (("bid", "b"), ("ask", "a")):
            lv = d.get(key) or []
            cols[f"{prefix}_px"].append([float(p) for p, _ in lv])
            cols[f"{prefix}_sz"].append([float(q) for _, q in lv])
    lf = pa.list_(pa.float64())
    return pa.table(cols, schema=pa.schema([
        ("recv_ns", pa.int64()), ("time_ms", pa.int64()), ("event_ms", pa.int64()),
        ("bid_px", lf), ("bid_sz", lf), ("ask_px", lf), ("ask_sz", lf),
    ]))


def bn_bbo_table(rows) -> pa.Table:
    cols = {k: [] for k in ("recv_ns", "time_ms", "event_ms", "bid_px", "bid_sz", "ask_px", "ask_sz")}
    for row in rows:
        d = row["d"]
        cols["recv_ns"].append(row["r"])
        cols["time_ms"].append(d.get("T", d.get("E")))
        cols["event_ms"].append(d.get("E"))
        cols["bid_px"].append(float(d["b"]))
        cols["bid_sz"].append(float(d["B"]))
        cols["ask_px"].append(float(d["a"]))
        cols["ask_sz"].append(float(d["A"]))
    return pa.table(cols, schema=pa.schema([
        ("recv_ns", pa.int64()), ("time_ms", pa.int64()), ("event_ms", pa.int64()),
        ("bid_px", pa.float64()), ("bid_sz", pa.float64()), ("ask_px", pa.float64()), ("ask_sz", pa.float64()),
    ]))


def bn_trades_table(rows) -> pa.Table:
    """Binance aggTrade. side uses the Hyperliquid convention: 'B' = buyer aggressed.
    m=True means the buyer was the maker, so the aggressor sold -> 'A'."""
    cols = {k: [] for k in ("recv_ns", "time_ms", "side", "px", "sz", "agg_id")}
    seen = set()
    for row in rows:
        d = row["d"]
        if d["a"] in seen:
            continue
        seen.add(d["a"])
        cols["recv_ns"].append(row["r"])
        cols["time_ms"].append(d["T"])
        cols["side"].append("A" if d["m"] else "B")
        cols["px"].append(float(d["p"]))
        cols["sz"].append(float(d["q"]))
        cols["agg_id"].append(int(d["a"]))
    return pa.table(cols, schema=pa.schema([
        ("recv_ns", pa.int64()), ("time_ms", pa.int64()), ("side", pa.string()),
        ("px", pa.float64()), ("sz", pa.float64()), ("agg_id", pa.int64()),
    ]))


BUILDERS = {
    "bbo": bbo_table, "l2Book": book_table, "l2Book20": book_table, "trades": trades_table,
    "bn_book5": bn_book_table, "bn_bbo": bn_bbo_table, "bn_trades": bn_trades_table,
}


def compact(root: str | Path, overwrite: bool = False, include_current: bool = False) -> list[Path]:
    """Convert raw hours to Parquet. Closed hours (.jsonl.gz) always; with
    `include_current`, also the hour still being recorded (re-converted on the next
    run once that hour is closed, because the .gz will be newer than the Parquet)."""
    root = Path(root)
    written = []
    sources = sorted((root / "raw").rglob("*.jsonl.gz"))
    if include_current:
        sources += sorted((root / "raw").rglob("*.jsonl"))
    for src in sources:
        rel = src.relative_to(root / "raw")
        channel = rel.parts[0]
        builder = BUILDERS.get(channel)
        if builder is None:
            continue
        out = root / "parquet" / rel.parent / (src.name.split(".")[0] + ".parquet")
        live = src.suffix == ".jsonl"
        if out.exists() and not overwrite and not live and out.stat().st_mtime >= src.stat().st_mtime:
            continue
        table = builder(read_raw(src))
        out.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, out, compression="zstd")
        written.append(out)
        log.info("wrote %s (%d rows)", out, table.num_rows)
    return written


def load(root: str | Path, channel: str, coin: str, start: str | None = None, end: str | None = None):
    """Load a channel/coin into a pandas DataFrame, optionally limited to dates
    (inclusive, 'YYYY-MM-DD'). Rows are sorted by receive time."""
    import pandas as pd

    base = Path(root) / "parquet" / channel / coin
    files = [
        p for p in sorted(base.rglob("*.parquet"))
        if (start is None or p.parent.name >= start) and (end is None or p.parent.name <= end)
    ]
    if not files:
        return pd.DataFrame()
    df = pa.concat_tables([pq.read_table(p) for p in files]).to_pandas()
    return df.sort_values("recv_ns", kind="stable").reset_index(drop=True)
