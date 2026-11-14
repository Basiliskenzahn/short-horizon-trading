"""Convert closed raw hours (*.jsonl.gz) into typed Parquet files for research.

<root>/raw/<channel>/<coin>/<date>/<HH>.jsonl.gz  ->  <root>/parquet/<channel>/<coin>/<date>/<HH>.parquet

Schemas (prices/sizes as float64; times: recv_ns = local receive time in ns,
time_ms = exchange timestamp in ms):
  bbo     recv_ns, time_ms, bid_px, bid_sz, bid_n, ask_px, ask_sz, ask_n
  l2Book / l2Book20
          recv_ns, time_ms, bid_px[], bid_sz[], bid_n[], ask_px[], ask_sz[], ask_n[]  (level 0 = best)
  trades  recv_ns, time_ms, side ('B' = buyer aggressed, 'A' = seller aggressed),
          px, sz, tid, hash, buyer, seller
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
    with gzip.open(path, "rb") as fh:
        for line in fh:
            if line.strip():
                yield orjson.loads(line)


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


BUILDERS = {"bbo": bbo_table, "l2Book": book_table, "l2Book20": book_table, "trades": trades_table}


def compact(root: str | Path, overwrite: bool = False) -> list[Path]:
    root = Path(root)
    written = []
    for gz in sorted((root / "raw").rglob("*.jsonl.gz")):
        rel = gz.relative_to(root / "raw")
        channel = rel.parts[0]
        builder = BUILDERS.get(channel)
        if builder is None:
            continue
        out = root / "parquet" / rel.parent / gz.name.replace(".jsonl.gz", ".parquet")
        if out.exists() and not overwrite and out.stat().st_mtime >= gz.stat().st_mtime:
            continue
        table = builder(read_raw(gz))
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
