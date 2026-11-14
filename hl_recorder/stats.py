"""Data-quality report: coverage per channel/coin and gaps.

Two gap sources:
  * events.jsonl: time between each `disconnect` and the following `connect`, per connection
    (main = l2Book/bbo/trades, depth = l2Book20, binance_book, binance_trades)
  * parquet files: largest silence between consecutive messages per hour
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import orjson
import pyarrow.compute as pc
import pyarrow.parquet as pq


def _fmt(ns: int) -> str:
    return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def outage_gaps(root: Path) -> list[tuple[int, float, str, str]]:
    """[(start_ns, seconds, connection, reason)] for each disconnect -> reconnect interval."""
    path = root / "events.jsonl"
    if not path.exists():
        return []
    gaps, down, known = [], {}, set()  # conn -> (since_ns, reason)
    for line in path.read_bytes().splitlines():
        ev = orjson.loads(line)
        kind = ev["event"]
        if kind == "shutdown":
            for conn in known:
                down.setdefault(conn, (ev["ts_ns"], "shutdown"))
            continue
        if "conn" in ev:
            known.add(ev["conn"])
        conn = ev.get("conn", "main")
        if kind == "disconnect" and conn not in down:
            down[conn] = (ev["ts_ns"], ev.get("reason", ""))
        elif kind == "connect" and conn in down:
            since, reason = down.pop(conn)
            gaps.append((since, (ev["ts_ns"] - since) / 1e9, conn, reason))
    return gaps


def report(root: str | Path, gap_threshold_s: float = 5.0) -> str:
    root = Path(root)
    lines = []

    raw = defaultdict(int)
    for p in (root / "raw").rglob("*.jsonl*"):
        rel = p.relative_to(root / "raw")
        raw[(rel.parts[0], rel.parts[1])] += 1

    pq_rows = defaultdict(int)
    worst = defaultdict(lambda: (0.0, ""))
    for p in (root / "parquet").rglob("*.parquet"):
        rel = p.relative_to(root / "parquet")
        key = (rel.parts[0], rel.parts[1])
        recv = pq.read_table(p, columns=["recv_ns"])["recv_ns"]
        pq_rows[key] += len(recv)
        if len(recv) > 1:
            diffs = pc.subtract(recv.slice(1), recv.slice(0, len(recv) - 1))
            mx = pc.max(diffs).as_py() / 1e9
            if mx > worst[key][0]:
                worst[key] = (mx, f"{rel.parts[2]} {p.stem}h")

    lines.append(f"{'channel':<8} {'coin':<8} {'hours':>6} {'rows (compacted)':>18} {'max silence':>12}  where")
    for key in sorted(raw):
        gap, where = worst[key]
        lines.append(f"{key[0]:<8} {key[1]:<8} {raw[key]:>6} {pq_rows[key]:>18,} {gap:>11.1f}s  {where}")

    gaps = [g for g in outage_gaps(root) if g[1] >= gap_threshold_s]
    lines.append("")
    lines.append(f"Outages >= {gap_threshold_s:.0f}s: {len(gaps)}")
    for start, secs, conn, reason in gaps:
        lines.append(f"  {_fmt(start)} UTC  {secs:8.1f}s  [{conn}] {reason}")

    silent = []
    ev_path = root / "events.jsonl"
    if ev_path.exists():
        for line in ev_path.read_bytes().splitlines():
            ev = orjson.loads(line)
            if ev["event"] == "silent_stream":
                silent.append(ev)
    if silent:
        lines.append("")
        lines.append(f"Silent-stream warnings: {len(silent)}")
        for ev in silent[-20:]:
            lines.append(f"  {_fmt(ev['ts_ns'])} UTC  [{ev['conn']}] {ev['channel']}/{ev['coin']} "
                         f"silent {ev['seconds']}s")
    return "\n".join(lines)
