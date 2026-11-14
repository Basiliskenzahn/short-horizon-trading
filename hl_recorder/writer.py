"""Hourly-rotating raw writer.

Layout:  <root>/raw/<channel>/<coin>/<YYYY-MM-DD>/<HH>.jsonl      (hour being written)
         <root>/raw/<channel>/<coin>/<YYYY-MM-DD>/<HH>.jsonl.gz   (closed hours)

Each line is one websocket payload plus the local receive timestamp:
    {"r": <recv time, ns since epoch>, "d": <exchange "data" field, untouched>}

The current hour is written uncompressed so that a crash never corrupts it; closed
hours are gzipped in a background thread. Gzip is opened in append mode, so if a
.gz for the same hour already exists the new data becomes an extra gzip member,
which gzip.open() reads transparently.
"""

from __future__ import annotations

import gzip
import logging
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import orjson

log = logging.getLogger(__name__)


def hour_key(ns: int) -> tuple[str, str]:
    """(YYYY-MM-DD, HH) in UTC for a nanosecond timestamp."""
    dt = datetime.fromtimestamp(ns / 1e9, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d"), dt.strftime("%H")


def compress_file(path: Path) -> Path:
    """Gzip `path` into `path.gz` (appending a member if it exists), then delete it."""
    gz_path = path.with_name(path.name + ".gz")
    with open(path, "rb") as src, gzip.open(gz_path, "ab", compresslevel=6) as dst:
        shutil.copyfileobj(src, dst, length=1 << 20)
    os.remove(path)
    return gz_path


class HourlyWriter:
    def __init__(self, root: str | Path, flush_interval_s: float = 1.0):
        self.raw_root = Path(root) / "raw"
        self.raw_root.mkdir(parents=True, exist_ok=True)
        self.flush_interval_s = flush_interval_s
        # (channel, coin) -> (hour_key, path, file handle)
        self._open: dict[tuple[str, str], tuple[tuple[str, str], Path, object]] = {}
        self._pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="gzip")
        self._pending = []
        self._last_flush = time.monotonic()
        self.lines_written = 0

    # ------------------------------------------------------------------ recovery
    def recover(self, now_ns: int | None = None) -> list[Path]:
        """Compress leftover plain .jsonl files from earlier runs (except the current hour)."""
        current = hour_key(now_ns or time.time_ns())
        done = []
        for p in sorted(self.raw_root.rglob("*.jsonl")):
            if (p.parent.name, p.stem) == current:
                continue  # we will keep appending to it
            log.info("recovering leftover file %s", p)
            done.append(compress_file(p))
        return done

    # ------------------------------------------------------------------ writing
    def write(self, channel: str, coin: str, recv_ns: int, data) -> None:
        key = (channel, coin)
        hk = hour_key(recv_ns)
        entry = self._open.get(key)
        if entry is None or entry[0] != hk:
            if entry is not None:
                self._rotate(entry)
            path = self.raw_root / channel / coin / hk[0] / f"{hk[1]}.jsonl"
            path.parent.mkdir(parents=True, exist_ok=True)
            fh = open(path, "ab", buffering=1 << 16)
            entry = (hk, path, fh)
            self._open[key] = entry
        entry[2].write(orjson.dumps({"r": recv_ns, "d": data}) + b"\n")
        self.lines_written += 1

        now = time.monotonic()
        if now - self._last_flush >= self.flush_interval_s:
            self.flush()
            self._last_flush = now

    def flush(self) -> None:
        for _, _, fh in self._open.values():
            fh.flush()

    def _rotate(self, entry) -> None:
        _, path, fh = entry
        fh.close()
        self._pending = [f for f in self._pending if not f.done()]
        self._pending.append(self._pool.submit(compress_file, path))
        log.info("rotated %s", path)

    def close(self, compress_current: bool = False) -> None:
        """Close all files. The current hour stays uncompressed unless asked
        (a restart within the same hour keeps appending to it)."""
        for entry in self._open.values():
            if compress_current:
                self._rotate(entry)
            else:
                entry[2].close()
        self._open.clear()
        self._pool.shutdown(wait=True)
