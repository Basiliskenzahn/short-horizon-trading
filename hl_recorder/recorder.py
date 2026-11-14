"""Websocket recorder for Hyperliquid market data.

Channels recorded per coin (as measured on the public API, Nov 2026):
  * l2Book    l2Book with fast=True: top 5 levels per side, ~every 0.5 s
  * l2Book20  l2Book default:        top 20 levels per side, ~every 6 s
  * bbo       best bid/offer on every change, ~every 50-100 ms
  * trades    every public trade (deduplicated by tid across reconnects)

The server cannot distinguish two l2Book subscriptions for the same coin on one
connection, so the 20-level book runs on its own connection ("depth"); everything
else shares the "main" connection. Each connection reconnects independently.

Robustness:
  * application-level ping every PING_INTERVAL_S (the server drops idle connections)
  * stale detection: no message for STALE_AFTER_S -> reconnect
  * exponential backoff with jitter on reconnect, reset after a healthy connection
  * every connect/disconnect is logged to <root>/events.jsonl so gaps can be found later
"""

from __future__ import annotations

import asyncio
import collections
import logging
import random
import signal
import time
from pathlib import Path

import orjson
import websockets

from .writer import HourlyWriter

log = logging.getLogger(__name__)

WS_URL = "wss://api.hyperliquid.xyz/ws"
PING_INTERVAL_S = 20
STALE_AFTER_S = 30
MAX_BACKOFF_S = 60
HEALTHY_AFTER_S = 60

CHANNELS = ("l2Book", "l2Book20", "bbo", "trades")
# channel -> (connection group, subscription type, extra params)
SPEC = {
    "l2Book": ("main", "l2Book", {"fast": True}),
    "bbo": ("main", "bbo", {}),
    "trades": ("main", "trades", {}),
    "l2Book20": ("depth", "l2Book", {}),
}
# (group, incoming channel) -> stored channel name
STORED_AS = {("depth", "l2Book"): "l2Book20"}


class StaleConnection(Exception):
    pass


class TidDeduper:
    """Remembers the last `maxlen` trade ids per coin. On (re)subscribe the server
    replays recent trades, which would otherwise be written twice."""

    def __init__(self, maxlen: int = 50_000):
        self.maxlen = maxlen
        self._seen: dict[str, set[int]] = collections.defaultdict(set)
        self._order: dict[str, collections.deque] = collections.defaultdict(collections.deque)

    def seed(self, coin: str, tids) -> None:
        seen, order = self._seen[coin], self._order[coin]
        for tid in tids:
            if tid not in seen:
                seen.add(tid)
                order.append(tid)
        while len(order) > self.maxlen:
            seen.discard(order.popleft())

    def filter(self, coin: str, trades: list[dict]) -> list[dict]:
        seen, order = self._seen[coin], self._order[coin]
        fresh = []
        for t in trades:
            tid = t.get("tid")
            if tid in seen:
                continue
            fresh.append(t)
            seen.add(tid)
            order.append(tid)
            if len(order) > self.maxlen:
                seen.discard(order.popleft())
        return fresh


def build_groups(coins, channels) -> dict[str, list[dict]]:
    """Group subscription messages by connection."""
    groups: dict[str, list[dict]] = collections.defaultdict(list)
    for ch in channels:
        group, sub_type, extra = SPEC[ch]
        for coin in coins:
            groups[group].append(
                {"method": "subscribe", "subscription": {"type": sub_type, "coin": coin, **extra}}
            )
    return dict(groups)


class Recorder:
    def __init__(self, coins: list[str], root: str | Path, channels=CHANNELS, url: str = WS_URL):
        self.coins = coins
        self.channels = tuple(channels)
        self.url = url
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.writer = HourlyWriter(self.root)
        self.dedupe = TidDeduper()
        self.counts = collections.Counter()
        self._stop = asyncio.Event()
        self._events = open(self.root / "events.jsonl", "ab", buffering=0)

    # ------------------------------------------------------------------ helpers
    def event(self, kind: str, **kw) -> None:
        rec = {"ts_ns": time.time_ns(), "event": kind, **kw}
        self._events.write(orjson.dumps(rec) + b"\n")
        log.info("%s %s", kind, kw or "")

    def stop(self) -> None:
        self._stop.set()

    def handle(self, raw: bytes | str, recv_ns: int, group: str = "main") -> None:
        msg = orjson.loads(raw)
        ch = msg.get("channel")
        data = msg.get("data")
        if ch in ("l2Book", "bbo"):
            stored = STORED_AS.get((group, ch), ch)
            self.writer.write(stored, data["coin"], recv_ns, data)
            self.counts[stored] += 1
        elif ch == "trades":
            if not data:
                return
            coin = data[0]["coin"]
            fresh = self.dedupe.filter(coin, data)
            if fresh:
                self.writer.write("trades", coin, recv_ns, fresh)
                self.counts["trades"] += len(fresh)
        elif ch == "error":
            self.event("server_error", conn=group, data=data)
        # "pong" and "subscriptionResponse" need no handling

    # ------------------------------------------------------------------ loops
    async def _pinger(self, ws) -> None:
        while True:
            await asyncio.sleep(PING_INTERVAL_S)
            await ws.send('{"method":"ping"}')

    async def _reporter(self) -> None:
        last = collections.Counter()
        while True:
            await asyncio.sleep(60)
            delta = self.counts - last
            last = self.counts.copy()
            log.info("last 60s: %s | lines total %d", dict(delta), self.writer.lines_written)

    async def _session(self, group: str, subs: list[dict]) -> float:
        """One connection lifetime. Returns how long it stayed up (s)."""
        connected_at = None
        async with websockets.connect(
            self.url, max_size=None, ping_interval=None, open_timeout=15, close_timeout=5
        ) as ws:
            for s in subs:
                await ws.send(orjson.dumps(s).decode())
            self.event("connect", conn=group, coins=self.coins)
            connected_at = time.monotonic()
            pinger = asyncio.create_task(self._pinger(ws))
            stopper = asyncio.create_task(self._stop.wait())
            try:
                while not self._stop.is_set():
                    recv = asyncio.create_task(ws.recv())
                    done, _ = await asyncio.wait(
                        {recv, stopper}, timeout=STALE_AFTER_S, return_when=asyncio.FIRST_COMPLETED
                    )
                    if recv not in done:
                        recv.cancel()
                        if self._stop.is_set():
                            break
                        raise StaleConnection(f"no message for {STALE_AFTER_S}s")
                    self.handle(recv.result(), time.time_ns(), group)
            finally:
                pinger.cancel()
                stopper.cancel()
                self._uptime[group] = time.monotonic() - connected_at

    async def _connection_loop(self, group: str, subs: list[dict]) -> None:
        attempt = 0
        while not self._stop.is_set():
            self._uptime[group] = 0.0
            try:
                await self._session(group, subs)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 - any failure means reconnect
                self.event("disconnect", conn=group, reason=f"{type(e).__name__}: {e}"[:300])
            if self._stop.is_set():
                break
            attempt = 0 if self._uptime[group] > HEALTHY_AFTER_S else attempt + 1
            delay = min(MAX_BACKOFF_S, 2 ** attempt) * random.uniform(0.5, 1.0)
            self.event("backoff", conn=group, seconds=round(delay, 2))
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

    def seed_dedupe_from_disk(self) -> None:
        """After a crash the in-memory tid window is gone; rebuild it from the two
        most recent trade files per coin so replayed trades are not written twice."""
        import gzip

        for coin in self.coins:
            files = sorted((self.root / "raw" / "trades" / coin).glob("*/*.jsonl*"),
                           key=lambda p: (p.parent.name, p.name.split(".")[0]))[-2:]
            for f in files:
                opener = gzip.open if f.suffix == ".gz" else open
                try:
                    with opener(f, "rb") as fh:
                        for line in fh:
                            if line.strip():
                                self.dedupe.seed(coin, (t["tid"] for t in orjson.loads(line)["d"]))
                except (EOFError, OSError, orjson.JSONDecodeError) as e:
                    log.warning("could not fully read %s for dedupe seeding: %s", f, e)

    async def run(self) -> None:
        self.writer.recover()
        self.seed_dedupe_from_disk()
        self._uptime: dict[str, float] = {}
        reporter = asyncio.create_task(self._reporter())
        groups = build_groups(self.coins, self.channels)
        try:
            await asyncio.gather(*(self._connection_loop(g, s) for g, s in groups.items()))
        finally:
            reporter.cancel()
            self.writer.close()
            self.event("shutdown")
            self._events.close()


async def record(coins: list[str], root: str | Path, channels=CHANNELS) -> None:
    rec = Recorder(coins, root, channels)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, rec.stop)
    await rec.run()
