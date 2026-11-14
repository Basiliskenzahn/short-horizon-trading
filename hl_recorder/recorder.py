"""Websocket recorder for Hyperliquid (and optionally Binance futures) market data.

Hyperliquid channels per coin (as measured on the public API, Nov 2026):
  * l2Book    l2Book with fast=True: top 5 levels per side, ~every 0.5 s
  * l2Book20  l2Book default:        top 20 levels per side, ~every 6 s
  * bbo       best bid/offer on every change, ~every 50-100 ms
  * trades    every public trade (deduplicated by tid across reconnects)

The server cannot distinguish two l2Book subscriptions for the same coin on one
connection, so the 20-level book runs on its own connection ("depth"); everything
else shares the "main" connection. Binance channels are described in binance.py.
Each connection reconnects independently.

Robustness:
  * application-level ping on Hyperliquid connections (the server drops idle ones)
  * stale detection: no message on a connection for STALE_AFTER_S -> reconnect
  * exponential backoff with jitter on reconnect, reset after a healthy connection
  * silent-stream watchdog: a subscribed stream that sends nothing for SILENT_WARN_S
    while its connection is up is logged (catches e.g. moved endpoints that accept a
    subscription and never deliver)
  * every connect/disconnect is logged to <root>/events.jsonl so gaps can be found later
"""

from __future__ import annotations

import asyncio
import collections
import gzip
import logging
import random
import signal
import time
from dataclasses import dataclass, field
from pathlib import Path

import orjson
import websockets

from . import binance
from .writer import HourlyWriter

log = logging.getLogger(__name__)

WS_URL = "wss://api.hyperliquid.xyz/ws"
PING_INTERVAL_S = 20
STALE_AFTER_S = 30
MAX_BACKOFF_S = 60
HEALTHY_AFTER_S = 60
SILENT_WARN_S = 180
REPORT_EVERY_S = 60

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


@dataclass
class Group:
    url: str
    subs: list[dict] = field(default_factory=list)  # sent after connecting
    app_ping: bool = False                          # Hyperliquid needs {"method":"ping"}
    streams: list[tuple[str, str]] = field(default_factory=list)  # (stored channel, coin) expected


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
    """Hyperliquid subscription messages grouped by connection."""
    groups: dict[str, list[dict]] = collections.defaultdict(list)
    for ch in channels:
        group, sub_type, extra = SPEC[ch]
        for coin in coins:
            groups[group].append(
                {"method": "subscribe", "subscription": {"type": sub_type, "coin": coin, **extra}}
            )
    return dict(groups)


class Recorder:
    def __init__(self, coins: list[str], root: str | Path, channels=CHANNELS, url: str = WS_URL,
                 binance_channels=(), binance_urls: dict[str, str] | None = None):
        self.coins = coins
        self.channels = tuple(channels)
        self.binance_channels = tuple(binance_channels)
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.writer = HourlyWriter(self.root)
        self.dedupe = TidDeduper()
        self.counts = collections.Counter()
        self.groups = self._build(url, binance_urls)
        self._bn_coin_of = {binance.symbol(c): c for c in coins}
        self._stop = asyncio.Event()
        self._events = open(self.root / "events.jsonl", "ab", buffering=0)
        self._last_seen: dict[tuple[str, str], float] = {}
        self._connected_since: dict[str, float] = {}
        self._silent: set[tuple[str, str]] = set()
        self._uptime: dict[str, float] = {}

    def _build(self, url, binance_urls) -> dict[str, Group]:
        groups: dict[str, Group] = {}
        for name, subs in build_groups(self.coins, self.channels).items():
            groups[name] = Group(url=url, subs=subs, app_ping=True)
        for ch in self.channels:
            groups[SPEC[ch][0]].streams += [(ch, c) for c in self.coins]
        if self.binance_channels:
            for name, bn_url in binance.build_groups(self.coins, self.binance_channels).items():
                groups[name] = Group(url=(binance_urls or {}).get(name, bn_url))
            for ch in self.binance_channels:
                groups[binance.BN_SPEC[ch][0]].streams += [(ch, c) for c in self.coins]
        return groups

    # ------------------------------------------------------------------ helpers
    def event(self, kind: str, **kw) -> None:
        rec = {"ts_ns": time.time_ns(), "event": kind, **kw}
        self._events.write(orjson.dumps(rec) + b"\n")
        (log.warning if kind in ("silent_stream", "disconnect", "server_error") else log.info)(
            "%s %s", kind, kw or "")

    def stop(self) -> None:
        self._stop.set()

    def _store(self, channel: str, coin: str, recv_ns: int, data, n: int = 1) -> None:
        self.writer.write(channel, coin, recv_ns, data)
        self.counts[channel] += n
        self._last_seen[(channel, coin)] = time.monotonic()

    def handle(self, raw: bytes | str, recv_ns: int, group: str = "main") -> None:
        msg = orjson.loads(raw)
        if group.startswith("binance"):
            routed = binance.route(msg, self._bn_coin_of)
            if routed:
                self._store(routed[0], routed[1], recv_ns, routed[2])
            return
        ch = msg.get("channel")
        data = msg.get("data")
        if ch in ("l2Book", "bbo"):
            self._store(STORED_AS.get((group, ch), ch), data["coin"], recv_ns, data)
        elif ch == "trades":
            if not data:
                return
            coin = data[0]["coin"]
            fresh = self.dedupe.filter(coin, data)
            if fresh:
                self._store("trades", coin, recv_ns, fresh, n=len(fresh))
        elif ch == "error":
            self.event("server_error", conn=group, data=data)
        # "pong" and "subscriptionResponse" need no handling

    def seed_dedupe_from_disk(self) -> None:
        """After a crash the in-memory tid window is gone; rebuild it from the two
        most recent trade files per coin so replayed trades are not written twice."""
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

    def check_silent(self, now: float | None = None) -> None:
        """Log streams that delivered nothing for SILENT_WARN_S while their connection was up."""
        now = time.monotonic() if now is None else now
        for name, g in self.groups.items():
            up_since = self._connected_since.get(name)
            for key in g.streams:
                last = max(self._last_seen.get(key, 0.0), up_since or 0.0)
                silent = up_since is not None and now - last > SILENT_WARN_S
                if silent and key not in self._silent:
                    self._silent.add(key)
                    self.event("silent_stream", conn=name, channel=key[0], coin=key[1],
                               seconds=round(now - last))
                elif not silent and key in self._silent and self._last_seen.get(key, 0) > (up_since or 0):
                    self._silent.discard(key)
                    self.event("stream_resumed", conn=name, channel=key[0], coin=key[1])

    # ------------------------------------------------------------------ loops
    async def _pinger(self, ws) -> None:
        while True:
            await asyncio.sleep(PING_INTERVAL_S)
            await ws.send('{"method":"ping"}')

    async def _reporter(self) -> None:
        last = collections.Counter()
        elapsed = 0
        while True:
            await asyncio.sleep(min(REPORT_EVERY_S, 10))
            self.check_silent()
            elapsed += min(REPORT_EVERY_S, 10)
            if elapsed >= REPORT_EVERY_S:
                elapsed = 0
                delta = self.counts - last
                last = self.counts.copy()
                log.info("last %ds: %s | lines total %d", REPORT_EVERY_S, dict(delta),
                         self.writer.lines_written)

    async def _session(self, name: str, g: Group) -> None:
        """One connection lifetime."""
        async with websockets.connect(
            g.url, max_size=None, ping_interval=None, open_timeout=15, close_timeout=5
        ) as ws:
            for s in g.subs:
                await ws.send(orjson.dumps(s).decode())
            self.event("connect", conn=name, coins=self.coins)
            connected_at = time.monotonic()
            self._connected_since[name] = connected_at
            pinger = asyncio.create_task(self._pinger(ws)) if g.app_ping else None
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
                    self.handle(recv.result(), time.time_ns(), name)
            finally:
                if pinger:
                    pinger.cancel()
                stopper.cancel()
                self._connected_since.pop(name, None)
                self._uptime[name] = time.monotonic() - connected_at

    async def _connection_loop(self, name: str, g: Group) -> None:
        attempt = 0
        while not self._stop.is_set():
            self._uptime[name] = 0.0
            try:
                await self._session(name, g)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 - any failure means reconnect
                self.event("disconnect", conn=name, reason=f"{type(e).__name__}: {e}"[:300])
            if self._stop.is_set():
                break
            attempt = 0 if self._uptime[name] > HEALTHY_AFTER_S else attempt + 1
            delay = min(MAX_BACKOFF_S, 2 ** attempt) * random.uniform(0.5, 1.0)
            self.event("backoff", conn=name, seconds=round(delay, 2))
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

    async def run(self) -> None:
        self.writer.recover()
        self.seed_dedupe_from_disk()
        reporter = asyncio.create_task(self._reporter())
        try:
            await asyncio.gather(*(self._connection_loop(n, g) for n, g in self.groups.items()))
        finally:
            reporter.cancel()
            self.writer.close()
            self.event("shutdown")
            self._events.close()


async def record(coins: list[str], root: str | Path, channels=CHANNELS, binance_channels=(),
                 keep_awake: bool = False) -> None:
    from .keepawake import keep_awake as _keep_awake

    rec = Recorder(coins, root, channels, binance_channels=binance_channels)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, rec.stop)
        except NotImplementedError:
            # Windows event loops don't support signal handlers; Ctrl+C raises
            # KeyboardInterrupt instead, which run() turns into a clean shutdown.
            pass
    with _keep_awake(keep_awake):
        await rec.run()
