"""Periodic snapshots of wallet positions (Hyperliquid `clearinghouseState`).

Which wallets: the `top` most active addresses by traded notional in the trades this recorder sees
(buyer and seller both count; exponentially decayed with a 12 h half-life), plus any addresses listed
in an optional pinned file (one 0x address per line, '#' comments allowed; re-read every cycle).
The list is rebuilt every REBUILD_S. Each cycle polls every wallet once, spread evenly over
`interval_s`, so the request rate stays flat (150 wallets / 60 s = 2.5 req/s, weight 2 each,
well under the 1200 weight/min per-IP limit).

Stored per poll (channel "positions", coin "wallets"), slimmed to what research needs; positions are
limited to the recorded coins (large market makers hold 100+ coins), account totals cover everything:
    {"u": address, "t": exchange time ms, "av": account value, "ntl": total notional,
     "p": [[coin, szi, entryPx, positionValue, unrealizedPnl, leverage], ...]}
"""

from __future__ import annotations

import asyncio
import collections
import logging
import math
import re
import time
import urllib.error
import urllib.request
from pathlib import Path

import orjson

log = logging.getLogger(__name__)

INFO_URL = "https://api.hyperliquid.xyz/info"
REBUILD_S = 6 * 3600
HALF_LIFE_S = 12 * 3600
ADDR = re.compile(r"^0x[0-9a-fA-F]{40}$")


class ActivityTracker:
    """Decayed traded notional per address, fed from the trades stream."""

    def __init__(self, half_life_s: float = HALF_LIFE_S):
        self.k = math.log(2) / half_life_s
        self.score: dict[str, float] = collections.defaultdict(float)
        self._t = time.monotonic()

    def _decay(self, now: float) -> None:
        f = math.exp(-self.k * (now - self._t))
        if f < 0.999:
            for a in list(self.score):
                self.score[a] *= f
                if self.score[a] < 1.0:
                    del self.score[a]
            self._t = now

    def add_trades(self, trades: list[dict], now: float | None = None) -> None:
        self._decay(time.monotonic() if now is None else now)
        for t in trades:
            users = t.get("users") or ()
            try:
                usd = float(t["px"]) * float(t["sz"])
            except (KeyError, TypeError, ValueError):
                continue
            for u in users:
                if u:
                    self.score[u] += usd

    def top(self, n: int) -> list[str]:
        return [a for a, _ in sorted(self.score.items(), key=lambda kv: -kv[1])[:n]]


def read_pinned(path: str | Path | None) -> list[str]:
    if not path:
        return []
    p = Path(path)
    if not p.exists():
        return []
    out = []
    for line in p.read_text().splitlines():
        a = line.split("#", 1)[0].strip()
        if ADDR.match(a):
            out.append(a.lower())
    return out


def slim(user: str, state: dict, coins=None) -> dict:
    ms = state.get("marginSummary") or {}
    pos = []
    for ap in state.get("assetPositions") or []:
        p = ap.get("position") or {}
        if coins is not None and p.get("coin") not in coins:
            continue
        lev = (p.get("leverage") or {}).get("value")
        pos.append([p.get("coin"), p.get("szi"), p.get("entryPx"), p.get("positionValue"),
                    p.get("unrealizedPnl"), lev])
    return {"u": user, "t": state.get("time"), "av": ms.get("accountValue"), "ntl": ms.get("totalNtlPos"), "p": pos}


def _post(body: dict, timeout: float = 10.0) -> dict:
    req = urllib.request.Request(INFO_URL, orjson.dumps(body), {"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return orjson.loads(r.read())


class WalletPoller:
    def __init__(self, store, event, tracker: ActivityTracker, top: int = 150,
                 pinned_file: str | None = None, interval_s: float = 60.0, fetch=_post,
                 warmup_s: float = 300.0, coins=None):
        self.store = store          # store(channel, coin, recv_ns, data)
        self.event = event          # event(kind, **kw)
        self.tracker = tracker
        self.top = top
        self.pinned_file = pinned_file
        self.interval_s = interval_s
        self.fetch = fetch
        self.wallets: list[str] = []
        self._built = -1e18
        self.errors = 0
        self.warmup_s = warmup_s
        self.coins = set(coins) if coins else None   # keep positions in these coins only

    def rebuild(self, now: float | None = None) -> list[str]:
        now = time.monotonic() if now is None else now
        pinned = read_pinned(self.pinned_file)
        if now - self._built >= REBUILD_S or not self.wallets:
            auto = self.tracker.top(self.top)
            if auto or pinned:
                self._built = now
                self.wallets = list(dict.fromkeys(pinned + auto))
                self.event("wallets", n=len(self.wallets), pinned=len(pinned), auto=len(auto))
        else:
            self.wallets = list(dict.fromkeys(pinned + [w for w in self.wallets if w not in pinned]))
        return self.wallets

    async def run(self, stop: asyncio.Event) -> None:
        # wait for the trades stream to show who is active before the first list
        try:
            await asyncio.wait_for(stop.wait(), timeout=self.warmup_s)
            return
        except asyncio.TimeoutError:
            pass
        while not stop.is_set():
            wallets = self.rebuild()
            if not wallets:
                await asyncio.sleep(60)
                continue
            gap = self.interval_s / len(wallets)
            for w in wallets:
                if stop.is_set():
                    return
                t0 = time.monotonic()
                try:
                    state = await asyncio.to_thread(self.fetch, {"type": "clearinghouseState", "user": w})
                    self.store("positions", "wallets", time.time_ns(), slim(w, state, self.coins))
                except urllib.error.HTTPError as e:
                    self.errors += 1
                    if e.code == 429:
                        self.event("rate_limited", conn="positions", seconds=30)
                        await asyncio.sleep(30)
                except Exception as e:  # noqa: BLE001 - network hiccups must not stop recording
                    self.errors += 1
                    if self.errors % 50 == 1:
                        self.event("positions_error", reason=f"{type(e).__name__}: {e}"[:200], total=self.errors)
                await asyncio.sleep(max(0.0, gap - (time.monotonic() - t0)))
