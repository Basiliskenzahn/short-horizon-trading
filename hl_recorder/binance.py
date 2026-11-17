"""Binance USD-M futures feeds, recorded next to Hyperliquid for lead-lag research.

Adapted from the Binance spot collector in Basiliskenzahn/Polymarket-bot (pmquant/binance.py):
same combined-stream URLs, aggressor side from the `m` flag, stale-socket detection.
Changes: futures instead of spot (Hyperliquid trades perps), 100 ms book instead of 1 s,
asyncio instead of threads, raw payloads stored in the hl_recorder layout.

Endpoints (verified Nov 2026):
  * book streams (depth5@100ms, bookTicker) only work on   wss://fstream.binance.com/stream
  * aggTrade only works on                                  wss://fstream.binance.com/market/stream
    -> on the old path aggTrade subscribes fine and then silently sends nothing.
So Binance runs on two connections: "binance_book" and "binance_trades".

Stored channels (coin = Hyperliquid coin name, e.g. BTC for BTCUSDT):
  bn_book5   <sym>@depth5@100ms  top 5 levels per side, every 100 ms     (~10 msg/s per coin)
  bn_trades  <sym>@aggTrade      aggregated trades                         (default)
  bn_bbo     <sym>@bookTicker    every best bid/ask change — very heavy    (opt-in: hundreds to
                                                                            thousands msg/s for BTC)
"""

from __future__ import annotations

BOOK_URL = "wss://fstream.binance.com/stream?streams="
TRADES_URL = "wss://fstream.binance.com/market/stream?streams="

BN_CHANNELS = ("bn_book5", "bn_trades", "bn_bbo")
BN_DEFAULT = ("bn_book5", "bn_trades")

# stored channel -> (connection group, stream suffix)
BN_SPEC = {
    "bn_book5": ("binance_book", "depth5@100ms"),
    "bn_bbo": ("binance_book", "bookTicker"),
    "bn_trades": ("binance_trades", "aggTrade"),
}
_SUFFIX_TO_CHANNEL = {suffix: ch for ch, (_, suffix) in BN_SPEC.items()}
_GROUP_URL = {"binance_book": BOOK_URL, "binance_trades": TRADES_URL}


def symbol(coin: str) -> str:
    return f"{coin.upper()}USDT"


def build_groups(coins, channels, bbo_coins=None) -> dict[str, str]:
    """{group: url} for the requested Binance channels. `bbo_coins` limits the heavy
    bookTicker channel to a subset of coins (default: all)."""
    streams: dict[str, list[str]] = {}
    for ch in channels:
        group, suffix = BN_SPEC[ch]
        for coin in (bbo_coins if ch == "bn_bbo" and bbo_coins else coins):
            streams.setdefault(group, []).append(f"{symbol(coin).lower()}@{suffix}")
    return {g: _GROUP_URL[g] + "/".join(s) for g, s in streams.items()}


def route(msg: dict, coin_of: dict[str, str]):
    """(stored channel, coin, data) for a combined-stream message, or None."""
    stream = msg.get("stream")
    data = msg.get("data")
    if not stream or not isinstance(data, dict):
        return None
    sym, _, suffix = stream.partition("@")
    channel = _SUFFIX_TO_CHANNEL.get(suffix)
    coin = coin_of.get(sym.upper())
    if channel is None or coin is None:
        return None
    return channel, coin, data
