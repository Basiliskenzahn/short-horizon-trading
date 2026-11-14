"""CLI:  python -m hl_recorder record|compact|stats ..."""

from __future__ import annotations

import argparse
import asyncio
import logging

from .compact import compact
from .binance import BN_CHANNELS, BN_DEFAULT
from .recorder import CHANNELS, record
from .stats import report

DEFAULT_COINS = ["BTC", "ETH", "SOL", "HYPE"]


def main() -> None:
    ap = argparse.ArgumentParser(prog="hl_recorder", description="Hyperliquid market data recorder")
    ap.add_argument("--data", default="data", help="data directory (default: ./data)")
    ap.add_argument("-v", "--verbose", action="store_true")
    sub = ap.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("record", help="record live data until stopped")
    r.add_argument("--coins", nargs="+", default=DEFAULT_COINS)
    r.add_argument("--channels", nargs="+", default=list(CHANNELS), choices=list(CHANNELS),
                   help="Hyperliquid channels")
    r.add_argument("--binance", nargs="*", default=list(BN_DEFAULT), choices=list(BN_CHANNELS),
                   help="Binance USD-M futures channels (default: bn_book5 bn_trades; "
                        "bn_bbo is very heavy). Pass --binance with no values to disable.")
    r.add_argument("--keep-awake", action="store_true",
                   help="prevent the computer from sleeping while recording (Windows/macOS)")

    c = sub.add_parser("compact", help="convert closed raw hours to Parquet")
    c.add_argument("--overwrite", action="store_true")
    c.add_argument("--include-current", action="store_true",
                   help="also convert the hour that is still being recorded")

    s = sub.add_parser("stats", help="coverage and gap report")
    s.add_argument("--gap", type=float, default=5.0, help="report outages longer than this (s)")

    args = ap.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.cmd == "record":
        try:
            asyncio.run(record(args.coins, args.data, tuple(args.channels), tuple(args.binance),
                               keep_awake=args.keep_awake))
        except KeyboardInterrupt:
            pass  # Windows: Ctrl+C; files were flushed in Recorder.run's finally block
    elif args.cmd == "compact":
        out = compact(args.data, overwrite=args.overwrite, include_current=args.include_current)
        print(f"wrote {len(out)} parquet file(s)")
    elif args.cmd == "stats":
        print(report(args.data, gap_threshold_s=args.gap))


if __name__ == "__main__":
    main()
