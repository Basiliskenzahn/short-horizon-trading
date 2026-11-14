"""CLI:  python -m hl_recorder record|compact|stats ..."""

from __future__ import annotations

import argparse
import asyncio
import logging

from .compact import compact
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
    r.add_argument("--channels", nargs="+", default=list(CHANNELS), choices=list(CHANNELS))

    c = sub.add_parser("compact", help="convert closed raw hours to Parquet")
    c.add_argument("--overwrite", action="store_true")

    s = sub.add_parser("stats", help="coverage and gap report")
    s.add_argument("--gap", type=float, default=5.0, help="report outages longer than this (s)")

    args = ap.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.cmd == "record":
        asyncio.run(record(args.coins, args.data, tuple(args.channels)))
    elif args.cmd == "compact":
        out = compact(args.data, overwrite=args.overwrite)
        print(f"wrote {len(out)} parquet file(s)")
    elif args.cmd == "stats":
        print(report(args.data, gap_threshold_s=args.gap))


if __name__ == "__main__":
    main()
