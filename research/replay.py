"""Market replay: render a time window of recorded data as a self-contained HTML viewer.

    python -m research.replay --coin ETH                       # last 15 minutes recorded
    python -m research.replay --coin BTC --start "2026-11-15 14:00" --minutes 30
    python -m research.replay --coin SOL --clock exchange      # exchange timestamps instead of receive time

The output is one HTML file (data embedded) that opens in any browser, no server needed.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

from hl_recorder import compact, load
from research import signals as S

TEMPLATE = Path(__file__).with_name("replay_template.html")
HL_TRADE_MAX_DELAY = "5s"


def _window(df: pd.DataFrame, start_ns: int, end_ns: int) -> pd.DataFrame:
    if df.empty:
        return df
    return df[(df.recv_ns >= start_ns) & (df.recv_ns < end_ns)].reset_index(drop=True)


def _rel_ms(ns: pd.Series, t0_ns: int) -> list[int]:
    return ((ns.to_numpy() - t0_ns) // 1_000_000).astype(np.int64).tolist()


def _decimals(prices: np.ndarray) -> int:
    """Smallest number of decimals that represents all prices (capped at 8)."""
    p = prices[np.isfinite(prices)][:2000]
    for d in range(9):
        if np.allclose(np.round(p, d), p, atol=10 ** -(d + 3)):
            return d
    return 8


def _r(x, d):
    return [None if (v is None or not math.isfinite(v)) else round(float(v), d) for v in x]


def build_payload(data: str | Path, coin: str, start: str | None = None, minutes: float = 15,
                  clock: str = "recv", refresh: bool = True) -> dict:
    data = Path(data)
    if refresh:
        compact(data, include_current=True)

    frames = {ch: load(data, ch, coin) for ch in ("bbo", "l2Book", "trades", "bn_book5", "bn_trades")}
    if frames["bbo"].empty:
        raise SystemExit(f"No bbo data for {coin} in {data}/parquet. Is the recorder running?")
    if clock == "exchange":
        frames = {k: S.exchange_clock(v) for k, v in frames.items()}

    # HL replays recent trades on every (re)connect; those are not live information.
    tr = frames["trades"]
    if not tr.empty:
        delay = tr.recv_ns - tr.time_ms.astype("int64") * 1_000_000
        frames["trades"] = tr[delay <= pd.Timedelta(HL_TRADE_MAX_DELAY).value]

    span = int(pd.Timedelta(minutes=minutes).value)
    if start is None:
        end_ns = int(frames["bbo"].recv_ns.max())
        start_ns = end_ns - span
    else:
        start_ns = pd.Timestamp(start, tz="UTC").value
        end_ns = start_ns + span
    w = {k: _window(v, start_ns, end_ns) for k, v in frames.items()}
    bbo = w["bbo"]
    if len(bbo) < 10:
        raise SystemExit(f"Only {len(bbo)} bbo rows for {coin} in that window. Pick another --start.")
    t0 = int(bbo.recv_ns.iloc[0])
    t1 = int(bbo.recv_ns.iloc[-1])
    dec = _decimals(np.concatenate([bbo.bid_px.to_numpy(), bbo.ask_px.to_numpy()]))

    out: dict = {
        "coin": coin, "clock": clock, "t0": t0 // 1_000_000, "duration_ms": (t1 - t0) // 1_000_000,
        "decimals": dec,
    }

    out["bbo"] = {"t": _rel_ms(bbo.recv_ns, t0), "bid": _r(bbo.bid_px, dec), "ask": _r(bbo.ask_px, dec),
                  "bsz": _r(bbo.bid_sz, 4), "asz": _r(bbo.ask_sz, 4)}

    book = w["l2Book"]
    if not book.empty:
        lv = 5
        def pad(col):
            return [[float(v) for v in (list(x)[:lv] + [math.nan] * (lv - len(x)))] for x in book[col]]
        out["book"] = {"t": _rel_ms(book.recv_ns, t0), "levels": lv,
                       "bpx": [_r(r, dec) for r in pad("bid_px")], "bsz": [_r(r, 4) for r in pad("bid_sz")],
                       "apx": [_r(r, dec) for r in pad("ask_px")], "asz": [_r(r, 4) for r in pad("ask_sz")]}

    tr = w["trades"]
    out["trades"] = {"t": _rel_ms(tr.recv_ns, t0), "px": _r(tr.px, dec), "sz": _r(tr.sz, 4),
                     "side": [1 if s == "B" else -1 for s in tr.side]} if not tr.empty else None

    bn = w["bn_book5"]
    if not bn.empty:
        bn_bid = bn.bid_px.str[0]
        bn_ask = bn.ask_px.str[0]
        # Binance quotes in USDT, Hyperliquid in USDC: shift Binance by the median basis
        # so both lines overlay. Only the *moves* matter for lead-lag.
        grid = S.make_grid(t0, t1, "1s")
        hl_mid = S.asof(grid, bbo.assign(m=(bbo.bid_px + bbo.ask_px) / 2), ["m"]).m
        bn_mid = S.asof(grid, bn.assign(m=(bn_bid + bn_ask) / 2), ["m"]).m
        basis = float((hl_mid - bn_mid).median())
        bn_dec = max(dec, _decimals(np.concatenate([bn_bid.to_numpy(), bn_ask.to_numpy()])))
        out["bn"] = {"t": _rel_ms(bn.recv_ns, t0), "basis": round(basis, bn_dec + 2), "decimals": bn_dec,
                     "bid": _r(bn_bid, bn_dec), "ask": _r(bn_ask, bn_dec),
                     "bpx": [_r(list(x)[:5], bn_dec) for x in bn.bid_px],
                     "bsz": [_r(list(x)[:5], 4) for x in bn.bid_sz],
                     "apx": [_r(list(x)[:5], bn_dec) for x in bn.ask_px],
                     "asz": [_r(list(x)[:5], 4) for x in bn.ask_sz]}
    else:
        out["bn"] = None

    bt = w["bn_trades"]
    if not bt.empty:
        # Binance prints far more trades than Hyperliquid; bucket them per 100 ms and side.
        b = bt.assign(bucket=(bt.recv_ns - t0) // 100_000_000, sgn=np.where(bt.side == "B", 1, -1),
                      notional=bt.px * bt.sz)
        g = b.groupby(["bucket", "sgn"]).agg(sz=("sz", "sum"), notional=("notional", "sum"),
                                             t=("recv_ns", "last")).reset_index()
        g["px"] = g.notional / g.sz
        out["bn_trades"] = {"t": _rel_ms(g.t, t0), "px": _r(g.px, dec + 2), "sz": _r(g.sz, 4),
                            "side": g.sgn.astype(int).tolist()}
    else:
        out["bn_trades"] = None

    # Indicators on a 100 ms grid, same definitions as the research notebook.
    grid = S.make_grid(t0, t1, "100ms")
    feat = S.asof(grid, S.bbo_features(bbo), ["imb1", "micro_dev_bps"])
    feat["tfi_5s"] = S.trade_flow(w["trades"], grid, "5s", max_delay=None)
    out["ind"] = {"t0": int((grid[0].value - t0) // 1_000_000), "step": 100,
                  "imb1": _r(feat.imb1, 3), "micro": _r(feat.micro_dev_bps, 3), "tfi": _r(feat.tfi_5s, 3)}
    return out


def render(payload: dict, out_path: str | Path) -> Path:
    html = TEMPLATE.read_text(encoding="utf-8")
    blob = json.dumps(payload, separators=(",", ":")).replace("</", "<\\/")
    html = html.replace("/*__PAYLOAD__*/null", blob)
    html = html.replace("<title>Market replay</title>", f"<title>{payload['coin']} market replay</title>")
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    return out_path


def main() -> None:
    ap = argparse.ArgumentParser(description="Render a market replay as a self-contained HTML file")
    ap.add_argument("--data", default="data")
    ap.add_argument("--coin", default="ETH")
    ap.add_argument("--start", default=None, help="UTC start, e.g. '2026-11-15 14:00' (default: last N minutes)")
    ap.add_argument("--minutes", type=float, default=15)
    ap.add_argument("--clock", choices=["recv", "exchange"], default="recv")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    payload = build_payload(args.data, args.coin, args.start, args.minutes, args.clock)
    stamp = pd.Timestamp(payload["t0"], unit="ms", tz="UTC").strftime("%Y%m%d-%H%M")
    out = args.out or f"replays/{args.coin}_{stamp}_{int(args.minutes)}min.html"
    path = render(payload, out)
    print(f"wrote {path} ({path.stat().st_size / 1e6:.1f} MB) - open it in your browser")


if __name__ == "__main__":
    main()
