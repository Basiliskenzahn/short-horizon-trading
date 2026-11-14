"""Feature and evaluation helpers for short-horizon signal research.

Conventions
-----------
* Clock = local receive time (`recv_ns`). Features are what you could have known
  at that moment on this machine; Hyperliquid and Binance are compared on the same
  clock, which is the one a live strategy would trade on.
* Everything is sampled onto a regular grid with backward as-of joins, so a grid
  point only ever sees data received at or before it (no lookahead).
* Returns are log mid-price returns in basis points.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

BPS = 1e4


# ----------------------------------------------------------------------------- holdout
def split_holdout(df: pd.DataFrame, holdout_start: str | None) -> pd.DataFrame:
    """Drop everything at/after `holdout_start` (UTC, e.g. '2026-12-08').
    Call this on every frame before research so the holdout stays untouched."""
    if holdout_start is None or df.empty:
        return df
    cutoff = pd.Timestamp(holdout_start, tz="UTC").value
    return df[df["recv_ns"] < cutoff]


def exchange_clock(df: pd.DataFrame) -> pd.DataFrame:
    """Re-time a frame on the exchange timestamp instead of local receive time.
    Use it to separate *price* lead-lag from *feed* latency: on the receive clock a
    venue whose feed arrives later looks like it lags even if its prices don't."""
    if df.empty:
        return df
    out = df.copy()
    out["recv_ns_local"] = out["recv_ns"]
    out["recv_ns"] = out["time_ms"].astype("int64") * 1_000_000
    return out.sort_values("recv_ns", kind="stable")


# ----------------------------------------------------------------------------- grid
def make_grid(start_ns: int, end_ns: int, freq: str = "100ms") -> pd.DatetimeIndex:
    return pd.date_range(pd.Timestamp(start_ns, tz="UTC").ceil(freq),
                         pd.Timestamp(end_ns, tz="UTC"), freq=freq)


def asof(grid: pd.DatetimeIndex, df: pd.DataFrame, cols: list[str],
         max_age: str | None = "2s") -> pd.DataFrame:
    """Last known value of `cols` at each grid time (backward as-of on recv time).
    Values older than `max_age` become NaN, so outages don't get forward-filled."""
    left = pd.DataFrame({"t": grid})
    right = df[["recv_ns", *cols]].copy()
    right["t"] = pd.to_datetime(right["recv_ns"], utc=True)
    right = right.sort_values("t")
    out = pd.merge_asof(left, right.drop(columns="recv_ns"), on="t", direction="backward",
                        tolerance=pd.Timedelta(max_age) if max_age else None)
    return out.set_index("t")


# ----------------------------------------------------------------------------- features
def bbo_features(bbo: pd.DataFrame) -> pd.DataFrame:
    """Top-of-book features from a bbo-style frame (bid_px, bid_sz, ask_px, ask_sz)."""
    out = bbo[["recv_ns"]].copy()
    mid = (bbo.bid_px + bbo.ask_px) / 2
    depth = bbo.bid_sz + bbo.ask_sz
    out["mid"] = mid
    out["spread_bps"] = (bbo.ask_px - bbo.bid_px) / mid * BPS
    out["imb1"] = (bbo.bid_sz - bbo.ask_sz) / depth                # L1 imbalance in [-1, 1]
    micro = (bbo.bid_px * bbo.ask_sz + bbo.ask_px * bbo.bid_sz) / depth
    out["micro_dev_bps"] = (micro - mid) / mid * BPS                # microprice - mid
    return out


def book_imbalance(book: pd.DataFrame, levels: int = 5, decay: float = 0.5) -> pd.DataFrame:
    """Depth imbalance over the top `levels`, level k weighted by decay**k."""
    w = decay ** np.arange(levels)

    def side(col):
        return np.array([np.pad(np.asarray(x[:levels], float), (0, max(0, levels - len(x))))
                         for x in book[col]]) @ w

    bid, ask = side("bid_sz"), side("ask_sz")
    out = book[["recv_ns"]].copy()
    out[f"imb{levels}"] = (bid - ask) / (bid + ask)
    return out


def trade_flow(trades: pd.DataFrame, grid: pd.DatetimeIndex, window: str = "5s",
               max_delay: str | None = "5s") -> pd.Series:
    """Trade-flow imbalance over the trailing `window` at each grid time:
    (buy-aggressor volume - sell-aggressor volume) / total volume, in [-1, 1].

    `max_delay` drops trades that arrived much later than they happened: on every
    (re)connect Hyperliquid replays recent trades, which are not fresh information."""
    if trades.empty:
        return pd.Series(np.nan, index=grid, name=f"tfi_{window}")
    t = trades
    if max_delay is not None and "time_ms" in t:
        delay_ns = t.recv_ns - t.time_ms.astype("int64") * 1_000_000
        t = t[delay_ns <= pd.Timedelta(max_delay).value]
    t = t.sort_values("recv_ns")
    signed = np.where(t.side.values == "B", 1.0, -1.0) * t.sz.values
    cum = pd.DataFrame({"recv_ns": t.recv_ns.values,
                        "cs": np.cumsum(signed), "cv": np.cumsum(t.sz.values)})
    now = asof(grid, cum, ["cs", "cv"], max_age=None).fillna(0.0)
    past = asof(grid - pd.Timedelta(window), cum, ["cs", "cv"], max_age=None).fillna(0.0)
    vol = now.cv.values - past.cv.values
    flow = now.cs.values - past.cs.values
    with np.errstate(invalid="ignore", divide="ignore"):
        tfi = np.where(vol > 0, flow / vol, 0.0)
    return pd.Series(tfi, index=grid, name=f"tfi_{window}")


# ----------------------------------------------------------------------------- returns
def forward_returns(mid: pd.Series, horizons: list[str]) -> pd.DataFrame:
    """Log mid return from t to t+h in bps, for a mid series on a regular grid."""
    step = mid.index[1] - mid.index[0]
    logm = np.log(mid)
    out = {}
    for h in horizons:
        k = int(pd.Timedelta(h) / step)
        out[f"fwd_{h}"] = (logm.shift(-k) - logm) * BPS
    return pd.DataFrame(out, index=mid.index)


def past_returns(mid: pd.Series, horizon: str) -> pd.Series:
    step = mid.index[1] - mid.index[0]
    k = int(pd.Timedelta(horizon) / step)
    logm = np.log(mid)
    return ((logm - logm.shift(k)) * BPS).rename(f"past_{horizon}")


# ----------------------------------------------------------------------------- evaluation
def decile_table(feature: pd.Series, fwd: pd.Series, q: int = 10, demean: bool = False) -> pd.DataFrame:
    """Mean forward return per feature quantile bucket. A real signal shows a clean
    monotonic pattern. `demean` subtracts the unconditional mean return first, so a
    trending sample doesn't tilt every bucket. Note: grid samples overlap, so `n`
    overstates independent observations."""
    df = pd.DataFrame({"f": feature, "r": fwd}).dropna()
    if demean:
        df["r"] -= df.r.mean()
    if df.f.nunique() < q:
        return pd.DataFrame()
    df["bucket"] = pd.qcut(df.f.rank(method="first"), q, labels=False) + 1
    g = df.groupby("bucket")
    return pd.DataFrame({"feature_mean": g.f.mean(), "fwd_mean_bps": g.r.mean(),
                         "fwd_std_bps": g.r.std(), "n": g.r.size()})


def ic(feature: pd.Series, fwd: pd.Series) -> float:
    """Spearman rank correlation between feature and forward return."""
    df = pd.DataFrame({"f": feature, "r": fwd}).dropna()
    if len(df) < 10:
        return np.nan
    return df.f.rank().corr(df.r.rank())


def effective_n(n_samples: int, horizon: str, freq: str = "100ms") -> int:
    """Rough count of non-overlapping forward-return windows."""
    return max(1, int(n_samples * pd.Timedelta(freq) / pd.Timedelta(horizon)))


def lead_lag(a: pd.Series, b: pd.Series, max_lag: str = "2s", ret: str = "100ms") -> pd.Series:
    """Correlation of `ret`-returns of a(t) with b(t - lag) for lags in [-max_lag, max_lag].
    Peak at a positive lag => b leads a by that much. Both inputs on the same regular grid."""
    step = a.index[1] - a.index[0]
    k_ret = int(pd.Timedelta(ret) / step)
    ra = np.log(a).diff(k_ret)
    rb = np.log(b).diff(k_ret)
    k_max = int(pd.Timedelta(max_lag) / step)
    lags = range(-k_max, k_max + 1)
    vals = [ra.corr(rb.shift(k)) for k in lags]
    return pd.Series(vals, index=[k * step.total_seconds() for k in lags], name="corr")
