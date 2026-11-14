import numpy as np
import pandas as pd
import pytest

from research import signals as S

T0 = 1_794_600_000 * 10**9
MS = 10**6


def _frame(times_ms, **cols):
    return pd.DataFrame({"recv_ns": [T0 + t * MS for t in times_ms], **cols})


def test_asof_never_looks_ahead_and_respects_max_age():
    df = _frame([0, 250, 5000], x=[1.0, 2.0, 3.0])
    grid = S.make_grid(T0, T0 + 5000 * MS, "100ms")
    out = S.asof(grid, df, ["x"], max_age="2s")
    assert out.x.iloc[0] == 1.0                      # t=0
    assert out.x.iloc[2] == 1.0                      # t=200ms: 250ms value not visible yet
    assert out.x.iloc[3] == 2.0                      # t=300ms
    assert np.isnan(out.x.iloc[30])                  # t=3s: last value 2.75s old > max_age
    assert out.x.iloc[-1] == 3.0


def test_bbo_features():
    bbo = _frame([0], bid_px=[99.0], ask_px=[101.0], bid_sz=[3.0], ask_sz=[1.0])
    f = S.bbo_features(bbo).iloc[0]
    assert f.mid == 100.0
    assert f.spread_bps == pytest.approx(200.0)
    assert f.imb1 == pytest.approx(0.5)
    assert f.micro_dev_bps > 0                       # heavy bid -> microprice above mid


def test_forward_and_past_returns():
    idx = pd.date_range("2026-11-14", periods=5, freq="1s", tz="UTC")
    mid = pd.Series([100, 101, 102, 103, 104], index=idx, dtype=float)
    fwd = S.forward_returns(mid, ["2s"])
    assert fwd["fwd_2s"].iloc[0] == pytest.approx(np.log(102 / 100) * 1e4)
    assert np.isnan(fwd["fwd_2s"].iloc[-1])
    assert S.past_returns(mid, "1s").iloc[1] == pytest.approx(np.log(101 / 100) * 1e4)


def test_trade_flow_window():
    trades = _frame([100, 200, 3000], side=["B", "B", "A"], sz=[1.0, 1.0, 2.0])
    grid = S.make_grid(T0, T0 + 4000 * MS, "1s")
    tfi = S.trade_flow(trades, grid, window="2s")
    assert tfi.iloc[1] == 1.0                        # t=1s: only buys in window
    assert tfi.iloc[4] == -1.0                       # t=4s: buys left the window, one sell
    assert tfi.iloc[0] == 0.0                        # no volume yet


def test_book_imbalance_padding():
    book = pd.DataFrame({"recv_ns": [T0], "bid_sz": [[2.0, 1.0]], "ask_sz": [[1.0, 1.0, 1.0]]})
    v = S.book_imbalance(book, levels=3, decay=1.0)["imb3"].iloc[0]
    assert v == pytest.approx((3 - 3) / 6)


def test_lead_lag_recovers_known_lag():
    rng = np.random.default_rng(0)
    idx = pd.date_range("2026-11-14", periods=5000, freq="100ms", tz="UTC")
    leader = pd.Series(100 * np.exp(np.cumsum(rng.normal(0, 1e-4, len(idx)))), index=idx)
    follower = leader.shift(3).bfill()               # follows 300 ms later
    corr = S.lead_lag(follower, leader, max_lag="1s", ret="100ms")
    assert corr.idxmax() == pytest.approx(0.3)


def test_decile_table_monotonic_on_predictive_feature():
    rng = np.random.default_rng(1)
    f = pd.Series(rng.normal(size=20_000))
    r = 0.5 * f + rng.normal(size=f.size)
    tab = S.decile_table(f, r)
    assert tab.fwd_mean_bps.is_monotonic_increasing
    assert S.ic(f, r) > 0.3


def test_holdout_split():
    df = pd.DataFrame({"recv_ns": [pd.Timestamp("2026-12-07", tz="UTC").value,
                                   pd.Timestamp("2026-12-08", tz="UTC").value]})
    assert len(S.split_holdout(df, "2026-12-08")) == 1
    assert len(S.split_holdout(df, None)) == 2


def test_trade_flow_drops_replayed_trades():
    trades = _frame([100, 200], side=["B", "A"], sz=[1.0, 5.0])
    trades["time_ms"] = [T0 // MS + 100, T0 // MS + 200 - 20_000]   # second one is a 20 s old replay
    grid = S.make_grid(T0, T0 + 1000 * MS, "1s")
    assert S.trade_flow(trades, grid, "2s").iloc[1] == 1.0
    assert S.trade_flow(trades, grid, "2s", max_delay=None).iloc[1] == pytest.approx((1 - 5) / 6)


def test_exchange_clock_and_demean():
    df = _frame([500, 100], x=[1.0, 2.0])
    df["time_ms"] = [T0 // MS + 10, T0 // MS + 20]
    ex = S.exchange_clock(df)
    assert list(ex.x) == [1.0, 2.0] and ex.recv_ns.is_monotonic_increasing
    f = pd.Series(np.arange(100.0)); r = pd.Series(np.full(100, 7.0))
    assert S.decile_table(f, r, demean=True).fwd_mean_bps.abs().max() < 1e-9
