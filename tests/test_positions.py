import asyncio

import orjson
import pyarrow.parquet as pq

from hl_recorder.compact import compact
from hl_recorder.positions import ActivityTracker, WalletPoller, read_pinned, slim
from hl_recorder.recorder import Recorder, build_groups

A, B, C = "0x" + "a" * 40, "0x" + "b" * 40, "0x" + "c" * 40


def test_ctx_is_subscribed_and_stored(tmp_path):
    subs = build_groups(["INJ"], ["ctx"])["main"]
    assert subs[0]["subscription"] == {"type": "activeAssetCtx", "coin": "INJ"}
    rec = Recorder(["INJ"], tmp_path, channels=("ctx",))
    msg = {"channel": "activeAssetCtx", "data": {"coin": "INJ", "ctx": {
        "funding": "0.0000125", "premium": "-0.0003", "oraclePx": "7.313", "markPx": "7.3096", "midPx": "7.3084",
        "impactPxs": ["7.30604", "7.31062"], "openInterest": "882900.2", "dayNtlVlm": "4290437.27"}}}
    rec.handle(orjson.dumps(msg), 1_794_960_000_000_000_000)
    rec.writer.close(compress_current=True)
    compact(tmp_path)
    t = pq.read_table(next((tmp_path / "parquet" / "ctx" / "INJ").rglob("*.parquet"))).to_pylist()[0]
    assert t["oracle_px"] == 7.313 and t["impact_ask"] == 7.31062 and t["open_interest"] == 882900.2


def test_activity_tracker_ranks_by_notional():
    tr = ActivityTracker()
    tr.add_trades([{"px": "10", "sz": "5", "users": [A, B]}, {"px": "10", "sz": "1", "users": [C, B]}], now=0)
    assert tr.top(2) == [B, A]


def test_trades_feed_the_tracker(tmp_path):
    rec = Recorder(["INJ"], tmp_path, channels=("trades",), track_wallets=5)
    msg = {"channel": "trades", "data": [{"coin": "INJ", "side": "B", "px": "7", "sz": "3", "time": 1,
                                          "tid": 1, "hash": "h", "users": [A, B]}]}
    rec.handle(orjson.dumps(msg), 1_794_960_000_000_000_000)
    assert set(rec.activity.top(5)) == {A, B}
    rec.writer.close()


def test_pinned_file_and_slim(tmp_path):
    f = tmp_path / "w.txt"
    f.write_text(f"# tracked\n{A.upper().replace('0X', '0x')}  # maker\nnot-an-address\n")
    assert read_pinned(f) == [A]
    s = slim(A, {"time": 5, "marginSummary": {"accountValue": "100", "totalNtlPos": "50"},
                 "assetPositions": [{"position": {"coin": "ETH", "szi": "-1", "entryPx": "1800",
                                                  "positionValue": "1790", "unrealizedPnl": "10",
                                                  "leverage": {"value": 5}}}]})
    assert s == {"u": A, "t": 5, "av": "100", "ntl": "50", "p": [["ETH", "-1", "1800", "1790", "10", 5]]}
    assert slim(A, {"assetPositions": [{"position": {"coin": "DOGE"}}]}, coins={"ETH"})["p"] == []


def test_poller_writes_positions_and_compacts(tmp_path):
    rec = Recorder(["INJ"], tmp_path, channels=("trades",))
    rec.activity.add_trades([{"px": "1", "sz": "10", "users": [A, B]}], now=0)
    calls = []

    def fake_fetch(body):
        calls.append(body["user"])
        if body["user"] == A:
            return {"time": 7, "marginSummary": {"accountValue": "10", "totalNtlPos": "0"}, "assetPositions": []}
        return {"time": 8, "marginSummary": {"accountValue": "20", "totalNtlPos": "5"},
                "assetPositions": [{"position": {"coin": "INJ", "szi": "2", "entryPx": "2.5", "positionValue": "5",
                                                 "unrealizedPnl": "0", "leverage": {"value": 3}}}]}

    p = WalletPoller(rec._store, rec.event, rec.activity, top=2, interval_s=0.2, fetch=fake_fetch, warmup_s=0)

    async def go():
        stop = asyncio.Event()
        task = asyncio.create_task(p.run(stop))
        await asyncio.sleep(0.5)
        stop.set()
        await asyncio.wait_for(task, 2)

    asyncio.run(go())
    assert {A, B} <= set(calls)
    rec.writer.close(compress_current=True)
    compact(tmp_path)
    rows = pq.read_table(next((tmp_path / "parquet" / "positions" / "wallets").rglob("*.parquet"))).to_pylist()
    flat = [r for r in rows if r["user"] == A][0]
    held = [r for r in rows if r["user"] == B][0]
    assert flat["coin"] == "" and flat["account_value"] == 10.0
    assert held["coin"] == "INJ" and held["szi"] == 2.0 and held["leverage"] == 3.0
