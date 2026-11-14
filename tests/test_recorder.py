import asyncio
import gzip
import json
from pathlib import Path

import orjson
import pytest
import websockets

from hl_recorder import compact, load, recorder
from hl_recorder.recorder import Recorder, TidDeduper, build_groups
from hl_recorder.stats import outage_gaps, report
from hl_recorder.writer import HourlyWriter, hour_key

FIX = json.loads((Path(__file__).parent / "fixtures" / "sample_messages.json").read_text())
HOUR_NS = 3600 * 10**9
T0 = 1_794_600_000 * 10**9  # some fixed UTC instant


# ---------------------------------------------------------------- writer
def test_rotation_compresses_previous_hour(tmp_path):
    w = HourlyWriter(tmp_path)
    w.write("bbo", "SOL", T0, {"x": 1})
    w.write("bbo", "SOL", T0 + HOUR_NS, {"x": 2})
    w.close()
    d0, h0 = hour_key(T0)
    d1, h1 = hour_key(T0 + HOUR_NS)
    base = tmp_path / "raw" / "bbo" / "SOL"
    assert (base / d0 / f"{h0}.jsonl.gz").exists()
    assert not (base / d0 / f"{h0}.jsonl").exists()
    assert (base / d1 / f"{h1}.jsonl").exists()  # current hour stays plain


def test_recover_and_gzip_append(tmp_path):
    w = HourlyWriter(tmp_path)
    w.write("bbo", "SOL", T0, {"x": 1})
    w.close()
    w = HourlyWriter(tmp_path)
    assert len(w.recover(now_ns=T0 + 5 * HOUR_NS)) == 1
    # a second leftover for the same hour is appended as a new gzip member
    w.write("bbo", "SOL", T0, {"x": 2})
    w.close()
    HourlyWriter(tmp_path).recover(now_ns=T0 + 5 * HOUR_NS)
    d, h = hour_key(T0)
    with gzip.open(tmp_path / "raw" / "bbo" / "SOL" / d / f"{h}.jsonl.gz") as fh:
        assert [orjson.loads(l)["d"]["x"] for l in fh] == [1, 2]


def test_recover_skips_current_hour(tmp_path):
    w = HourlyWriter(tmp_path)
    w.write("bbo", "SOL", T0, {"x": 1})
    w.close()
    assert HourlyWriter(tmp_path).recover(now_ns=T0) == []


# ---------------------------------------------------------------- dedupe
def test_dedupe_drops_replayed_trades():
    d = TidDeduper(maxlen=3)
    trades = [{"tid": i} for i in range(3)]
    assert len(d.filter("SOL", trades)) == 3
    assert d.filter("SOL", trades) == []
    assert len(d.filter("ETH", trades)) == 3  # per coin
    d.filter("SOL", [{"tid": 10}, {"tid": 11}, {"tid": 12}])
    assert len(d.filter("SOL", [{"tid": 0}])) == 1  # evicted from the window


# ---------------------------------------------------------------- handle + compaction
def _feed(rec: Recorder, start_ns: int):
    t = start_ns
    for ch in ("l2Book", "bbo", "trades"):
        for m in FIX[ch]:
            rec.handle(json.dumps(m), t)
            t += 10**8
    rec.handle(json.dumps(FIX["trades"][0]), t)  # replay -> deduped
    rec.handle('{"channel":"pong"}', t)


def test_handle_compact_load(tmp_path):
    async def go():
        rec = Recorder(["SOL"], tmp_path)
        _feed(rec, T0)
        rec.writer.close(compress_current=True)
        rec._events.close()
    asyncio.run(go())

    assert compact(tmp_path)
    assert compact(tmp_path) == []  # idempotent

    book = load(tmp_path, "l2Book", "SOL")
    assert len(book) == 3
    assert book.bid_px[0][0] < book.ask_px[0][0]
    assert len(book.bid_px[0]) == 5  # fast feed = 5 levels

    bbo = load(tmp_path, "bbo", "SOL")
    assert len(bbo) == 3 and (bbo.bid_px < bbo.ask_px).all()

    trades = load(tmp_path, "trades", "SOL")
    n_unique = len({t["tid"] for m in FIX["trades"] for t in m["data"]})
    assert len(trades) == n_unique
    assert set(trades.side) <= {"B", "A"}
    assert trades.tid.is_unique


# ---------------------------------------------------------------- live reconnect against a fake server
@pytest.mark.timeout(30)
def test_reconnects_after_stale_and_drop(tmp_path, monkeypatch):
    monkeypatch.setattr(recorder, "STALE_AFTER_S", 1)
    monkeypatch.setattr(recorder, "PING_INTERVAL_S", 0.3)
    monkeypatch.setattr(recorder, "MAX_BACKOFF_S", 0.2)
    connections = []

    async def handler(ws):
        connections.append(ws)
        n = len(connections)
        subs = [json.loads(await ws.recv()) for _ in range(3)]
        assert {s["subscription"]["type"] for s in subs} == {"l2Book", "bbo", "trades"}
        for m in FIX["bbo"]:
            await ws.send(json.dumps(m))
        if n == 1:
            await asyncio.sleep(3)  # go silent -> stale detection
        elif n == 2:
            await ws.close()        # hard drop
        else:
            await asyncio.sleep(10)

    async def go():
        async with websockets.serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            rec = Recorder(["SOL"], tmp_path, channels=("l2Book", "bbo", "trades"),
                           url=f"ws://127.0.0.1:{port}")
            task = asyncio.create_task(rec.run())
            while len(connections) < 3:
                await asyncio.sleep(0.05)
            await asyncio.sleep(0.3)
            rec.stop()
            await asyncio.wait_for(task, 5)
            return rec

    rec = asyncio.run(go())
    events = [orjson.loads(l)["event"] for l in (tmp_path / "events.jsonl").read_bytes().splitlines()]
    assert events.count("connect") == 3
    assert events.count("disconnect") == 2
    assert events[-1] == "shutdown"
    assert rec.counts["bbo"] == 9
    assert len(outage_gaps(tmp_path)) == 2
    assert "Outages" in report(tmp_path, gap_threshold_s=0)


def test_depth_connection_routing(tmp_path):
    groups = build_groups(["BTC", "ETH"], ("l2Book", "l2Book20", "bbo", "trades"))
    assert len(groups["main"]) == 6 and len(groups["depth"]) == 2
    assert all("fast" not in s["subscription"] for s in groups["depth"])
    assert all(s["subscription"].get("fast") for s in groups["main"] if s["subscription"]["type"] == "l2Book")

    async def go():
        rec = Recorder(["SOL"], tmp_path)
        rec.handle(json.dumps(FIX["l2Book"][0]), T0, group="depth")
        rec.handle(json.dumps(FIX["l2Book"][0]), T0, group="main")
        rec.writer.close(compress_current=True)
        rec._events.close()
        return rec
    rec = asyncio.run(go())
    assert rec.counts == {"l2Book20": 1, "l2Book": 1}
    compact(tmp_path)
    assert len(load(tmp_path, "l2Book20", "SOL")) == 1


def test_dedupe_survives_crash(tmp_path):
    """Simulate SIGKILL: no clean close, then a new Recorder gets the server replay."""
    async def first():
        rec = Recorder(["SOL"], tmp_path)
        for m in FIX["trades"]:
            rec.handle(json.dumps(m), T0)
        rec.writer.flush()  # data on disk, but process 'dies' without close()
    asyncio.run(first())

    async def second():
        rec = Recorder(["SOL"], tmp_path)
        rec.seed_dedupe_from_disk()
        for m in FIX["trades"]:
            rec.handle(json.dumps(m), T0 + 10**9)
        return rec
    rec = asyncio.run(second())
    assert rec.counts["trades"] == 0


def test_compact_include_current_tolerates_partial_line(tmp_path):
    w = HourlyWriter(tmp_path)
    for m in FIX["bbo"]:
        w.write("bbo", "SOL", T0, m["data"])
    w.close()
    d, h = hour_key(T0)
    with open(tmp_path / "raw" / "bbo" / "SOL" / d / f"{h}.jsonl", "ab") as fh:
        fh.write(b'{"r": 1, "d": {"coin": "SO')          # writer caught mid-line
    assert compact(tmp_path) == []                      # closed hours only by default
    assert len(compact(tmp_path, include_current=True)) == 1
    assert len(load(tmp_path, "bbo", "SOL")) == 3
