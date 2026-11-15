import asyncio
import json
import time
from pathlib import Path

import orjson
import pytest
import websockets

from hl_recorder import binance, compact, load, recorder
from hl_recorder.recorder import Recorder

BN = json.loads((Path(__file__).parent / "fixtures" / "binance_messages.json").read_text())
T0 = 1_794_600_000 * 10**9


def test_groups_split_book_and_trade_endpoints():
    groups = binance.build_groups(["BTC", "SOL"], ("bn_book5", "bn_trades", "bn_bbo"))
    assert groups["binance_book"].startswith(binance.BOOK_URL)
    assert groups["binance_trades"].startswith(binance.TRADES_URL)
    assert "/market/" in groups["binance_trades"]          # aggTrade only works here
    assert "btcusdt@depth5@100ms" in groups["binance_book"]
    assert "solusdt@bookTicker" in groups["binance_book"]
    assert "aggTrade" not in groups["binance_book"]


def test_route_maps_symbol_to_coin():
    coin_of = {"SOLUSDT": "SOL"}
    assert binance.route(BN["bn_book5"][0], coin_of)[:2] == ("bn_book5", "SOL")
    assert binance.route(BN["bn_trades"][0], coin_of)[:2] == ("bn_trades", "SOL")
    assert binance.route(BN["bn_bbo"][0], coin_of)[:2] == ("bn_bbo", "SOL")
    assert binance.route(BN["bn_book5"][0], {}) is None     # unknown symbol
    assert binance.route({"result": None, "id": 1}, coin_of) is None


def test_handle_and_compact(tmp_path):
    async def go():
        rec = Recorder(["SOL"], tmp_path, channels=(), binance_channels=binance.BN_CHANNELS)
        t = T0
        for ch in ("bn_book5", "bn_bbo", "bn_trades"):
            for m in BN[ch]:
                rec.handle(json.dumps(m), t, group=binance.BN_SPEC[ch][0])
                t += 10**8
        rec.handle(json.dumps(BN["bn_trades"][0]), t, group="binance_trades")  # duplicate agg id
        rec.writer.close(compress_current=True)
        rec._events.close()
    asyncio.run(go())
    compact(tmp_path)

    book = load(tmp_path, "bn_book5", "SOL")
    assert len(book) == 3 and len(book.bid_px[0]) == 5
    assert book.bid_px[0][0] < book.ask_px[0][0]

    bbo = load(tmp_path, "bn_bbo", "SOL")
    assert len(bbo) == 3 and (bbo.bid_px <= bbo.ask_px).all()

    trades = load(tmp_path, "bn_trades", "SOL")
    assert len(trades) == 3 and trades.agg_id.is_unique
    raw = BN["bn_trades"][0]["data"]
    assert trades.side[0] == ("A" if raw["m"] else "B")   # m=True: buyer was maker -> seller aggressed


def test_silent_stream_watchdog(tmp_path, monkeypatch):
    monkeypatch.setattr(recorder, "SILENT_WARN_S", 10)

    async def go():
        rec = Recorder(["SOL"], tmp_path, channels=("bbo",), binance_channels=("bn_trades",))
        rec._connected_since = {"main": 0.0, "binance_trades": 0.0}
        rec._last_seen[("bbo", "SOL")] = 15.0          # bbo alive, bn_trades never delivered
        rec.check_silent(now=20.0)
        rec._last_seen[("bn_trades", "SOL")] = 21.0    # stream comes back
        rec.check_silent(now=22.0)
        rec._events.close()
        rec.writer.close()
    asyncio.run(go())
    events = [orjson.loads(l) for l in (tmp_path / "events.jsonl").read_bytes().splitlines()]
    silent = [e for e in events if e["event"] == "silent_stream"]
    assert [(e["channel"], e["coin"]) for e in silent] == [("bn_trades", "SOL")]
    assert any(e["event"] == "stream_resumed" for e in events)


@pytest.mark.timeout(30)
def test_binance_connection_against_fake_server(tmp_path, monkeypatch):
    """Binance groups subscribe via the URL (no messages sent) and survive a drop."""
    monkeypatch.setattr(recorder, "MAX_BACKOFF_S", 0.2)
    paths = []

    async def handler(ws):
        paths.append(ws.request.path)
        msgs = BN["bn_trades"] if "/market/" in ws.request.path else BN["bn_book5"]
        now = int(time.time() * 1000)
        for m in msgs:
            m = {**m, "data": {**m["data"], "E": now, "T": now}}  # fresh, not "late"
            await ws.send(json.dumps(m))
        if len(paths) <= 2:
            await ws.close()            # drop each connection once
        else:
            await asyncio.sleep(10)

    async def go():
        async with websockets.serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            base = f"ws://127.0.0.1:{port}"
            urls = {g: u.replace("wss://fstream.binance.com", base)
                    for g, u in binance.build_groups(["SOL"], binance.BN_DEFAULT).items()}
            rec = Recorder(["SOL"], tmp_path, channels=(), binance_channels=binance.BN_DEFAULT,
                           binance_urls=urls)
            task = asyncio.create_task(rec.run())
            while len(paths) < 4:
                await asyncio.sleep(0.05)
            await asyncio.sleep(0.3)
            rec.stop()
            await asyncio.wait_for(task, 5)
            return rec

    rec = asyncio.run(go())
    assert sum("/market/" in p for p in paths) == 2
    assert rec.counts["bn_trades"] == 6 and rec.counts["bn_book5"] == 6
