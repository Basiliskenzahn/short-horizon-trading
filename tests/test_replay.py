import asyncio
import copy
import json
from pathlib import Path

from hl_recorder.recorder import Recorder
from research.replay import build_payload, render

FIX = Path(__file__).parent / "fixtures"
HL = json.loads((FIX / "sample_messages.json").read_text())
BN = json.loads((FIX / "binance_messages.json").read_text())
T0 = 1_794_600_000 * 10**9


def _synthetic_recording(root):
    """60 s of data built from the captured messages, one update every 200 ms."""
    async def go():
        rec = Recorder(["SOL"], root, binance_channels=("bn_book5", "bn_trades"))
        for i in range(300):
            recv = T0 + i * 200_000_000
            ms = recv // 1_000_000
            for ch, group in (("bbo", "main"), ("l2Book", "main")):
                m = copy.deepcopy(HL[ch][i % 3]); m["data"]["time"] = ms
                rec.handle(json.dumps(m), recv, group)
            if i % 5 == 0:
                m = copy.deepcopy(HL["trades"][0])
                for k, t in enumerate(m["data"]):
                    t["time"], t["tid"] = ms, i * 1000 + k
                rec.handle(json.dumps(m), recv, "main")
            for ch, group in (("bn_book5", "binance_book"), ("bn_trades", "binance_trades")):
                m = copy.deepcopy(BN[ch][i % 3]); m["data"]["T"] = m["data"]["E"] = ms
                if ch == "bn_trades":
                    m["data"]["a"] = i
                rec.handle(json.dumps(m), recv, group)
        rec.writer.close()
        rec._events.close()
    asyncio.run(go())


def test_payload_and_render(tmp_path):
    _synthetic_recording(tmp_path)
    p = build_payload(tmp_path, "SOL", minutes=5)
    assert p["coin"] == "SOL" and 55_000 < p["duration_ms"] < 61_000
    assert len(p["bbo"]["t"]) == len(p["bbo"]["bid"]) > 250
    assert p["book"]["levels"] == 5 and len(p["book"]["bpx"][0]) == 5
    assert p["trades"] and set(p["trades"]["side"]) <= {1, -1}
    assert p["bn"] and isinstance(p["bn"]["basis"], float) and p["bn"]["decimals"] >= p["decimals"]
    assert p["bn_trades"] and len(p["ind"]["imb1"]) > 500
    assert all(b <= a for b, a in zip(p["bbo"]["bid"], p["bbo"]["ask"]))

    out = render(p, tmp_path / "r.html")
    html = out.read_text()
    assert "/*__PAYLOAD__*/" not in html and '"coin":"SOL"' in html


def test_window_selection(tmp_path):
    _synthetic_recording(tmp_path)
    start = "2026-11-14 00:00"  # before the data -> clear error, not a broken file
    import pytest
    with pytest.raises(SystemExit):
        build_payload(tmp_path, "SOL", start=start, minutes=1)
