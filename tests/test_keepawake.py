import ctypes
import sys
import types

from hl_recorder import keepawake


def test_windows_sets_and_restores_execution_state(monkeypatch):
    calls = []
    fake = types.SimpleNamespace(kernel32=types.SimpleNamespace(
        SetThreadExecutionState=lambda flags: calls.append(flags) or 1))
    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setattr(ctypes, "windll", fake, raising=False)
    with keepawake.keep_awake(True) as ok:
        assert ok
        assert calls == [keepawake.ES_CONTINUOUS | keepawake.ES_SYSTEM_REQUIRED]
    assert calls[-1] == keepawake.ES_CONTINUOUS          # released on exit


def test_disabled_and_unsupported_platforms(monkeypatch):
    with keepawake.keep_awake(False) as ok:
        assert ok is False
    monkeypatch.setattr(sys, "platform", "linux")
    with keepawake.keep_awake(True) as ok:
        assert ok is False
