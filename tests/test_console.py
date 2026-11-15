import ctypes
import logging
import sys
import threading
import types

from hl_recorder import console


def test_quickedit_cleared_and_restored(monkeypatch):
    state = {"mode": 0x01F7}   # typical console mode with QuickEdit (0x40) on
    calls = []

    def get_mode(handle, ptr):
        ptr._obj.value = state["mode"]
        return 1

    def set_mode(handle, mode):
        calls.append(mode); state["mode"] = mode
        return 1

    k32 = types.SimpleNamespace(GetStdHandle=lambda h: 7, GetConsoleMode=get_mode, SetConsoleMode=set_mode)
    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setattr(ctypes, "windll", types.SimpleNamespace(kernel32=k32), raising=False)
    with console.quickedit_disabled() as ok:
        assert ok
        assert not state["mode"] & console.ENABLE_QUICK_EDIT_MODE
        assert state["mode"] & console.ENABLE_EXTENDED_FLAGS
    assert calls[-1] == 0x01F7        # original mode restored


def test_quickedit_noop_off_windows(monkeypatch):
    monkeypatch.setattr(sys, "platform", "linux")
    with console.quickedit_disabled() as ok:
        assert ok is False


def test_logging_does_not_block_when_output_is_stuck(monkeypatch):
    """Simulate a frozen console: writes block forever. Logging must still return."""
    gate = threading.Event()

    class StuckStream:
        def write(self, s): gate.wait()
        def flush(self): pass

    monkeypatch.setattr(sys, "stderr", StuckStream())
    saved = logging.getLogger().handlers[:]
    listener = console.background_logging(logging.INFO, "%(message)s")
    try:
        done = threading.Event()
        t = threading.Thread(target=lambda: ([logging.info("x") for _ in range(100)], done.set()))
        t.start()
        assert done.wait(2), "logging blocked the caller"
    finally:
        gate.set()
        listener.stop()
        logging.getLogger().handlers[:] = saved
