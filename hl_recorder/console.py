"""Console safety for long-running recordings.

Two ways a console can freeze a recorder, both seen in practice on Windows:

1. QuickEdit: clicking into a classic console window (or selecting text to copy it)
   puts it in selection mode, and *any write to the console blocks* until a key is
   pressed. The recorder's once-a-minute log line then stalls the whole event loop,
   and with it the recording. `quickedit_disabled()` switches QuickEdit off for this
   console while the recorder runs and restores the previous mode afterwards.
2. Any other blocked console write. `background_logging()` moves log output to a
   separate thread, so a stuck console can only delay log lines, never the recording.
"""

from __future__ import annotations

import logging
import logging.handlers
import queue
import sys
from contextlib import contextmanager

ENABLE_QUICK_EDIT_MODE = 0x0040
ENABLE_EXTENDED_FLAGS = 0x0080
STD_INPUT_HANDLE = -10


@contextmanager
def quickedit_disabled():
    """Disable QuickEdit selection for this console (Windows only); no-op elsewhere."""
    if sys.platform != "win32":
        yield False
        return
    import ctypes
    from ctypes import wintypes

    k32 = ctypes.windll.kernel32
    handle = k32.GetStdHandle(STD_INPUT_HANDLE)
    old = wintypes.DWORD()
    if not k32.GetConsoleMode(handle, ctypes.byref(old)):
        yield False  # not attached to a classic console (e.g. output redirected)
        return
    new = (old.value & ~ENABLE_QUICK_EDIT_MODE) | ENABLE_EXTENDED_FLAGS
    ok = bool(k32.SetConsoleMode(handle, new))
    try:
        yield ok
    finally:
        if ok:
            k32.SetConsoleMode(handle, old.value)


def background_logging(level: int, fmt: str) -> logging.handlers.QueueListener:
    """Route all logging through a queue; a daemon thread writes it to stderr."""
    q: queue.SimpleQueue = queue.SimpleQueue()
    stream = logging.StreamHandler(sys.stderr)
    stream.setFormatter(logging.Formatter(fmt))
    listener = logging.handlers.QueueListener(q, stream)
    root = logging.getLogger()
    root.handlers[:] = [logging.handlers.QueueHandler(q)]
    root.setLevel(level)
    listener.start()
    return listener
