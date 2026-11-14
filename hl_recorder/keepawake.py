"""Keep the computer from going to sleep while the recorder runs (like macOS `caffeinate -i`).

* Windows: SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED). The request belongs to
  the process: when the recorder exits (or crashes) Windows drops it automatically, so normal
  power settings come back without any cleanup. The display may still turn off.
* macOS: runs `caffeinate -i -w <pid>`, which ends by itself when the recorder exits.
* Linux: not implemented (use `systemd-inhibit` around the command if needed).

It prevents *idle* sleep. Closing a laptop lid, choosing Sleep manually or a forced
Windows Update restart still interrupt recording; the recorder logs those as gaps.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from contextlib import contextmanager

log = logging.getLogger(__name__)

ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001


@contextmanager
def keep_awake(enabled: bool = True):
    if not enabled:
        yield False
        return
    if sys.platform == "win32":
        import ctypes

        kernel32 = ctypes.windll.kernel32
        ok = kernel32.SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED) != 0
        log.info("keep-awake %s (Windows SetThreadExecutionState)", "on" if ok else "FAILED")
        try:
            yield ok
        finally:
            kernel32.SetThreadExecutionState(ES_CONTINUOUS)
    elif sys.platform == "darwin":
        proc = subprocess.Popen(["caffeinate", "-i", "-w", str(os.getpid())])
        log.info("keep-awake on (caffeinate pid %d)", proc.pid)
        try:
            yield True
        finally:
            proc.terminate()
    else:
        log.warning("keep-awake not supported on %s; run under `systemd-inhibit` instead", sys.platform)
        yield False
