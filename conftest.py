"""Pytest bootstrap shared by every test module.

The two-process concurrency tests spawn ``tests/server_runner.py`` as plain
subprocesses; Python puts the *script's* directory (not the current working
directory) on ``sys.path`` there, so the ``app`` package would not resolve
unless the repository root is exported through ``PYTHONPATH``.  Setting it here
keeps a bare ``pytest`` invocation working from any checkout location.
"""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
existing = os.environ.get("PYTHONPATH", "")
os.environ["PYTHONPATH"] = (
    ROOT + os.pathsep + existing if existing else ROOT
)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
