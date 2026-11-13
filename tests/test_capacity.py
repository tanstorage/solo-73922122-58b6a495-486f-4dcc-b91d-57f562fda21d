"""Capacity, 503 and cancellation guarantees for POST /api/v1/batches.

These tests start *real* uvicorn processes (see tests/server_runner.py) so that
HTTP client disconnects and thread-pool scheduling behave exactly as in the
Docker Compose deployment.  Three properties are exercised:

* a process accepts up to its configured in-flight capacity (40 by default),
  rejects anything beyond it with a retryable 503 + ``Retry-After`` and writes
  nothing;
* while batches fill every executor slot (some blocked on the SQLite write
  lock), ``/health`` and ``/api/v1/state`` still answer inside one second;
* a request cancelled while queued never lands late, whereas a request whose
  batch already started still commits even though the client went away, and
  the executor slot is released only after commit/rollback + connection close.
"""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import threading
import time
from contextlib import contextmanager

import httpx
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@contextmanager
def api_server(db_path, env_overrides=None):
    port = _free_port()
    stop_file = str(db_path) + ".stop"
    env = dict(
        os.environ,
        GIS_DB_PATH=str(db_path),
        GIS_BUSY_TIMEOUT_S=os.environ.get("GIS_BUSY_TIMEOUT_S", "30"),
    )
    if env_overrides:
        env.update(env_overrides)
    proc = subprocess.Popen(
        [
            sys.executable,
            os.path.join(os.path.dirname(__file__), "server_runner.py"),
            str(port),
            stop_file,
        ],
        cwd=ROOT,
        env=env,
    )
    base = f"http://127.0.0.1:{port}"
    deadline = time.time() + 30
    try:
        while True:
            try:
                if httpx.get(base + "/health", timeout=1).status_code == 200:
                    break
            except Exception:
                if time.time() > deadline:
                    raise RuntimeError(f"{base} did not start")
                time.sleep(0.1)
        yield base
    finally:
        open(stop_file, "w").close()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:  # pragma: no cover
            proc.kill()
            proc.wait(timeout=5)


def _body(i: int) -> dict:
    # keep coordinates inside the WGS84 range for every index
    return {"points": [{"id": f"CAP-{i}", "lon": 100.0 + (i % 80), "lat": 20.0}]}


def _hold_slot(client: httpx.Client, base: str, idx: int, pause_ms: int, box: dict):
    """Fire a paused batch in a background thread; return once it has started."""

    def run():
        try:
            box[idx] = client.post(
                base + "/api/v1/batches",
                params={"pause_ms": pause_ms},
                json=_body(idx),
                timeout=60,
            )
        except Exception as exc:  # pragma: no cover - failure path
            box[idx] = exc

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t


def test_default_process_accepts_40_concurrent_and_rejects_41st(tmp_path):
    db = tmp_path / "gis40.db"
    with api_server(db) as base:
        client = httpx.Client(timeout=60.0)
        threads, box = [], {}
        try:
            # 40 long-paused batches: the first executor_slots (8) start, the
            # other 32 wait in the queue -> exactly the 40-request capacity.
            for i in range(40):
                threads.append(_hold_slot(client, base, i, 8000, box))
            # let every request reach the gate (no probe is sent before that,
            # since an admitted probe would itself consume a permit)
            time.sleep(1.0)

            # the 41st in-flight request is rejected without touching the DB
            r = client.post(base + "/api/v1/batches", json=_body(999), timeout=10)
            assert r.status_code == 503
            assert r.headers.get("Retry-After") == "1"
            assert "capacity" in r.json()["detail"]

            for t in threads:
                t.join(timeout=30)
            assert all(
                isinstance(resp, httpx.Response) and resp.status_code == 201
                for resp in box.values()
            ), box
            assert len(box) == 40

            state = client.get(base + "/api/v1/state", timeout=10).json()
            ids = {p["id"] for p in state["points"]}
            assert {f"CAP-{i}" for i in range(40)} <= ids
            assert "CAP-999" not in ids
        finally:
            client.close()


def test_reads_stay_under_one_second_while_ingests_are_saturated(tmp_path):
    db = tmp_path / "reads.db"
    # tiny executor (2) so both workers can be parked on a long assembly pause
    with api_server(
        db,
        {"GIS_BATCH_EXECUTOR_SLOTS": "2", "GIS_BATCH_QUEUE_CAPACITY": "4"},
    ) as base:
        client = httpx.Client(timeout=60.0)
        box: dict = {}
        threads = [_hold_slot(client, base, i, 6000, box) for i in range(6)]
        try:
            # give the six batches time to settle (2 running, 4 queued)
            time.sleep(1.0)
            for _ in range(5):
                t0 = time.time()
                h = client.get(base + "/health", timeout=5)
                health_ms = (time.time() - t0) * 1000
                t0 = time.time()
                s = client.get(base + "/api/v1/state", timeout=5)
                state_ms = (time.time() - t0) * 1000
                assert h.status_code == 200
                assert s.status_code == 200
                assert health_ms < 1000, f"/health took {health_ms:.0f}ms"
                assert state_ms < 1000, f"/state took {state_ms:.0f}ms"

            # a seventh request is over capacity (2 + 4 = 6 admitted)
            r = client.post(base + "/api/v1/batches", json=_body(77), timeout=10)
            assert r.status_code == 503
            assert r.headers.get("Retry-After") == "1"
        finally:
            for t in threads:
                t.join(timeout=30)
            client.close()

        state = httpx.get(base + "/api/v1/state", timeout=10).json()
        assert {f"CAP-{i}" for i in range(6)} <= {p["id"] for p in state["points"]}
        assert "CAP-77" not in {p["id"] for p in state["points"]}


def test_queued_request_cancelled_never_lands(tmp_path):
    db = tmp_path / "queue_cancel.db"
    with api_server(
        db,
        {"GIS_BATCH_EXECUTOR_SLOTS": "1", "GIS_BATCH_QUEUE_CAPACITY": "3"},
    ) as base:
        client = httpx.Client(timeout=60.0)
        box: dict = {}
        # occupy the single executor slot for ~5 s
        holder = _hold_slot(client, base, 0, 5000, box)
        time.sleep(0.6)

        queued = httpx.Client(timeout=60.0)
        t0 = time.time()
        try:
            with pytest.raises(httpx.ReadTimeout):
                # queued behind the holder; give up after 0.8 s, well before it
                # could have started
                queued.post(
                    base + "/api/v1/batches",
                    params={"pause_ms": 200},
                    json=_body(1),
                    timeout=0.8,
                )
        finally:
            queued.close()
        cancelled_at = time.time() - t0
        assert cancelled_at < 1.5

        # give the server time to observe the disconnect and prove the queued
        # batch never slipped through (a buggy server would have committed it)
        time.sleep(2.0)
        state = client.get(base + "/api/v1/state", timeout=10).json()
        assert "CAP-1" not in {p["id"] for p in state["points"]}

        holder.join(timeout=20)
        assert box[0].status_code == 201

        # the freed slot + permit serve a fresh request immediately
        r = client.post(base + "/api/v1/batches", json=_body(2), timeout=10)
        assert r.status_code == 201
        client.close()


def test_started_batch_commits_after_client_disconnect(tmp_path):
    db = tmp_path / "started_cancel.db"
    with api_server(
        db,
        {"GIS_BATCH_EXECUTOR_SLOTS": "1", "GIS_BATCH_QUEUE_CAPACITY": "2"},
    ) as base:
        # request starts (pause is inside the executor, outside the write lock)
        # and the client disconnects while it is still running
        gone = httpx.Client(timeout=60.0)
        with pytest.raises(httpx.ReadTimeout):
            gone.post(
                base + "/api/v1/batches",
                params={"pause_ms": 4000},
                json=_body(0),
                timeout=0.8,
            )
        gone.close()

        # give the shielded store work time to finish its commit
        deadline = time.time() + 10
        client = httpx.Client(timeout=10.0)
        try:
            while time.time() < deadline:
                ids = {
                    p["id"]
                    for p in client.get(base + "/api/v1/state").json()["points"]
                }
                if "CAP-0" in ids:
                    break
                time.sleep(0.1)
            assert "CAP-0" in ids

            # slot was returned only after commit: a new batch is accepted
            r = client.post(base + "/api/v1/batches", json=_body(1), timeout=10)
            assert r.status_code == 201
        finally:
            client.close()


def test_lock_wait_timeout_returns_503_and_changes_nothing(tmp_path):
    """A writer holding the SQLite write lock makes the batch wait out
    busy_timeout and fail with a retryable 503; no row may be written."""
    import sqlite3

    db = tmp_path / "lock.db"
    with api_server(db, {"GIS_BUSY_TIMEOUT_S": "2"}) as base:
        client = httpx.Client(timeout=30.0)
        before = client.get(base + "/api/v1/state", timeout=10).json()

        # external connection takes the same write lock the API needs
        locker = sqlite3.connect(str(db), isolation_level=None)
        locker.execute("PRAGMA busy_timeout = 0")
        locker.execute("BEGIN IMMEDIATE")
        try:
            t0 = time.time()
            r = client.post(base + "/api/v1/batches", json=_body(42), timeout=15)
            elapsed = time.time() - t0
            assert r.status_code == 503
            assert r.headers.get("Retry-After") == "1"
            assert "busy" in r.json()["detail"].lower()
            assert elapsed >= 1.5  # it really waited out the busy timeout
        finally:
            locker.execute("ROLLBACK")
            locker.close()

        after = client.get(base + "/api/v1/state", timeout=10).json()
        assert after == before
        assert "CAP-42" not in {p["id"] for p in after["points"]}

        # no restart needed: once the lock is released, ingests succeed again
        r = client.post(base + "/api/v1/batches", json=_body(43), timeout=10)
        assert r.status_code == 201
        client.close()
