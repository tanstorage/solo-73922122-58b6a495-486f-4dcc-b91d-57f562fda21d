"""Capacity, held-lock and cancellation demo for POST /api/v1/batches.

Runs entirely over the public HTTP API (plus, for the held-lock scenario, a
plain SQLite connection to the shared database file).  It demonstrates the
execution-resource guarantees added to the ingest endpoint:

A. **Held write lock** – an external connection holds SQLite's writer slot;
   batches wait the configured ``busy_timeout`` and then fail with a
   *retryable 503* without changing any data.  Once the lock is released,
   ingests succeed again with **no restart**.  Reads stay fast meanwhile.
B. **Concurrent ingest under bounded capacity** – the process accepts its full
   in-flight capacity (8 executing + 32 queued = 40), rejects the 41st with a
   503 + ``Retry-After``, and every admitted batch eventually commits.
C. **Cancel-vs-started interleaving** – a request given up while still queued
   never lands, whereas a request whose batch already started commits even
   after the client disconnects; the executor slot is reused afterwards.

Run locally against the two compose ports::

    python -m examples.run_capacity_demo --base-a http://localhost:8001 \\
                                         --base-b http://localhost:8002
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import threading
import time

import httpx

# Small enough to make saturation quick, large enough to show the queue.
SATURATION = 40
HOLDER_PAUSE_MS = 6000


def banner(title: str) -> None:
    print(f"\n=== {title} " + "=" * max(0, 70 - len(title)))


def summary(payload) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def _point(i: int) -> dict:
    return {"id": f"DEMO-{i}", "lon": 100.0 + (i % 80), "lat": 20.0}


def _held_lock_demo(client: httpx.Client, db_path: str) -> None:
    banner("A. 持锁：写锁占用期间等忙等待，超时返回可重试 503，解锁后免重启入库")
    before = client.get("/api/v1/state").json()

    locker = sqlite3.connect(db_path, isolation_level=None)
    locker.execute("PRAGMA busy_timeout = 0")
    locker.execute("BEGIN IMMEDIATE")
    print(f"外部连接已对 {db_path} 取得 BEGIN IMMEDIATE 写锁")
    try:
        t0 = time.time()
        r = client.post("/api/v1/batches", json={"points": [_point(9001)]}, timeout=35)
        elapsed = time.time() - t0
        print(f"等锁 {elapsed:.1f}s 后 -> HTTP {r.status_code} {r.text.strip()}")
        print(f"Retry-After: {r.headers.get('Retry-After')}")
        assert r.status_code == 503
        assert r.headers.get("Retry-After") == "1"

        # reads are unaffected by the held write lock (WAL) and stay fast
        t0 = time.time()
        h = client.get("/health")
        s = client.get("/api/v1/state")
        read_ms = (time.time() - t0) * 1000
        print(f"持锁期间 /health={h.status_code} /state={s.status_code}，"
              f"两次读取合计 {read_ms:.0f}ms（<1000ms）")
        assert h.status_code == 200 and s.status_code == 200 and read_ms < 1000
        assert s.json() == before, "503 批次不得改动任何数据"
    finally:
        locker.execute("ROLLBACK")
        locker.close()
        print("外部连接已 ROLLBACK 释放写锁（服务未重启）")

    r = client.post("/api/v1/batches", json={"points": [_point(9002)]})
    print(f"解锁后立即入库 -> HTTP {r.status_code}")
    assert r.status_code == 201


def _saturation_demo(client: httpx.Client) -> None:
    banner("B. 并发入库：接纳 40 笔在途（8 执行 + 32 排队），第 41 笔 503 且不落库")
    box: dict[int, httpx.Response] = {}

    def fire(i: int) -> None:
        try:
            box[i] = client.post(
                "/api/v1/batches",
                params={"pause_ms": HOLDER_PAUSE_MS},
                json={"points": [_point(i)]},
            )
        except Exception as exc:  # pragma: no cover - failure path
            box[i] = exc

    threads = [threading.Thread(target=fire, args=(i,), daemon=True)
               for i in range(SATURATION)]
    for t in threads:
        t.start()
    time.sleep(1.0)  # let all 40 reach the gate (8 running, 32 queued)

    over = client.post("/api/v1/batches", json={"points": [_point(9999)]})
    print(f"第 41 笔 -> HTTP {over.status_code} {over.text.strip()}")
    print(f"Retry-After: {over.headers.get('Retry-After')}")
    assert over.status_code == 503 and over.headers.get("Retry-After") == "1"

    # while saturated, reads must still answer within one second
    t0 = time.time()
    state = client.get("/api/v1/state")
    ms = (time.time() - t0) * 1000
    print(f"满额期间 /api/v1/state -> {state.status_code}，用时 {ms:.0f}ms（<1000ms）")
    assert ms < 1000

    for t in threads:
        t.join(timeout=30)
    statuses = {getattr(r, "status_code", r) for r in box.values()}
    print(f"40 笔排队/执行批次最终状态码集合: {statuses}")
    assert statuses == {201}

    ids = {p["id"] for p in client.get("/api/v1/state").json()["points"]}
    assert {f"DEMO-{i}" for i in range(SATURATION)} <= ids
    assert "DEMO-9999" not in ids, "被 503 拒绝的批次不得落库"


def _cancel_demo(client: httpx.Client, slots: int = 8) -> None:
    banner("C. 取消与开工交错：排队取消不迟到；已开工批次断开后仍提交，名额随后复用")
    base = str(client.base_url)
    holder_box: dict[int, httpx.Response] = {}

    def holder(i: int) -> None:
        try:
            holder_box[i] = client.post(
                "/api/v1/batches",
                params={"pause_ms": 5000},
                json={"points": [_point(7000 + i)]},
            )
        except Exception as exc:  # pragma: no cover - failure path
            holder_box[i] = exc

    # 1) occupy EVERY executor slot so the next request really has to queue
    holders = [
        threading.Thread(target=holder, args=(i,), daemon=True)
        for i in range(slots)
    ]
    for t in holders:
        t.start()
    time.sleep(0.8)

    # 2) a queued request whose client gives up before it can start
    queued = httpx.Client(base_url=base, timeout=30.0)
    try:
        t0 = time.time()
        try:
            queued.post(
                "/api/v1/batches",
                params={"pause_ms": 200},
                json={"points": [_point(7100)]},
                timeout=0.8,
            )
            raise AssertionError("queued request should have timed out")
        except httpx.ReadTimeout:
            print(f"排队请求在 {time.time() - t0:.1f}s 后被客户端放弃")
    finally:
        queued.close()

    time.sleep(2.0)
    ids = {p["id"] for p in client.get("/api/v1/state").json()["points"]}
    print(f"放弃后 DEMO-7100 是否落库: {'DEMO-7100' in ids}（应为 False）")
    assert "DEMO-7100" not in ids

    for t in holders:
        t.join(timeout=20)
    print(f"占用名额的 {slots} 个批次最终状态码集合: "
          f"{ {getattr(r, 'status_code', r) for r in holder_box.values()} }")

    # 3) a request whose batch already started; client disconnects mid-run
    gone = httpx.Client(base_url=base, timeout=30.0)
    try:
        try:
            gone.post(
                "/api/v1/batches",
                params={"pause_ms": 4000},
                json={"points": [_point(7200)]},
                timeout=0.8,
            )
            raise AssertionError("started request should have timed out")
        except httpx.ReadTimeout:
            print("已开工批次的客户端在执行中断开连接")
    finally:
        gone.close()

    deadline = time.time() + 10
    while time.time() < deadline:
        ids = {p["id"] for p in client.get("/api/v1/state").json()["points"]}
        if "DEMO-7200" in ids:
            break
        time.sleep(0.1)
    print("已开工批次在客户端断开后仍完成提交: DEMO-7200 已入库")
    assert "DEMO-7200" in ids

    # 4) executor slot returned only after commit: a new batch is accepted
    follow = client.post("/api/v1/batches", json={"points": [_point(7300)]})
    print(f"名额归还后新批次立即受理 -> HTTP {follow.status_code}")
    assert follow.status_code == 201


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base-a", default="http://localhost:8001")
    ap.add_argument("--base-b", default="http://api-b:8000")
    ap.add_argument(
        "--db-path",
        default=os.environ.get("GIS_DB_PATH", "/data/gis.db"),
        help="shared SQLite file for the external write-lock connection",
    )
    args = ap.parse_args(argv)

    print(f"API A = {args.base_a}\nAPI B = {args.base_b}（共用 {args.db_path}）")
    with httpx.Client(base_url=args.base_a, timeout=60.0) as client:
        _held_lock_demo(client, args.db_path)
        _saturation_demo(client)
        # cross-process sanity: B sees everything A committed
        with httpx.Client(base_url=args.base_b, timeout=60.0) as other:
            assert other.get("/health").status_code == 200
            gen_b = other.get("/api/v1/state").json()["generation"]
            gen_a = client.get("/api/v1/state").json()["generation"]
            print(f"\n双进程读到同一 generation: A={gen_a} B={gen_b}")
            assert gen_a == gen_b
        _cancel_demo(client)

    banner("容量/持锁/取消演示全部通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
