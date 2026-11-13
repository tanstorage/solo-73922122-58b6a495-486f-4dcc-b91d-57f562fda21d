"""Unit tests for the two-tier admission gate (no HTTP, no SQLite)."""
from __future__ import annotations

import anyio
import pytest

from app.flowcontrol import BatchGate, CapacityFull, ClientGone

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    # run the async gate tests on asyncio only (the runtime the app uses)
    return "asyncio"


@pytest.fixture()
def gate():
    return BatchGate(executor_slots=2, queue_capacity=3)


def test_gate_capacity_accounting(gate):
    assert gate.total_capacity == 5
    assert gate.executor_slots == 2
    assert gate.queue_capacity == 3
    assert gate.admitted == 0 and gate.executing == 0


async def test_over_capacity_rejected_immediately(gate):
    async def hold():
        async with gate.admit():
            await anyio.sleep_forever()

    with anyio.CancelScope() as scope:
        async with anyio.create_task_group() as tg:
            for _ in range(5):
                tg.start_soon(hold)
            await anyio.sleep(0.1)
            assert gate.admitted == 5
            assert gate.executing == 2  # 2 running, 3 queued

            with pytest.raises(CapacityFull):
                async with gate.admit():
                    pass
            scope.cancel()

    # every permit came back after the holders were cancelled
    assert gate.admitted == 0 and gate.executing == 0


async def test_cancelled_while_queued_never_executes(gate):
    started = anyio.Event()
    executed = False

    async def worker():
        async with gate.admit():
            started.set()
            await anyio.sleep_forever()  # parked until the scope is cancelled

    holder_scope = anyio.CancelScope()
    with holder_scope:
        async with anyio.create_task_group() as tg:
            for _ in range(2):
                tg.start_soon(worker)
            await anyio.sleep(0.1)
            assert started.is_set()
            assert gate.executing == 2

            queued_scope = anyio.CancelScope()

            async def queued():
                nonlocal executed
                with queued_scope:
                    async with gate.admit():
                        executed = True  # must never run

            tg.start_soon(queued)
            await anyio.sleep(0.1)
            queued_scope.cancel()  # client gives up while still in the queue
            await anyio.sleep(0.1)

            assert executed is False
            assert gate.admitted == 2  # only the two holders remain
            holder_scope.cancel()

    assert gate.admitted == 0 and gate.executing == 0

    # gate is fully reusable after the cancellation storm
    async with gate.admit():
        assert gate.executing == 1
    assert gate.executing == 0


async def test_disconnect_while_queued_skips_execution(gate):
    """ASGI-driven path used by the real endpoint."""
    async def worker():
        async with gate.admit():
            await anyio.sleep_forever()

    disconnect = anyio.Event()
    scope = anyio.CancelScope()
    with scope:
        async with anyio.create_task_group() as tg:
            for _ in range(2):
                tg.start_soon(worker)
            await anyio.sleep(0.1)
            assert gate.executing == 2

            executed = False
            queued_error: Exception | None = None

            async def queued():
                nonlocal executed, queued_error
                try:
                    async with gate.admit(wait_disconnect=disconnect.wait):
                        executed = True
                except ClientGone as exc:
                    queued_error = exc

            tg.start_soon(queued)
            await anyio.sleep(0.1)
            assert gate.admitted == 3
            disconnect.set()  # client TCP disconnect observed
            await anyio.sleep(0.1)

            assert executed is False
            assert isinstance(queued_error, ClientGone)
            # permit returned: still only the two holders admitted
            assert gate.admitted == 2

            # an already-disconnected request is rejected immediately rather
            # than entering the queue
            with pytest.raises(ClientGone):
                async with gate.admit(wait_disconnect=disconnect.wait):
                    pass
            scope.cancel()

    assert gate.admitted == 0 and gate.executing == 0


async def test_slot_arrives_before_disconnect_runs_normally(gate):
    disconnect = anyio.Event()
    async with gate.admit(wait_disconnect=disconnect.wait):
        assert gate.executing == 1
    assert gate.executing == 0
    # disconnect firing afterwards must not corrupt accounting
    disconnect.set()
    await anyio.sleep(0.05)
    assert gate.admitted == 0


async def test_shielded_started_work_runs_to_completion(gate):
    """Mimic the endpoint: shield the work while the slot is held."""
    finished = anyio.Event()

    async def endpoint_like():
        async with gate.admit():
            with anyio.CancelScope(shield=True):
                await anyio.sleep(0.2)
                finished.set()

    with anyio.CancelScope() as scope:
        async with anyio.create_task_group() as tg:
            tg.start_soon(endpoint_like)
            await anyio.sleep(0.05)
            scope.cancel()  # client disconnects after work started

    assert finished.is_set()
    assert gate.executing == 0 and gate.admitted == 0


def test_invalid_dimensions_rejected():
    with pytest.raises(ValueError):
        BatchGate(executor_slots=0, queue_capacity=3)
    with pytest.raises(ValueError):
        BatchGate(executor_slots=2, queue_capacity=0)
