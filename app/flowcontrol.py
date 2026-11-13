"""Bounded admission control for batch ingest requests.

Why this exists
---------------
A batch that has entered its ``BEGIN IMMEDIATE`` write transaction waits on
SQLite's single writer slot for up to ``busy_timeout`` (30 s).  With an
unbounded endpoint, 40 concurrent batches would occupy 40 workers of the
shared anyio thread pool while blocked on that lock, starving read endpoints
(``/health``, ``/api/v1/state``) of execution threads.

The gate splits each batch request into two phases:

1. **Admission** – every accepted request takes one *admission permit*.
   Total accepted (executing + queued) capacity is ``executor_slots +
   queue_capacity`` (40 per process by default).  A request that cannot get an
   admission permit immediately is rejected with a retryable 503 and never
   touches the database.
2. **Execution** – admitted requests acquire one *executor slot* before any
   CPU/SQL work runs.  While queued they wait on the event loop, holding
   neither a thread nor a database connection, so read endpoints keep running
   on the shared pool and return within a second even while writers wait on
   the lock.

Cancellation contract
----------------------
* A request cancelled while **queued** drops its admission permit and never
   executes – a late batch cannot land after the client gave up.
* Once execution has started, cancellation is *shielded*: the store call runs
   to COMMIT or ROLLBACK and its connection is closed before the executor slot
   (and then the admission permit) is returned.  A started batch therefore
   finishes exactly once, with no leaked slot or connection.
"""
from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

from anyio import Semaphore, create_task_group

from .config import settings


class CapacityFull(Exception):
    """No admission permit is available; the request is retryable as-is."""

    retry_after = 1


class ClientGone(Exception):
    """The HTTP client disconnected while the batch was still queued.

    The batch never started, so no data was changed and no slot was consumed.
    """

    retry_after = 1


@asynccontextmanager
async def background_task(func: Callable[[], Awaitable]) -> AsyncIterator[None]:
    """Run ``func`` in an isolated task that never joins the caller's
    exception flow.

    A plain ``async with create_task_group()`` around the caller wraps every
    exception the caller raises in an :class:`ExceptionGroup` (because the
    group sees an in-flight sibling task), which FastAPI renders as a generic
    500.  Here the task group is driven manually and always closed with a clean
    ``(None, None, None)`` after cancelling its *own* scope, so the background
    task can neither surface nor mask an exception; whatever the caller raised
    propagates untouched.  ``func`` itself must not raise.
    """
    tg = create_task_group()
    await tg.__aenter__()
    tg.start_soon(func)
    try:
        yield
    finally:
        # cancel only the group's internal scope, never the caller's task
        tg.cancel_scope.cancel()
        await tg.__aexit__(None, None, None)


class BatchGate:
    """Two-tier semaphore: total admission limit plus execution limit."""

    def __init__(self, executor_slots: int, queue_capacity: int) -> None:
        if executor_slots < 1 or queue_capacity < 1:
            raise ValueError("executor slots and queue capacity must be >= 1")
        self._executor_slots = executor_slots
        self._queue_capacity = queue_capacity
        self._admission = Semaphore(executor_slots + queue_capacity)
        self._execution = Semaphore(executor_slots)

    @property
    def executor_slots(self) -> int:
        return self._executor_slots

    @property
    def queue_capacity(self) -> int:
        return self._queue_capacity

    @property
    def total_capacity(self) -> int:
        return self._executor_slots + self._queue_capacity

    @property
    def admitted(self) -> int:
        """Requests currently admitted (executing or queued)."""
        return self.total_capacity - self._admission.value

    @property
    def executing(self) -> int:
        return self._executor_slots - self._execution.value

    @asynccontextmanager
    async def admit(
        self,
        wait_disconnect: Callable[[], Awaitable] | None = None,
    ) -> AsyncIterator[None]:
        """Acquire admission, then an executor slot; release in reverse order.

        Raises :class:`CapacityFull` immediately when the total accepted
        capacity is saturated.  When ``wait_disconnect`` is supplied and the
        client goes away while the request is still **queued**, raises
        :class:`ClientGone`, releases the admission permit and skips execution
        entirely — a cancelled request can never land late.  The callback is
        an awaitable that only resolves once a disconnect was observed (e.g.
        ``anyio.Event.wait`` driven by an ASGI disconnect watcher).

        Callers are expected to shield the actual store work while holding the
        executor slot so a started batch always reaches COMMIT/ROLLBACK.
        """
        if self._admission.value == 0:
            raise CapacityFull(
                f"batch capacity saturated: {self.total_capacity} accepted "
                f"requests per process ({self._executor_slots} executing, "
                f"{self._queue_capacity} queued); retry shortly"
            )
        self._admission.acquire_nowait()
        try:
            if wait_disconnect is None:
                # No disconnect source (e.g. background jobs): plain wait.
                await self._execution.acquire()
            else:
                # Race the slot acquisition against the disconnect event.
                # uvicorn does not cancel the endpoint task on disconnect, so
                # the gate has to observe the event itself.
                async with create_task_group() as tg:
                    got_slot = False

                    async def wait_slot() -> None:
                        nonlocal got_slot
                        await self._execution.acquire()
                        got_slot = True
                        tg.cancel_scope.cancel()

                    async def watch_disconnect() -> None:
                        await wait_disconnect()
                        tg.cancel_scope.cancel()

                    tg.start_soon(wait_slot)
                    tg.start_soon(watch_disconnect)

                if not got_slot:
                    raise ClientGone(
                        "client disconnected while the batch was queued; "
                        "the batch was not executed"
                    )
        except BaseException:
            self._admission.release()
            raise
        try:
            yield
        finally:
            self._execution.release()
            self._admission.release()


gate = BatchGate(
    executor_slots=max(1, settings.batch_executor_slots),
    queue_capacity=max(1, settings.batch_queue_capacity),
)
