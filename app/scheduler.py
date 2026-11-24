"""Process-local fair scheduler for concurrent source-shard reads.

The service reads gateway log shards from disk. Every read -- initial scan,
forced rescan or repair/backfill -- is submitted as a :class:`ReadRequest` to
the single :class:`ReadScheduler` owned by the Engine, so all kinds and all
jobs in one service process share one fixed cap (``SOURCE_READ_CONCURRENCY``).

Scheduling policy, applied whenever a slot is free:

1. **Idle jobs first.** A job that has read requests queued but currently has
   zero reads in flight is served before any job that already has a shard
   being read. Between several idle jobs, the one waiting longest gets the
   slot (FIFO fairness, so two contending jobs take turns).
2. **Same-job parallel reads with spare slots.** When no job is idle-with-queue
   -- e.g. only one job is running, or every contending job already owns one
   slot -- remaining slots run several shards of the same job at once. Spare
   slots are spread by least-in-flight then oldest request.

A slot is held for the whole ``read`` + ``complete`` pair. The Engine's
``complete`` callback persists that shard's progress, and the scheduler always
releases the slot afterwards, including when the read or the progress save
fails.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, Callable

from .models import utcnow


@dataclass
class ReadTicket:
    """Timing information for one dispatched source read."""

    shard_id: str
    kind: str
    started_at: str | None = None
    finished_at: str | None = None
    duration_ms: float | None = None


@dataclass
class ReadRequest:
    """One shard read scheduled through the pool.

    ``read`` performs only I/O and returns an arbitrary outcome (it is
    recommended to catch expected read errors and return them as data).
    ``complete`` is invoked exactly once with the ticket, the outcome and any
    exception escaped from ``read``; the Engine uses it to update state and
    persist per-shard progress. ``complete`` runs on a scheduler worker while
    the slot is still held.
    """

    shard_id: str
    kind: str
    read: Callable[[], Any]
    complete: Callable[[ReadTicket, Any, BaseException | None], None]


@dataclass
class _Task:
    seq: int
    job_id: str
    request: ReadRequest
    future: Future


class ReadScheduler:
    """Fixed-size worker pool implementing the two-level fairness policy."""

    def __init__(self, limit: int = 2, *, thread_name_prefix: str = "source-reader") -> None:
        self.limit = max(1, int(limit))
        self._cond = threading.Condition(threading.Lock())
        self._queues: dict[str, deque[_Task]] = {}
        self._active: dict[str, int] = {}
        # job_id -> fairness token; present exactly while the job is
        # "queued and has zero reads in flight". Smaller token = waited longer.
        self._starved_order: dict[str, int] = {}
        self._total_active = 0
        self._task_seq = 0
        self._fair_seq = 0
        self._closed = False
        self._threads: list[threading.Thread] = []
        for index in range(self.limit):
            thread = threading.Thread(
                target=self._worker_loop,
                name=f"{thread_name_prefix}-{index + 1}",
                daemon=True,
            )
            thread.start()
            self._threads.append(thread)

    # ------------------------------------------------------------- submission
    def submit(self, job_id: str, request: ReadRequest) -> Future:
        with self._cond:
            if self._closed:
                raise RuntimeError("ReadScheduler is shut down")
            self._task_seq += 1
            task = _Task(seq=self._task_seq, job_id=job_id, request=request, future=Future())
            self._queues.setdefault(job_id, deque()).append(task)
            self._active.setdefault(job_id, 0)
            self._mark_starved_locked(job_id)
            self._cond.notify()
        return task.future

    def run_batch(self, job_id: str, requests: list[ReadRequest]) -> None:
        """Queue all requests for ``job_id`` and wait for every one.

        All requests are queued before waiting so the scheduler can see the
        job's full demand. Every task is allowed to finish (releasing its slot
        and persisting progress) before this returns; the first callback/read
        error is then re-raised so the calling run fails cleanly.
        """

        if not requests:
            return
        futures = [self.submit(job_id, request) for request in requests]
        first_error: BaseException | None = None
        for future in futures:
            try:
                future.result()
            except BaseException as exc:  # propagate after siblings finish
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error

    # ------------------------------------------------------------ introspection
    def snapshot(self) -> dict[str, Any]:
        with self._cond:
            return {
                "limit": self.limit,
                "active_reads": self._total_active,
                "active_reads_by_job": {
                    job_id: count for job_id, count in self._active.items() if count
                },
                "queued_shards_by_job": {
                    job_id: len(queue)
                    for job_id, queue in self._queues.items()
                    if queue
                },
                "idle_jobs_with_queue": sorted(self._starved_order),
            }

    def active_read_count(self) -> int:
        with self._cond:
            return self._total_active

    def shutdown(self, *, join_timeout: float = 5.0) -> None:
        with self._cond:
            self._closed = True
            self._cond.notify_all()
        for thread in self._threads:
            thread.join(timeout=join_timeout)

    # --------------------------------------------------------------- internals
    def _mark_starved_locked(self, job_id: str) -> None:
        queue = self._queues.get(job_id)
        if queue and self._active.get(job_id, 0) == 0 and job_id not in self._starved_order:
            self._fair_seq += 1
            self._starved_order[job_id] = self._fair_seq

    def _acquire_task(self) -> _Task | None:
        with self._cond:
            while True:
                if self._closed:
                    return None
                if self._total_active < self.limit:
                    task = self._pick_locked()
                    if task is not None:
                        self._total_active += 1
                        self._active[task.job_id] = self._active.get(task.job_id, 0) + 1
                        # It now has a read in flight, so it is no longer idle.
                        self._starved_order.pop(task.job_id, None)
                        return task
                self._cond.wait()

    def _pick_locked(self) -> _Task | None:
        # Priority 1: idle jobs with queued shards, longest-waiting first.
        if self._starved_order:
            job_id = min(self._starved_order, key=self._starved_order.get)  # type: ignore[arg-type]
            queue = self._queues[job_id]
            task = queue.popleft()
            if not queue:
                self._starved_order.pop(job_id, None)
            return task

        # Priority 2: spare slots -> allow multiple shards of one job at once.
        choices: list[tuple[int, int, _Task]] = []
        for job_id, queue in self._queues.items():
            if queue:
                choices.append((self._active.get(job_id, 0), queue[0].seq, queue[0]))
        if choices:
            _, _, task = min(choices, key=lambda item: (item[0], item[1]))
            task_queue = self._queues[task.job_id]
            picked = task_queue.popleft()
            return picked
        return None

    def _worker_loop(self) -> None:
        while True:
            task = self._acquire_task()
            if task is None:
                return

            request = task.request
            ticket = ReadTicket(shard_id=request.shard_id, kind=request.kind)
            started_perf = time.perf_counter()
            ticket.started_at = utcnow()

            outcome: Any = None
            read_error: BaseException | None = None
            try:
                outcome = request.read()
            except BaseException as exc:  # surfaced through the future below
                read_error = exc
            finally:
                ticket.finished_at = utcnow()
                ticket.duration_ms = round((time.perf_counter() - started_perf) * 1000.0, 3)
                # The slot bounds concurrent *source reads*. Release it the
                # moment the read attempt is over, success or failure, and
                # only then persist progress below without occupying a slot.
                with self._cond:
                    self._active[task.job_id] -= 1
                    self._total_active -= 1
                    # If this was the job's last in-flight read and more shards
                    # are queued, it re-enters the idle-job priority class with
                    # a fresh token; older tokens from other waiting jobs win.
                    self._mark_starved_locked(task.job_id)
                    self._cond.notify_all()

            callback_error: BaseException | None = None
            try:
                # The Engine persists this shard's progress here. This no
                # longer occupies a read slot; run_batch still waits for it.
                request.complete(ticket, outcome, read_error)
            except BaseException as exc:
                callback_error = exc

            if callback_error is not None:
                task.future.set_exception(callback_error)
            elif read_error is not None:
                task.future.set_exception(read_error)
            else:
                task.future.set_result(outcome)
