"""Parallel source reads, fair cross-job scheduling and slot release.

These tests pin the concurrency contract added for the parallel /run endpoint:

* one process-local cap shared by scans, rescans and repairs;
* freed slots prioritize idle jobs with queued shards;
* spare slots read multiple shards of one job concurrently;
* per-shard progress is persisted as each read finishes;
* failed reads still release their slot;
* one read attempt per shard per run, with start/end timestamps in the report.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from concurrent.futures import Future
from pathlib import Path

import pytest

import app.engine as engine_mod
from app.config import Settings, load_settings
from app.engine import Engine
from app.models import JobCreateRequest, RunRequest
from app.scheduler import ReadRequest, ReadScheduler, _Task
from tests.conftest import EXPECTED_TOTALS


# ------------------------------------------------------------ scheduler unit
def _noop_complete(*_args) -> None:
    return None


def _task(job_id: str, seq: int) -> _Task:
    return _Task(seq=seq, job_id=job_id, request=ReadRequest("s", "scan", lambda: None, _noop_complete), future=Future())


def test_pick_policy_prefers_idle_jobs_then_same_job_spare_slots() -> None:
    scheduler = ReadScheduler(limit=4)
    try:
        with scheduler._cond:
            task_a1 = _task("A", 1)
            task_a2 = _task("A", 2)
            task_b1 = _task("B", 3)
            scheduler._queues = {
                "A": deque([task_a1, task_a2]),
                "B": deque([task_b1]),
            }
            scheduler._active = {"A": 0, "B": 0}
            scheduler._mark_starved_locked("A")
            scheduler._mark_starved_locked("B")

            first = scheduler._pick_locked()
            assert first is task_a1  # longest-waiting idle job (FIFO token)
            scheduler._active["A"] = 1
            scheduler._starved_order.pop("A", None)

            second = scheduler._pick_locked()
            assert second is task_b1  # the other idle job before an A extra read
            scheduler._active["B"] = 1
            scheduler._starved_order.pop("B", None)

            third = scheduler._pick_locked()
            assert third is task_a2  # no idle job left: spare slot -> same job
    finally:
        scheduler.shutdown()


def test_a_freshly_idle_job_goes_behind_an_older_waiting_job() -> None:
    """A just-completed job's new token is younger than an already-waiting job."""

    scheduler = ReadScheduler(limit=2)
    try:
        with scheduler._cond:
            task_a2 = _task("A", 2)
            task_b1 = _task("B", 3)
            scheduler._queues = {"A": deque([task_a2]), "B": deque([task_b1])}
            scheduler._active = {"A": 0, "B": 0}
            # B has already been idle-waiting while A's first shard was reading.
            scheduler._mark_starved_locked("B")
            # A's read just returned and A still has a queued shard.
            scheduler._mark_starved_locked("A")

            picked = scheduler._pick_locked()
            assert picked is task_b1
    finally:
        scheduler.shutdown()


def test_scheduler_two_job_contention_frees_slot_to_idle_job_first() -> None:
    scheduler = ReadScheduler(limit=2)
    started: dict[tuple[str, int], threading.Event] = {}
    release: dict[tuple[str, int], threading.Event] = {}
    order: list[tuple[str, int]] = []
    order_lock = threading.Lock()
    completed_tickets = []

    for job in ("A", "B"):
        for index in range(3):
            started[(job, index)] = threading.Event()
            release[(job, index)] = threading.Event()

    def make_read(job: str, index: int):
        def read() -> str:
            with order_lock:
                order.append((job, index))
            started[(job, index)].set()
            assert release[(job, index)].wait(timeout=5), "test gate timed out"
            return f"{job}-{index}"

        return read

    def complete(ticket, outcome, error) -> None:
        assert error is None
        completed_tickets.append(ticket)

    def requests_for(job: str, count: int) -> list[ReadRequest]:
        return [
            ReadRequest(f"{job.lower()}-{i}.log", "scan", make_read(job, i), complete)
            for i in range(count)
        ]

    worker_a = threading.Thread(target=scheduler.run_batch, args=("A", requests_for("A", 3)))
    worker_b = threading.Thread(target=scheduler.run_batch, args=("B", requests_for("B", 3)))
    worker_a.start()
    try:
        assert started[("A", 0)].wait(5)
        assert started[("A", 1)].wait(5)
        # Both slots are occupied by A; its third shard must wait.
        assert ("A", 2) not in order

        worker_b.start()
        deadline = time.time() + 5
        while time.time() < deadline:
            snapshot = scheduler.snapshot()
            if snapshot["queued_shards_by_job"].get("B") == 3:
                break
            time.sleep(0.01)
        assert scheduler.snapshot()["queued_shards_by_job"].get("B") == 3

        # Free one A slot: idle-and-queued B must receive it before A's 3rd.
        release[("A", 0)].set()
        assert started[("B", 0)].wait(5)
        assert ("A", 2) not in order
        assert scheduler.active_read_count() == 2

        for event in release.values():
            event.set()
        worker_a.join(timeout=10)
        worker_b.join(timeout=10)
        assert not worker_a.is_alive() and not worker_b.is_alive()
    finally:
        for event in release.values():
            event.set()
        worker_a.join(timeout=5)
        worker_b.join(timeout=5)
        scheduler.shutdown()

    assert scheduler.active_read_count() == 0
    assert len(completed_tickets) == 6
    assert all(ticket.started_at and ticket.finished_at for ticket in completed_tickets)


def test_failed_read_releases_slot_and_worker_keeps_serving() -> None:
    scheduler = ReadScheduler(limit=1)
    completions = []

    def bad_read() -> None:
        raise RuntimeError("boom")

    def complete(ticket, outcome, error) -> None:
        completions.append((ticket, error))

    try:
        with pytest.raises(RuntimeError, match="boom"):
            scheduler.run_batch(
                "j", [ReadRequest("x.log", "scan", bad_read, complete)]
            )
        assert scheduler.active_read_count() == 0
        assert len(completions) == 1

        ran = threading.Event()
        scheduler.run_batch(
            "j",
            [ReadRequest("y.log", "scan", lambda: ran.set() or "ok", _noop_complete)],
        )
        assert ran.is_set()
        assert scheduler.active_read_count() == 0
    finally:
        scheduler.shutdown()


# ------------------------------------------------------------- engine helpers
def _make_settings(tmp_path: Path, concurrency: int = 2) -> Settings:
    data = tmp_path / "data"
    work = tmp_path / "work"
    data.mkdir()
    work.mkdir()
    return Settings(
        data_dir=data,
        work_dir=work,
        allow_simulation=True,
        source_read_concurrency=concurrency,
    )


def _write_shards(data: Path, count: int) -> list[str]:
    names = [f"gw-{i}.log" for i in range(count)]
    for i, name in enumerate(names):
        (data / name).write_text(
            "".join(
                f"domain-{j % 4}.example.com {200 if j % 3 else 500}\n" for j in range(20)
            ),
            encoding="utf-8",
        )
    return names


# --------------------------------------------------------- engine-level tests
def test_scan_reads_run_in_parallel_with_timestamps_and_single_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _make_settings(tmp_path, concurrency=4)
    names = _write_shards(settings.data_dir, 4)
    engine = Engine(settings)
    try:
        engine.create_job(JobCreateRequest(shard_ids=names, job_id="parallel-1"))

        real_read = engine_mod.read_shard
        calls: list[str] = []
        calls_lock = threading.Lock()

        def slow_read(path, shard_id, **kwargs):
            with calls_lock:
                calls.append(shard_id)
            time.sleep(0.15)
            return real_read(path, shard_id, **kwargs)

        monkeypatch.setattr(engine_mod, "read_shard", slow_read)

        started = time.perf_counter()
        response = engine.run_job("parallel-1", RunRequest())
        elapsed = time.perf_counter() - started

        # 4 x 0.15s serial would be >=0.6s; cap 4 must run them together.
        assert elapsed < 0.45, f"reads did not run in parallel: {elapsed:.3f}s"
        assert sorted(calls) == sorted(names)
        run = response.run
        assert run.source_read_counts == {name: 1 for name in names}
        assert len(run.source_reads) == 4
        for read in run.source_reads:
            assert read.success is True
            assert read.started_at and read.finished_at
            assert read.duration_ms is not None and read.duration_ms >= 100
    finally:
        engine.shutdown()


def test_each_shard_progress_is_persisted_as_soon_as_it_finishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _make_settings(tmp_path, concurrency=2)
    (settings.data_dir / "gw-block.log").write_text("blocked.example.com 200\n")
    (settings.data_dir / "gw-a.log").write_text("a.example.com 200\n")
    (settings.data_dir / "gw-b.log").write_text("b.example.com 200\n")
    engine = Engine(settings)
    release_block = threading.Event()
    real_read = engine_mod.read_shard

    def gated_read(path, shard_id, **kwargs):
        if shard_id == "gw-block.log":
            assert release_block.wait(timeout=10)
        return real_read(path, shard_id, **kwargs)

    monkeypatch.setattr(engine_mod, "read_shard", gated_read)
    try:
        engine.create_job(
            JobCreateRequest(
                # block first so it certainly owns one of the two initial slots
                shard_ids=["gw-block.log", "gw-a.log", "gw-b.log"],
                job_id="persist-1",
            )
        )
        outcome: dict[str, object] = {}

        def run() -> None:
            try:
                outcome["response"] = engine.run_job("persist-1", RunRequest())
            except BaseException as exc:  # pragma: no cover - failure path
                outcome["error"] = exc

        runner = threading.Thread(target=run)
        runner.start()

        # Wait until gw-a has finished and been persisted while gw-block is
        # still gated inside its read.
        deadline = time.time() + 10
        saw_progress = False
        while time.time() < deadline:
            persisted = engine.store.load_job("persist-1")
            if persisted.shards["gw-a.log"].scan_status.value == "done":
                assert persisted.shards["gw-block.log"].scan_status.value != "done"
                saw_progress = True
                break
            time.sleep(0.01)
        assert saw_progress, "gw-a progress was not persisted before the blocked shard finished"

        release_block.set()
        runner.join(timeout=15)
        assert not runner.is_alive()
        assert "error" not in outcome
        response = outcome["response"]
        assert response.job.status.value == "completed"
    finally:
        release_block.set()
        engine.shutdown()


def test_two_jobs_contend_with_shared_cap_and_fair_interleaving(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _make_settings(tmp_path, concurrency=2)
    names = _write_shards(settings.data_dir, 6)
    engine = Engine(settings)
    real_read = engine_mod.read_shard

    proceed = threading.Event()
    active = 0
    max_active = 0
    tracker_lock = threading.Lock()

    def tracked_read(path, shard_id, **kwargs):
        nonlocal active, max_active
        # Hold at entry until both jobs have queued their batches, making the
        # contention deterministic.
        assert proceed.wait(timeout=10)
        with tracker_lock:
            active += 1
            max_active = max(max_active, active)
        try:
            time.sleep(0.12)
            return real_read(path, shard_id, **kwargs)
        finally:
            with tracker_lock:
                active -= 1

    monkeypatch.setattr(engine_mod, "read_shard", tracked_read)

    try:
        engine.create_job(JobCreateRequest(shard_ids=names, job_id="job-a"))
        engine.create_job(JobCreateRequest(shard_ids=names, job_id="job-b"))

        results: dict[str, object] = {}

        def run(job_id: str) -> None:
            try:
                results[job_id] = engine.run_job(job_id, RunRequest())
            except BaseException as exc:  # pragma: no cover - failure path
                results[job_id + ":error"] = exc

        thread_a = threading.Thread(target=run, args=("job-a",))
        thread_b = threading.Thread(target=run, args=("job-b",))
        thread_a.start()
        thread_b.start()

        deadline = time.time() + 10
        both_queued = False
        while time.time() < deadline:
            with engine.read_scheduler._cond:
                queued_jobs = set(engine.read_scheduler._queues)
            if {"job-a", "job-b"} <= queued_jobs:
                both_queued = True
                break
            time.sleep(0.01)
        assert both_queued
        proceed.set()

        thread_a.join(timeout=30)
        thread_b.join(timeout=30)
        assert not thread_a.is_alive() and not thread_b.is_alive()
        assert "job-a:error" not in results and "job-b:error" not in results

        # The process-wide cap was never exceeded by actual on-disk reads.
        assert max_active <= 2
        assert max_active == 2

        response_a = results["job-a"]
        response_b = results["job-b"]
        for response in (response_a, response_b):
            assert response.job.status.value == "completed"
            assert response.run.source_read_counts == {name: 1 for name in names}
            for read in response.run.source_reads:
                assert read.started_at and read.finished_at

        # Fairness produced real cross-job overlap under the 2-slot cap.
        reads_a = response_a.run.source_reads
        reads_b = response_b.run.source_reads

        iso_intervals = []
        for read in reads_a:
            iso_intervals.append(("A", _iso(read.started_at), _iso(read.finished_at)))
        for read in reads_b:
            iso_intervals.append(("B", _iso(read.started_at), _iso(read.finished_at)))
        overlapped = False
        for job_x, sx, ex in iso_intervals:
            for job_y, sy, ey in iso_intervals:
                if job_x == job_y:
                    continue
                if sx < ey and sy < ex:
                    overlapped = True
        assert overlapped, "the two jobs were never read concurrently"
        assert engine.read_scheduler.active_read_count() == 0
    finally:
        proceed.set()
        engine.shutdown()


def _iso(value: str) -> float:
    from datetime import datetime

    return datetime.fromisoformat(value).timestamp()


def test_part_fault_simulation_happens_only_after_scan_batch_barrier(
    client, make_job, monkeypatch: pytest.MonkeyPatch
) -> None:
    job_id = make_job(["gw-a.log", "gw-b.log"], job_id="barrier-1")
    engine = client.app.state.engine
    client.post(f"/jobs/{job_id}/run", json={})

    original = engine._apply_part_fault
    seen: list[int] = []

    def spy(job, fault, run) -> None:
        seen.append(engine.read_scheduler.active_read_count())
        return original(job, fault, run)

    monkeypatch.setattr(engine, "_apply_part_fault", spy)
    response = client.post(
        f"/jobs/{job_id}/run",
        json={
            "fault_injection": [
                {"shard_id": "gw-a.log", "mode": "delete_part", "domain": "api.example.com"}
            ]
        },
    )
    assert response.status_code == 200
    assert seen == [0]  # no source read still active when partition damage is applied
    body = response.json()
    assert body["run"]["repaired_shards"] == ["gw-a.log"]
    assert body["run"]["source_reads"][0]["started_at"]
    assert body["run"]["source_reads"][0]["finished_at"]


def test_read_failure_releases_shared_slot_and_keeps_job_retryable(
    client, make_job
) -> None:
    job_id = make_job(["gw-a.log", "gw-b.log"], job_id="failure-slots")
    engine = client.app.state.engine
    marked = client.post(
        f"/jobs/{job_id}/sources/unavailable", json={"shard_ids": ["gw-a.log"]}
    )
    assert marked.status_code == 200

    response = client.post(f"/jobs/{job_id}/run", json={})
    assert response.status_code == 200
    run = response.json()["run"]
    failed = [read for read in run["source_reads"] if not read["success"]]
    assert [read["shard_id"] for read in failed] == ["gw-a.log"]
    assert failed[0]["error_type"] == "simulated_unavailable"
    assert failed[0]["started_at"] and failed[0]["finished_at"]
    # The failed read gave its slot back.
    assert engine.read_scheduler.active_read_count() == 0

    # Another job can immediately use the shared scheduler.
    other = make_job(["gw-c.log"], job_id="healthy-after-failure")
    ok = client.post(f"/jobs/{other}/run", json={})
    assert ok.status_code == 200, ok.text
    assert engine.read_scheduler.active_read_count() == 0

    # Restoring the source and retrying succeeds (no leaked slot / attempt lock).
    restored = client.post(f"/jobs/{job_id}/sources/restore", json={"all": True})
    assert restored.status_code == 200
    retry = client.post(f"/jobs/{job_id}/run", json={})
    assert retry.status_code == 200
    assert retry.json()["run"]["source_read_counts"] == {"gw-a.log": 1}
    assert engine.read_scheduler.active_read_count() == 0


def test_corruption_after_scan_and_rescan_progress_is_verified_and_repaired(
    client, make_job
) -> None:
    """Brief sequence: initial scan, forced rescan, then corrupt a partition."""

    job_id = make_job(["gw-a.log", "gw-b.log", "gw-c.log"], job_id="corrupt-flow")
    first = client.post(f"/jobs/{job_id}/run", json={})
    assert first.status_code == 200
    assert first.json()["job"]["status"] == "completed"

    rescanned = client.post(
        f"/jobs/{job_id}/run", json={"rescan_shards": ["gw-a.log", "gw-b.log"]}
    )
    assert rescanned.status_code == 200
    assert rescanned.json()["run"]["source_read_counts"] == {
        "gw-a.log": 1,
        "gw-b.log": 1,
    }

    corrupted = client.post(
        f"/jobs/{job_id}/run",
        json={
            "fault_injection": [
                {"shard_id": "gw-b.log", "mode": "truncate_part", "domain": "api.example.com"}
            ]
        },
    ).json()
    assert corrupted["run"]["reread_shards"] == ["gw-b.log"]
    assert corrupted["run"]["repaired_shards"] == ["gw-b.log"]

    results = {
        item["domain"]: (item["total_requests"], item["status_5xx"])
        for item in client.get(f"/jobs/{job_id}/results").json()["results"]
    }
    assert results == EXPECTED_TOTALS


def test_concurrency_setting_is_read_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOG_DATA_DIR", "/tmp")
    monkeypatch.setenv("WORK_DIR", "/tmp")
    monkeypatch.setenv("SOURCE_READ_CONCURRENCY", "3")
    assert load_settings().source_read_concurrency == 3

    monkeypatch.delenv("SOURCE_READ_CONCURRENCY")
    monkeypatch.setenv("READ_SOURCE_CONCURRENCY", "5")
    assert load_settings().source_read_concurrency == 5

    monkeypatch.setenv("SOURCE_READ_CONCURRENCY", "not-an-int")
    with pytest.raises(ValueError):
        load_settings()


def test_docker_compose_wires_the_process_read_cap() -> None:
    root = Path(__file__).resolve().parent.parent
    compose = (root / "docker-compose.yml").read_text(encoding="utf-8")
    assert "SOURCE_READ_CONCURRENCY" in compose
    # The cap is process-local; the image must keep a single uvicorn worker.
    dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
    assert "--workers" in dockerfile and '"1"' in dockerfile
