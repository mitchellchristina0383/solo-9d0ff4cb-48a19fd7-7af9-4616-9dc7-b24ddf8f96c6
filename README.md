# Gateway Log Aggregator

FastAPI service that sums gateway access-log shards **by domain** and reports
**total requests** and **5xx responses**. Each job owns a fixed set of source
shards. Shards are first scanned into per-domain temporary count files, then
merged per domain.

It is built for the operational cases in the brief:

* shards are read **in parallel** through one process-local, configurable read
  concurrency pool (`SOURCE_READ_CONCURRENCY`, set in `docker-compose.yml`);
  initial scans, rescans and repair/backfill reads share the same slots;
* freed slots are offered first to a **job that has shards queued and no shard
  currently reading** (two contending jobs take turns fairly); leftover slots
  let one job read several shards at once;
* each shard's progress is persisted the moment its read finishes, and a
  failed read releases its slot; each run report lists every source read's
  **start/end timestamps and duration**;
* a temporary part file goes missing or gets truncated between partition and merge;
* repair re-reads **only** the source shards that own damaged parts and reports
  the re-read shard numbers;
* every source is counted **once** — repair replaces a shard's contribution, it
  never adds a second one, and a shard is attempted at most once per run;
* if the source shard is itself unreadable, its partitions are **withheld** (no
  half result is published), the run can be retried after recovery, and
  partitions whose sources are all healthy remain queryable throughout.

---

## 1. Quick start (Docker Compose)

```bash
python3 scripts/make_samples.py          # regenerate samples/data if needed
docker compose up --build -d
docker compose logs -f aggregator        # wait for "engine ready … source_read_concurrency=2"
```

The per-process source-read cap defaults to `2` and is configured in
`docker-compose.yml` (`SOURCE_READ_CONCURRENCY`, overridable from the host,
e.g. `SOURCE_READ_CONCURRENCY=4 docker compose up`). `/health` shows the live
scheduler state.

Open **http://localhost:8000/docs** and use *Try it out*:

1. `GET /sources` → `gw-01.log`, `gw-02.log`, `gw-03.log`.
2. `POST /jobs`
   ```json
   {"shard_ids": ["gw-01.log", "gw-02.log", "gw-03.log"], "job_id": "demo-1"}
   ```
3. `POST /jobs/demo-1/run` with `{}` → per-domain totals.
4. `GET /jobs/demo-1/results` → published partitions.

To demo repair with lost/truncated temp files:

5. `POST /jobs/demo-1/run`
   ```json
   {"fault_injection": [
     {"shard_id": "gw-02.log", "mode": "truncate_part", "domain": "api.example.com"}
   ]}
   ```
   The damaged part is detected and the same run re-reads only `gw-02.log`.
   The response contains
   `"reread_shards": ["gw-02.log"], "reread_shard_numbers": [2]`.

To demo an unrecoverable source (partition withheld, then retried):

6. `POST /jobs/demo-1/run`
   ```json
   {"fault_injection": [
     {"shard_id": "gw-02.log", "mode": "delete_part", "domain": "api.example.com"},
     {"shard_id": "gw-02.log", "mode": "source_unavailable"}
   ]}
   ```
   `status: partial`, `failed_repairs: ["gw-02.log"]`.
7. `GET /jobs/demo-1/results` → `api.example.com`, `www.example.com`, … appear
   under **`blocked`** with `blocked_shards: ["gw-02.log"]`; `ledger.example.io`
   and `media.example.dev` (not served by `gw-02.log`) stay published.
8. `POST /jobs/demo-1/sources/restore` `{"all": true}`
9. `POST /jobs/demo-1/run` `{}` → `completed`, all totals restored and equal to
   the baseline.

The same sequence is scripted: `BASE_URL=http://localhost:8000 bash scripts/demo.sh`.

Stop with `docker compose down` (add `-v` to also drop the work volume).

## 2. Run without Docker

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
LOG_DATA_DIR=./samples/data WORK_DIR=./work ALLOW_SIMULATION=true \
  uvicorn app.asgi:app --reload --port 8000
pytest -q
```

## 3. API

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | liveness + directory writability |
| GET | `/sources` | source shard files available under `LOG_DATA_DIR` |
| POST | `/jobs` | create a job with a fixed, ordered shard list |
| GET | `/jobs` | list jobs with partition counters |
| GET | `/jobs/{id}` | full state: shard manifests, scan status, last run report |
| POST | `/jobs/{id}/run` | scan → partition → verify → repair → merge; returns run report |
| GET | `/jobs/{id}/results` | published per-domain results; `blocked` and `pending` lists |
| POST | `/jobs/{id}/sources/unavailable` | demo: simulate an unreadable source (needs `ALLOW_SIMULATION=true`) |
| POST | `/jobs/{id}/sources/restore` | demo: clear simulated outages, then `POST /run` to retry |

`POST /jobs/{id}/run` body (all optional):

```json
{
  "fault_injection": [
    {"shard_id": "gw-02.log", "mode": "delete_part", "domain": "api.example.com"},
    {"shard_id": "gw-02.log", "mode": "truncate_part", "domain": "api.example.com",
     "truncate_bytes": 10},
    {"shard_id": "gw-02.log", "mode": "source_unavailable"}
  ],
  "rescan_shards": ["gw-01.log"]
}
```

* `delete_part` / `truncate_part` need a `domain`; they act on that shard's
  temporary count file.
* `source_unavailable` is sticky: it persists in the job until
  `/sources/restore` is called (or the job is re-created). It is also available
  from request bodies even when the REST simulation endpoints are disabled.
* `rescan_shards` forces a fresh read of listed shards (e.g. after log
  rotation); their parts are replaced, never appended.

| field | meaning |
|---|---|
| `source_reads` | every source-read attempt in this run, with `kind` (`scan`/`retry_scan`/`rescan`/`repair`), `success`, `started_at`, `finished_at`, `duration_ms`, and `error_type`/`error` on failure |
| `reread_shards` | sources re-read in this run (repairs, retry scans, rescans) |
| `reread_shard_numbers` | their 1-based positions in the job's fixed shard list |
| `repaired_shards` / `failed_repairs` | repair outcome per source |
| `source_read_counts` | successful full reads per source **in this run** (max 1) |
| `faults` | applied / no-op / failed simulations |
| `warnings`, `errors` | malformed-line samples, withheld partitions, failures |

`GET /health` additionally reports `source_read_concurrency` and a
`read_scheduler` snapshot (`active_reads`, `active_reads_by_job`,
`queued_shards_by_job`, `idle_jobs_with_queue`) that is useful while watching
two jobs contend.

`GET /jobs/{id}/results` also exposes cumulative `reread_shards` /
`reread_shard_numbers` and `unscanned_shards`.

## 4. Accepted log line shapes

Blank lines and `#` comments are ignored. Everything else must resolve to a host
and an HTTP status in 100–599; otherwise it is counted as malformed and excluded
from totals (samples are reported in `run.warnings`).

| Shape | Example |
|---|---|
| gateway | `api.example.com 200 10.0.0.1 GET /v1/items` |
| timestamped | `2026-11-22T10:00:00Z api.example.com 503` |
| combined log | `api.example.com - - [22/Nov/2026:10:00:00 +0000] "GET / HTTP/1.1" 500 512` |
| CSV | `api.example.com,201` |
| JSON | `{"domain": "api.example.com", "status": 502}` / `status_code` / `host` |

Hosts are lower-cased and a trailing root dot is dropped; `host:port`,
`https://host/path` and bracketed IPv6 are accepted. 5xx means 500–599.

## 5. Design

### Pipeline

```
source shards (read-only, LOG_DATA_DIR)
   │  one streaming pass per shard: sha256 + per-domain counters
   ▼
parts/<shard>/<sha256(domain)>.json     temporary count files, atomic writes
   │  verify size + sha256 + JSON schema + manifest totals
   ▼
repair: for each shard owning a damaged part → re-read that source once,
        rewrite all of its parts, replace its contribution
   ▼
merge: for each domain, sum at most one contribution per shard in fixed order
   ▼
job.json (atomic) ──► GET /results
```

### Parallel reads and fair slots

`app/scheduler.py` owns one process-local `ReadScheduler` per Engine (one
Engine in the single uvicorn process). It runs exactly
`SOURCE_READ_CONCURRENCY` daemon reader workers (default `2`; Compose sets it
explicitly and it can be overridden from the host shell). Every disk read —
initial scan, forced rescan, retry scan, repair/backfill — is a `ReadRequest`
through this one pool, so all kinds and all jobs share the cap.

Whenever a read slot frees, the next shard is picked under one condition lock:

1. **idle jobs first** — a job that has shards queued and zero reads in flight
   wins; among several, the job waiting longest (a monotonic fairness token
   assigned when it becomes idle-with-queue) is served first;
2. **then same-job parallelism** — if no job is idle-with-queue, spare slots
   go to the job with the fewest in-flight reads, then the oldest request, so
   one job reads several shards at once.

The slot bounds the actual source read and is released in a `finally` as soon
as the read returns or raises; per-shard progress is then persisted under a
per-job state lock (several reader threads can finish together). Each shard
save is the same temp-file + fsync + `os.replace` write as before. A batch
barrier waits for every shard callback, so after the scan phase all
initial-scan/rescan progress is durably on disk before part faults are
applied and verify → repair → merge begins. `run_batch` waits for sibling
shards even when one callback fails, then re-raises the first error.

The image starts uvicorn with `--workers 1` deliberately: the fairness pool is
process-local. Running more workers/replicas multiplies the effective cap.

### Why no double counting

* A partition is always **recomputed** from the manifest as
  `sum(contribution[shard] for shard in job.shard_ids)`; nothing is accumulated
  in place. Repair overwrites the shard's parts and manifest entry.
* Counters live in `parts/`, keyed by shard and domain, one file per pair. A
  re-read deletes parts for domains that vanished.
* Each `POST /run` **attempts** a given source at most once, even with parallel
  readers: the scan plan contains each eligible shard once, and the repair
  phase skips any shard already present in `run.source_reads` (the
  `attempted` set), so a rescan followed by a fault never produces a second
  read in the same round.
* `job.json` and every part file are written temp-file + fsync + `os.replace`,
  so a crash leaves either the old or the new file, never a torn one.

### Withholding instead of half results

`_merge` publishes a domain only if **every** shard either provides a verified
part or has verified zero coverage:

1. a shard that never completed a scan has unknown coverage → **all** domains
   are withheld while it is in that state;
2. a shard that has a verified snapshot but whose part for this domain is
   damaged/unverifiable → this domain is withheld;
3. a shard whose last successful scan did not contain the domain correctly
   contributes zero.

Blocked partitions are zeroed (`total_requests: 0`) and only listed under
`blocked` with the responsible shards, so a consumer never sees a partial sum.
Other partitions keep their previous published values.

### Interruption and retry

* Cross-process `flock` on `jobs/<id>/.lock` plus an in-process thread lock;
  a second run gets `409` (`JobBusy`) immediately (non-blocking).
* Scan progress is persisted after every shard. A killed process (SIGKILL, no
  cleanup) leaves `status: processing`; the OS releases the lock, and the next
  `POST /run` resumes, reusing trusted shard snapshots and reporting an
  `interrupted` warning.
* A crash between writing a part and updating `job.json` is harmless: parts are
  content-addressed and verified, and the manifest is rewritten on the next
  successful scan.
* Retry after `source_unavailable` is a plain `POST /run`; only the still-unread
  shards are read (reported in `reread_shards`).

### Persistence layout

```
WORK_DIR/jobs/<job_id>/job.json                  authoritative state (atomic)
WORK_DIR/jobs/<job_id>/.lock                     flock target
WORK_DIR/jobs/<job_id>/parts/<shard>/<hash>.json temporary count files
```

`LOG_DATA_DIR` is mounted read-only; shard IDs are validated as plain file names
and resolved paths must stay inside it (symlink escapes are rejected).

## 6. Verification performed

* `pytest -q` → **87 passed** (the original 75 unit/API/crash/retry tests plus
  12 new tests in `tests/test_concurrency.py` for the fair scheduler, parallel
  scans, per-shard persistence, two-job contention, failure slot release, the
  corruption→rescan/repair flow and the Compose wiring). The concurrency tests
  were re-run repeatedly to rule out timing flakes.
* Two-job contention, automated (`tests/test_concurrency.py`):
  * scheduler white-box test proves the pick order is idle-job (longest
    waiting token) → other idle job → same-job spare slot, and that a freshly
    idle job ranks behind an already-waiting job;
  * deterministic gate test with cap 2: two shards of job A fill the slots,
    job B queues, and when one A slot frees **B's first read starts before
    A's third shard**; global active reads never exceed 2;
  * real `Engine` two-job run (6 shards each, cap 2): observed max in-read
    concurrency 2, real cross-job overlap, every shard read exactly once,
    correct totals, all slots released;
  * a gated blocked shard proves another shard's `done` progress is already
    durably in `job.json` while the blocked shard is still reading;
  * a failing read releases the slot and the same scheduler worker immediately
    serves the next batch.
* Live HTTP self-test with `uvicorn` started with the Compose-equivalent
  environment (`SOURCE_READ_CONCURRENCY=2`, `ALLOW_SIMULATION=true`):
  * `GET /docs` → 200 and `/openapi.json` exposes `started_at` / `finished_at`
    / `duration_ms` on `SourceRead`; `GET /health` reports the cap and
    scheduler snapshot;
  * two jobs launched simultaneously over HTTP (4–6 sizeable shards each):
    report intervals show max overlap depth exactly 2 and cross-job overlap;
    health sampling repeatedly caught `active_reads_by_job={job-a:1, job-b:1}`
    (one fair slot per job); every `source_read_counts` entry is 1;
  * real read-source failure: `chmod 000` on `gw-03.log` →
    `error_type: permission_denied` with start/end timestamps,
    `active_reads` back to 0 immediately; after `chmod 644` the retry reads
    exactly `gw-03.log` once and completes;
  * brief sequence over HTTP: initial run `completed` →
    `rescan_shards: [gw-01, gw-02]` reads each once → confirmed `job.json` and
    part files persisted → externally truncated `gw-02`'s
    `api.example.com` part → next run reports
    `reread_shards: ["gw-02.log"]`, numbers `[2]`,
    `repaired_shards: ["gw-02.log"]`, and merge totals equal the baseline;
  * `BASE_URL=... bash scripts/demo.sh` passes end to end (baseline, truncate
    repair, delete + unavailable → `partial`/withheld, restore → completed).
* Live `uvicorn` run on the sample shards (`scripts/demo.sh`) baseline totals:
  `api.example.com 48/3`, `cdn.shop.example.org 47/2`,
  `ledger.example.io 29/1`, `media.example.dev 9/1`, `shop.example.org 47/3`,
  `static.example.net 47/3`, `www.example.com 48/2` (total/5xx).
* Crash tests: SIGKILL of a real child process mid-run (400k-line shard) followed
  by a successful retry; state rewound to "never scanned gw-c" followed by a
  retry that reads only `gw-c.log` and lands on the exact baseline totals.

> The live contention run used deliberately large shards on a 2-CPU-cgroup
> QEMU VM; per-read wall times are inflated by that environment's CPU
> scheduling, but the scheduling invariants (cap, fairness order, slot
> release, exactly-once) are asserted independently of timing.

## 7. Remaining items / limits

* **No source-diff detection without `rescan_shards`.** If a source file is
  replaced in place between jobs (log rotation), totals stay at the previous
  snapshot until a rescan is requested. Automatic detection would need mtime/size
  comparison per run; deliberately not implied, because readers may replay
  immutable archives.
* **One active run per job id.** Same-job runs are still serialized by the
  in-process lock + cross-process `flock` (a concurrent same-job request gets
  `409`). Different jobs run their reads concurrently and share the fair
  scheduler; there is no separate durable worker queue.
* **The read pool is per process.** `SOURCE_READ_CONCURRENCY` bounds one uvicorn
  worker; the Dockerfile pins `--workers 1`. Multi-worker or multi-replica
  deployments would get one pool per process (an N-worker server multiplies
  the effective cap) and would need a shared/lock-backed scheduler for a
  cluster-wide bound.
* **Simulation endpoints are demo-only.** They are gated by
  `ALLOW_SIMULATION` (enabled in Compose); production use should disable them.
  `fault_injection` in `POST /run` is also test-only and should be refused at the
  ingress in production.
* **Whole-file reads.** Shards are read in a single pass and held per-domain in
  memory (one small dict per shard), which is fine for the sample sizes; a shard
  with an extreme number of distinct domains would need external sorting.
* **No auth/TLS.** The service is intended for a trusted local network; put it
  behind a reverse proxy if exposed.
* **`docker compose up` was not executed from this sandbox.** There is no
  Docker CLI/daemon here and the process runs with an empty effective capability
  set (`CapEff=0`, no rootless helper), so an actual Compose boot could not be
  performed in the delivery environment. `docker-compose.yml` was YAML-validated
  and its environment/command were exercised equivalently by running uvicorn
  locally with the same `LOG_DATA_DIR`, `WORK_DIR`, `ALLOW_SIMULATION` and
  `SOURCE_READ_CONCURRENCY=2`; `/docs` and `/openapi.json` were checked over
  HTTP (the Swagger page itself was not clicked in a browser).
