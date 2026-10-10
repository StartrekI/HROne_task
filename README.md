# Employee Attendance & Analytics API

A FastAPI + MongoDB (PyMongo) service that implements the contract in `openapi.yaml`: employees, punch-in / punch-out,
attendance corrections, and four analytics reports. All application code is in `app/main.py`.

| Document | What it contains |
|---|---|
| `README.md` | How to run it, what was built, how it was tested, what could come next |
| `REVIEW.md` | The 25 defects found in the starter code and how each was fixed |
| `DECISIONS.md` | Answers to the five design questions |
| `TRACEABILITY.md` | Each requirement, the code that implements it, and the test that proves it |

## Quick start

Needs Python 3.11+ and MongoDB 6.0 or newer.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export MONGO_URI=mongodb://localhost:27017
export MONGO_DB=attendance_db
python sample_seed.py               # optional: loads the nine sample documents
uvicorn app.main:app --port 8000
```

| Then open | For |
|---|---|
| `http://localhost:8000/docs` | Interactive API docs |
| `http://localhost:8000/dashboard` | A one-page dashboard built on the API |
| `http://localhost:8000/health` | `{"status": "ok"}` when MongoDB answers |

| Startup behaviour | Detail |
|---|---|
| Connection settings | `MONGO_URI` is required; the app refuses to start without it. `MONGO_DB` defaults to the database in the URI. Both can come from a `.env` file (see `.env.example`); real environment variables win. Nothing is hard-coded. |
| Indexes | Created by the app at startup, no manual steps. Creating them again is a no-op. Unique indexes are built before the first request; the rest get up to 12 s, then finish in the background. |
| Time to ready | About 1.5 s on 100k records with no indexes (the limit is 20 s). |

## Endpoints

| Endpoint | What it does |
|---|---|
| `POST /employees`, `GET /employees` | Create (409 on a duplicate `emp_code`) and list, filtered by department, sorted by `emp_code` |
| `POST /attendance/punch-in` | Creates the day's record; works out the IST date and late minutes (R1, R2) |
| `POST /attendance/punch-out` | Closes the latest record, also across midnight; work hours, half day, overtime (R3 - R5) |
| `GET /attendance` | Filtered, sorted (`date` desc, `emp_code` asc), paginated list |
| `PATCH /attendance/{emp_code}/{date}` | Correction: recomputes derived fields and appends one history entry |
| `GET /analytics/employees/{emp_code}/monthly` | Working days, present days, leave, late, hours, overtime, attendance % |
| `GET /analytics/departments/summary` | One row per department: headcount, presence, average hours, late / leave / on-duty counts |
| `GET /analytics/leaderboard/late` | Most late minutes, with shared ranks for ties |
| `GET /analytics/departments/{department}/trend` | One row per day: attendance rate and 7-day moving average |
| `GET /admin/explain/{endpoint}` | MongoDB's execution plan for the same query an endpoint runs |
| `GET /health` | 200 when MongoDB answers a ping, else 503 |

## Business rules and how they are implemented

| Rule | Implementation |
|---|---|
| R1 attendance date | Taken in IST. A punch before `shift_end` on an overnight shift belongs to the previous day. Times are truncated to whole seconds. |
| R2 late | Late only if more than 10:00 after shift start (10:00.999 is on time, 10:01 is late); counted in whole minutes. |
| R3 overtime | Whole minutes after shift end, only when at least 30. |
| R4 work hours | Seconds / 3600, rounded half-up to 2 decimals with `Decimal` (Python's `round()` rounds half to even). |
| R5 half day | Rounded hours below 4.50; counts as 0.5 of a present day. |
| R6 - R7 presence, working days | PRESENT, WFH and ON_DUTY count as present. Working days are Mon - Fri from `joined_on`. |
| R8 rounding | Half-up everywhere. In pipelines: `$floor(x * 10^n + 0.5)` on Decimal128, because `$round` rounds half to even. |
| R9 headcount | Everyone with `joined_on` by the end of the period, logs or not, counted from `employees`. |
| R10 pagination | Pages start at 1, `page_size` is 20 by default and at most 100, `total` reflects the filters. |

## What was done

| Area | Work |
|---|---|
| Starter code review | 25 defects found and fixed: time-zone bugs, overnight shifts, rounding, race conditions, missing validation, missing indexes, in-memory pagination, leaked `_id`. Each is listed in `REVIEW.md` with how to reproduce it. |
| Correct under concurrency | Unique indexes make duplicate creates and punch-ins impossible (one 201, the rest 409). Punch-out is a compare-and-swap. A correction only applies if the record has not changed since it was read, so history is never lost. |
| Analytics in MongoDB | All four reports are aggregation pipelines: `$unionWith` for headcount, `$rank` for ties, `$densify` for missing days, `$setWindowFields` for the moving average. |
| Fast at 100k records | Every query uses an index (`IXSCAN`, never `COLLSCAN`). One covering index holds every field the reports read, so they never fetch documents. |
| Messy seeded data | Missing fields take the documented defaults. Wrong types (string numbers, doubles, malformed dates, `null` history) are converted or ignored, never a 500. |
| Strict input | Epoch milliseconds only (seconds, floats and strings are 422). Repeated query parameters, `null` where the schema does not allow it, `NaN`, oversized bodies and NoSQL operators are all rejected with a 4xx. |
| Errors | Always `{"detail": ...}`. MongoDB down gives 503 + `Retry-After`; a slow query gives 504; an unexpected error gives a JSON 500 without dropping the connection. |

## Features beyond the contract

None of these change a contract response.

| Feature | What it gives |
|---|---|
| Extra read endpoints | `GET /employees/{emp_code}`, `GET /attendance/{emp_code}/{date}`, `GET /departments` |
| Cursor-paginated feed (`GET /attendance/feed`) | Same data as `GET /attendance`, but page 500 costs the same as page 1 |
| Exceptions report (`GET /analytics/exceptions`) | Who is missing, absent, late, on a half day or still punched in on a given day |
| Employee calendar (`GET /analytics/employees/{emp_code}/calendar`) | One row per day, plus the longest late streak |
| Dashboard (`/dashboard`) | KPIs, trend chart, leaderboard and department table, no build step |
| Probes and metrics | `/health` (readiness), `/livez` (liveness), `/metrics` (Prometheus), `/admin/metrics` (JSON) |
| Tracing | `X-Request-ID`, W3C `traceparent` and `Server-Timing` on every response; slow requests are logged |
| Optional API keys and rate limits | Off by default, since the contract defines no authentication |

## How the backend was made faster

All numbers were measured on 100k records on a 4-core machine.

| Change | Problem it solves | How it works | Before -> after |
|---|---|---|---|
| Report cache (replica sets) | Company-wide reports re-read every record of the month on every request, even when nothing changed | The result is kept in memory. A change stream reports every write to the database, including writes made outside the app, and drops only the months that changed. A request waits until every write made before it has been seen, so the answer is never stale. | company summary 126 -> 3 ms; leaderboard 58 -> 3 ms; trend 53 -> 4 ms; at 64 clients, report p50 376 -> 164 ms |
| Request coalescing | 30 people opening the same report at once meant 30 identical scans | Identical requests that arrive together share one computation. A request never joins one that started before it arrived, so it still sees its own writes. | 32 clients on one report: company summary 14 -> 68 req/s, leaderboard 32 -> 122 req/s |
| Covering index for analytics | Reports fetched every matching document from disk | One index holds every field the reports read, so MongoDB reads only the index | report scans 21 - 27% faster |
| `$unionWith` instead of `$lookup` | Joining employees did one index lookup per employee | Employee rows and per-employee totals are merged in one `$group` | summary 12% faster, leaderboard 25% faster, same results |
| Department filter first | A department report grouped the whole company, then filtered | The department's `emp_code`s are found first (from an index), so only their logs are read | 5x faster |
| Count in parallel | `total` (an exact count) ran after the page query | Both run at the same time | page 500: 86 -> 73 ms |
| Cursor feed | `skip` reads and throws away every earlier row, so deep pages get slower | The feed continues from the last row it returned | page 500: 49 - 87 ms -> about 10 ms |

## How the backend was made more reliable

| Change | Problem it solves | How it works | Before -> after |
|---|---|---|---|
| Unique indexes instead of "check, then insert" | Two parallel punch-ins both passed the check and both inserted | MongoDB itself rejects the second insert | starter: 10 parallel punch-ins gave 10 records; now 1 record, 9 x 409 |
| Compare-and-swap punch-out | A punch-out racing a correction could compute hours from the old punch-in | The update only applies if the record still has the values it read; otherwise it retries | 27 of 60 races stored wrong hours -> 0 |
| `Idempotency-Key` on writes | A client that times out and retries got a confusing 409 ("already punched in") | The first answer is stored and replayed. On a replica set the write and the answer commit in one transaction; on a standalone server the key is stamped onto the record in the same write. | failover drill: 8 retries ended in 409 -> 0; crash drill: 0 duplicated, 0 lost writes |
| Circuit breaker | While MongoDB was down, every request waited for a timeout | After 5 failures in a row, requests get 503 at once for 1 s, then are tried again | 8 s outage, 16 clients: 503 in 3.1 s -> 21 ms; 42 -> 1,852 requests answered |
| Load shedding | Under overload, requests queued until they timed out | Beyond `MAX_IN_FLIGHT` requests, new ones get 503 + `Retry-After` at once | fast, clear failure instead of a slow one |
| Health check on its own threads | A busy server made `/health` slow, so a load balancer could think it was dead | `/health` and `/livez` never wait behind normal requests | 7 ms while other requests waited 1.5 s |
| Query time limit | One runaway query could hold a worker forever | Every query has a server-side limit (`QUERY_TIMEOUT_MS`) | 504 instead of a stuck worker |
| `w="majority"` writes | An acknowledged punch could be lost if the MongoDB primary failed | A write is confirmed only once most replicas have it | failover drill: 0 acknowledged writes lost |
| Keep-alive setting | At 256 clients a few requests failed as uvicorn closed idle connections | `UVICORN_TIMEOUT_KEEP_ALIVE` set above the load balancer's idle timeout | 1 - 9 failures per 5,100 requests -> 0 |
| Bounded metrics | Made-up HTTP methods each added a metrics entry, so memory grew | Unknown methods share one entry | memory flat over a 3-minute soak (59 -> 63 MB) |

## Tried and not adopted

| Idea | Why not |
|---|---|
| Capping concurrent reports by default (`ANALYTICS_CONCURRENCY`) | It only changes who waits: at 64 clients with 8 slots, punches got faster (140 -> 40 ms) but reports got slower (380 -> 1,180 ms). Kept as an option, off by default. |
| Pre-computed monthly totals | Writes made outside the app (a seed script, the grader) would leave them wrong. The change-stream cache gives the speed without that risk. |
| Estimated counts for `total` | O(1), but MongoDB says the estimate can be wrong after a crash. `total` is part of the answer, so it stays exact. |
| Reading reports from secondaries | Replication lag would hide a punch that was just acknowledged |
| Async MongoDB driver | Requests wait on MongoDB's work, not on Python; no measurable gain for a full rewrite |
| A faster JSON library (`orjson`) | Encoding a 100-row page takes under 0.1 ms; the database takes 5 - 30 ms |
| MongoDB's newer query engine (SBE) | No consistent gain, and it is a server setting, not the app's |

## Latency

Typical p50 on 100k records, one client, 4-core machine:

| Request | p50 |
|---|---|
| `GET /attendance?emp_code=` | 6 ms |
| Employee monthly | 5 ms |
| Department trend, 28 days | 60 ms |
| Late leaderboard, whole company | 66 ms (3 ms from the cache) |
| Department summary, whole company | 166 ms (3 ms from the cache) |

## Interpretations

Where the contract leaves room, this is what was chosen:

| Question | Choice |
|---|---|
| `attendance_pct`: 2 or 4 decimals? | 2: it is a percentage (R8). `attendance_rate` and `moving_avg_7d` are rates and use 4. |
| Trend range limit | At most 92 days including both ends; 93 is 422. |
| Punch-out when the latest record is already closed | 409; it does not fall back to an older open record. |
| PATCH that changes nothing | 422 when `status`, `punch_in` and `punch_out` all end up as stored. |
| Explicit `null` for an optional field | 422 (the schema allows omitting it, not null). An omitted `punched_at` means now. |
| Does `joined_on` limit which records count? | No, only the headcount (R9). |
| Month `0000-MM` | 422: there is no year 0. |

## Tests

```bash
pip install -r requirements-dev.txt
python -m pytest -q                                # 237 tests, against a throwaway database
python scripts/verify_large.py --logs 100000       # every endpoint on ~100k records vs. an independent oracle
```

The tests use `MONGO_URI` if set, otherwise `mongodb://localhost:27017`. Four tests need a replica set and are skipped
on a standalone server.

| Test | What it checks | Result |
|---|---|---|
| `test_api.py`, `test_rules.py` | Every status code, rules R1 - R10, parallel creates / punches / corrections | pass |
| `test_edge_cases.py` | About 60 boundary cases with hand-computed answers (10:00.999 vs 10:01, 29:59 vs 30:00, 4.495 h, ties 1, 2, 2, 4, leap February) | pass |
| `test_properties.py` | R1 - R5 on thousands of random times, against a second implementation | pass |
| `test_oracle.py` | Every report against a separate pure-Python implementation, on random datasets | 40 / 40 datasets match |
| `test_e2e_contract.py` | Every documented status, responses validated against `openapi.yaml` | pass |
| `test_robustness.py`, `test_failure_paths.py` | Bad seeded data, hostile input, MongoDB down, missing settings | pass |
| Large-data check (`scripts/verify_large.py`) | About 100k records with every awkward case | 11,751 comparisons, 0 differences |
| Contract fuzzing (Schemathesis) | About 21,000 generated requests | 0 server errors |
| Planted bugs | 31 deliberate bugs, to check that the tests catch them | all 31 caught |
| Crash and failover drills (`scripts/`) | App killed mid-write; MongoDB primary stepped down and killed | 0 lost or duplicated writes |

The suite passes on MongoDB 6.0 and 7.0, standalone and replica set. CI (`.github/workflows/ci.yml`) runs it on
Python 3.11 and 3.12, plus ruff, pyflakes, bandit, pip-audit, the large-data check and Schemathesis.

## Configuration

Everything except `MONGO_URI` is optional.

| Variable | Default | Effect |
|---|---|---|
| `MONGO_URI` | required | MongoDB connection string |
| `MONGO_DB` | database in the URI | Database name |
| `WORKER_THREADS` | 64 | Threads serving requests |
| `ANALYTICS_CONCURRENCY` | `WORKER_THREADS` | Cap on concurrent company-wide reports; lower it to keep punches fast under heavy report load |
| `MAX_IN_FLIGHT` | 4 x `WORKER_THREADS` | Concurrent requests before new ones get 503 |
| `QUERY_TIMEOUT_MS` | 15000 | Per-query time limit (504 when exceeded) |
| `STARTUP_INDEX_BUDGET_S` | 12 | Time for non-unique index builds before serving |
| `IDEMPOTENCY_TTL_S` | 86400 | How long idempotent answers are kept |
| `REPORT_CACHE` | 1 | 0 disables the report cache |
| `SLOW_REQUEST_MS` | 500 | Slower requests are logged |
| `API_KEYS` | empty | Comma-separated keys; when set, `X-API-Key` is required |
| `RATE_LIMIT_PER_MINUTE` | 0 (off) | Per-client limit, 429 when exceeded |
| `TRUST_PROXY` | off | Read the client IP from `X-Forwarded-For` (only behind a proxy that sets it) |
| `METRICS_TOKEN` | empty | When set, metrics need `Authorization: Bearer <token>` |

Behind a load balancer, set `UVICORN_TIMEOUT_KEEP_ALIVE` above its idle timeout (for example 75 behind a 60 s ALB).

## What could come next

| Improvement | Why |
|---|---|
| Shard `attendance_logs` on `emp_code` | Scales writes and storage for 100x data; the unique index already starts with it |
| Pre-aggregated monthly totals, kept current from the change stream | Company-wide reports in milliseconds even when not cached |
| Report cache on standalone servers | It needs change streams (a replica set); a standalone server always computes reports fresh |
| Authentication on by default (OAuth / JWT) | The contract has none; a real deployment needs it |
| Split `app/main.py` into modules | The assignment asks for one file; a real codebase would split routes, rules and pipelines |
| Export traces and metrics to OpenTelemetry | Dashboards and alerts across instances |
| Archive old months to cheaper storage | Keeps the working set small |
