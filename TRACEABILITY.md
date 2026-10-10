# Requirements traceability

Every requirement in `PROBLEM_STATEMENT.docx`, `openapi.yaml` and `DATA_MODEL.md`, the code that implements it
(`app/main.py`), and the evidence that it holds. "Verifier" is `scripts/verify_large.py` (about 100,000 seeded
records against an independent oracle: 11,751 value comparisons and 11,697 responses validated against the
contract's schemas with strict types, 0 differences on MongoDB 7.0, 6.0 and 1- and 3-node replica sets).

## Environment and submission (problem statement sections 2, 4, 12)

| Requirement | Implementation | Evidence |
|---|---|---|
| All application code in `app/main.py` | single module | repository layout |
| Starts with `uvicorn app.main:app --port 8000`, ready within 20 s | lifespan builds the unique indexes, then the others within a 12 s budget | clean clone + fresh venv: ready in 1.7 s on 100k unindexed records; 12.5 s on 1M; `test_failure_paths.py` (budget) |
| `MONGO_URI` / `MONGO_DB` from the environment; real variables beat `.env`; nothing hard-coded | `load_dotenv(override=False)`; no built-in defaults: without `MONGO_URI` the app stops at startup with a clear message, without `MONGO_DB` it uses the database named in the URI | probe: a `.env` with a dead URI is used alone (app not ready) and loses to real variables (its database is never created); `test_connection_details_are_never_hard_coded` |
| All indexes at startup, idempotently | `ensure_indexes` (create_index is a no-op when present; TTL changes via collMod) | two starts on the same database; `test_failure_paths.py` (duplicate seeded data, budget) |
| Seeded database has only `_id` | indexes built by the app | verifier and clean-clone start on an `_id`-only database |
| No network access other than MongoDB | no HTTP client in the app | `app/main.py` imports |
| Concurrent requests handled correctly | unique indexes, compare-and-swap updates, optimistic PATCH | `test_api.py`, verifier (20 parallel punch-ins / punch-outs, 10 parallel corrections), stress and chaos drills |
| `requirements.txt` includes everything imported | fastapi, uvicorn, pymongo, pydantic, python-dotenv, starlette, anyio (all pinned) | fresh venv: `pip check` clean, app imports |
| No `.env`, secrets, Dockerfile, virtualenv, data dumps | `.gitignore` | `git ls-files` |
| REVIEW.md, DECISIONS.md filled in; README says how to run | 25 defects in the template's columns plus "not a defect"; 5 answers in 175 words | the files |

## Data model and conventions (problem statement section 5, DATA_MODEL.md)

| Requirement | Implementation | Evidence |
|---|---|---|
| `emp_code` is the identity; `_id` never in a request or response | lookups by `emp_code`; `{"_id": 0}` projections; `employee_out` / `record_out` pick fields | probe: no `_id` in any contract response |
| Instants are epoch ms; seconds-looking values, floats, strings, out-of-range -> 422 | `EpochMs = int, strict, 100000000000..4102444800000` (the contract's bounds) | `test_edge_cases.py` (99999999999, 4102444800001, float, string, bool, seconds) |
| PyMongo naive datetimes handled | `tz_aware=True`; `dt_to_ms` treats naive as UTC | `test_rules.py`, server time zones Kolkata / New York / Kiritimati |
| Calendar values are strings (`YYYY-MM-DD`, `YYYY-MM`, `HH:MM`) | `DATE_RE`, `MONTH_PATTERN`, `HHMM_PATTERN` = the contract's patterns | `test_edge_cases.py` (invalid employees), contract fuzzing |
| Derived fields computed by the API, never sent; a sent one is ignored | pydantic ignores unknown keys; `derived_fields` | probe and `test_edge_cases.py`: sent `late_minutes`, `work_hours`, `half_day` are ignored on punch-in, punch-out and PATCH |
| What a written record holds in each state (DATA_MODEL table) | `derived_fields` | verifier (every response and stored document after punch-in, punch-out and correction) |
| Analytics read the stored derived values | pipelines read `late_minutes`, `work_hours`, `half_day`, `overtime_minutes` | verifier (seeded values unrelated to the punch times) |
| Missing `history` / `half_day` / `late_minutes` / `overtime_minutes` mean empty / false / 0 | `record_out`, `$convert` defaults in pipelines | `test_e2e_contract.py` (sample #9), verifier (legacy records) |
| A weekday with no record differs from ABSENT | trend counts no record as not present but keeps the day; ABSENT is a record | `test_edge_cases.py` (rate 0.0 on a day without records) |
| At most one record per (`emp_code`, `date`) | unique index `uniq_emp_code_date` | concurrency tests |
| Status table: PRESENT/WFH/ON_DUTY via punch-in; ABSENT/LEAVE only seeded or by correction, with both times null | `PunchIn.status: Literal[PRESENT, WFH, ON_DUTY]`; PATCH clears times for ABSENT/LEAVE | `test_edge_cases.py` |
| History: corrections only; one entry per PATCH; only fields that changed; times are BSON dates in MongoDB, ms in the API | `regularize_op`, `CHANGE_FIELDS`, `change_value` | `test_edge_cases.py` (exact `changes`), verifier, `test_e2e_contract.py` (sample #8) |

## Business rules R1 - R10 (openapi.yaml)

| Rule | Implementation | Evidence |
|---|---|---|
| R1 IST date of punch-in; overnight punch earlier than `shift_end` belongs to the previous day; truncate to seconds | `attendance_date`, `shift_bounds`, `ms_to_dt` | `test_edge_cases.py` (exactly `shift_end`, 05:59:59.999, 23:59:59.999, 00:00 shifts), `test_properties.py` (about 11,500 random instants), verifier |
| R2 late only if strictly more than 10:00 after start; minutes since start | `compute_late_minutes`: `seconds > 600`, `seconds // 60` | 10:00.999 vs 10:01 (edge cases, properties, verifier) |
| R3 overtime floor minutes after `shift_end`, only if >= 30 | `compute_overtime` | 29:59 / 30:00 / 30:59 (edge cases, verifier) |
| R4 work hours = seconds / 3600, 2 decimals half-up | `compute_work_hours` (Decimal, ROUND_HALF_UP) | 4.495, 4.485, 1.125 h (edge cases tell half-up from half-to-even) |
| R5 half day when the rounded hours < 4.50; counts 0.5 | `derived_fields`; `PRESENT_VALUE` | 4.495 h -> 4.50, not half (edge cases, verifier) |
| R6 PRESENT, WFH, ON_DUTY are present | `PRESENCE`, `IS_PRESENT` | oracle, verifier |
| R7 Mon-Fri working days from `joined_on`; present_days Mon-Fri only; weekend records still count for late / overtime | `working_days`, `monthly_working_days`, `IS_WEEKDAY` | Saturday joiner, leap February, weekend record (edge cases), oracle |
| R8 2 decimals half-up, rates 4 | `round_half_up` in Decimal128 | 3.125 % -> 3.13, 9.125 h -> 9.13, 0.3333 / 0.6667 (edge cases) |
| R9 headcount = `joined_on` <= end of period, logs or not | `joined_by` | edge cases, oracle, verifier (no-log and future joiners) |
| R10 page from 1, `page_size` default 20 max 100, `total` reflects filters | `Page`, `PageSize`, `paged` (exact `count_documents`) | edge cases (bounds, filtered totals), verifier (every page of `/employees`, 23 attendance filters) |

## Endpoints (openapi.yaml)

| Endpoint and documented behaviour | Implementation | Evidence |
|---|---|---|
| `GET /health`: 200 `{"status": "ok"}` only if MongoDB answers a ping, else 503 | `health` | `test_failure_paths.py`, `test_e2e_contract.py` |
| `POST /employees`: 201 Employee; 409 duplicate (unique index, one 201 under concurrency); 422 invalid (patterns, lengths, `shift_start != shift_end`); `created_at` by server | `create_employee`, `EmployeeIn` | edge cases, `test_api.py`, e2e |
| `GET /employees`: department exact and case-sensitive, sorted by `emp_code`, filtered `total` | `list_employees` | verifier (every page; departments `Eng` and `eng`) |
| `POST /attendance/punch-in`: R1 date, R2 late, `punch_out`/`work_hours` null, overtime 0, half_day false, history []; `punched_at` defaults to now; status default PRESENT; 404 / 409 / 422 | `punch_in_op` | edge cases, verifier, e2e |
| `POST /attendance/punch-out`: closes the latest record with `punch_in` <= `punched_at` (across midnight); 404 unknown employee or no punch-in; 409 already out; 422 if <= punch_in or more than 24 h later | `punch_out_op` (compare-and-swap) | edge cases (closed earlier record, exactly 24 h, same second), verifier |
| `GET /attendance`: filters, sort `date` desc then `emp_code` asc, inclusive dates, `date_from > date_to` -> 422, fast at 100k | `list_attendance`, index `date_emp_code_covering` | verifier, explain `IXSCAN` |
| `PATCH /attendance/{emp_code}/{date}`: omitted fields unchanged; recompute; append one history entry; ABSENT/LEAVE + times -> 422; presence needs a punch_in; punch_in stays on the attendance date; punch_out after punch_in within 24 h; no change -> 422; 404; concurrent edits never lose history (409 for the loser) | `regularize_op` | edge cases (each rule), verifier (10 parallel corrections) |
| Monthly: working_days (R7), present_days, leave_days, late_count, totals, attendance_pct (null when no working days); 404 | `employee_monthly_pipeline` | oracle, verifier (every employee, 8 months), edge cases |
| Department summary: one item per department sorted; headcount R9; present_days; avg over records with present status and a value; late/leave/on-duty counts; headcount 0 omitted; department from employees | `department_summary_pipeline` | oracle, verifier, edge cases |
| Leaderboard: totals > 0; competition ranking; limit after ranking; order total desc, emp_code asc; ranking inside a department; unknown employees ignored | `late_leaderboard_pipeline` (`$rank`) | edge cases (1, 2, 2, 4 under every limit), oracle, verifier |
| Trend: one row per day (`$densify`); is_working_day; headcount by day; present_count any day; late_count; rate only on working days with headcount; 7-row moving average inside the range; 404 unknown department; 422 `to` < `from` or > 92 days | `department_trend_pipeline` | edge cases (12-day window, 92 / 93 days), oracle, verifier |
| `GET /admin/explain/{endpoint}`: executionStats of the same query; same parameters; missing required -> 422; `IXSCAN`, no `COLLSCAN` at 100k | `explain` (same builders as the endpoints) | verifier (9 variants), clean clone, e2e |
| Errors: `{"detail": ...}`; 422 in FastAPI's validation shape; a 500 is always a failed test | exception handlers; middleware catch-all | e2e (every documented status, strict schema), contract fuzzing (0 server errors), `test_robustness.py` |

## Problem statement, parts A - C

| Requirement | Evidence |
|---|---|
| Part A: more than ten starter defects found, fixed and listed with where / what / how noticed / fix, plus what was examined and fine | REVIEW.md (25 defects) |
| Part B: three non-trivial aggregations, in MongoDB, not Python loops | pipeline builders in section 7 of `app/main.py` |
| Part B: own test data with the awkward cases (no logs, ties, weekends, mid-month joiner, overnight shift, missing weekday) | `test_oracle.py`, `test_edge_cases.py`, `scripts/verify_large.py` |
| Part C: explain endpoint; DECISIONS.md, five answers, about 150 - 200 words | explain endpoint above; DECISIONS.md (175 words) |
