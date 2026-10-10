# REVIEW.md

Defects found in the starter `app/main.py`. Line numbers refer to the **starter** file. Everything was fixed in the
rewritten `app/main.py`; the "Fix" column names the function that now does it.

| # | Where (function / line) | What is wrong | How you'd notice it (test, input, or symptom) | How you fixed it |
|---|---|---|---|---|
| 1 | `compute_late_minutes`, L35 | The shift start is built with `punch_in.replace(hour, minute)` on whatever time zone `punch_in` has (naive server-local time or UTC), not IST. | Server in UTC, shift 09:30, punch-in 10:05 IST (04:35 UTC): start becomes 09:30 **UTC**, the difference is negative, so the result is 0 instead of 35. | The shift start is an IST instant built from the record's attendance date (`shift_bounds`). |
| 2 | `compute_late_minutes`, L36-37 | The grace check runs on minutes that have already been floored (`int(...) > 10`), so anything up to 10:59 after the shift start counts as on time. R2 says strictly more than 10:00. | Shift 09:30: 09:40:01 returns 0, expected 10 (and 09:40:59 returns 0, expected 10). | Compare whole seconds: `seconds > 600`, then `seconds // 60`. |
| 3 | `compute_late_minutes`, L35 | Overnight shifts are ignored. A punch-in after midnight is compared with the *same* calendar day's shift start. | Shift 22:00-06:00, punch-in 00:30: returns 0, expected 150. | The start comes from the attendance date (the previous day, by R1). |
| 4 | `compute_work_hours`, L41 | `round()` is float, banker's-style rounding, not half-up (R4). Milliseconds are not truncated first (R1). | 3 h 30 min 18 s = 3.505 h: `round(3.505, 2)` gives 3.5, expected 3.51, which also flips `half_day`. 0.125 gives 0.12. | `Decimal(seconds) / 3600` with `ROUND_HALF_UP`, on whole seconds. |
| 5 | `compute_overtime`, L47 | No 30-minute minimum (R3). | Punch-out 18:35 on an 18:30 shift: returns 5, expected 0. | Return the minutes only when they are >= 30. |
| 6 | `compute_overtime`, L46 | The shift end of an overnight shift is put on the same date as the shift start. | Sample #7 (22:00-06:00, out 06:40 next day): returns about 1480, expected 40. | `shift_bounds` adds a day when `shift_end <= shift_start`. |
| 7 | `compute_overtime`, L47 + client L22 | The client returns **naive** datetimes, and `end` is aware: `punch_out - end` raises `TypeError`, which is a 500. | The first punch-out that reads a stored `punch_out` crashes. | `MongoClient(..., tz_aware=True)`; every time value is aware UTC internally. |
| 8 | Module, L22 | `MongoClient` uses the default 30 s server selection, and no indexes are created at all. | `/health` would hang 30 s on a dead database. `/attendance` does a COLLSCAN. Uniqueness is not enforced. | `serverSelectionTimeoutMS=3000`; `ensure_indexes()` at startup (idempotent), and retried from `/health` if it failed. |
| 9 | `EmployeeIn`, L53-60 | No validation at all: `emp_code` format, email, lengths, `HH:MM`, `joined_on` date, `shift_start != shift_end`. | `{"emp_code": "x", "shift_start": "25:99", "joined_on": "2026-02-30"}` returns 201. | Pydantic constraints from `openapi.yaml`, plus validators for the date and the equal shift times. |
| 10 | `PunchInIn`, L65-66 | `punched_at: Optional[int]` in lax mode accepts `"1783312500000"`, `1783312500000.0` and `true`, and has no range check. Values in seconds (`1783312500`) give dates in 1970. `status` accepts any string, including `ABSENT`. | `punched_at: 1783312500` returns 201 with date 1970-01-21. `status: "FOO"` returns 201. | `EpochMs = int` with `strict=True`, `ge=1e11`, `le=4102444800000`; `status: Literal[PRESENT, WFH, ON_DUTY]`. |
| 11 | `health`, L72-74 | Always returns 200, even when MongoDB is down. | Stop mongod: `/health` still says ok. | `ping`; 503 on `PyMongoError`. |
| 12 | `create_employee`, L79-83 | Check-then-insert race with no unique index: concurrent creates all get 201 and duplicates are stored. | 10 parallel POSTs with the same `emp_code` give 10 x 201. | Unique index on `emp_code`; insert and map `DuplicateKeyError` to 409. |
| 13 | `create_employee`, L82-85 | `created_at = datetime.now()` is naive *local* time (stored wrongly when the server is not UTC), and the response returns it as an ISO string, not epoch ms. | Response `created_at: "2026-..."`, expected an integer. | `now_utc()`; `employee_out` converts to ms. |
| 14 | `list_employees`, L93 | `skip = page * page_size` skips one page too many. | `page=1` with 5 employees returns `[]`. | `(page - 1) * page_size`. |
| 15 | `list_employees`, L94 | `total` counts the whole collection and ignores the department filter (R10). | `?department=Sales` returns `total` = all employees. | `count_documents(q)`. |
| 16 | `list_employees`, L95 | No sort, so the order is whatever MongoDB returns, and paging is not stable. `created_at` is not converted. `page` and `page_size` are not validated. | `page=0`, `page_size=1000` are accepted; the order changes after updates. | `.sort("emp_code")`; `Query(ge=1)`, `Query(ge=1, le=100)`. |
| 17 | `punch_in`, L101/113 | Unknown employee: `emp` is `None` and `emp["shift_start"]` raises `TypeError`, a 500. | `emp_code: "EMP9999"` returns 500, expected 404. | `get_employee()` raises 404. |
| 18 | `punch_in`, L102-103 | `datetime.fromtimestamp` uses the **server's local time zone**, and `date` is that local date, not the IST date. The R1 overnight rule is missing. | Server in UTC, punch 02:00 IST on the 10th: date "2026-07-09". Overnight punch 00:30 gets the new day instead of the previous one. | `ms_to_dt` (UTC, truncated to seconds) and `attendance_date` (IST + overnight rule). |
| 19 | `punch_in`, L104-118 | Check-then-insert race: two parallel punch-ins both pass `find_one` and both insert (no unique index). | 10 parallel identical punch-ins give 10 x 201 and 10 documents. | Unique index `(emp_code, date)`; insert and catch `DuplicateKeyError`, giving 409. |
| 20 | `punch_in`, L102/110 | No truncation to whole seconds (R1), and `if body.punched_at` treats `0` as "now". | `...900` ms is stored and returned. | `ms // 1000`; `is not None`. |
| 21 | `punch_in`, L119-121 | The response contains `id` (the contract never exposes `_id`), and `punch_in` is a datetime, not epoch ms. | Response has `"id": "66a..."` and `"punch_in": "2026-07-06T..."`. | `record_out()` builds the contract shape. |
| 22 | `list_attendance`, L144-147 | Loads **every matching document into memory** and sorts and paginates in Python, with no index. | About 100k records: seconds per request and high memory. Explain shows COLLSCAN. | `find(q).sort(date desc, emp_code asc).skip().limit()` backed by indexes; `total = count_documents(q)`. |
| 23 | `list_attendance`, L145 | Sorts by `date` only; the contract says `date` desc, **then `emp_code` asc**. | Rows for the same day come back in insertion order. | Compound sort and a matching index. |
| 24 | `list_attendance`, L149 | Leaks `_id` as `id`, returns raw datetimes (also inside `history[].changes`), and does not default missing `history` / `half_day`. | A legacy record (sample #9) has no `history` / `half_day` in the response. | `record_out()` / `history_out()`. |
| 25 | `list_attendance`, L125-143 | No validation: `date_from > date_to` (contract: 422), bad date strings, unknown `status`, `page`/`page_size` bounds. Truthiness checks (`if emp_code:`) silently drop an empty value. | `?date_from=2026-07-10&date_to=2026-07-01` returns 200 with an empty list. | `parse_date`, a `Status` Literal, `Query` bounds, `is not None` checks. |

### Looked at and decided NOT to be a defect
- **`load_dotenv()`** defaults to `override=False`, so real environment variables already win over `.env`, as the contract
  requires. I made it explicit (`override=False`) rather than changing the behaviour.
- **The fallback `mongodb://localhost:27017` / `attendance_db`** were local-development defaults, not credentials, and
  real `MONGO_URI` / `MONGO_DB` always took precedence. I still removed them, because the submission checklist says
  nothing may be hard-coded: the app now refuses to start without `MONGO_URI`.
- **Sync `def` endpoints with one shared `MongoClient`** are fine: FastAPI runs them in a thread pool and `MongoClient`
  is thread-safe with its own connection pool. Correctness under concurrency comes from unique indexes and conditional
  updates, not from Python locks.
- **`IST = timezone(timedelta(hours=5, minutes=30))`** is correct: India has no DST, so a fixed offset is enough.
- **Pydantic ignoring unknown body fields** matches the contract ("if a client sends a derived field, ignore it").
- **`date` stored as a `"YYYY-MM-DD"` string** sorts and compares correctly as a string, so range queries and the
  `(emp_code, date)` key work without converting to datetimes.
- **The helpers' general structure** (pure functions for R2-R4) was worth keeping. I kept the idea, but the helpers
  could not be reused as they were: they needed the employee's whole shift and the attendance date, not just one time
  string (overnight shifts).
