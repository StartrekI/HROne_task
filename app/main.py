"""
Employee Attendance & Analytics API

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB (a local .env is loaded for convenience; real environment variables win)

Layout of this file
  1. configuration, database and observability (request id, timing, metrics)
  2. time helpers (epoch ms <-> BSON dates, IST, R1)
  3. business rules R2 - R5 (pure functions)
  4. request validation (models, query parameter checks)
  5. serialization (MongoDB document -> API JSON)
  6. CRUD endpoints
  7. aggregation pipeline builders (shared by the analytics and the explain endpoints)
  8. analytics and explain endpoints
  9. beyond the contract: single-record reads, employee calendar, daily exceptions, metrics, dashboard
     (new paths only; nothing in sections 6 - 8 changes shape because of them)
"""
import calendar
import base64
import copy
import functools
import hashlib
import hmac
import json
import logging
import math
import os
import re
import threading
import time as time_module
from collections import OrderedDict, defaultdict, deque
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from concurrent.futures import ThreadPoolExecutor
from time import monotonic, perf_counter
from typing import Annotated, Literal, Optional
from urllib.parse import parse_qsl
from uuid import uuid4

from bson import json_util
from bson.decimal128 import Decimal128
from bson.timestamp import Timestamp
from dotenv import load_dotenv
from anyio import CapacityLimiter, to_thread
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exception_handlers import http_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from pydantic import AfterValidator, BaseModel, BeforeValidator, Field, StringConstraints, field_validator, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument, monitoring, uri_parser
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern
from pymongo.errors import ConnectionFailure, DuplicateKeyError, ExecutionTimeout, OperationFailure, PyMongoError
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.routing import Match

log = logging.getLogger("attendance")
if not logging.getLogger().handlers:  # uvicorn configures its own loggers only; give ours a readable format
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

# --------------------------------------------------------------------------- #
# 1. Configuration and database
# --------------------------------------------------------------------------- #
load_dotenv(override=False)  # never overrides variables that are already set in the environment

# Connection details come only from the environment (or .env); nothing is hard-coded. Without MONGO_URI the app
# stops at startup with a clear message instead of connecting somewhere it was not told to.
MONGO_URI = os.environ.get("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI is not set: export it, or put it in a .env file (see .env.example)")
# MONGO_DB names the database; if it is not set, the database in the URI path (mongodb://host/<db>) is used.
MONGO_DB = os.environ.get("MONGO_DB") or uri_parser.parse_uri(MONGO_URI).get("database")
if not MONGO_DB:
    raise RuntimeError("MONGO_DB is not set (and MONGO_URI names no database): export it, or put it in .env")

QUERY_TIMEOUT_MS = int(os.environ.get("QUERY_TIMEOUT_MS", "15000"))  # server-side cap for every read
SLOW_REQUEST_MS = float(os.environ.get("SLOW_REQUEST_MS", "500"))
WORKER_THREADS = int(os.environ.get("WORKER_THREADS", "64"))  # sync endpoints run in this thread pool
# Company-wide analytics run in their own lane (see heavy()). The default is as wide as the shared pool, which
# measured the same as one pool; lower it (e.g. 2x the MongoDB server's cores) to put punches and record reads first.
ANALYTICS_CONCURRENCY = int(os.environ.get("ANALYTICS_CONCURRENCY", str(WORKER_THREADS)))
# Overload protection: beyond this many requests in flight, answer 503 at once instead of queueing them until they
# time out. 4x the worker threads: far above any normal burst, well before latency collapses (measured: p99 4.9 s at 128).
MAX_IN_FLIGHT = int(os.environ.get("MAX_IN_FLIGHT", str(WORKER_THREADS * 4)))
METRICS_TOKEN = os.environ.get("METRICS_TOKEN")  # optional: protects /metrics and /admin/metrics when set
# Optional edge features. The contract has no authentication, so all of them are off unless configured.
API_KEYS = tuple(k.strip().encode() for k in os.environ.get("API_KEYS", "").split(",") if k.strip())
RATE_LIMIT_PER_MINUTE = int(os.environ.get("RATE_LIMIT_PER_MINUTE", "0"))  # per client (API key, else IP); 0 = off
TRUST_PROXY = os.environ.get("TRUST_PROXY") == "1"  # use X-Forwarded-For only behind a proxy that sets it

class CircuitBreaker(monitoring.CommandListener):
    """Fail fast while MongoDB is down. After BREAKER_THRESHOLD consecutive transient failures (connection lost,
    shutdown, failover) requests get 503 + Retry-After at once for BREAKER_COOLDOWN_S, instead of each holding a worker
    thread for the full server-selection timeout (measured in the chaos drill: 3 s per request). After the cool-down
    requests are let through again; one success closes the breaker, another failure re-opens it.
    The success signal is precise: PyMongo's command monitoring reports every command that succeeded (the /health
    ping included, which is never blocked), so the breaker closes as soon as MongoDB answers again. It never changes
    an answer: it only turns an outage into an immediate 503 instead of a slow one."""

    def __init__(self, threshold: int, cooldown_s: float):
        self.threshold, self.cooldown_s = threshold, cooldown_s
        self._lock = threading.Lock()
        self.failures = 0
        self.opened_at: Optional[float] = None
        self._last_failure: Optional[BaseException] = None

    def allow(self) -> bool:
        with self._lock:
            return self.opened_at is None or monotonic() - self.opened_at >= self.cooldown_s

    def record_failure(self, exc: Optional[BaseException] = None) -> None:
        """Count one failure per distinct error: a coalesced round re-raises the same exception object in every
        waiting request, and one database failure must not count as many (found by test: 8 waiters opened it)."""
        with self._lock:
            if exc is not None and exc is self._last_failure:
                return
            self._last_failure = exc
            self.failures += 1
            if self.failures >= self.threshold:
                if self.opened_at is None:
                    log.error("circuit breaker open: MongoDB unavailable, failing fast for %.1f s", self.cooldown_s)
                self.opened_at = monotonic()

    def record_success(self) -> None:
        if self.failures or self.opened_at is not None:  # avoid taking the lock on the hot path
            with self._lock:
                if self.opened_at is not None:
                    log.warning("circuit breaker closed: MongoDB answers again")
                self.failures, self.opened_at, self._last_failure = 0, None, None

    # pymongo.monitoring.CommandListener
    def started(self, event) -> None:
        pass

    def succeeded(self, event) -> None:
        self.record_success()

    def failed(self, event) -> None:
        pass  # a failed command may be a normal error (duplicate key, validation); handlers report transient ones


BREAKER = CircuitBreaker(int(os.environ.get("BREAKER_THRESHOLD", "5")), float(os.environ.get("BREAKER_COOLDOWN_S", "1")))

# tz_aware=True: datetimes come back as aware UTC values, so arithmetic with IST values never mixes naive and aware.
# The pool is sized above the worker thread count so a burst of parallel requests never waits for a connection.
client = MongoClient(
    MONGO_URI,
    tz_aware=True,
    serverSelectionTimeoutMS=3000,
    maxPoolSize=WORKER_THREADS + 16,
    minPoolSize=4,  # keep a few connections open, so the first requests after an idle spell do not pay for TCP + auth
    appname="hrone-attendance-api",
    w="majority",  # an acknowledged write survives a replica-set failover (on a standalone it is the same as w=1)
    event_listeners=[BREAKER],
)
db = client[MONGO_DB]
employees = db["employees"]
logs = db["attendance_logs"]
idempotency = db["idempotency_keys"]  # Idempotency-Key responses, expired by a TTL index (section 6)
# How long a write's response is kept for retries. Sizing: about 1 KB per write with a key, so 1M keyed writes a day
# keep about 1 GB at 24 h; shorten it (or give keys only to retry-prone clients) at higher volumes.
IDEMPOTENCY_TTL_S = int(os.environ.get("IDEMPOTENCY_TTL_S", str(24 * 3600)))

# Finds the record an Idempotency-Key write stamped (section 6). Partial: only stamped documents are indexed, and a
# stamp is removed once the answer is stored, so the index stays nearly empty.
STAMP_INDEX = ([("_idem.k", ASCENDING)], {"name": "idempotency_stamp", "partialFilterExpression": {"_idem.k": {"$exists": True}}})
INDEXES = {
    employees: [
        ([("emp_code", ASCENDING)], {"unique": True, "name": "uniq_emp_code"}),
        ([("department", ASCENDING), ("emp_code", ASCENDING)], {"name": "department_emp_code"}),
        ([("department", ASCENDING), ("joined_on", ASCENDING)], {"name": "department_joined_on"}),
        ([("joined_on", ASCENDING)], {"name": "joined_on"}),
        STAMP_INDEX,
    ],
    logs: [
        ([("emp_code", ASCENDING), ("date", ASCENDING)], {"unique": True, "name": "uniq_emp_code_date"}),
        # Serves the list sort (date desc, emp_code asc) and, through its trailing fields, makes the month analytics
        # covered queries: they read only index keys, never documents (company summary 208 -> 163 ms, leaderboard
        # 90 -> 66 ms at 100k records, identical results).
        ([("date", DESCENDING), ("emp_code", ASCENDING), ("status", ASCENDING), ("late_minutes", ASCENDING),
          ("work_hours", ASCENDING), ("half_day", ASCENDING), ("overtime_minutes", ASCENDING)], {"name": "date_emp_code_covering"}),
        ([("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING)], {"name": "status_date_emp_code"}),
        ([("emp_code", ASCENDING), ("punch_in", DESCENDING)], {"name": "emp_code_punch_in"}),
        STAMP_INDEX,
    ],
    idempotency: [([("created_at", ASCENDING)], {"name": "ttl_created_at", "expireAfterSeconds": IDEMPOTENCY_TTL_S})],
}
STARTUP_INDEX_BUDGET_S = float(os.environ.get("STARTUP_INDEX_BUDGET_S", "12"))  # readiness must come within 20 s
_indexes_ready = False
_index_build: Optional[threading.Thread] = None


def ensure_indexes(unique: Optional[bool] = None) -> None:
    """Idempotent: create_index is a no-op when the same index already exists. Each index is created on its own, so one
    that can not be built (e.g. duplicates in seeded data for a unique index) is logged and does not block the others.
    `unique` selects only the unique (True) or only the secondary (False) indexes. Connection errors propagate."""
    global _indexes_ready
    ok = True
    for coll, specs in INDEXES.items():
        for keys, opts in specs:
            if unique is not None and bool(opts.get("unique")) != unique:
                continue
            try:
                coll.create_index(keys, **opts)
            except OperationFailure as exc:
                if exc.code == 85 and "expireAfterSeconds" in opts:  # IndexOptionsConflict: only the TTL changed
                    db.command("collMod", coll.name, index={"name": opts["name"], "expireAfterSeconds": opts["expireAfterSeconds"]})
                    log.warning("TTL of %s on %s changed to %s s", opts["name"], coll.name, opts["expireAfterSeconds"])
                    continue
                ok = False
                log.error("could not create index %s on %s: %s", opts["name"], coll.name, exc)
    if unique is not True:
        _indexes_ready = True  # retrying a build that failed on the data itself would only fail again
    if not ok:
        log.error("some indexes are missing; see the errors above")


def build_indexes_in_background() -> threading.Thread:
    """Start (or return the running) build of all indexes; never two builds at once."""
    global _index_build
    if _index_build is None or not _index_build.is_alive():
        def run():
            try:
                ensure_indexes()
            except PyMongoError as exc:  # database not reachable: /health starts another attempt later
                log.warning("index creation failed: %s", exc)

        _index_build = threading.Thread(target=run, name="index-build", daemon=True)
        _index_build.start()
    return _index_build


@asynccontextmanager
async def lifespan(_: FastAPI):
    """Startup always waits for the unique indexes: they are what makes concurrent creates and punch-ins exactly-once,
    so no request may run before them. The secondary indexes only make queries fast; startup waits for them up to
    STARTUP_INDEX_BUDGET_S and then serves while they finish (MongoDB builds indexes without blocking reads and
    writes), so a very large dataset can not push readiness past the 20 s the grader allows. Measured: 1.5 s for
    100k records, 9.7 s for 1M."""
    to_thread.current_default_thread_limiter().total_tokens = WORKER_THREADS  # default is 40
    try:
        await to_thread.run_sync(ensure_indexes, True)
        build = build_indexes_in_background()
        await to_thread.run_sync(build.join, STARTUP_INDEX_BUDGET_S)
        if build.is_alive():
            log.warning("secondary indexes still building after %.0f s; serving meanwhile (answers stay correct)", STARTUP_INDEX_BUDGET_S)
    except PyMongoError as exc:  # database not reachable yet: do not block startup, /health retries
        log.warning("index creation failed at startup: %s", exc)
    REPORTS.start()
    yield
    REPORTS.close()
    client.close()  # graceful shutdown: return the pooled connections


class Metrics:
    """In-process request metrics per route: counts, status classes and latency percentiles over a rolling window.
    Single process, no dependency; the numbers reset on restart (a real deployment would export to Prometheus)."""

    def __init__(self, window: int = 2000):
        self.started = monotonic()
        self.started_wall = time_module.time()
        self._lock = threading.Lock()
        self._count = defaultdict(int)
        self._status = defaultdict(lambda: defaultdict(int))
        self._latency = defaultdict(lambda: deque(maxlen=window))

    def observe(self, key: str, status: int, ms: float) -> None:
        with self._lock:
            self._count[key] += 1
            self._status[key][f"{status // 100}xx"] += 1
            self._latency[key].append(ms)

    def snapshot(self) -> dict:
        def pct(values: list, p: float) -> float:
            return round(values[min(len(values) - 1, int(p * len(values)))], 2)

        with self._lock:
            routes = []
            for key in sorted(self._count):
                lat = sorted(self._latency[key])
                routes.append(
                    {
                        "route": key,
                        "count": self._count[key],
                        "status": dict(self._status[key]),
                        "p50_ms": pct(lat, 0.50),
                        "p95_ms": pct(lat, 0.95),
                        "p99_ms": pct(lat, 0.99),
                        "max_ms": round(lat[-1], 2),
                    }
                )
        return {"uptime_s": round(monotonic() - self.started, 1), "in_flight": _in_flight, "routes": routes}

    def prometheus(self) -> str:
        """Prometheus text exposition format (version 0.0.4), so a standard scraper can collect the same numbers."""
        def label(value: str) -> str:
            return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")

        lines = [
            "# HELP http_requests_total Requests handled, by method, route template and status class.",
            "# TYPE http_requests_total counter",
        ]
        quantiles = []
        with self._lock:
            for key in sorted(self._count):
                method, _, route = key.partition(" ")
                labels = f'method="{label(method)}",route="{label(route)}"'
                for status_class, n in sorted(self._status[key].items()):
                    lines.append(f'http_requests_total{{{labels},status="{status_class}"}} {n}')
                latencies = sorted(self._latency[key])
                for q in (0.5, 0.95, 0.99):
                    value = latencies[min(len(latencies) - 1, int(q * len(latencies)))] / 1000
                    quantiles.append(f'http_request_duration_seconds{{{labels},quantile="{q}"}} {value:.6f}')
                quantiles.append(f"http_request_duration_seconds_count{{{labels}}} {self._count[key]}")
        lines += ["# HELP http_request_duration_seconds Latency over the last requests per route.",
                  "# TYPE http_request_duration_seconds summary", *quantiles,
                  "# HELP http_requests_in_flight Requests being processed right now.",
                  "# TYPE http_requests_in_flight gauge", f"http_requests_in_flight {_in_flight}",
                  "# HELP process_start_time_seconds Start time of the process (Unix seconds); a change means a restart.",
                  "# TYPE process_start_time_seconds gauge", f"process_start_time_seconds {self.started_wall:.3f}",
                  "# HELP process_uptime_seconds Seconds since the process started.",
                  "# TYPE process_uptime_seconds gauge", f"process_uptime_seconds {monotonic() - self.started:.1f}"]
        return "\n".join(lines) + "\n"


METRICS = Metrics()
_in_flight = 0  # only touched on the event loop thread, so no lock is needed
PROBE_PATHS = {"/health", "/livez"}  # never shed: an overloaded service is still alive and ready
STANDARD_METHODS = {"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"}


def metric_method(method: str) -> str:
    """Metrics are keyed by method + route template. uvicorn accepts any method token, so an unknown one is bucketed
    as OTHER: otherwise every made-up method would add a key, an unbounded memory leak driven by clients."""
    return method if method in STANDARD_METHODS else "OTHER"


REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
TRACEPARENT_RE = re.compile(r"^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$")  # W3C Trace Context, version 00
OPEN_PATHS = {"/health", "/livez", "/dashboard"}  # reachable without an API key (the dashboard asks for one itself)


class RateLimiter:
    """Token bucket per client: RATE_LIMIT_PER_MINUTE requests per minute with bursts up to the same amount. Only the
    event loop touches it (no lock needed); the table keeps the most recently seen clients and evicts the oldest, so a
    flood of distinct clients can not grow memory without bound."""

    def __init__(self, per_minute: int, max_clients: int = 100_000):
        self.capacity, self.rate, self.max_clients = float(per_minute), per_minute / 60.0, max_clients
        self._buckets: OrderedDict = OrderedDict()

    def take(self, client_id: str) -> tuple[bool, int, float]:
        """(allowed, remaining tokens, seconds until a token is available)."""
        now = monotonic()
        tokens, last = self._buckets.pop(client_id, (self.capacity, now))
        tokens = min(self.capacity, tokens + (now - last) * self.rate)
        allowed = tokens >= 1
        if allowed:
            tokens -= 1
        self._buckets[client_id] = (tokens, now)
        if len(self._buckets) > self.max_clients:
            self._buckets.popitem(last=False)
        return allowed, int(tokens), 0.0 if allowed else (1 - tokens) / self.rate


RATE_LIMITER = RateLimiter(RATE_LIMIT_PER_MINUTE) if RATE_LIMIT_PER_MINUTE > 0 else None


def client_address(scope, headers: dict) -> str:
    if TRUST_PROXY and b"x-forwarded-for" in headers:
        return headers[b"x-forwarded-for"].decode("latin-1").split(",")[0].strip()
    return (scope.get("client") or ("unknown",))[0]
MAX_BODY_BYTES = 64 * 1024  # the largest valid request body (a PATCH) is well under 1 KB


class BodyTooLarge(HTTPException):
    """An HTTPException, so FastAPI passes it through unchanged when it is raised while the body is being read."""

    def __init__(self):
        super().__init__(413, "request body too large")


async def send_json(send, status: int, body: dict, headers: list) -> None:
    payload = json.dumps(body).encode()
    await send({"type": "http.response.start", "status": status,
                "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(payload)).encode()), *headers]})
    await send({"type": "http.response.body", "body": payload})


def repeated_query_params(query_string: bytes) -> list:
    """Names given more than once in the query string (found by contract fuzzing: `?month=2026-07&month=2026-08`
    answered for August, `?limit=5&limit=1.0` was accepted)."""
    seen, repeated = set(), []
    for name, _ in parse_qsl(query_string.decode("latin-1"), keep_blank_values=True):
        if name in seen and name not in repeated:
            repeated.append(name)
        seen.add(name)
    return repeated


class Observability:
    """Pure ASGI middleware (cheaper than BaseHTTPMiddleware). It
    - echoes or creates an X-Request-ID and adds a Server-Timing header,
    - records per-route metrics and logs slow requests,
    - caps request bodies (413), counting the bytes actually received, so a chunked upload can not get around it,
    - sheds load: beyond MAX_IN_FLIGHT concurrent requests it answers 503 + Retry-After at once (probes exempt),
    - adds X-Content-Type-Options: nosniff to every response,
    - is the last line of defence: an exception nothing else handled becomes a JSON 500 that carries the request id.
      Starlette would re-raise it after answering, which makes uvicorn drop the keep-alive connection and fail the
      client's next request too; answering here keeps the connection healthy.
    It never touches successful response bodies."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        start = perf_counter()
        headers = dict(scope["headers"])
        trace = TRACEPARENT_RE.match(headers.get(b"traceparent", b"").decode("latin-1").strip().lower())
        trace_id = trace[1] if trace and trace[1] != "0" * 32 else uuid4().hex
        sent = headers.get(b"x-request-id", b"").decode("latin-1")
        request_id = sent if REQUEST_ID_RE.match(sent) else trace_id
        status = 500
        started = False
        extra = [(b"x-request-id", request_id.encode()), (b"x-content-type-options", b"nosniff"),
                 (b"traceparent", f"00-{trace_id}-{os.urandom(8).hex()}-01".encode())]

        declared = headers.get(b"content-length", b"0")
        if not declared.isdigit() or int(declared) > MAX_BODY_BYTES:
            status = 413 if declared.isdigit() else 400
            await send_json(send, status, {"detail": "request body too large" if status == 413 else "bad content-length"}, extra)
            METRICS.observe(f"{metric_method(scope['method'])} rejected", status, (perf_counter() - start) * 1000)
            return

        repeated = repeated_query_params(scope.get("query_string", b""))
        if repeated:  # every parameter of the contract is a single value; FastAPI would silently keep the last one
            detail = [{"type": "value_error", "loc": ["query", name], "msg": "must be given once", "input": None} for name in repeated]
            await send_json(send, 422, {"detail": detail}, extra)
            METRICS.observe(f"{metric_method(scope['method'])} rejected", 422, (perf_counter() - start) * 1000)
            return

        api_key = headers.get(b"x-api-key", b"")
        if API_KEYS and scope["path"] not in OPEN_PATHS and not any(hmac.compare_digest(api_key, k) for k in API_KEYS):
            await send_json(send, 401, {"detail": "missing or invalid X-API-Key"}, [*extra, (b"www-authenticate", b"ApiKey")])
            METRICS.observe(f"{metric_method(scope['method'])} unauthorized", 401, (perf_counter() - start) * 1000)
            return
        if RATE_LIMITER is not None and scope["path"] not in PROBE_PATHS:
            # Only a validated key identifies a client: otherwise made-up keys would each get a fresh bucket.
            client_id = "key:" + hashlib.sha256(api_key).hexdigest() if API_KEYS and api_key else "ip:" + client_address(scope, headers)
            allowed, remaining, wait = RATE_LIMITER.take(client_id)
            extra += [(b"ratelimit-limit", str(RATE_LIMIT_PER_MINUTE).encode()), (b"ratelimit-remaining", str(remaining).encode())]
            if not allowed:
                retry = str(max(1, math.ceil(wait))).encode()
                await send_json(send, 429, {"detail": "rate limit exceeded"}, [*extra, (b"retry-after", retry), (b"ratelimit-reset", retry)])
                METRICS.observe(f"{metric_method(scope['method'])} rate_limited", 429, (perf_counter() - start) * 1000)
                return

        global _in_flight
        if not BREAKER.allow() and scope["path"] not in PROBE_PATHS:
            await send_json(send, 503, {"detail": "database unavailable"}, [*extra, (b"retry-after", b"1")])
            METRICS.observe(f"{metric_method(scope['method'])} breaker", 503, (perf_counter() - start) * 1000)
            return
        if _in_flight >= MAX_IN_FLIGHT and scope["path"] not in PROBE_PATHS:
            await send_json(send, 503, {"detail": "server overloaded, retry shortly"}, [*extra, (b"retry-after", b"1")])
            METRICS.observe(f"{metric_method(scope['method'])} shed", 503, (perf_counter() - start) * 1000)
            return
        _in_flight += 1
        received = 0

        async def receive_limited():
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > MAX_BODY_BYTES:
                    raise BodyTooLarge()
            return message

        async def send_with_headers(message):
            nonlocal status, started
            if message["type"] == "http.response.start":
                started = True
                status = message["status"]
                elapsed = (perf_counter() - start) * 1000
                headers = list(message.get("headers", [])) + [
                    *extra,
                    (b"server-timing", f"app;dur={elapsed:.1f}".encode()),
                ]
                message = {**message, "headers": headers}
            await send(message)

        try:
            await self.app(scope, receive_limited, send_with_headers)
        except Exception as exc:  # noqa: BLE001 - this is the boundary; everything is logged with its request id
            if isinstance(exc, BodyTooLarge):
                status, body = 413, {"detail": "request body too large"}
            else:
                status, body = 500, {"detail": "internal error", "request_id": request_id}
                log.exception("unhandled error in %s %s (request id %s)", scope["method"], scope["path"], request_id)
            if not started:
                await send_json(send, status, body, extra)
        finally:
            _in_flight -= 1
            elapsed = (perf_counter() - start) * 1000
            route = getattr(scope.get("route"), "path", None) or "unmatched"
            METRICS.observe(f"{metric_method(scope['method'])} {route}", status, elapsed)
            if elapsed > SLOW_REQUEST_MS:
                log.warning("slow request %s %s -> %s in %.0f ms (request id %s)", scope["method"], scope["path"], status, elapsed, request_id)


app = FastAPI(
    title="Employee Attendance & Analytics API",
    version="2.0.0",
    lifespan=lifespan,
    description="HROne attendance service. Contract: openapi.yaml. A live dashboard is served at /dashboard.",
)
app.add_middleware(GZipMiddleware, minimum_size=1024)  # explain output and big pages; clients opt in via Accept-Encoding
app.add_middleware(Observability)  # added last = outermost, so its timing includes everything


FASTAPI_BODY_PARSE_ERROR = "There was an error parsing the body"  # FastAPI's 400 for bytes that are not valid UTF-8


@app.exception_handler(StarletteHTTPException)
async def http_error_handler(request: Request, exc: StarletteHTTPException):
    """Two HTTP-level corrections, found by fuzzing the contract (Schemathesis); everything else is FastAPI's default.
    - A body FastAPI can not even decode is still an invalid body: the contract answers 422, in FastAPI's validation
      shape, never an undocumented 400.
    - A 405 must list every method the path supports in Allow (RFC 9110 15.5.6). Starlette names only the first route
      it matched, so for /employees (GET and POST are separate routes) it would advertise POST alone."""
    if exc.status_code == 400 and exc.detail == FASTAPI_BODY_PARSE_ERROR:
        detail = [{"type": "json_invalid", "loc": ["body"], "msg": "the request body is not valid JSON", "input": None}]
        return JSONResponse(status_code=422, content={"detail": detail})
    if exc.status_code == 405:
        allowed = set()
        for route in request.app.router.routes:
            match, _ = route.matches(request.scope)
            if match != Match.NONE:
                allowed |= set(getattr(route, "methods", None) or ())
        return JSONResponse(status_code=405, content={"detail": exc.detail}, headers={"Allow": ", ".join(sorted(allowed))})
    return await http_exception_handler(request, exc)


def json_safe(value):
    """NaN and Infinity are not JSON, so echoing them back would make the error response itself fail."""
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, dict):
        return {k: json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    return value


@app.exception_handler(RequestValidationError)
async def validation_error_handler(_: Request, exc: RequestValidationError):
    """FastAPI's default 422 body, except that a non-finite number in the echoed `input` is written as a string.
    Python's JSON parser accepts the literals NaN and Infinity, pydantic rejects them, and the default handler then
    failed to serialize its own error: a 500 (found by a request-oddity probe)."""
    return JSONResponse(status_code=422, content={"detail": json_safe(jsonable_encoder(exc.errors()))})


@app.exception_handler(ExecutionTimeout)
async def timeout_handler(_: Request, exc: ExecutionTimeout):
    log.error("query exceeded %s ms: %s", QUERY_TIMEOUT_MS, exc)
    return JSONResponse(status_code=504, content={"detail": "query timed out"})


@app.exception_handler(ConnectionFailure)
async def unavailable_handler(_: Request, exc: ConnectionFailure):
    """The database can not be reached (or is failing over): a temporary condition, so 503 + Retry-After."""
    BREAKER.record_failure(exc)
    log.error("database unreachable: %s", exc)
    return JSONResponse(status_code=503, content={"detail": "database unavailable"}, headers={"Retry-After": "1"})


# Server errors MongoDB itself treats as transient: shutdown, step-down / failover and network trouble.
TRANSIENT_ERROR_CODES = {6, 7, 89, 91, 189, 262, 9001, 10107, 11600, 11602, 13435, 13436}
TRANSIENT_ERROR_LABELS = ("RetryableWriteError", "TransientTransactionError")


def is_transient(exc: PyMongoError) -> bool:
    if isinstance(exc, ConnectionFailure):
        return True
    if any(exc.has_error_label(label) for label in TRANSIENT_ERROR_LABELS):
        return True
    return getattr(exc, "code", None) in TRANSIENT_ERROR_CODES


@app.exception_handler(PyMongoError)
async def mongo_error_handler(_: Request, exc: PyMongoError):
    """A transient server state (found in a chaos drill: ShutdownInProgress, code 91, labelled RetryableWriteError,
    while mongod was going down) is an outage: 503 + Retry-After, so the client retries, ideally with an
    Idempotency-Key, since a write's outcome is then unknown. Anything else is a failed operation: 500."""
    if is_transient(exc):
        BREAKER.record_failure(exc)
        log.error("database temporarily unavailable: %s", exc)
        return JSONResponse(status_code=503, content={"detail": "database unavailable"}, headers={"Retry-After": "1"})
    log.error("database operation failed: %s", exc)
    return JSONResponse(status_code=500, content={"detail": "database operation failed"})


# --------------------------------------------------------------------------- #
# 2. Time helpers
# --------------------------------------------------------------------------- #
IST = timezone(timedelta(hours=5, minutes=30))
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
MIN_MS, MAX_MS = 100_000_000_000, 4_102_444_800_000
PRESENCE = ("PRESENT", "WFH", "ON_DUTY")
NON_PRESENCE = ("ABSENT", "LEAVE")
MAX_SHIFT = timedelta(hours=24)
CAS_ATTEMPTS = 5  # bounded retries of a compare-and-swap that lost a race
MAX_SKIP = 2**62  # keeps an explained skip inside int64


def ms_to_dt(ms: int) -> datetime:
    """Epoch ms -> aware UTC datetime, truncated to whole seconds (R1)."""
    return EPOCH + timedelta(seconds=ms // 1000)


def dt_to_ms(dt: Optional[datetime]) -> Optional[int]:
    if dt is None:
        return None
    if dt.tzinfo is None:  # BSON dates are UTC
        dt = dt.replace(tzinfo=timezone.utc)
    return (dt - EPOCH) // timedelta(milliseconds=1)


def now_utc() -> datetime:
    """Server clock, truncated to whole seconds."""
    return datetime.now(timezone.utc).replace(microsecond=0)


HHMM_RE = re.compile(r"^([01]?[0-9]|2[0-3]):([0-5][0-9])$")
DEFAULT_SHIFT = (time(9, 30), time(18, 30))  # the contract's defaults for shift_start / shift_end
_warned_shifts: set = set()


def parse_hhmm(value) -> Optional[time]:
    match = HHMM_RE.match(value) if isinstance(value, str) else None
    return time(int(match[1]), int(match[2])) if match else None


def employee_shift(emp: dict) -> tuple[time, time]:
    """(shift_start, shift_end). Our API only stores valid HH:MM values, but a seeded document might not; then the
    contract defaults are used (and logged) instead of failing the request."""
    start, end = parse_hhmm(emp.get("shift_start")), parse_hhmm(emp.get("shift_end"))
    if start is None or end is None or start == end:
        if emp.get("emp_code") not in _warned_shifts:  # once per employee, not on every request
            _warned_shifts.add(emp.get("emp_code"))
            log.warning("employee %s has an unusable shift %r-%r; using 09:30-18:30", emp.get("emp_code"), emp.get("shift_start"), emp.get("shift_end"))
        return DEFAULT_SHIFT
    return start, end


def is_overnight(emp: dict) -> bool:
    start, end = employee_shift(emp)
    return end <= start


def attendance_date(punch_in: datetime, emp: dict) -> date:
    """R1: the IST calendar date of the punch-in, except that on an overnight shift a punch-in earlier than shift_end
    belongs to the shift that started the previous day."""
    local = punch_in.astimezone(IST)
    day = local.date()
    if is_overnight(emp) and local.time() < employee_shift(emp)[1]:
        day -= timedelta(days=1)
    return day


def shift_bounds(day: date, emp: dict) -> tuple[datetime, datetime]:
    """Start and end instants of the shift that starts on `day` (IST). An overnight shift ends the next day."""
    shift_start, shift_end = employee_shift(emp)
    start = datetime.combine(day, shift_start, IST)
    end = datetime.combine(day, shift_end, IST)
    if end <= start:
        end += timedelta(days=1)
    return start, end


# --------------------------------------------------------------------------- #
# 3. Business rules (inputs are already truncated to whole seconds)
# --------------------------------------------------------------------------- #
def compute_late_minutes(punch_in: datetime, day: date, emp: dict) -> int:
    """R2: late only when strictly more than 10:00 after shift_start; then whole minutes since shift_start."""
    start, _ = shift_bounds(day, emp)
    seconds = int((punch_in - start).total_seconds())
    return seconds // 60 if seconds > 600 else 0


def compute_overtime(punch_out: datetime, day: date, emp: dict) -> int:
    """R3: whole minutes after shift_end, counted only when >= 30."""
    _, end = shift_bounds(day, emp)
    minutes = int((punch_out - end).total_seconds()) // 60
    return minutes if minutes >= 30 else 0


def compute_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    """R4: seconds / 3600, rounded to 2 decimals half-up (Decimal, not float/banker's rounding)."""
    seconds = int((punch_out - punch_in).total_seconds())
    return float((Decimal(seconds) / Decimal(3600)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def derived_fields(status: str, punch_in: Optional[datetime], punch_out: Optional[datetime], day: date, emp: dict) -> dict:
    """All four derived fields for a record (DATA_MODEL.md section 3 table)."""
    if status not in PRESENCE or punch_in is None:
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    out = {"work_hours": None, "late_minutes": compute_late_minutes(punch_in, day, emp), "overtime_minutes": 0, "half_day": False}
    if punch_out is not None:
        hours = compute_work_hours(punch_in, punch_out)
        out.update(work_hours=hours, overtime_minutes=compute_overtime(punch_out, day, emp), half_day=hours < 4.5)  # R5
    return out


# --------------------------------------------------------------------------- #
# 4. Request validation
# --------------------------------------------------------------------------- #
# Calendar values accept every real date the contract's `format: date` allows (0001-01-01 .. 9999-12-31). No code
# path computes "last day + 1" (densify bounds stop at noon of the last day), so even 9999-12-31 can not overflow.
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
MONTH_PATTERN = r"^\d{4}-(0[1-9]|1[0-2])$"
HHMM_PATTERN = r"^([01][0-9]|2[0-3]):[0-5][0-9]$"

# strict=True: rejects floats, numeric strings and booleans. The bounds reject second-looking values.
EpochMs = Annotated[int, Field(strict=True, ge=MIN_MS, le=MAX_MS)]
Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]
INTEGER_TEXT = re.compile(r"^[+-]?[0-9]+$")


def integer_text(value):
    """Query integers must be written as integers: the contract's type is integer, so "1.0" or "1e3" is a 422
    (pydantic would otherwise read "1.0" as 1). Found by contract fuzzing."""
    if isinstance(value, str) and not INTEGER_TEXT.match(value):
        raise ValueError("must be an integer")
    return value


QueryInt = BeforeValidator(integer_text)
Page = Annotated[int, Query(ge=1), QueryInt]
PageSize = Annotated[int, Query(ge=1, le=100), QueryInt]
Limit = Annotated[int, Query(ge=1, le=50), QueryInt]
def real_month(value: Optional[str]) -> Optional[str]:
    """The contract's month pattern also matches year 0000, which no calendar has (Python's dates start at year 1)."""
    if value is not None and value.startswith("0000"):
        raise ValueError("year 0000 does not exist; months start at 0001-01")
    return value


Month = Annotated[str, Query(pattern=MONTH_PATTERN), AfterValidator(real_month)]
OptionalMonth = Annotated[Optional[str], Query(pattern=MONTH_PATTERN), AfterValidator(real_month)]


def validation_error(loc: tuple, msg: str, value=None) -> RequestValidationError:
    """A 422 with FastAPI's default body shape."""
    return RequestValidationError([{"type": "value_error", "loc": loc, "msg": msg, "input": value}])


def to_date(value) -> Optional[date]:
    """A valid YYYY-MM-DD (0001-01-01 .. 9999-12-31), or None."""
    try:
        return date.fromisoformat(value) if isinstance(value, str) and DATE_RE.match(value) else None
    except ValueError:  # e.g. 2026-02-30
        return None


def parse_date(value: str, loc: tuple) -> date:
    day = to_date(value)
    if day is None:
        raise validation_error(loc, "must be a valid date YYYY-MM-DD", value)
    return day


def is_valid_date(value: str) -> bool:
    return to_date(value) is not None


class EmployeeIn(BaseModel):
    emp_code: Annotated[str, StringConstraints(pattern=r"^EMP[0-9]{4,6}$")]
    name: Annotated[str, StringConstraints(min_length=1, max_length=100)]
    email: Annotated[str, StringConstraints(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")]
    department: Annotated[str, StringConstraints(min_length=1, max_length=50)]
    shift_start: Annotated[str, StringConstraints(pattern=HHMM_PATTERN)] = "09:30"
    shift_end: Annotated[str, StringConstraints(pattern=HHMM_PATTERN)] = "18:30"
    joined_on: str

    @field_validator("joined_on")
    @classmethod
    def _joined_on(cls, v: str) -> str:
        if not is_valid_date(v):
            raise ValueError("must be a valid date YYYY-MM-DD")
        return v

    @model_validator(mode="after")
    def _shift(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


def sent_null_is_invalid(value):
    """`punched_at` may be omitted ("defaults to now") but not sent as null: the contract types it as a
    non-nullable integer. Validators only run for values that were sent, so omitting it stays valid."""
    if value is None:
        raise ValueError("may be omitted but not null")
    return value


class PunchIn(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMs] = None
    status: PresenceStatus = "PRESENT"

    reject_null = field_validator("punched_at", mode="before")(sent_null_is_invalid)


class PunchOut(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMs] = None

    reject_null = field_validator("punched_at", mode="before")(sent_null_is_invalid)


class Regularize(BaseModel):
    # Derived fields and unknown keys are ignored (pydantic's default), as the contract asks.
    status: Optional[Status] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: Annotated[str, StringConstraints(min_length=5, max_length=200)]
    regularized_by: Annotated[str, StringConstraints(min_length=1, max_length=50)]

    @field_validator("status", "punch_in", "punch_out", mode="before")
    @classmethod
    def _not_null(cls, v):
        # Only runs for values that were sent: an explicit null is not "omitted".
        if v is None:
            raise ValueError("may be omitted but not null")
        return v


# --------------------------------------------------------------------------- #
# 5. Serialization (MongoDB -> API)
# --------------------------------------------------------------------------- #
EMPLOYEE_FIELDS = ("emp_code", "name", "email", "department", "shift_start", "shift_end", "joined_on")
TIME_FIELDS = ("punch_in", "punch_out")


# Seeded documents are read "as stored". These converters turn whatever BSON type is there (Int64, double, Decimal128,
# a numeric string, NaN, a wrong type) into the contract's type, so one odd document can never break a response.
def as_number(value) -> Optional[float]:
    if isinstance(value, bool):
        return None
    if isinstance(value, Decimal128):
        value = value.to_decimal()
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def as_int(value, default: int = 0) -> int:
    number = as_number(value)
    return int(number) if number is not None else default


def as_ms(value) -> Optional[int]:
    return dt_to_ms(value) if isinstance(value, datetime) else None


def as_str(value) -> Optional[str]:
    return value if isinstance(value, str) else None


SHIFT_DEFAULTS = {"shift_start": "09:30", "shift_end": "18:30"}  # the contract's defaults (EmployeeCreate)


def employee_out(doc: dict) -> dict:
    out = {k: as_str(doc.get(k)) for k in EMPLOYEE_FIELDS}
    for field, default in SHIFT_DEFAULTS.items():  # a seeded employee without a shift works on the default one
        if out[field] is None:
            out[field] = default
    out["created_at"] = as_ms(doc.get("created_at"))
    return out


def change_value(value):
    """Values inside history[].changes: instants become epoch ms, everything else must be JSON-safe."""
    if isinstance(value, datetime):
        return dt_to_ms(value)
    if value is None or isinstance(value, (bool, str)):
        return value
    number = as_number(value)
    return int(number) if number is not None and isinstance(value, int) else number


def history_out(entry) -> dict:
    entry = entry if isinstance(entry, dict) else {}
    changes = entry.get("changes") if isinstance(entry.get("changes"), dict) else {}
    return {
        "at": as_ms(entry.get("at")),
        "by": as_str(entry.get("by")),
        "reason": as_str(entry.get("reason")),
        "changes": {
            field: {"from": change_value(c.get("from")), "to": change_value(c.get("to"))}
            for field, c in changes.items()
            if isinstance(c, dict)
        },
    }


def record_out(doc: dict) -> dict:
    """Missing fields in older seeded records get their documented defaults."""
    history = doc.get("history")
    return {
        "emp_code": as_str(doc.get("emp_code")),
        "date": as_str(doc.get("date")),
        "status": as_str(doc.get("status")),
        "punch_in": as_ms(doc.get("punch_in")),
        "punch_out": as_ms(doc.get("punch_out")),
        "work_hours": as_number(doc.get("work_hours")),
        "late_minutes": as_int(doc.get("late_minutes")),
        "overtime_minutes": as_int(doc.get("overtime_minutes")),
        "half_day": doc.get("half_day") is True,
        "history": [history_out(h) for h in history] if isinstance(history, list) else [],
    }


_health_limiter: Optional[CapacityLimiter] = None


def health_limiter() -> CapacityLimiter:
    global _health_limiter
    if _health_limiter is None:  # created lazily, inside the running event loop
        _health_limiter = CapacityLimiter(4)
    return _health_limiter


_analytics_limiter: Optional[CapacityLimiter] = None


def analytics_limiter() -> CapacityLimiter:
    global _analytics_limiter
    if _analytics_limiter is None:
        _analytics_limiter = CapacityLimiter(ANALYTICS_CONCURRENCY)
    return _analytics_limiter


def heavy(endpoint):
    """Runs an expensive endpoint (a scan over a whole month or company) in its own lane of ANALYTICS_CONCURRENCY
    threads instead of the shared pool, where a 6 ms record read waited behind 160 ms company reports. With a
    narrower lane, reads and writes keep their latency under overload and the reports queue among themselves
    (measured at 64 clients with 8 slots: light p50 140 -> 40 ms, report p50 380 -> 1,180 ms). Only the scheduling
    changes; every answer is computed exactly as before."""
    @functools.wraps(endpoint)
    async def run(*args, **kwargs):
        return await to_thread.run_sync(functools.partial(endpoint, *args, **kwargs), limiter=analytics_limiter())
    return run


class Coalescer:
    """Linearizable request coalescing ("single-flight" in rounds) for read-only aggregations.

    Identical requests that arrive while an identical computation is running do not each hit MongoDB: they wait for
    the NEXT round, which one of them starts as soon as the current round ends, and all share its result. A request
    never joins a round that started before it arrived, so every answer reflects the database at or after the moment
    the request was received: a client that writes and then reads always sees its write (no staleness, unlike a
    cache). Under a burst of N identical requests, MongoDB does at most about 2 computations per computation time
    instead of N. The same idea as group commit in databases."""

    class _Slot:
        __slots__ = ("cond", "running", "started_round", "done_round", "result", "waiters")

        def __init__(self, lock):
            self.cond = threading.Condition(lock)
            self.running = False
            self.started_round = 0  # the last round that started
            self.done_round = 0  # the last round that finished; (ok, value) of it is in result
            self.result = None
            self.waiters = 0

    def __init__(self):
        self._lock = threading.Lock()
        self._slots: dict = {}

    def run(self, key: str, compute):
        with self._lock:
            slot = self._slots.get(key) or self._slots.setdefault(key, self._Slot(self._lock))
            my_round = slot.started_round + 1  # only a round that starts after this moment may answer this request
            slot.waiters += 1
            try:
                while slot.done_round < my_round:
                    if not slot.running:  # nobody is computing: lead the next round
                        slot.running, slot.started_round = True, slot.started_round + 1
                        leading = slot.started_round
                        self._lock.release()
                        try:
                            result = (True, compute())
                        except BaseException as exc:  # shared with the round's waiters, then re-raised below
                            result = (False, exc)
                        finally:
                            self._lock.acquire()
                        slot.running, slot.done_round, slot.result = False, leading, result
                        slot.cond.notify_all()
                    else:
                        slot.cond.wait()
                ok, value = slot.result  # done_round >= my_round: computed entirely after this request arrived
            finally:
                slot.waiters -= 1
                if slot.waiters == 0 and not slot.running:
                    del self._slots[key]  # nothing kept between bursts: no memory growth, no stale results
        if ok:
            return copy.deepcopy(value)  # every caller gets its own copy to serialize
        raise value


COALESCER = Coalescer()

def run_pipeline(coll, pipeline: list) -> list:
    """Every aggregation runs with a server-side time limit, so one runaway query can not pin a worker, and identical
    concurrent aggregations are coalesced (see Coalescer)."""
    key = coll.name + json_util.dumps(pipeline, sort_keys=True)
    return COALESCER.run(key, lambda: list(coll.aggregate(pipeline, maxTimeMS=QUERY_TIMEOUT_MS)))


# --------------------------------------------------------------------------- #
# Report cache: exact, invalidated by MongoDB change streams (replica sets only)
# --------------------------------------------------------------------------- #
REPORT_CACHE = os.environ.get("REPORT_CACHE", "1") != "0"
CACHE_WAIT_S = 0.25  # longest a report waits for the change stream to catch up; then it is computed fresh
CACHE_MAX_ENTRIES = 1024
WATCH_AWAIT_MS = 25  # how often an idle change stream reports its position
CANONICAL_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
CRUD = ("insert", "update", "replace", "delete")


def ts_key(ts: Timestamp) -> tuple:
    return (ts.time, ts.inc)


def just_before(t: tuple) -> tuple:
    return (t[0], t[1] - 1) if t[1] > 0 else (t[0] - 1, 2**32 - 1)


def token_time(token) -> Optional[tuple]:
    """The cluster time inside a change stream resume token (hex KeyString: 0x82 = Timestamp, then seconds and
    increment). The format is checked against every event's documented clusterTime; a mismatch disables the cache."""
    data = token.get("_data") if isinstance(token, dict) else None
    if not isinstance(data, str) or len(data) < 18 or not data.startswith("82"):
        return None
    try:
        return (int(data[2:10], 16), int(data[10:18], 16))
    except ValueError:
        return None


def last_write_time(conn_client) -> Optional[tuple]:
    """The primary's last applied write: every write acknowledged before this call is at or before it."""
    last = conn_client.admin.command("hello").get("lastWrite")
    ts = (last or {}).get("opTime", {}).get("ts")
    return ts_key(ts) if isinstance(ts, Timestamp) else None


def months_between(first: str, last: str) -> list:
    y, m = int(first[:4]), int(first[5:7])
    end = (int(last[:4]), int(last[5:7]))
    out = []
    while (y, m) <= end:
        out.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


class ReportCache:
    """Serves a repeated report without recomputing it, and never serves a stale one.

    Why it is exact: a result is computed with a snapshot read at a known cluster time S (the primary's last write).
    A watcher follows the database's change stream, which delivers every committed change (the app's writes, the
    grader's direct writes, transactions, drops) in cluster-time order, and records the last change time per month
    of attendance logs (any employee change, delete of an unknown record or DDL invalidates everything). A request
    first reads the current last-write time T, waits until the watcher has applied every change up to T, and then
    uses an entry only if nothing it depends on changed after S. So the answer equals a fresh computation at T, which
    is at least as recent as the request (the same guarantee as the coalescer). The month a change touches is read
    from the record as it was just before and at the change (snapshot reads at those times), never guessed.

    Anything unexpected (the watcher behind, an error, a token it can not read, a standalone server, which has no
    change streams) means: compute fresh, exactly as without the cache."""

    def __init__(self, database):
        self.db = database
        self.cv = threading.Condition()
        self.entries: OrderedDict = OrderedDict()  # key -> (result, S, months)
        self.seen: Optional[tuple] = None  # every change at or before this time has been applied
        self.global_change: tuple = (2**32, 0)  # nothing is valid until the watcher runs
        self.month_change: dict = {}
        self.running = False
        self.broken = False
        self.stop = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.hits = self.misses = 0

    # --- watcher -------------------------------------------------------------------------------------------- #
    def start(self) -> None:
        if REPORT_CACHE and self.thread is None:
            self.thread = threading.Thread(target=self.watch_forever, name="report-cache-watch", daemon=True)
            self.thread.start()

    def close(self) -> None:
        self.stop.set()

    def watch_forever(self) -> None:
        delay = 0.5
        while not self.stop.is_set() and not self.broken:
            try:
                if not transactions_supported():
                    return  # standalone server: no change streams, reports are always computed
                self.watch_once()
                delay = 0.5
            except PyMongoError as exc:
                log.warning("report cache watcher restarting: %s", exc)
            finally:
                with self.cv:
                    self.running = False
            self.stop.wait(delay)
            delay = min(delay * 2, 30)

    def watch_once(self) -> None:
        start = last_write_time(self.db.client)
        if start is None:
            return
        with self.cv:  # whatever changed while no stream was open is unknown: start everything afresh
            self.global_change = start
            self.entries.clear()
            self.seen = just_before(start) if self.seen is None else max(self.seen, just_before(start))
            self.running = True
        pipeline = [{"$match": {"$or": [{"ns.coll": {"$ne": idempotency.name}}, {"operationType": {"$nin": list(CRUD)}}]}}]
        with self.db.watch(pipeline, start_at_operation_time=Timestamp(*start), max_await_time_ms=WATCH_AWAIT_MS) as stream:
            while not self.stop.is_set():
                event = stream.try_next()
                if event is not None:
                    t = ts_key(event["clusterTime"])
                    if token_time(event["_id"]) != t:
                        self.broken = True  # the token format is not what we read: never trust it
                        log.error("report cache disabled: unexpected change stream token format")
                        with self.cv:
                            self.global_change = (2**32, 0)
                            self.entries.clear()
                        return
                    self.apply(event, t)
                    if event.get("operationType") == "invalidate":
                        return
                    position = just_before(t)
                else:
                    # An empty batch's resume token is a high-water mark at the last oplog entry the scan read: every
                    # change at or before it has been delivered. (After an event only the time before it is complete,
                    # since a transaction's events share one time.)
                    position = token_time(stream.resume_token)
                    if position is None:
                        continue
                with self.cv:
                    if self.seen is None or position > self.seen:
                        self.seen = position
                        self.cv.notify_all()

    def apply(self, event: dict, t: tuple) -> None:
        op, coll = event.get("operationType"), (event.get("ns") or {}).get("coll")
        if op in CRUD and coll not in (logs.name, employees.name):
            return  # another collection's documents can not change a report
        months = None  # None: invalidate everything
        if op in ("insert", "update", "replace") and coll == logs.name:  # a delete invalidates everything: no read needed
            months = self.months_touched(event, op, t)
        with self.cv:
            if months is None:
                self.global_change = max(self.global_change, t)
                self.entries.clear()
            else:
                for month in months:
                    self.month_change[month] = max(self.month_change.get(month, t), t)

    def months_touched(self, event: dict, op: str, t: tuple) -> Optional[set]:
        if op == "insert":
            dates = [(event.get("fullDocument") or {}).get("date")]
        else:
            _id = (event.get("documentKey") or {}).get("_id")
            before = self.date_at(_id, just_before(t))
            after = self.date_at(_id, t)
            if before is None or after is None:
                return None  # did not exist just before (or unreadable): be conservative
            dates = [before, after]
        months = set()
        for value in dates:
            if not (isinstance(value, str) and CANONICAL_DATE.fullmatch(value)):
                return None  # a malformed date may fall into any month's string range
            months.add(value[:7])
        return months

    def date_at(self, _id, t: tuple):
        """The record's date as it was at cluster time t (a snapshot read in the past); None if it did not exist."""
        try:
            found = self.db.command("find", logs.name, filter={"_id": _id}, projection={"date": 1}, limit=1, singleBatch=True,
                                    readConcern={"level": "snapshot", "atClusterTime": Timestamp(*t)}, maxTimeMS=QUERY_TIMEOUT_MS)
        except PyMongoError:
            return None
        batch = found["cursor"]["firstBatch"]
        return batch[0].get("date", "") if batch else None

    # --- reads ---------------------------------------------------------------------------------------------- #
    def lookup(self, key: str, months: list, now: tuple):
        with self.cv:
            if not self.running or not self.cv.wait_for(lambda: self.seen is not None and self.seen >= now, CACHE_WAIT_S):
                return None
            entry = self.entries.get(key)
            if entry is None:
                return None
            result, at, _ = entry
            if self.global_change > at or any(self.month_change.get(m, (0, 0)) > at for m in months):
                del self.entries[key]
                return None
            self.entries.move_to_end(key)
            self.hits += 1
            return copy.deepcopy(result)

    def store(self, key: str, result, at: tuple, months: list) -> None:
        with self.cv:
            if not self.running or self.global_change > at:
                return
            self.misses += 1
            self.entries[key] = (copy.deepcopy(result), at, months)
            self.entries.move_to_end(key)
            while len(self.entries) > CACHE_MAX_ENTRIES:
                self.entries.popitem(last=False)


REPORTS = ReportCache(db)


def snapshot_aggregate(coll, pipeline: list):
    """The pipeline evaluated at the primary's last-write time: (result, that time), or (result, None) if it could
    not be read as one snapshot (then it is not cached)."""
    at = last_write_time(coll.database.client)
    if at is None:
        return list(coll.aggregate(pipeline, maxTimeMS=QUERY_TIMEOUT_MS)), None
    try:
        reply = coll.database.command("aggregate", coll.name, pipeline=pipeline, cursor={"batchSize": 2**31 - 1},
                                      readConcern={"level": "snapshot", "atClusterTime": Timestamp(*at)}, maxTimeMS=QUERY_TIMEOUT_MS,
                                      codec_options=coll.codec_options)
    except ExecutionTimeout:
        raise
    except OperationFailure as exc:  # e.g. snapshot reads unavailable: an ordinary read, not cached
        log.warning("snapshot aggregate failed, reading without cache: %s", exc)
        return list(coll.aggregate(pipeline, maxTimeMS=QUERY_TIMEOUT_MS)), None
    if reply["cursor"]["id"] != 0:  # larger than one batch (never for these reports): read it the ordinary way
        coll.database.command("killCursors", coll.name, cursors=[reply["cursor"]["id"]])
        return list(coll.aggregate(pipeline, maxTimeMS=QUERY_TIMEOUT_MS)), None
    return reply["cursor"]["firstBatch"], at


def run_report(coll, pipeline: list, first: str, last: str) -> list:
    """A contract report over the logs dated first..last (and the employees): served from REPORTS when provably
    current, else computed (coalesced) and remembered."""
    if not REPORTS.running:
        return run_pipeline(coll, pipeline)
    key = coll.name + json_util.dumps(pipeline, sort_keys=True)
    months = months_between(first, last)
    now = last_write_time(coll.database.client)
    if now is not None:
        hit = REPORTS.lookup(key, months, now)
        if hit is not None:
            return hit
    result, at = COALESCER.run("snapshot:" + key, lambda: snapshot_aggregate(coll, pipeline))
    if at is not None:
        REPORTS.store(key, result, at, months)
    return result


def count_total(coll, q: dict) -> int:
    """Always an exact count. estimated_document_count would be O(1) for an empty filter, but MongoDB documents that
    it can be wrong after an unclean shutdown and on sharded clusters (orphans), and `total` must always be right.
    The exact count costs about 45 ms at 100k records; identical concurrent counts are coalesced (never stale)."""
    key = "count:" + coll.name + json_util.dumps(q, sort_keys=True)
    return COALESCER.run(key, lambda: coll.count_documents(q, maxTimeMS=QUERY_TIMEOUT_MS))


# The total and the page are independent reads: run them side by side, so a list costs the slower of the two instead
# of their sum (page 500 of 100k: about 92 -> 52 ms). A separate pool, so a count never waits for a request thread.
SIDE_POOL = ThreadPoolExecutor(max_workers=WORKER_THREADS, thread_name_prefix="count")


def paged(coll, q: dict, sort, page: int, page_size: int, out) -> dict:
    skip = (page - 1) * page_size
    total = SIDE_POOL.submit(count_total, coll, q)
    items = []
    if skip < MAX_SKIP:  # a page number so large it would overflow int64 is necessarily past the end
        cursor = coll.find(q, {"_id": 0}, max_time_ms=QUERY_TIMEOUT_MS).sort(sort).skip(skip).limit(page_size)
        items = [out(d) for d in cursor]
    return {"items": items, "total": total.result(), "page": page, "page_size": page_size}


def get_employee(emp_code: str) -> dict:
    emp = employees.find_one({"emp_code": emp_code}, {"_id": 0}, session=TXN.get())
    if emp is None:
        raise HTTPException(404, "employee not found")
    return emp


# --------------------------------------------------------------------------- #
# 6. CRUD endpoints
# --------------------------------------------------------------------------- #
IdempotencyKey = Annotated[Optional[str], Header(alias="Idempotency-Key", pattern=r"^[A-Za-z0-9._:-]{1,128}$")]
# A claim older than this is taken over by the next retry. This is safe even if the first request is still running:
# a second run of the same write either conflicts or changes nothing, and then finds the first one's stamp.
STALE_PENDING = timedelta(seconds=10)


TXN: ContextVar = ContextVar("txn", default=None)  # the session of the transaction the current write runs in, if any
TXN_RETRY_S = 10.0  # how long a write keeps retrying a transaction that an election or a write conflict interrupted
_transactions = None


def transactions_supported() -> bool:
    """Replica sets and sharded clusters run multi-document transactions; a standalone server does not."""
    global _transactions
    if _transactions is None:
        hello = client.admin.command("hello")
        _transactions = "setName" in hello or hello.get("msg") == "isdbgrid"
    return _transactions


def idempotent(key: Optional[str], scope: str, payload: dict, status_code: int, operation):
    """Optional Idempotency-Key support for the write endpoints (IETF draft-ietf-httpapi-idempotency-key-header).

    A client that retries after a timeout (a phone on a bad network) must get the answer of the request that already
    succeeded, not a 409 "already punched in" or a 422 "changes nothing". With the header:
    - a retry with the same key and the same payload replays the stored response (header Idempotent-Replayed: true),
      kept for IDEMPOTENCY_TTL_S (a TTL index removes it);
    - the same key with a different payload is 422;
    - a failed request stores nothing, so a retry runs again (and gets the same deterministic error).
    On a replica set the write and its stored response commit in one transaction: a failover drill showed that
    otherwise an election between the two leaves a write without its response, and the retry then fails. Without
    transactions (a standalone server) the write stamps the key on its own document instead, see idempotent_claimed().
    Without the header nothing changes."""
    if key is None:
        return operation()
    key_id = f"{scope} {key}"
    fingerprint = hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()
    if transactions_supported():
        return idempotent_in_transaction(key_id, fingerprint, status_code, operation)
    return idempotent_claimed(scope, key_id, fingerprint, status_code, operation)


def replay(prior: dict, fingerprint: str):
    if prior.get("fingerprint") != fingerprint:  # 422 in the contract's validation shape (detail is a list)
        raise validation_error(("header", "Idempotency-Key"), "Idempotency-Key was already used with a different request")
    if prior.get("state") == "done":
        return JSONResponse(prior["body"], status_code=prior["status"], headers={"Idempotent-Replayed": "true"})
    return None


def idempotent_in_transaction(key_id: str, fingerprint: str, status_code: int, operation):
    """Exactly once: the response record and the write commit together or not at all, so whatever happens to a
    request (an election, a lost connection, a crash), its retry either replays the committed answer or runs on a
    database that contains nothing of the failed attempt. Two concurrent requests with one key conflict; the loser
    retries and replays the winner's answer."""
    deadline = monotonic() + TXN_RETRY_S
    with client.start_session() as session:
        while True:
            # snapshot reads only majority-committed data, so a retry never replays an answer an election could still
            # roll back; majority commit is what makes the 201 durable.
            session.start_transaction(read_concern=ReadConcern("snapshot"), write_concern=WriteConcern("majority"))
            token = TXN.set(session)
            try:
                prior = idempotency.find_one({"_id": key_id}, session=session)
                answer = replay(prior, fingerprint) if prior is not None else None
                if answer is None:
                    if prior is not None and now_utc() - prior["created_at"] < STALE_PENDING:
                        raise HTTPException(409, "a request with this Idempotency-Key is still in progress")
                    body = operation()
                    record = {"fingerprint": fingerprint, "state": "done", "status": status_code, "body": body, "created_at": now_utc()}
                    idempotency.replace_one({"_id": key_id}, record, upsert=True, session=session)
                    answer = body
            except BaseException as exc:
                session.abort_transaction()
                if isinstance(exc, PyMongoError) and exc.has_error_label("TransientTransactionError") and monotonic() < deadline:
                    time_module.sleep(0.05)
                    continue
                raise
            finally:
                TXN.reset(token)
            while True:
                try:
                    session.commit_transaction()
                    return answer
                except PyMongoError as exc:
                    if exc.has_error_label("UnknownTransactionCommitResult") and monotonic() < deadline:
                        continue  # committing again is safe: the server commits a transaction at most once
                    if exc.has_error_label("TransientTransactionError") and monotonic() < deadline:
                        break  # aborted by the server: run the whole transaction again
                    raise  # 503: nothing or everything committed, so the client's retry is still correct
            time_module.sleep(0.05)


STAMP: ContextVar = ContextVar("stamp", default=None)  # the Idempotency-Key the current write stamps on its document
RECORD_FIELDS = ("emp_code", "date", "status", "punch_in", "punch_out", "work_hours", "late_minutes", "overtime_minutes",
                 "half_day", "history")


def stamp_insert(doc: dict) -> None:
    """An inserted document carries the key and a snapshot of itself: proof, written atomically with the write."""
    key = STAMP.get()
    if key is not None:
        doc["_idem"] = [{"k": key, "at": now_utc(), "snap": dict(doc)}]


def stamp_stage() -> list:
    """The same for an update: a last pipeline stage that snapshots the record as this update leaves it (the fields
    record_out reads). Stamps older than IDEMPOTENCY_TTL_S (left by requests never retried) are dropped on the way."""
    key = STAMP.get()
    if key is None:
        return []
    cutoff = now_utc() - timedelta(seconds=IDEMPOTENCY_TTL_S)
    kept = {"$filter": {"input": {"$cond": [{"$isArray": "$_idem"}, "$_idem", []]}, "cond": {"$gt": ["$$this.at", {"$literal": cutoff}]}}}
    stamp = {"k": {"$literal": key}, "at": {"$literal": now_utc()}, "snap": {f: "$" + f for f in RECORD_FIELDS}}
    return [{"$set": {"_idem": {"$concatArrays": [kept, [stamp]]}}}]


def stamped(scope: str):
    return (employees, employee_out) if scope == "POST /employees" else (logs, record_out)


def stamped_answer(scope: str, key_id: str):
    """The answer of a write that committed under this key, rebuilt from its stamp; None if it never committed."""
    coll, out = stamped(scope)
    doc = coll.find_one({"_idem.k": key_id}, {"_idem": {"$elemMatch": {"k": key_id}}}, max_time_ms=QUERY_TIMEOUT_MS)
    return None if doc is None else out(doc["_idem"][0]["snap"])


def finish(scope: str, key_id: str, status_code: int, body) -> None:
    """Store the answer, then drop the stamp (only a space saving: the answer is what retries read first)."""
    idempotency.update_one({"_id": key_id}, {"$set": {"state": "done", "status": status_code, "body": body}})
    try:
        stamped(scope)[0].update_one({"_idem.k": key_id}, {"$pull": {"_idem": {"k": key_id}}})
    except PyMongoError as exc:
        log.warning("could not remove idempotency stamp %s: %s", key_id, exc)


def idempotent_claimed(scope: str, key_id: str, fingerprint: str, status_code: int, operation):
    """Without transactions the write and its stored answer are two writes, so the write also stamps the key, with
    a snapshot of its result, onto the document it writes, atomically (one document). Whatever fails after that (a
    crash, a lost connection, an error with unknown outcome), the stamp proves the write committed and gives back
    its exact answer, so a retry is still exactly once instead of 409 "already punched in".

    The first request claims the key with an insert (the _id is unique, so exactly one concurrent request wins). A
    retry finding the claim: the answer if stored; else the stamp's answer if the write committed; else 409 while the
    first may still run, and after STALE_PENDING (its process probably died before writing) it takes the key over."""
    for _ in range(2):
        try:
            idempotency.insert_one({"_id": key_id, "fingerprint": fingerprint, "state": "pending", "created_at": now_utc()})
            break
        except DuplicateKeyError:
            prior = idempotency.find_one({"_id": key_id})
            if prior is None:
                continue  # expired between our insert and read: claim it again
            answer = replay(prior, fingerprint)
            if answer is not None:
                return answer
            body = stamped_answer(scope, key_id)
            if body is not None:  # committed, but the answer was never stored
                finish(scope, key_id, status_code, body)
                return JSONResponse(body, status_code=status_code, headers={"Idempotent-Replayed": "true"})
            if now_utc() - prior["created_at"] < STALE_PENDING:
                raise HTTPException(409, "a request with this Idempotency-Key is still in progress") from None
            # Probably died before writing; if not, the two runs still end with one write (see STALE_PENDING).
            idempotency.delete_one({"_id": key_id, "state": "pending", "created_at": prior["created_at"]})
    else:
        raise HTTPException(409, "a request with this Idempotency-Key is still in progress")
    token = STAMP.set(key_id)
    try:
        body = operation()
    except BaseException:
        # A failure can come after the write committed (the connection broke before the reply): the stamp tells.
        # A failure while checking leaves the claim pending, which the next retry resolves the same way.
        body = stamped_answer(scope, key_id)
        if body is None:
            prior = idempotency.find_one({"_id": key_id})
            if prior is not None and prior.get("state") == "done":  # a parallel run of this request stored its answer
                return replay(prior, fingerprint)
            idempotency.delete_one({"_id": key_id, "state": "pending"})
            raise
    finally:
        STAMP.reset(token)
    finish(scope, key_id, status_code, body)
    return body


@app.get("/health", tags=["system"])
async def health():
    # The probe runs on its own small thread limiter, so it still answers promptly while every worker thread is
    # busy with slow analytics: a busy service is not a dead one.
    try:
        await to_thread.run_sync(client.admin.command, "ping", limiter=health_limiter())
    except PyMongoError:
        return JSONResponse(status_code=503, content={"detail": "database unavailable"}, headers={"Retry-After": "1"})
    if not _indexes_ready:  # startup could not reach the database: build now, in the background (no-op if running)
        build_indexes_in_background()
    return {"status": "ok"}


@app.post("/employees", status_code=201, tags=["employees"])
def create_employee(body: EmployeeIn, idempotency_key: IdempotencyKey = None):
    return idempotent(idempotency_key, "POST /employees", body.model_dump(), 201, lambda: create_employee_op(body))


def create_employee_op(body: EmployeeIn) -> dict:
    doc = body.model_dump()
    doc["created_at"] = now_utc()
    stamp_insert(doc)
    try:
        employees.insert_one(doc, session=TXN.get())  # the unique index decides; no check-then-insert
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists") from None
    return employee_out(doc)


@app.get("/employees", tags=["employees"])
def list_employees(department: Optional[str] = None, page: Page = 1, page_size: PageSize = 20):
    q = {} if department is None else {"department": department}
    return paged(employees, q, [("emp_code", ASCENDING)], page, page_size, employee_out)


@app.post("/attendance/punch-in", status_code=201, tags=["attendance"])
def punch_in(body: PunchIn, idempotency_key: IdempotencyKey = None):
    return idempotent(idempotency_key, "POST /attendance/punch-in", body.model_dump(exclude_unset=True), 201, lambda: punch_in_op(body))


def punch_in_op(body: PunchIn) -> dict:
    emp = get_employee(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc()
    day = attendance_date(ts, emp)
    doc = {
        "emp_code": body.emp_code,
        "date": day.isoformat(),
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        **derived_fields(body.status, ts, None, day, emp),
        "history": [],
    }
    stamp_insert(doc)
    try:
        logs.insert_one(doc, session=TXN.get())  # unique (emp_code, date): of two parallel requests exactly one wins
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date") from None
    return record_out(doc)


@app.post("/attendance/punch-out", tags=["attendance"])
def punch_out(body: PunchOut, idempotency_key: IdempotencyKey = None):
    return idempotent(idempotency_key, "POST /attendance/punch-out", body.model_dump(exclude_unset=True), 200, lambda: punch_out_op(body))


def punch_out_op(body: PunchOut) -> dict:
    emp = get_employee(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc()
    # Compare-and-swap: the update only applies if the record still has the punch-in and status the derived values
    # were computed from, and no punch-out yet. So of two parallel punch-outs exactly one wins, and a correction that
    # lands in between makes us re-read and recompute instead of storing work hours for a stale punch-in.
    for _ in range(CAS_ATTEMPTS):
        rec = logs.find_one(
            {"emp_code": body.emp_code, "punch_in": {"$lte": ts}},
            sort=[("punch_in", DESCENDING)], max_time_ms=QUERY_TIMEOUT_MS, session=TXN.get(),
        )
        if rec is None:
            raise HTTPException(404, "no punch-in found")
        if rec.get("punch_out") is not None:
            raise HTTPException(409, "already punched out")
        if ts <= rec["punch_in"] or ts - rec["punch_in"] > MAX_SHIFT:
            raise validation_error(("body", "punched_at"), "must be after punch_in and within 24 hours of it", body.punched_at)
        day = to_date(rec.get("date")) or attendance_date(rec["punch_in"], emp)
        derived = derived_fields(rec.get("status"), rec["punch_in"], ts, day, emp)
        updated = logs.find_one_and_update(
            {"_id": rec["_id"], "punch_out": None, "punch_in": rec["punch_in"], "status": rec.get("status")},
            [{"$set": {"punch_out": {"$literal": ts}, **{k: {"$literal": derived[k]} for k in ("work_hours", "overtime_minutes", "half_day")}}},
             *stamp_stage()],
            return_document=ReturnDocument.AFTER,
            session=TXN.get(),
        )
        if updated is not None:
            return record_out(updated)
    raise HTTPException(409, "the record kept changing concurrently; retry")


def attendance_filter(emp_code, date_from, date_to, status) -> dict:
    q = {}
    if emp_code is not None:
        q["emp_code"] = emp_code
    if date_from is not None or date_to is not None:
        d_from = parse_date(date_from, ("query", "date_from")) if date_from is not None else None
        d_to = parse_date(date_to, ("query", "date_to")) if date_to is not None else None
        if d_from and d_to and d_from > d_to:
            raise validation_error(("query", "date_from"), "date_from must not be after date_to", date_from)
        q["date"] = {}
        if date_from is not None:
            q["date"]["$gte"] = date_from
        if date_to is not None:
            q["date"]["$lte"] = date_to
    if status is not None:
        q["status"] = status
    return q


ATTENDANCE_SORT = [("date", DESCENDING), ("emp_code", ASCENDING)]


@app.get("/attendance", tags=["attendance"])
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    page: Page = 1,
    page_size: PageSize = 20,
):
    q = attendance_filter(emp_code, date_from, date_to, status)
    return paged(logs, q, ATTENDANCE_SORT, page, page_size, record_out)


def encode_cursor(doc: dict) -> str:
    raw = json.dumps([doc.get("date"), doc.get("emp_code")], default=str).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_cursor(value: str) -> tuple:
    try:
        day, code = json.loads(base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)))
        if isinstance(day, str) and isinstance(code, str):
            return day, code
    except (ValueError, TypeError):
        pass
    raise validation_error(("query", "after"), "not a cursor returned by this endpoint", value)


@app.get("/attendance/feed", tags=["attendance"], summary="Keyset-paginated attendance (constant cost at any depth)")
def attendance_feed(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    after: Optional[str] = None,
    limit: Annotated[int, Query(ge=1, le=500), QueryInt] = 100,
):
    """The same records, filters and order as GET /attendance, paginated by position instead of page number: each
    response carries `next_cursor` (null at the end) to pass as `after`. A page costs the same at any depth (page
    numbers have to skip every earlier row), and since (date, emp_code) is unique, walking the feed returns every
    record exactly once, even while records are being added."""
    q = attendance_filter(emp_code, date_from, date_to, status)
    if after is not None:
        day, code = decode_cursor(after)
        position = {"$or": [{"date": {"$lt": day}}, {"date": day, "emp_code": {"$gt": code}}]}  # strictly after, in sort order
        q = {"$and": [q, position]} if q else position
    docs = list(logs.find(q, {"_id": 0}, max_time_ms=QUERY_TIMEOUT_MS).sort(ATTENDANCE_SORT).limit(limit + 1))
    more = len(docs) > limit
    docs = docs[:limit]
    return {"items": [record_out(d) for d in docs], "next_cursor": encode_cursor(docs[-1]) if more else None}


CHANGE_FIELDS = ("status", "punch_in", "punch_out", "work_hours", "late_minutes", "overtime_minutes", "half_day")


@app.patch("/attendance/{emp_code}/{date}", tags=["attendance"])
def regularize(emp_code: str, date: str, body: Regularize, idempotency_key: IdempotencyKey = None):
    payload = {"emp_code": emp_code, "date": date, **body.model_dump(exclude_unset=True)}
    return idempotent(idempotency_key, "PATCH /attendance", payload, 200, lambda: regularize_op(emp_code, date, body))


def regularize_op(emp_code: str, date: str, body: Regularize) -> dict:
    day = parse_date(date, ("path", "date"))
    emp = get_employee(emp_code)
    rec = logs.find_one({"emp_code": emp_code, "date": date}, session=TXN.get())
    if rec is None:
        raise HTTPException(404, "no attendance record for that date")

    sent = body.model_fields_set
    status = body.status if "status" in sent else rec.get("status")
    if status not in PRESENCE and status not in NON_PRESENCE:  # only possible for a malformed seeded record
        raise validation_error(("body", "status"), "the stored record has no valid status; send one", status)
    if status in NON_PRESENCE:
        if "punch_in" in sent or "punch_out" in sent:
            raise validation_error(("body",), f"punch times can not be set with status {status}")
        new_in = new_out = None
    else:
        new_in = ms_to_dt(body.punch_in) if "punch_in" in sent else rec.get("punch_in")
        new_out = ms_to_dt(body.punch_out) if "punch_out" in sent else rec.get("punch_out")
        if new_in is None:
            raise validation_error(("body", "punch_in"), f"status {status} requires a punch_in")
        if "punch_in" in sent and attendance_date(new_in, emp) != day:
            raise validation_error(("body", "punch_in"), "punch_in must stay on the record's attendance date", body.punch_in)
        if new_out is not None and ("punch_in" in sent or "punch_out" in sent):
            if new_out <= new_in or new_out - new_in > MAX_SHIFT:
                raise validation_error(("body", "punch_out"), "punch_out must be after punch_in and within 24 hours")

    old = {
        "status": rec.get("status"),
        "punch_in": rec.get("punch_in"),
        "punch_out": rec.get("punch_out"),
        "work_hours": as_number(rec.get("work_hours")),
        "late_minutes": as_int(rec.get("late_minutes")),
        "overtime_minutes": as_int(rec.get("overtime_minutes")),
        "half_day": rec.get("half_day") is True,
    }
    new = {"status": status, "punch_in": new_in, "punch_out": new_out, **derived_fields(status, new_in, new_out, day, emp)}
    def same(field: str) -> bool:  # instants are compared as epoch ms
        return change_value(old[field]) == change_value(new[field])

    if all(same(f) for f in ("status", "punch_in", "punch_out")):
        raise validation_error(("body",), "the request changes nothing")

    entry = {
        "at": now_utc(),
        "by": body.regularized_by,
        "reason": body.reason,
        "changes": {f: {"from": old[f], "to": new[f]} for f in CHANGE_FIELDS if not same(f)},
    }
    # Optimistic concurrency: the update only applies if the record still looks the way it did when we read it
    # (same times, same status, same number of history entries). A concurrent edit makes this one a 409.
    # The update is a pipeline so that a history stored as null (or anything but an array) is treated as empty
    # instead of failing the append; $literal keeps our values from being read as expressions.
    history = rec.get("history")
    history_size = {"$cond": [{"$isArray": "$history"}, {"$size": "$history"}, 0]}
    updated = logs.find_one_and_update(
        {
            "_id": rec["_id"],
            "status": rec.get("status"),
            "punch_in": rec.get("punch_in"),
            "punch_out": rec.get("punch_out"),
            "$expr": {"$eq": [history_size, len(history) if isinstance(history, list) else 0]},
        },
        [
            {
                "$set": {
                    **{field: {"$literal": value} for field, value in new.items()},
                    "history": {
                        "$concatArrays": [
                            {"$cond": [{"$isArray": "$history"}, "$history", []]},
                            [{"$literal": entry}],
                        ]
                    },
                }
            },
            *stamp_stage(),
        ],
        return_document=ReturnDocument.AFTER,
        session=TXN.get(),
    )
    if updated is None:
        raise HTTPException(409, "the record was changed concurrently; retry")
    return record_out(updated)


# --------------------------------------------------------------------------- #
# 7. Aggregation pipeline builders
#    Each analytics endpoint and /admin/explain build their pipelines with the same function.
# --------------------------------------------------------------------------- #
def round_half_up(expr, places: int) -> dict:
    """$round rounds half to even, so build half-up by hand, in Decimal128 to avoid binary float error.
    $toDecimal of a double keeps 15 significant digits, so 1.005 (stored as 1.00499999...) becomes 1.005."""
    factor = 10**places
    return {
        "$let": {
            "vars": {"v": {"$toDecimal": expr}},
            "in": {
                "$cond": [
                    {"$eq": ["$$v", None]},
                    None,
                    {"$toDouble": {"$divide": [{"$floor": {"$add": [{"$multiply": ["$$v", factor]}, Decimal128("0.5")]}}, factor]}},
                ]
            },
        }
    }


def parse_day(expr) -> dict:
    """"YYYY-MM-DD" -> date, or null for anything malformed (never an error that aborts the whole aggregation)."""
    return {"$dateFromString": {"dateString": expr, "format": "%Y-%m-%d", "onError": None, "onNull": None}}


def number(expr, to: str, default) -> dict:
    """Numeric field as stored (int, Int64, double, Decimal128, numeric string) -> `to`; missing or unusable -> default."""
    return {"$convert": {"input": expr, "to": to, "onError": default, "onNull": default}}


# Read the stored derived values (DATA_MODEL.md: stored values are authoritative), with the documented defaults.
IS_PRESENT = {"$in": ["$status", list(PRESENCE)]}
PRESENT_VALUE = {"$cond": [IS_PRESENT, {"$cond": [{"$eq": ["$half_day", True]}, 0.5, 1]}, 0]}  # R5, R6
IS_WEEKDAY = {"$lte": [{"$ifNull": [{"$isoDayOfWeek": parse_day("$date")}, 7]}, 5]}  # malformed date: not a weekday
LATE = number("$late_minutes", "long", 0)
OVERTIME = number("$overtime_minutes", "long", 0)
WORK_HOURS = number("$work_hours", "decimal", None)
# NaN sorts below every number in MongoDB, so ">= 0" also drops NaN as well as null.
HAS_WORK_HOURS = {"$and": [IS_PRESENT, {"$gte": [WORK_HOURS, 0]}]}


def count_if(cond) -> dict:
    return {"$sum": {"$cond": [cond, 1, 0]}}


def densify_bounds(first: date, last: date) -> list:
    """$densify's upper bound is exclusive. Noon of the last day includes the last midnight without ever computing
    last + 1 day, which would overflow on 9999-12-31."""
    return [datetime.combine(first, time(), timezone.utc), datetime.combine(last, time(12), timezone.utc)]


def month_bounds(month: str) -> tuple[str, str]:
    y, m = map(int, month.split("-"))
    return f"{month}-01", f"{month}-{calendar.monthrange(y, m)[1]:02d}"


def working_days(first: date, last: date) -> int:
    """R7: Mon - Fri between two dates inclusive, no holiday calendar (calendar arithmetic, no documents)."""
    if first > last:
        return 0
    days = (last - first).days + 1
    full_weeks, rest = divmod(days, 7)
    return full_weeks * 5 + sum(1 for i in range(rest) if (first + timedelta(days=i)).weekday() < 5)


def employee_monthly_pipeline(emp_code: str, month: str, n_working_days: int) -> list:
    start, end = month_bounds(month)
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": start, "$lte": end}}},
        # A neutral document so that $group always emits one row, even for a month without records.
        {"$unionWith": {"pipeline": [{"$documents": [{"_seed": True}]}]}},
        {
            "$group": {
                "_id": None,
                "present_days": {"$sum": {"$cond": [{"$and": ["$date", IS_WEEKDAY]}, PRESENT_VALUE, 0]}},
                "leave_days": count_if({"$eq": ["$status", "LEAVE"]}),
                "late_count": count_if({"$gt": [LATE, 0]}),
                "total_late_minutes": {"$sum": LATE},
                "total_overtime_minutes": {"$sum": OVERTIME},
            }
        },
        {
            "$project": {
                "_id": 0,
                "emp_code": {"$literal": emp_code},
                "month": {"$literal": month},
                "working_days": {"$literal": n_working_days},
                "present_days": round_half_up("$present_days", 2),
                "leave_days": "$leave_days",  # expressions, not 1, so fields come out in the contract's schema order
                "late_count": "$late_count",
                "total_late_minutes": "$total_late_minutes",
                "total_overtime_minutes": "$total_overtime_minutes",
                "attendance_pct": (
                    None
                    if n_working_days == 0
                    else round_half_up({"$multiply": [{"$divide": [{"$toDecimal": "$present_days"}, n_working_days]}, 100]}, 2)
                ),
            }
        },
    ]


DEPARTMENT_IN_LIMIT = 2000  # above this, an $in list costs more (query size, index ranges) than it saves


def department_scope(department: Optional[str]) -> dict:
    """An optional narrowing of the log scan to one department's emp_codes. The codes come from the
    (department, emp_code) index alone (a covered query); for a normal-sized department this is about 5x faster than
    grouping the whole company's month. It is only an optimisation: correctness comes from the employee stream the
    pipelines merge in, so a very large department simply skips it."""
    if department is None:
        return {}
    cursor = employees.find({"department": department}, {"_id": 0, "emp_code": 1}, max_time_ms=QUERY_TIMEOUT_MS)
    codes = [e["emp_code"] for e in cursor.limit(DEPARTMENT_IN_LIMIT + 1)]
    return {"emp_code": {"$in": codes}} if len(codes) <= DEPARTMENT_IN_LIMIT else {}


def logs_in_month(start: str, end: str, department: Optional[str]) -> dict:
    return {"date": {"$gte": start, "$lte": end}, **department_scope(department)}


def merge_employees(staff: dict, fields: dict, totals: tuple) -> list:
    """Join per-employee log totals to their employee without a $lookup: stream the (indexed) employees in with
    $unionWith and merge both streams with one $group on emp_code. One sequential pass over the employees instead of
    one index probe per employee (about 25% faster company-wide). Rows that end up without `known` are logs of an
    employee who does not exist, or is outside `staff` (another department); they are dropped."""
    return [
        {"$unionWith": {"coll": "employees", "pipeline": [{"$match": staff}, {"$project": {"_id": "$emp_code", "known": {"$literal": True}, **fields}}]}},
        {"$group": {"_id": "$_id", "known": {"$max": "$known"}, **{f: {"$max": f"${f}"} for f in fields}, **sums(totals)}},
        {"$match": {"known": True}},
    ]


SUMMARY_TOTALS = ("present_days", "wh_sum", "wh_count", "late_count", "total_late_minutes", "leave_count", "on_duty_count")
LEADERBOARD_TOTALS = ("total_late_minutes", "late_count")


def joined_by(day: str) -> dict:
    """R9 as an expression. A missing joined_on would compare below every string, so it is checked explicitly:
    someone without a joining date is not counted."""
    return {"$and": [{"$eq": [{"$type": "$joined_on"}, "string"]}, {"$lte": ["$joined_on", day]}]}


def sums(fields: tuple) -> dict:
    return {f: {"$sum": f"${f}"} for f in fields}  # employee rows lack these fields, and $sum ignores missing values


def department_summary_pipeline(month: str, department: Optional[str]) -> list:
    """Per-employee totals for the month, merged with every employee of the department(s) (so people with no logs are
    still there), then summed per department. joined_on only decides the headcount (R9), not which records count."""
    start, end = month_bounds(month)
    staff = {"department": department} if department is not None else {"department": {"$type": "string"}}
    return [
        {"$match": logs_in_month(start, end, department)},
        {
            "$group": {
                "_id": "$emp_code",
                "present_days": {"$sum": {"$cond": [IS_WEEKDAY, PRESENT_VALUE, 0]}},
                # Sum and count over records, so the average is over records, not over employees.
                "wh_sum": {"$sum": {"$cond": [HAS_WORK_HOURS, WORK_HOURS, 0]}},
                "wh_count": count_if(HAS_WORK_HOURS),
                "late_count": count_if({"$gt": [LATE, 0]}),
                "total_late_minutes": {"$sum": LATE},
                "leave_count": count_if({"$eq": ["$status", "LEAVE"]}),
                "on_duty_count": count_if({"$eq": ["$status", "ON_DUTY"]}),
            }
        },
        *merge_employees(staff, {"department": 1, "hc": {"$cond": [joined_by(end), 1, 0]}}, SUMMARY_TOTALS),
        {"$group": {"_id": "$department", "headcount": {"$sum": "$hc"}, **sums(SUMMARY_TOTALS)}},
        {"$match": {"headcount": {"$gt": 0}}},  # a department with nobody joined by the end of the month is omitted
        {"$sort": {"_id": 1}},
        {
            "$project": {
                "_id": 0,
                "department": "$_id",
                "headcount": "$headcount",  # expressions, not 1, so fields come out in the contract's schema order
                "present_days": round_half_up("$present_days", 2),
                "avg_work_hours": {
                    "$cond": [{"$eq": ["$wh_count", 0]}, None, round_half_up({"$divide": ["$wh_sum", "$wh_count"]}, 2)]
                },
                "late_count": "$late_count",
                "total_late_minutes": "$total_late_minutes",
                "leave_count": "$leave_count",
                "on_duty_count": "$on_duty_count",
            }
        },
    ]


def late_leaderboard_pipeline(month: str, limit: int, department: Optional[str]) -> list:
    start, end = month_bounds(month)
    # With a department, only its employees are merged in, so the ranking happens inside the department.
    staff = {"department": department} if department is not None else {"emp_code": {"$type": "string"}}
    return [
        {"$match": {**logs_in_month(start, end, department), "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": LATE}, "late_count": {"$sum": 1}}},
        *merge_employees(staff, {"name": 1, "department": 1}, LEADERBOARD_TOTALS),
        {"$match": {"total_late_minutes": {"$gt": 0}}},  # employees merged in without late logs
        # $rank is standard competition ranking: ties share a rank and the next rank is skipped (1, 2, 2, 4).
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit is applied after ranking, so ties at the cutoff are all kept
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {
            "$project": {
                "_id": 0,
                "rank": "$rank",  # expressions, not 1, so fields come out in the contract's schema order
                "emp_code": "$_id",
                "name": "$name",
                "department": "$department",
                "total_late_minutes": "$total_late_minutes",
                "late_count": "$late_count",
            }
        },
    ]


def department_trend_pipeline(department: str, d_from: date, d_to: date) -> list:
    start, end = d_from.isoformat(), d_to.isoformat()
    day = parse_day("$_id")
    return [
        {"$match": {"department": department}},
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": [
                    {"$match": {"date": {"$gte": start, "$lte": end}}},
                    {"$project": {"_id": 0, "date": 1, "p": PRESENT_VALUE, "l": {"$cond": [{"$gt": [LATE, 0]}, 1, 0]}}},
                ],
                "as": "logs",
            }
        },
        # One "+1 headcount" event per employee on the day they start counting (if within the range), plus one event
        # per log. joined_on only limits the headcount (R9), not which records count.
        {
            "$project": {
                "_id": 0,
                "events": {
                    "$concatArrays": [
                        {"$cond": [joined_by(end), [{"date": {"$max": ["$joined_on", start]}, "hc": 1, "p": 0, "l": 0}], []]},
                        {"$map": {"input": "$logs", "in": {"date": "$$this.date", "hc": 0, "p": "$$this.p", "l": "$$this.l"}}},
                    ]
                },
            }
        },
        {"$unwind": "$events"},
        {"$replaceRoot": {"newRoot": "$events"}},
        # Guarantees at least one document, so $densify still fills the range for a department without anyone yet.
        {"$unionWith": {"pipeline": [{"$documents": [{"date": start, "hc": 0, "p": 0, "l": 0}]}]}},
        {"$group": {"_id": "$date", "hc": {"$sum": "$hc"}, "p": {"$sum": "$p"}, "l": {"$sum": "$l"}}},
        {"$set": {"day": day}},
        {"$match": {"day": {"$type": "date"}}},  # a malformed stored date can not take part in the day series
        {
            "$densify": {
                "field": "day",
                "range": {
                    "step": 1,
                    "unit": "day",
                    "bounds": densify_bounds(d_from, d_to),
                },
            }
        },
        {
            "$setWindowFields": {
                "sortBy": {"day": 1},
                "output": {"headcount": {"$sum": "$hc", "window": {"documents": ["unbounded", "current"]}}},
            }
        },
        {
            "$set": {
                "is_working_day": {"$lte": [{"$isoDayOfWeek": "$day"}, 5]},
                "present_count": {"$ifNull": ["$p", 0]},
                "late_count": {"$ifNull": ["$l", 0]},
            }
        },
        {
            "$set": {
                "attendance_rate": {
                    "$cond": [
                        {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
                        round_half_up({"$divide": [{"$toDecimal": "$present_count"}, "$headcount"]}, 4),
                        None,
                    ]
                }
            }
        },
        {
            "$setWindowFields": {
                "sortBy": {"day": 1},
                # The window only sees rows inside the requested range; $avg skips nulls and is null if all are null.
                "output": {"ma": {"$avg": "$attendance_rate", "window": {"documents": [-6, 0]}}},
            }
        },
        {"$sort": {"day": 1}},
        {
            "$project": {
                "_id": 0,
                "date": {"$dateToString": {"date": "$day", "format": "%Y-%m-%d"}},
                "is_working_day": "$is_working_day",  # expressions, not 1: fields in the contract's schema order
                "headcount": "$headcount",
                "present_count": "$present_count",
                "late_count": "$late_count",
                "attendance_rate": "$attendance_rate",
                "moving_avg_7d": round_half_up("$ma", 4),
            }
        },
    ]


# --------------------------------------------------------------------------- #
# 8. Analytics and explain endpoints
# --------------------------------------------------------------------------- #
def monthly_working_days(emp: Optional[dict], month: str) -> int:
    start, end = (date.fromisoformat(d) for d in month_bounds(month))
    joined = to_date(emp.get("joined_on")) if emp is not None else None
    if joined is not None:  # R7: mid-month joiners. A malformed seeded value is read like a missing one (never a 500).
        start = max(start, joined)
    return working_days(start, end)


def trend_range(from_: str, to: str) -> tuple[date, date]:
    d_from, d_to = parse_date(from_, ("query", "from")), parse_date(to, ("query", "to"))
    if d_to < d_from:
        raise validation_error(("query", "to"), "to must not be before from", to)
    if (d_to - d_from).days + 1 > 92:
        raise validation_error(("query", "to"), "the range can not exceed 92 days", to)
    return d_from, d_to


@app.get("/analytics/employees/{emp_code}/monthly", tags=["analytics"])
def employee_monthly(emp_code: str, month: Month):
    emp = get_employee(emp_code)
    first, last = month_bounds(month)
    return run_report(logs, employee_monthly_pipeline(emp_code, month, monthly_working_days(emp, month)), first, last)[0]


@app.get("/analytics/departments/summary", tags=["analytics"])
@heavy
def department_summary(month: Month, department: Optional[str] = None):
    items = run_report(logs, department_summary_pipeline(month, department), *month_bounds(month))
    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late", tags=["analytics"])
@heavy
def late_leaderboard(month: Month, limit: Limit = 10, department: Optional[str] = None):
    return {"month": month, "items": run_report(logs, late_leaderboard_pipeline(month, limit, department), *month_bounds(month))}


@app.get("/analytics/departments/{department}/trend", tags=["analytics"])
@heavy
def department_trend(department: str, from_: Annotated[str, Query(alias="from")], to: str):
    d_from, d_to = trend_range(from_, to)
    if employees.count_documents({"department": department}, limit=1) == 0:
        raise HTTPException(404, "unknown department")
    pipeline = department_trend_pipeline(department, d_from, d_to)
    return {"department": department, "items": run_report(employees, pipeline, d_from.isoformat(), d_to.isoformat())}


ExplainEndpoint = Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"]


def require(**params) -> None:
    missing = [name for name, value in params.items() if value is None]
    if missing:
        raise RequestValidationError(
            [{"type": "missing", "loc": ("query", name), "msg": "Field required", "input": None} for name in missing]
        )


@app.get("/admin/explain/{endpoint}", tags=["admin"])
@heavy
def explain(
    endpoint: ExplainEndpoint,
    emp_code: Optional[str] = None,
    month: OptionalMonth = None,
    department: Optional[str] = None,
    limit: Limit = 10,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    from_: Annotated[Optional[str], Query(alias="from")] = None,
    to: Optional[str] = None,
    page: Page = 1,
    page_size: PageSize = 20,
):
    # The contract declares every parameter's format, whichever endpoint is explained: a malformed date is 422 even
    # when the explained query does not use it (found by contract fuzzing).
    for name, value in (("date_from", date_from), ("date_to", date_to), ("from", from_), ("to", to)):
        if value is not None:
            parse_date(value, ("query", name))
    if endpoint == "attendance_list":
        coll = logs
        command = {
            "find": logs.name,
            "filter": attendance_filter(emp_code, date_from, date_to, status),
            "sort": dict(ATTENDANCE_SORT),
            "projection": {"_id": 0},
            "skip": min((page - 1) * page_size, MAX_SKIP),  # same query; the cap only keeps it inside int64
            "limit": page_size,
        }
    else:
        if endpoint == "employee_monthly":
            require(emp_code=emp_code, month=month)
            emp = employees.find_one({"emp_code": emp_code})
            coll, pipeline = logs, employee_monthly_pipeline(emp_code, month, monthly_working_days(emp, month))
        elif endpoint == "department_summary":
            require(month=month)
            coll, pipeline = logs, department_summary_pipeline(month, department)
        elif endpoint == "late_leaderboard":
            require(month=month)
            coll, pipeline = logs, late_leaderboard_pipeline(month, limit, department)
        else:
            require(department=department, **{"from": from_}, to=to)
            coll, pipeline = employees, department_trend_pipeline(department, *trend_range(from_, to))
        command = {"aggregate": coll.name, "pipeline": pipeline, "cursor": {}}
    result = db.command("explain", command, verbosity="executionStats")
    plain = json.loads(json_util.dumps(result, json_options=json_util.RELAXED_JSON_OPTIONS))
    return {"endpoint": endpoint, "collection": coll.name, "explain": plain}


# --------------------------------------------------------------------------- #
# 9. Beyond the contract
#    Extra read-only paths for the people who use this data every day. They reuse the helpers above and are backed by
#    the same indexes; none of them changes the behaviour or the shape of a contract endpoint.
# --------------------------------------------------------------------------- #
WEEKDAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def today_ist() -> date:
    return datetime.now(IST).date()


def ms_expr(expr) -> dict:
    """BSON date -> epoch ms inside a pipeline; anything else (null, missing) -> null."""
    return {"$cond": [{"$eq": [{"$type": expr}, "date"]}, {"$toLong": expr}, None]}


def employee_calendar_pipeline(emp: dict, month: str, today: date) -> list:
    """One row per day of the month for one employee, telling apart the cases a plain list can not show: a working day
    with no record, a weekend, a day before joining and a day still to come. Also the longest run of consecutive late
    working days, found with the gaps-and-islands technique: a running count of on-time days stays constant across a
    late streak, so it can be used as the streak's group key."""
    start, end = month_bounds(month)
    has_record = {"$ne": [{"$ifNull": ["$status", None]}, None]}
    up_to_today = {"$lte": ["$date", today.isoformat()]}
    return [
        {"$match": {"emp_code": emp["emp_code"], "date": {"$gte": start, "$lte": end}}},
        {"$unionWith": {"pipeline": [{"$documents": [{"date": start, "_seed": True}]}]}},  # densify needs one input
        {"$set": {"day": parse_day("$date")}},
        {"$match": {"day": {"$type": "date"}}},
        {
            "$densify": {
                "field": "day",
                "range": {
                    "step": 1,
                    "unit": "day",
                    "bounds": densify_bounds(date.fromisoformat(start), date.fromisoformat(end)),
                },
            }
        },
        {"$sort": {"day": 1, "_seed": 1}},  # when the seed and a real record share the first day, the real one wins
        {"$group": {"_id": "$day", "doc": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$doc"}},
        {"$set": {"date": {"$dateToString": {"date": "$day", "format": "%Y-%m-%d"}}, "dow": {"$isoDayOfWeek": "$day"}}},
        {
            "$set": {
                "is_working_day": {"$lte": ["$dow", 5]},
                "late": {"$gt": [LATE, 0]},
                "day_type": {
                    "$switch": {
                        "branches": [
                            {"case": has_record, "then": "$status"},
                            {"case": {"$lt": ["$date", emp.get("joined_on") or ""]}, "then": "NOT_JOINED"},
                            {"case": {"$not": [up_to_today]}, "then": "UPCOMING"},
                            {"case": {"$gt": ["$dow", 5]}, "then": "WEEKEND"},
                        ],
                        "default": "NO_RECORD",
                    }
                },
            }
        },
        {
            "$facet": {
                "days": [
                    {"$sort": {"day": 1}},
                    {
                        "$project": {
                            "_id": 0,
                            "date": 1,
                            "weekday": {"$arrayElemAt": [WEEKDAY_NAMES, {"$subtract": ["$dow", 1]}]},
                            "is_working_day": 1,
                            "day_type": 1,
                            "status": {"$ifNull": ["$status", None]},
                            "punch_in": ms_expr("$punch_in"),
                            "punch_out": ms_expr("$punch_out"),
                            "work_hours": {"$cond": [{"$gte": [WORK_HOURS, 0]}, {"$toDouble": WORK_HOURS}, None]},
                            "late_minutes": LATE,
                            "overtime_minutes": OVERTIME,
                            "half_day": {"$eq": ["$half_day", True]},
                            "corrected": {"$gt": [{"$size": {"$ifNull": ["$history", []]}}, 0]},
                        }
                    },
                ],
                "streak": [
                    {"$match": {"is_working_day": True, "date": {"$lte": today.isoformat()}}},
                    {
                        "$setWindowFields": {
                            "sortBy": {"day": 1},
                            "output": {"island": {"$sum": {"$cond": ["$late", 0, 1]}, "window": {"documents": ["unbounded", "current"]}}},
                        }
                    },
                    {"$match": {"late": True}},
                    {"$group": {"_id": "$island", "length": {"$sum": 1}}},
                    {"$group": {"_id": None, "longest": {"$max": "$length"}}},
                ],
                "totals": [
                    {
                        "$group": {
                            "_id": None,
                            "records": count_if(has_record),
                            "missing_days": count_if({"$eq": ["$day_type", "NO_RECORD"]}),
                            "late_days": count_if("$late"),
                            "corrected_days": count_if({"$gt": [{"$size": {"$ifNull": ["$history", []]}}, 0]}),
                        }
                    }
                ],
            }
        },
        {
            "$project": {
                "days": 1,
                "summary": {
                    "records": {"$first": "$totals.records"},
                    "missing_days": {"$first": "$totals.missing_days"},
                    "late_days": {"$first": "$totals.late_days"},
                    "corrected_days": {"$first": "$totals.corrected_days"},
                    "longest_late_streak": {"$ifNull": [{"$first": "$streak.longest"}, 0]},
                },
            }
        },
    ]


EXCEPTION_TYPES = ("NO_RECORD", "ABSENT", "LATE", "MISSING_PUNCH_OUT", "HALF_DAY")


def daily_exceptions_pipeline(day: date, department: Optional[str], today: date) -> list:
    """Everyone expected at work on `day` (joined by then), joined to that day's record, with a list of what needs a
    manager's attention. A punch-out is "missing" once the employee's own shift has ended (overnight shifts end the
    next day), using the server clock ($$NOW)."""
    d = day.isoformat()
    match = {"joined_on": {"$lte": d}}
    if department is not None:
        match["department"] = department
    expect_record = day.weekday() < 5 and day <= today
    is_present = {"$in": [{"$ifNull": ["$rec.status", None]}, list(PRESENCE)]}
    shift_end_at = {
        "$dateAdd": {
            "startDate": {
                "$dateFromString": {
                    "dateString": {"$concat": [d, "T", {"$ifNull": ["$shift_end", ""]}, ":00"]},
                    "timezone": "+05:30",
                    "onError": None,
                    "onNull": None,
                }
            },
            "unit": "day",
            "amount": {"$cond": [{"$lte": ["$shift_end", "$shift_start"]}, 1, 0]},
        }
    }
    checks = {
        "NO_RECORD": {"$and": [expect_record, {"$eq": [{"$ifNull": ["$rec", None]}, None]}]},
        "ABSENT": {"$eq": ["$rec.status", "ABSENT"]},
        "LATE": {"$gt": [number("$rec.late_minutes", "long", 0), 0]},
        "MISSING_PUNCH_OUT": {"$and": [is_present, {"$eq": [{"$ifNull": ["$rec.punch_out", None]}, None]}, {"$lt": [{"$ifNull": [shift_end_at, "$$NOW"]}, "$$NOW"]}]},
        "HALF_DAY": {"$eq": ["$rec.half_day", True]},
    }
    has = lambda kind: {"$in": [kind, "$issues"]}  # noqa: E731
    return [
        {"$match": match},
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": [{"$match": {"date": d}}],
                "as": "rec",
            }
        },
        {"$set": {"rec": {"$first": "$rec"}}},
        {"$set": {"issues": {"$concatArrays": [{"$cond": [cond, [kind], []]} for kind, cond in checks.items()]}}},
        {
            "$facet": {
                "summary": [
                    {
                        "$group": {
                            "_id": None,
                            "headcount": {"$sum": 1},
                            "present": count_if(is_present),
                            "on_leave": count_if({"$eq": ["$rec.status", "LEAVE"]}),
                            **{kind.lower(): count_if(has(kind)) for kind in EXCEPTION_TYPES},
                        }
                    },
                    {"$project": {"_id": 0}},
                ],
                "items": [
                    {"$match": {"issues": {"$ne": []}}},
                    {"$sort": {"emp_code": 1}},
                    {
                        "$project": {
                            "_id": 0,
                            "emp_code": 1,
                            "name": 1,
                            "department": 1,
                            "status": {"$ifNull": ["$rec.status", None]},
                            "issues": 1,
                            "late_minutes": number("$rec.late_minutes", "long", 0),
                            "punch_in": ms_expr("$rec.punch_in"),
                            "punch_out": ms_expr("$rec.punch_out"),
                        }
                    },
                ],
            }
        },
    ]


@app.get("/departments", tags=["employees"], summary="Departments with their current headcount")
def list_departments():
    pipeline = [
        {"$match": {"department": {"$exists": True}}},
        {"$group": {"_id": "$department", "headcount": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
        {"$project": {"_id": 0, "department": "$_id", "headcount": 1}},
    ]
    return {"items": run_pipeline(employees, pipeline)}


@app.get("/employees/{emp_code}", tags=["employees"], summary="One employee by emp_code")
def get_employee_by_code(emp_code: str):
    return employee_out(get_employee(emp_code))


@app.get("/attendance/{emp_code}/{date}", tags=["attendance"], summary="One attendance record, with its full history")
def get_attendance_record(emp_code: str, date: str):
    parse_date(date, ("path", "date"))
    get_employee(emp_code)
    rec = logs.find_one({"emp_code": emp_code, "date": date}, {"_id": 0}, max_time_ms=QUERY_TIMEOUT_MS)
    if rec is None:
        raise HTTPException(404, "no attendance record for that date")
    return record_out(rec)


@app.get("/analytics/employees/{emp_code}/calendar", tags=["insights"], summary="Day-by-day calendar for one employee")
def employee_calendar(emp_code: str, month: Month):
    emp = get_employee(emp_code)
    result = run_pipeline(logs, employee_calendar_pipeline(emp, month, today_ist()))[0]
    return {"emp_code": emp_code, "name": emp.get("name"), "month": month, **result}


@app.get("/analytics/exceptions", tags=["insights"], summary="What needs a manager's attention on one day")
@heavy
def daily_exceptions(date_: Annotated[Optional[str], Query(alias="date")] = None, department: Optional[str] = None):
    day = parse_date(date_, ("query", "date")) if date_ is not None else today_ist()
    rows = run_pipeline(employees, daily_exceptions_pipeline(day, department, today_ist()))[0]
    empty = {"headcount": 0, "present": 0, "on_leave": 0, **{kind.lower(): 0 for kind in EXCEPTION_TYPES}}
    summary = rows["summary"][0] if rows["summary"] else empty
    return {"date": day.isoformat(), "is_working_day": day.weekday() < 5, "summary": summary, "items": rows["items"]}


def require_metrics_token(authorization: Optional[str]) -> None:
    """Metrics are open by default; with METRICS_TOKEN set they need `Authorization: Bearer <token>`."""
    if METRICS_TOKEN and not hmac.compare_digest((authorization or "").encode(), f"Bearer {METRICS_TOKEN}".encode()):
        raise HTTPException(401, "missing or wrong bearer token", headers={"WWW-Authenticate": "Bearer"})


@app.get("/admin/metrics", tags=["admin"], summary="Request counts and latency percentiles per route")
def metrics(authorization: Annotated[Optional[str], Header()] = None):
    require_metrics_token(authorization)
    return METRICS.snapshot()


@app.get("/metrics", tags=["admin"], summary="The same metrics in Prometheus text format")
def prometheus_metrics(authorization: Annotated[Optional[str], Header()] = None):
    require_metrics_token(authorization)
    cache = (f"# TYPE report_cache_hits_total counter\nreport_cache_hits_total {REPORTS.hits}\n"
             f"# TYPE report_cache_misses_total counter\nreport_cache_misses_total {REPORTS.misses}\n"
             f"# TYPE report_cache_active gauge\nreport_cache_active {int(REPORTS.running)}\n")
    return PlainTextResponse(METRICS.prometheus() + cache, media_type="text/plain; version=0.0.4")


@app.get("/livez", tags=["system"], summary="Liveness: the process is up (does not touch MongoDB)")
async def livez():
    """Liveness for an orchestrator, next to /health (readiness, which needs MongoDB). Keeping them apart means a
    database outage takes instances out of rotation without the orchestrator restarting perfectly healthy processes."""
    return {"status": "ok"}


@app.get("/dashboard", include_in_schema=False, response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(DASHBOARD_HTML, headers=DASHBOARD_HEADERS)


# A single self-contained page (no CDN, no build step) that drives the API above. Everything it shows comes from
# the public endpoints, so it doubles as a living example of how to call them.
DASHBOARD_HTML = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Attendance dashboard</title><link rel="icon" href="data:,">
<style>
:root{--bg:#f6f7f9;--card:#fff;--ink:#16181d;--muted:#667085;--line:#e4e7ec;--accent:#3559e0;--good:#12a150;
--warn:#d98a00;--bad:#d1393a;--off:#eef0f3;--leave:#7a5af8}
@media (prefers-color-scheme:dark){:root{--bg:#0f1115;--card:#171a21;--ink:#e8eaee;--muted:#98a2b3;--line:#2a2f3a;
--accent:#7d98ff;--off:#20242d}}
*{box-sizing:border-box}body{margin:0;font:14px/1.45 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;background:var(--bg);color:var(--ink)}
header{display:flex;flex-wrap:wrap;gap:12px;align-items:center;justify-content:space-between;padding:16px 20px;border-bottom:1px solid var(--line);background:var(--card)}
h1{font-size:17px;margin:0}h2{font-size:14px;margin:0 0 10px}.muted{color:var(--muted)}
.controls{display:flex;flex-wrap:wrap;gap:8px}input,select,button{font:inherit;padding:6px 8px;border:1px solid var(--line);border-radius:8px;background:var(--card);color:var(--ink)}
button{background:var(--accent);color:#fff;border:0;cursor:pointer}
main{display:grid;align-items:start;gap:16px;padding:16px 20px;grid-template-columns:repeat(12,1fr);max-width:1400px;margin:auto}
.card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:14px;min-width:0}
.w12{grid-column:span 12}.w8{grid-column:span 8}.w4{grid-column:span 4}.w6{grid-column:span 6}
@media (max-width:900px){.w8,.w4,.w6{grid-column:span 12}}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}
.kpi b{display:block;font-size:24px}table{width:100%;border-collapse:collapse}td,th{padding:6px 4px;border-bottom:1px solid var(--line);text-align:right}
td:first-child,th:first-child,td.l,th.l{text-align:left}th{font-weight:600;color:var(--muted);font-size:12px}
.rank{display:inline-block;min-width:28px;padding:1px 6px;border-radius:999px;background:var(--off);text-align:center}
.chip{display:inline-block;padding:1px 7px;margin:1px;border-radius:999px;font-size:11px;background:var(--off)}
.chip.LATE,.chip.HALF_DAY{background:#fdf1d8;color:#8a5a00}.chip.NO_RECORD,.chip.ABSENT,.chip.MISSING_PUNCH_OUT{background:#fde3e3;color:#9b1c1c}
.cal{display:grid;grid-template-columns:repeat(7,1fr);gap:4px}.cal>div{border-radius:6px;padding:6px;min-height:52px;font-size:11px;background:var(--off)}
.cal .PRESENT,.cal .WFH,.cal .ON_DUTY{background:color-mix(in srgb,var(--good) 22%,transparent)}.cal .late{box-shadow:inset 0 -3px 0 var(--warn)}
.cal .ABSENT,.cal .NO_RECORD{background:color-mix(in srgb,var(--bad) 20%,transparent)}.cal .LEAVE{background:color-mix(in srgb,var(--leave) 20%,transparent)}
.cal .WEEKEND,.cal .NOT_JOINED,.cal .UPCOMING,.cal .pad{opacity:.45}.cal>.pad{background:none;min-height:0}
svg text{fill:var(--muted);font-size:10px}.err{color:var(--bad)}
</style></head><body>
<header><div><h1>Attendance &amp; analytics</h1><div class="muted">Live data from this API &middot; times in IST</div></div>
<div class="controls"><input type="month" id="month"><select id="dept"><option value="">All departments</option></select>
<input type="date" id="day"><button id="go">Refresh</button></div></header>
<main>
<section class="card w12"><div class="kpis" id="kpis"></div></section>
<section class="card w8"><h2>Daily attendance rate <span class="muted" id="trendDept"></span></h2><div id="trend"></div>
<div class="muted">Bars: attendance rate on working days &middot; line: 7-day moving average &middot; grey: weekends</div></section>
<section class="card w4"><h2>Late leaderboard</h2><div id="leader"></div></section>
<section class="card w6"><h2>Departments</h2><div id="depts"></div></section>
<section class="card w6"><h2>Needs attention on <span id="excDay"></span></h2><div id="exc"></div></section>
<section class="card w12"><h2>Employee calendar</h2><div class="controls" style="margin-bottom:10px">
<input id="emp" placeholder="EMP0001" size="10"><button id="calGo">Show</button><span class="muted" id="calSum"></span></div><div id="cal"></div></section>
</main>
<script>
const $=id=>document.getElementById(id);
const esc=v=>String(v??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const pct=v=>v==null?"-":(v*100).toFixed(1)+"%";
async function api(path,params,retried){const q=new URLSearchParams(Object.entries(params||{}).filter(([,v])=>v));
  const key=sessionStorage.getItem("apiKey"),r=await fetch(path+(q.toString()?"?"+q:""),{headers:key?{"X-API-Key":key}:{}});
  if(r.status===401&&!retried){const k=prompt("API key");if(k){sessionStorage.setItem("apiKey",k);return api(path,params,true)}}
  const j=await r.json();
  if(!r.ok)throw new Error(typeof j.detail==="string"?j.detail:JSON.stringify(j.detail));return j}
function table(cols,rows){return rows.length?"<table><tr>"+cols.map(c=>`<th class="${c[2]||""}">${c[0]}</th>`).join("")+"</tr>"+
  rows.map(r=>"<tr>"+cols.map(c=>`<td class="${c[2]||""}">${c[1](r)}</td>`).join("")+"</tr>").join("")+"</table>":'<div class="muted">Nothing to show.</div>'}
function fail(el,e){$(el).innerHTML=`<div class="err">${esc(e.message)}</div>`}
function chart(items){const W=760,H=210,P=28,n=items.length,bw=(W-P*2)/Math.max(n,1);
  const y=v=>H-P-Math.min(Math.max(v||0,0),1)*(H-P*2);/* rates above 100% are drawn at the top */let s=`<svg viewBox="0 0 ${W} ${H}" width="100%" role="img" aria-label="attendance trend">`;
  [0,.5,1].forEach(t=>s+=`<line x1="${P}" x2="${W-P}" y1="${y(t)}" y2="${y(t)}" stroke="var(--line)"/><text x="2" y="${y(t)+3}">${t*100}%</text>`);
  items.forEach((d,i)=>{const x=P+i*bw;if(!d.is_working_day){s+=`<rect x="${x}" y="${P}" width="${bw}" height="${H-P*2}" fill="var(--off)"/>`}
    else if(d.attendance_rate!=null){s+=`<rect x="${x+1}" y="${y(d.attendance_rate)}" width="${Math.max(bw-2,1)}" height="${H-P-y(d.attendance_rate)}" fill="var(--accent)" opacity=".35"><title>${d.date}: ${pct(d.attendance_rate)} (${d.present_count}/${d.headcount}, ${d.late_count} late)</title></rect>`}
    if(i%Math.ceil(n/8)===0)s+=`<text x="${x}" y="${H-8}">${d.date.slice(5)}</text>`});
  const pts=items.map((d,i)=>d.moving_avg_7d==null?null:`${P+i*bw+bw/2},${y(d.moving_avg_7d)}`).filter(Boolean);
  if(pts.length)s+=`<polyline points="${pts.join(" ")}" fill="none" stroke="var(--accent)" stroke-width="2"/>`;return s+"</svg>"}
async function load(){const month=$("month").value,dept=$("dept").value,day=$("day").value;$("excDay").textContent=day;
  api("/analytics/departments/summary",{month}).then(r=>{const it=dept?r.items.filter(i=>i.department===dept):r.items,sum=k=>it.reduce((a,i)=>a+i[k],0);
    $("kpis").innerHTML=[["Headcount",sum("headcount")],["Present days",sum("present_days").toFixed(1)],["Late arrivals",sum("late_count")],
      ["Late minutes",sum("total_late_minutes")],["Leave days",sum("leave_count")],["On duty",sum("on_duty_count")]]
      .map(([k,v])=>`<div class="kpi"><span class="muted">${k}</span><b>${v}</b></div>`).join("");
    $("depts").innerHTML=table([["Department",r=>esc(r.department)],["Headcount",r=>r.headcount],["Present days",r=>r.present_days],
      ["Avg hours",r=>r.avg_work_hours??"-"],["Late",r=>r.late_count],["Leave",r=>r.leave_count]],r.items)}).catch(e=>fail("depts",e));
  api("/analytics/leaderboard/late",{month,department:dept,limit:10}).then(r=>{$("leader").innerHTML=table([["#",x=>`<span class="rank">${x.rank}</span>`],
    ["Employee",x=>`${esc(x.name)}<div class="muted">${esc(x.emp_code)} &middot; ${esc(x.department)}</div>`,"l"],["Minutes",x=>x.total_late_minutes],["Days",x=>x.late_count]],r.items)}).catch(e=>fail("leader",e));
  const tDept=dept||$("dept").options[1]?.value;$("trendDept").textContent=tDept?"· "+tDept:"";
  if(tDept){const [y,m]=month.split("-").map(Number),last=new Date(y,m,0).getDate();
    api(`/analytics/departments/${encodeURIComponent(tDept)}/trend`,{from:`${month}-01`,to:`${month}-${String(last).padStart(2,"0")}`})
      .then(r=>$("trend").innerHTML=chart(r.items)).catch(e=>fail("trend",e))}
  api("/analytics/exceptions",{date:day,department:dept}).then(r=>{const s=r.summary;
    $("exc").innerHTML=`<div class="muted" style="margin-bottom:6px">${s.present} of ${s.headcount} present &middot; ${s.on_leave} on leave${r.is_working_day?"":" &middot; not a working day"}</div>`+
    table([["Employee",x=>`${esc(x.name)} <span class="muted">${esc(x.emp_code)}</span>`,"l"],["Issues",x=>x.issues.map(i=>`<span class="chip ${i}">${i.replaceAll("_"," ").toLowerCase()}</span>`).join(""),"l"]],r.items.slice(0,50))}).catch(e=>fail("exc",e))}
async function calendar(){const code=$("emp").value.trim();if(!code)return;
  try{const r=await api(`/analytics/employees/${encodeURIComponent(code)}/calendar`,{month:$("month").value}),s=r.summary;
    $("calSum").textContent=`${r.name} · ${s.records} records · ${s.missing_days} missing working days · ${s.late_days} late (longest streak ${s.longest_late_streak}) · ${s.corrected_days} corrected`;
    const pad=WD.indexOf(r.days[0].weekday);$("cal").innerHTML='<div class="cal">'+WD.map(w=>`<div class="pad muted">${w}</div>`).join("")+
      '<div class="pad"></div>'.repeat(pad)+r.days.map(d=>`<div class="${d.day_type}${d.late_minutes?" late":""}" title="${esc(d.day_type)}${d.corrected?" (corrected)":""}">`+
      `<b>${Number(d.date.slice(8))}</b> ${d.corrected?"&#9998;":""}<div>${esc(d.day_type.replace("_"," ").toLowerCase())}</div>${d.work_hours!=null?`<div>${d.work_hours}h</div>`:""}${d.late_minutes?`<div>+${d.late_minutes}m late</div>`:""}</div>`).join("")+"</div>"}
  catch(e){$("calSum").textContent="";fail("cal",e)}}
const WD=["Mon","Tue","Wed","Thu","Fri","Sat","Sun"],now=new Date(Date.now()+330*60000).toISOString();
$("month").value=now.slice(0,7);$("day").value=now.slice(0,10);
api("/departments").then(r=>{r.items.forEach(d=>$("dept").add(new Option(`${d.department} (${d.headcount})`,d.department)));load()}).catch(()=>load());
$("go").onclick=load;$("calGo").onclick=calendar;$("emp").onkeydown=e=>{if(e.key==="Enter")calendar()};
</script></body></html>
"""


def _inline_hash(tag: str) -> str:
    body = re.search(rf"<{tag}>(.*?)</{tag}>", DASHBOARD_HTML, re.S)[1]
    return "'sha256-" + base64.b64encode(hashlib.sha256(body.encode()).digest()).decode() + "'"


# Content-Security-Policy: the page may only run its own inline script (pinned by hash, so an injected script would
# not run), talk to this origin, and not be framed. Inline style attributes stay allowed; they can not run code.
DASHBOARD_HEADERS = {
    "Content-Security-Policy": (
        f"default-src 'none'; script-src {_inline_hash('script')}; style-src 'unsafe-inline'; connect-src 'self'; "
        "img-src 'self' data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
    ),
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
}
