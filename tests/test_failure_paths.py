"""Every failure path the service has, executed on purpose (a coverage run showed these were the untested lines).

In-process tests drive the real ASGI app, middleware included, and inject the failure with monkeypatch; startup
scenarios start a real server against a prepared database."""
import os
import socket
import subprocess
import sys
import time
from datetime import timedelta

import anyio
import httpx
import pytest
from pymongo.errors import AutoReconnect, ExecutionTimeout, OperationFailure, WriteConcernError

import app.main as main
from tests.conftest import MONGO_URI, ROOT, ist_ms, make_employee


@pytest.fixture()
def inproc():
    """The real app (middleware, handlers) without a network hop. raise_app_exceptions=False: we assert on responses."""
    transport = httpx.ASGITransport(app=main.app, raise_app_exceptions=False)
    return httpx.AsyncClient(transport=transport, base_url="http://app")


def boom(exc):
    def raise_it(*_args, **_kwargs):
        raise exc
    return raise_it


@pytest.mark.anyio
@pytest.mark.parametrize("exc, status, retry_after", [
    (RuntimeError("a bug nobody anticipated"), 500, None),          # middleware catch-all
    (ExecutionTimeout("operation exceeded time limit", 50), 504, None),
    (AutoReconnect("connection reset by peer"), 503, "1"),           # ConnectionFailure
    (WriteConcernError("Replication is being shut down", 91, {"code": 91, "errorLabels": ["RetryableWriteError"]}), 503, "1"),
    (OperationFailure("bad value", 2, {"code": 2}), 500, None),      # a real failed operation
])
async def test_errors_map_to_the_right_status(inproc, monkeypatch, exc, status, retry_after):
    monkeypatch.setattr(main, "run_pipeline", boom(exc))
    async with inproc as client:
        r = await client.get("/departments", headers={"X-Request-ID": "trace-42"})
        assert r.status_code == status and r.headers.get("retry-after") == retry_after
        assert r.headers["x-request-id"] == "trace-42" and "detail" in r.json()
        if status == 500 and isinstance(exc, RuntimeError):
            assert r.json() == {"detail": "internal error", "request_id": "trace-42"}  # no internals leak
        assert (await client.get("/livez")).status_code == 200  # the app keeps serving


@pytest.mark.anyio
async def test_health_is_503_when_mongodb_does_not_answer(inproc, monkeypatch):
    class Unreachable:  # client.admin builds a new Database object on every access, so replace the client itself
        class admin:
            command = staticmethod(boom(AutoReconnect("down")))

    monkeypatch.setattr(main, "client", Unreachable)
    async with inproc as client:
        r = await client.get("/health")
    assert r.status_code == 503 and r.headers["retry-after"] == "1"


@pytest.mark.anyio
async def test_slow_requests_are_logged_with_their_request_id(inproc, monkeypatch, caplog):
    monkeypatch.setattr(main, "SLOW_REQUEST_MS", -1.0)
    async with inproc as client:
        await client.get("/livez", headers={"X-Request-ID": "slow-1"})
    assert any("slow request" in m and "slow-1" in m for m in caplog.messages)


def test_punch_out_gives_up_after_bounded_retries(monkeypatch):
    """If the record changes under every attempt, punch-out answers 409 after CAS_ATTEMPTS instead of looping."""
    punch_in = main.ms_to_dt(ist_ms(2026, 7, 6, 9, 0))
    attempts = []

    class RecordThatKeepsChanging:
        def find_one(self, *args, **kwargs):
            return {"_id": 1, "emp_code": "EMP0001", "date": "2026-07-06", "status": "PRESENT", "punch_in": punch_in, "punch_out": None}

        def find_one_and_update(self, *args, **kwargs):
            attempts.append(1)
            return None  # a concurrent writer won every time

    monkeypatch.setattr(main, "logs", RecordThatKeepsChanging())
    monkeypatch.setattr(main, "get_employee", lambda code: {"emp_code": code, "shift_start": "09:30", "shift_end": "18:30"})
    with pytest.raises(main.HTTPException) as caught:
        main.punch_out_op(main.PunchOut(emp_code="EMP0001", punched_at=ist_ms(2026, 7, 6, 18, 0)))
    assert caught.value.status_code == 409 and len(attempts) == main.CAS_ATTEMPTS


def test_idempotency_key_in_progress_and_crash_takeover(api, mongo):
    make_employee(api, "EMP0001")
    body = {"emp_code": "EMP0001", "punched_at": ist_ms(2026, 7, 6, 9, 0)}
    fingerprint = main.hashlib.sha256(main.json.dumps(body, sort_keys=True, default=str).encode()).hexdigest()
    key_id = "POST /attendance/punch-in k-busy"
    mongo.idempotency_keys.insert_one({"_id": key_id, "fingerprint": fingerprint, "state": "pending", "created_at": main.now_utc()})
    r = api.post("/attendance/punch-in", json=body, headers={"Idempotency-Key": "k-busy"})
    assert r.status_code == 409  # the first request is still running
    # A claim older than STALE_PENDING belongs to a process that died: the next retry takes it over and runs.
    mongo.idempotency_keys.update_one({"_id": key_id}, {"$set": {"created_at": main.now_utc() - timedelta(minutes=5)}})
    r = api.post("/attendance/punch-in", json=body, headers={"Idempotency-Key": "k-busy"})
    assert r.status_code == 201
    assert mongo.idempotency_keys.find_one({"_id": key_id})["state"] == "done"


def test_contract_rules_without_a_test_before(api, mongo):
    make_employee(api, "EMP0001", dept="Eng")
    api.post("/attendance/punch-in", json={"emp_code": "EMP0001", "punched_at": ist_ms(2026, 7, 6, 9, 0)})
    fix = {"reason": "fixing it", "regularized_by": "hr"}
    assert api.patch("/attendance/EMP0001/2026-07-06", json={**fix, "status": "ABSENT"}).status_code == 200
    # A presence status needs a punch_in, supplied or stored; ABSENT cleared it.
    assert api.patch("/attendance/EMP0001/2026-07-06", json={**fix, "status": "WFH"}).status_code == 422
    trend = "/analytics/departments/Eng/trend"
    assert api.get(trend, params={"from": "2026-07-01", "to": "2026-09-30"}).status_code == 200  # 92 days
    assert api.get(trend, params={"from": "2026-07-01", "to": "2026-10-01"}).status_code == 422  # 93 days
    r = api.get("/analytics/exceptions", params={"date": "2026-07-06", "department": "Nope"}).json()
    assert r["summary"]["headcount"] == 0 and r["items"] == []


def start_server(db_name: str, **env):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    proc = subprocess.Popen([sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(port)], cwd=ROOT,
                            env={**os.environ, "MONGO_URI": MONGO_URI, "MONGO_DB": db_name, **env},
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    started = time.time()
    while time.time() - started < 20:
        try:
            if httpx.get(f"http://127.0.0.1:{port}/health").status_code == 200:
                return proc, port, time.time() - started
        except httpx.HTTPError:
            pass
        time.sleep(0.1)
    proc.kill()
    pytest.fail("not ready within 20 s")


def test_startup_survives_duplicate_seeded_data(mongo):
    """A unique index that can not be built (duplicates in seeded data) is logged; the app still becomes ready."""
    db = mongo.client[mongo.name + "_dup"]
    db.attendance_logs.insert_many([{"emp_code": "EMP0001", "date": "2026-07-01", "status": "ABSENT"} for _ in range(2)])
    proc, port, _ = start_server(db.name)
    try:
        names = [i["name"] for i in db.attendance_logs.list_indexes()]
        assert "uniq_emp_code_date" not in names and "date_emp_code_covering" in names  # the others were still built
        assert httpx.get(f"http://127.0.0.1:{port}/attendance").json()["total"] == 2
    finally:
        proc.terminate()
        proc.wait(10)
        mongo.client.drop_database(db.name)


def test_startup_budget_serves_before_secondary_indexes_finish(mongo):
    db = mongo.client[mongo.name + "_budget"]
    db.attendance_logs.insert_one({"emp_code": "EMP0001", "date": "2026-07-01", "status": "ABSENT"})
    proc, port, _ = start_server(db.name, STARTUP_INDEX_BUDGET_S="0")
    try:
        assert "uniq_emp_code_date" in [i["name"] for i in db.attendance_logs.list_indexes()]  # always before ready
        deadline = time.time() + 10
        while len(list(db.attendance_logs.list_indexes())) < 6 and time.time() < deadline:
            time.sleep(0.1)
        assert len(list(db.attendance_logs.list_indexes())) == 6  # the rest finished in the background
    finally:
        proc.terminate()
        proc.wait(10)
        mongo.client.drop_database(db.name)


@pytest.mark.anyio
async def test_circuit_breaker_fails_fast_during_an_outage_and_closes_on_recovery(inproc, monkeypatch):
    breaker = main.CircuitBreaker(threshold=3, cooldown_s=0.3)
    monkeypatch.setattr(main, "BREAKER", breaker)
    calls = []

    def down(*_args, **_kwargs):
        calls.append(1)
        raise AutoReconnect("connection refused")

    monkeypatch.setattr(main, "run_pipeline", down)
    async with inproc as client:
        for _ in range(3):  # three real attempts reach the database and fail
            assert (await client.get("/departments")).status_code == 503
        assert len(calls) == 3 and not breaker.allow()
        r = await client.get("/departments")  # open: answered at once, the database is not touched
        assert r.status_code == 503 and r.headers["retry-after"] == "1" and len(calls) == 3
        assert (await client.get("/livez")).status_code == 200  # probes are never blocked
        await anyio.sleep(0.35)  # cool-down over: requests go through again
        monkeypatch.setattr(main, "run_pipeline", lambda *a, **k: [{"department": "Eng", "headcount": 1}])
        breaker.record_success()  # what PyMongo's command monitoring reports when a command succeeds
        assert (await client.get("/departments")).status_code == 200 and breaker.allow() and breaker.failures == 0


def test_circuit_breaker_hears_every_successful_mongodb_command():
    breaker = main.CircuitBreaker(threshold=1, cooldown_s=60)
    breaker.record_failure()
    assert not breaker.allow()
    breaker.succeeded(object())  # the CommandListener callback
    assert breaker.allow() and breaker.opened_at is None


@pytest.mark.anyio
async def test_one_failure_shared_by_coalesced_requests_counts_once(inproc, monkeypatch):
    """Found by review: a failed coalesced round re-raises the same exception in every waiter; counting each one
    opened the breaker after a single blip."""
    import asyncio

    breaker = main.CircuitBreaker(threshold=5, cooldown_s=1)
    monkeypatch.setattr(main, "BREAKER", breaker)
    blip = AutoReconnect("one primary step-down")

    def failing_round():
        time.sleep(0.2)
        raise blip

    coalescer = main.Coalescer()
    monkeypatch.setattr(main, "run_pipeline", lambda coll, pipeline: coalescer.run("k", failing_round))
    async with inproc as client:
        responses = await asyncio.gather(*[client.get("/departments") for _ in range(8)])
    assert {r.status_code for r in responses} == {503} and breaker.failures == 1 and breaker.allow()


def test_a_changed_idempotency_ttl_is_applied_on_restart(mongo):
    """create_index fails with IndexOptionsConflict when only expireAfterSeconds changed; startup applies the new TTL
    with collMod instead of leaving the old one (found uncovered by a coverage run)."""
    db = mongo.client[mongo.name + "_ttl"]
    db.idempotency_keys.create_index("created_at", name="ttl_created_at", expireAfterSeconds=86400)
    proc, _, _ = start_server(db.name, IDEMPOTENCY_TTL_S="600")
    try:
        ttl = next(i for i in db.idempotency_keys.list_indexes() if i["name"] == "ttl_created_at")["expireAfterSeconds"]
        assert ttl == 600
    finally:
        proc.terminate()
        proc.wait(10)
        mongo.client.drop_database(db.name)


def test_client_address_uses_x_forwarded_for_only_behind_a_trusted_proxy(monkeypatch):
    scope, headers = {"client": ("10.0.0.9", 5000)}, {b"x-forwarded-for": b"203.0.113.7, 10.0.0.1"}
    assert main.client_address(scope, headers) == "10.0.0.9"  # not trusted: a client could claim any address
    monkeypatch.setattr(main, "TRUST_PROXY", True)
    assert main.client_address(scope, headers) == "203.0.113.7"


def test_report_cache_keeps_a_bounded_number_of_entries(monkeypatch):
    monkeypatch.setattr(main, "CACHE_MAX_ENTRIES", 2)
    cache = main.ReportCache(main.db)
    cache.running, cache.global_change = True, (0, 0)
    for i in range(3):
        cache.store(f"k{i}", [i], (1, 0), ["2026-07"])
    assert list(cache.entries) == ["k1", "k2"]  # the least recently used one was evicted


def test_connection_details_are_never_hard_coded(tmp_path):
    """Checklist: "MONGO_URI and MONGO_DB are read from the environment; nothing is hard-coded". Without them the app
    refuses to start with a clear message; a database named in the URI is used when MONGO_DB is not set."""
    if (ROOT / ".env").exists():
        pytest.skip("a local .env would supply the variables")
    base = {"PATH": os.environ.get("PATH", ""), "PYTHONPATH": str(ROOT)}
    probe = "import app.main as m; print(m.MONGO_DB)"
    r = subprocess.run([sys.executable, "-c", probe], cwd=tmp_path, env=base, capture_output=True, text=True)
    assert r.returncode != 0 and "MONGO_URI is not set" in r.stderr
    r = subprocess.run([sys.executable, "-c", probe], cwd=tmp_path, env={**base, "MONGO_URI": "mongodb://localhost:27017"},
                       capture_output=True, text=True)
    assert r.returncode != 0 and "MONGO_DB is not set" in r.stderr
    r = subprocess.run([sys.executable, "-c", probe], cwd=tmp_path, env={**base, "MONGO_URI": "mongodb://localhost:27017/from_uri"},
                       capture_output=True, text=True)
    assert r.returncode == 0 and r.stdout.strip() == "from_uri"
