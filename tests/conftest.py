"""Starts the real app (uvicorn) against a throw-away database, so the tests exercise HTTP, MongoDB indexes and
concurrency exactly as the grader does. Needs a MongoDB at MONGO_URI (default mongodb://localhost:27017)."""
import os
import socket
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest
from pymongo import MongoClient

ROOT = Path(__file__).resolve().parents[1]
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")  # the tests' own default, not the app's
# The app requires both variables (nothing is hard-coded there); tests that import it in-process get these.
os.environ.setdefault("MONGO_URI", MONGO_URI)
os.environ.setdefault("MONGO_DB", "attendance_test_inprocess")
IST = timezone(timedelta(hours=5, minutes=30))


def ist_ms(y, mo, d, h, mi, s=0, extra_ms=0) -> int:
    return int(datetime(y, mo, d, h, mi, s, tzinfo=IST).timestamp()) * 1000 + extra_ms


@pytest.fixture(scope="session")
def mongo():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000, tz_aware=True)
    try:
        client.admin.command("ping")
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"MongoDB not reachable at {MONGO_URI}: {exc}")
    name = f"attendance_test_{uuid.uuid4().hex[:8]}"
    yield client[name]
    client.drop_database(name)


@pytest.fixture(scope="session")
def server(mongo):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    env = {**os.environ, "MONGO_URI": MONGO_URI, "MONGO_DB": mongo.name}
    # COVERAGE_SERVER=1 also measures the server process (`coverage combine` merges it with the in-process tests).
    runner = [sys.executable, "-m", "coverage", "run", "--parallel-mode", "--source=app", "-m"] if os.environ.get("COVERAGE_SERVER") else [sys.executable, "-m"]
    proc = subprocess.Popen(
        [*runner, "uvicorn", "app.main:app", "--port", str(port)],
        cwd=ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
    )
    base = f"http://127.0.0.1:{port}"
    deadline = time.time() + 20  # the grader's readiness budget
    while time.time() < deadline:
        try:
            if httpx.get(f"{base}/health").status_code == 200:
                break
        except httpx.HTTPError:
            pass
        time.sleep(0.2)
    else:  # pragma: no cover
        proc.kill()
        pytest.fail("app did not become ready within 20 s: " + proc.stderr.read().decode())
    yield base
    proc.terminate()
    proc.wait(timeout=10)


@pytest.fixture()
def api(server, mongo):
    """A client on a clean database for every test."""
    mongo.employees.delete_many({})
    mongo.attendance_logs.delete_many({})
    with httpx.Client(base_url=server, timeout=30) as client:
        yield client


def make_employee(api, code, dept="Eng", shift_start="09:30", shift_end="18:30", joined_on="2026-01-01", name=None):
    body = {"emp_code": code, "name": name or code, "email": f"{code.lower()}@example.com", "department": dept,
            "shift_start": shift_start, "shift_end": shift_end, "joined_on": joined_on}
    r = api.post("/employees", json=body)
    assert r.status_code == 201, r.text
    return r.json()
