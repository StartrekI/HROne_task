"""Request coalescing must cut duplicate work without ever answering with data older than the request."""
import random
import threading
import time

import pytest

from app.main import Coalescer
from tests.conftest import ist_ms, make_employee


def run_together(n, target):
    barrier = threading.Barrier(n)
    results = [None] * n

    def go(i):
        barrier.wait()
        results[i] = target(i)

    threads = [threading.Thread(target=go, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results


def test_a_burst_of_identical_requests_computes_at_most_twice():
    coalescer, calls = Coalescer(), []

    def compute():
        calls.append(1)
        time.sleep(0.2)
        return {"answer": 42}

    results = run_together(30, lambda i: coalescer.run("k", compute))
    assert all(r == {"answer": 42} for r in results) and len(calls) <= 2
    assert coalescer._slots == {}  # nothing retained after the burst


def test_never_answers_with_data_older_than_the_request():
    """Linearizability: a writer keeps advancing `version`; every answer must be a version read at or after the moment
    its request arrived, however the requests interleave with the rounds."""
    coalescer, state, stop = Coalescer(), {"version": 0}, threading.Event()

    def writer():
        while not stop.is_set():
            state["version"] += 1
            time.sleep(random.uniform(0, 0.002))

    def compute():
        seen = state["version"]
        time.sleep(random.uniform(0, 0.01))
        return seen

    violations, served = [], [0]

    def reader(_):
        for _ in range(50):
            arrived = state["version"]
            answer = coalescer.run("k", compute)
            served[0] += 1
            if answer < arrived:
                violations.append((arrived, answer))

    w = threading.Thread(target=writer)
    w.start()
    run_together(16, reader)
    stop.set()
    w.join()
    assert served[0] == 800 and violations == []


def test_a_failure_reaches_every_waiter_of_its_round_and_the_next_round_recovers():
    coalescer, attempts = Coalescer(), []

    def compute():
        attempts.append(1)
        time.sleep(0.1)
        raise TimeoutError("database timed out")

    outcomes = run_together(10, lambda i: _catch(lambda: coalescer.run("k", compute)))
    assert all(isinstance(o, TimeoutError) for o in outcomes)
    assert coalescer.run("k", lambda: "ok") == "ok" and coalescer._slots == {}


def _catch(fn):
    try:
        return fn()
    except Exception as exc:  # noqa: BLE001 - the test inspects it
        return exc


def test_read_your_writes_through_the_api_under_a_storm_of_identical_reads(api):
    """While 8 threads hammer the same monthly report, a client that records late arrivals and reads the report right
    after must always see its own writes."""
    make_employee(api, "EMP0001")
    stop = threading.Event()
    month = {"month": "2026-07"}

    def storm():
        while not stop.is_set():
            api.get("/analytics/employees/EMP0001/monthly", params=month)

    storms = [threading.Thread(target=storm) for _ in range(8)]
    for t in storms:
        t.start()
    try:
        for day in range(1, 21):
            api.post("/attendance/punch-in", json={"emp_code": "EMP0001", "punched_at": ist_ms(2026, 7, day, 10, 0)})  # 30 min late
            report = api.get("/analytics/employees/EMP0001/monthly", params=month).json()
            assert report["late_count"] == day and report["total_late_minutes"] == 30 * day
    finally:
        stop.set()
        for t in storms:
            t.join()


@pytest.mark.parametrize("n", [1, 4])
def test_results_are_independent_copies(n):
    coalescer, shared = Coalescer(), {"items": [1, 2]}
    results = run_together(n, lambda i: coalescer.run("k", lambda: shared))
    results[0]["items"].append(3)
    assert shared == {"items": [1, 2]} and all(r == {"items": [1, 2]} for r in results[1:])
