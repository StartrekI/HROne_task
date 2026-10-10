"""Idempotency-Key: a retried write gets the original answer instead of a misleading 409 / 422."""
import json
import threading

import pytest

from tests.conftest import ist_ms, make_employee


def punch(api, key, **body):
    return api.post("/attendance/punch-in", json={"emp_code": "EMP0001", **body}, headers={"Idempotency-Key": key})


def test_retried_punch_in_replays_the_original_answer(api, mongo):
    make_employee(api, "EMP0001")
    first = punch(api, "k-1")  # no punched_at: the server clock decides, so a re-run would differ
    retry = punch(api, "k-1")
    assert first.status_code == retry.status_code == 201
    assert retry.json() == first.json() and retry.headers["idempotent-replayed"] == "true"
    assert mongo.attendance_logs.count_documents({}) == 1
    assert api.post("/attendance/punch-in", json={"emp_code": "EMP0001"}).status_code == 409  # without a key: unchanged


def test_same_key_with_another_payload_is_rejected(api):
    make_employee(api, "EMP0001")
    assert punch(api, "k-2", punched_at=ist_ms(2026, 7, 6, 9, 0)).status_code == 201
    assert punch(api, "k-2", punched_at=ist_ms(2026, 7, 7, 9, 0)).status_code == 422


def test_retried_correction_does_not_turn_into_changes_nothing(api, mongo):
    make_employee(api, "EMP0001")
    api.post("/attendance/punch-in", json={"emp_code": "EMP0001", "punched_at": ist_ms(2026, 7, 6, 10, 5)})
    body = {"punch_in": ist_ms(2026, 7, 6, 9, 28), "reason": "biometric glitch", "regularized_by": "hr"}
    first = api.patch("/attendance/EMP0001/2026-07-06", json=body, headers={"Idempotency-Key": "fix-1"})
    retry = api.patch("/attendance/EMP0001/2026-07-06", json=body, headers={"Idempotency-Key": "fix-1"})
    assert first.status_code == retry.status_code == 200 and retry.json() == first.json()
    assert len(mongo.attendance_logs.find_one({})["history"]) == 1
    assert api.patch("/attendance/EMP0001/2026-07-06", json=body).status_code == 422  # without a key: unchanged


def test_a_failed_request_releases_its_key(api):
    assert punch(api, "k-3", punched_at=ist_ms(2026, 7, 6, 9, 0)).status_code == 404  # employee does not exist yet
    make_employee(api, "EMP0001")
    assert punch(api, "k-3", punched_at=ist_ms(2026, 7, 6, 9, 0)).status_code == 201


def test_concurrent_requests_with_one_key_write_once(api, mongo):
    make_employee(api, "EMP0001")
    results, barrier = [], threading.Barrier(8)

    def go():
        barrier.wait()
        r = punch(api, "k-4", punched_at=ist_ms(2026, 7, 6, 9, 0))
        results.append((r.status_code, r.json() if r.status_code == 201 else None))

    threads = [threading.Thread(target=go) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert {code for code, _ in results} <= {201, 409} and any(code == 201 for code, _ in results)
    assert len({str(body) for code, body in results if code == 201}) == 1  # every 201 carries the same record
    assert mongo.attendance_logs.count_documents({}) == 1


def test_invalid_key_is_rejected(api):
    make_employee(api, "EMP0001")
    assert punch(api, "has spaces", punched_at=ist_ms(2026, 7, 6, 9, 0)).status_code == 422


def test_a_write_and_its_stored_answer_commit_together(mongo, monkeypatch):
    """Found by the failover drill (scripts/failover_drill.py): an election between the write and the storing of its
    answer left the write without an answer, so the client's retry failed with 409. On a replica set both now commit
    in one transaction: a failure in between leaves nothing behind and the retry succeeds."""
    from pymongo.errors import AutoReconnect

    import app.main as main

    if not main.transactions_supported():
        pytest.skip("standalone server: no transactions, and no elections either")
    db = main.client[mongo.name]  # a session only works with collections of the client that started it

    class ElectionAfterTheWrite:
        def __init__(self, coll):
            self.coll, self.failures = coll, 1

        def __getattr__(self, name):
            return getattr(self.coll, name)

        def replace_one(self, *args, **kwargs):
            if self.failures:
                self.failures -= 1
                raise AutoReconnect("primary stepped down")
            return self.coll.replace_one(*args, **kwargs)

    monkeypatch.setattr(main, "employees", db.employees)
    monkeypatch.setattr(main, "logs", db.attendance_logs)
    monkeypatch.setattr(main, "idempotency", ElectionAfterTheWrite(db.idempotency_keys))
    db.employees.insert_one({"emp_code": "EMP0042", "name": "N", "department": "Ops", "shift_start": "09:30", "shift_end": "18:30"})
    body = main.PunchIn(emp_code="EMP0042", punched_at=ist_ms(2026, 7, 6, 9, 0))
    run = lambda: main.idempotent("k-tx", "POST /attendance/punch-in", body.model_dump(), 201, lambda: main.punch_in_op(body))  # noqa: E731

    with pytest.raises(AutoReconnect):
        run()
    assert db.attendance_logs.count_documents({"emp_code": "EMP0042"}) == 0  # the punch-in was rolled back with it
    assert db.idempotency_keys.count_documents({"_id": {"$regex": "k-tx$"}}) == 0
    first = run()  # the client's retry
    assert first["emp_code"] == "EMP0042" and db.attendance_logs.count_documents({"emp_code": "EMP0042"}) == 1
    replay = run()
    assert replay.status_code == 201 and replay.headers["idempotent-replayed"] == "true"


class FailAfter:
    """Lets a write happen, then raises as if the connection broke before the reply came back."""

    def __init__(self, coll, method):
        self.coll, self.method, self.failures = coll, method, 1

    def __getattr__(self, name):
        attr = getattr(self.coll, name)
        if name != self.method:
            return attr

        def call(*args, **kwargs):
            result = attr(*args, **kwargs)
            if self.failures:
                self.failures -= 1
                from pymongo.errors import AutoReconnect
                raise AutoReconnect("connection reset after the write")
            return result
        return call


@pytest.fixture()
def claimed(mongo, monkeypatch):
    """The path without transactions (a standalone server), forced so it also runs on a replica set."""
    import app.main as main

    monkeypatch.setattr(main, "_transactions", False)
    db = main.client[mongo.name]
    for name in ("employees", "attendance_logs", "idempotency_keys"):
        db[name].delete_many({})
    main.ensure_indexes()  # the partial stamp index
    monkeypatch.setattr(main, "employees", db.employees)
    monkeypatch.setattr(main, "logs", db.attendance_logs)
    monkeypatch.setattr(main, "idempotency", db.idempotency_keys)
    db.employees.insert_one({"emp_code": "EMP0042", "name": "N", "department": "Ops", "shift_start": "09:30", "shift_end": "18:30"})
    return main, db


def keyed(main, key, scope, body, status, op):
    return main.idempotent(key, scope, body.model_dump(exclude_unset=True), status, op)


def no_stamps_left(db):
    return all(db[c].count_documents({"_idem.k": {"$exists": True}}) == 0 for c in ("employees", "attendance_logs"))


def test_standalone_reply_lost_after_each_kind_of_write(claimed, monkeypatch):
    """The write commits, the reply is lost: the same request answers with the committed result (no 503, no 409)."""
    main, db = claimed
    punch_in = main.PunchIn(emp_code="EMP0042", punched_at=ist_ms(2026, 7, 6, 9, 50))
    monkeypatch.setattr(main, "logs", FailAfter(db.attendance_logs, "insert_one"))
    got = keyed(main, "a", "POST /attendance/punch-in", punch_in, 201, lambda: main.punch_in_op(punch_in))
    assert got["punch_in"] == ist_ms(2026, 7, 6, 9, 50) and got["late_minutes"] == 20

    punch_out = main.PunchOut(emp_code="EMP0042", punched_at=ist_ms(2026, 7, 6, 19, 5))
    monkeypatch.setattr(main, "logs", FailAfter(db.attendance_logs, "find_one_and_update"))
    got = keyed(main, "b", "POST /attendance/punch-out", punch_out, 200, lambda: main.punch_out_op(punch_out))
    assert got["punch_out"] == ist_ms(2026, 7, 6, 19, 5) and got["overtime_minutes"] == 35

    fix = main.Regularize(punch_in=ist_ms(2026, 7, 6, 9, 20), reason="glitch", regularized_by="hr")
    monkeypatch.setattr(main, "logs", FailAfter(db.attendance_logs, "find_one_and_update"))
    got = keyed(main, "c", "PATCH /attendance", fix, 200, lambda: main.regularize_op("EMP0042", "2026-07-06", fix))
    assert got["late_minutes"] == 0 and len(got["history"]) == 1
    assert got == main.record_out(db.attendance_logs.find_one({"emp_code": "EMP0042"}))  # the snapshot is exact

    new = main.EmployeeIn(emp_code="EMP0043", name="M", email="m@x.io", department="Ops", joined_on="2026-01-01")
    monkeypatch.setattr(main, "employees", FailAfter(db.employees, "insert_one"))
    got = keyed(main, "d", "POST /employees", new, 201, lambda: main.create_employee_op(new))
    assert got["emp_code"] == "EMP0043" and db.employees.count_documents({"emp_code": "EMP0043"}) == 1
    assert no_stamps_left(db) and db.idempotency_keys.count_documents({"state": "done"}) == 4


def test_standalone_answer_not_stored_then_the_retry_still_gets_it(claimed, monkeypatch):
    """The write commits, then storing the answer fails (or the process dies): the retry replays the committed
    answer at once, instead of 409 'in progress' and later 409 'already punched in'."""
    main, db = claimed
    punch_in = main.PunchIn(emp_code="EMP0042", punched_at=ist_ms(2026, 7, 6, 9, 0))
    run = lambda: keyed(main, "e", "POST /attendance/punch-in", punch_in, 201, lambda: main.punch_in_op(punch_in))  # noqa: E731
    class DropTheAnswer(FailAfter):  # the answer write itself never happens
        def __getattr__(self, name):
            if name == "update_one" and self.failures:
                def fail(*a, **k):
                    self.failures -= 1
                    from pymongo.errors import AutoReconnect
                    raise AutoReconnect("primary gone")
                return fail
            return getattr(self.coll, name)

    monkeypatch.setattr(main, "idempotency", DropTheAnswer(db.idempotency_keys, "update_one"))
    from pymongo.errors import AutoReconnect
    with pytest.raises(AutoReconnect):
        run()
    assert db.idempotency_keys.find_one()["state"] == "pending"  # what a crash at this point would also leave
    retry = run()
    assert retry.status_code == 201 and retry.headers["idempotent-replayed"] == "true"
    assert json.loads(retry.body)["punch_in"] == ist_ms(2026, 7, 6, 9, 0)
    assert db.attendance_logs.count_documents({}) == 1 and no_stamps_left(db)
    again = run()  # now from the stored answer
    assert again.status_code == 201 and json.loads(again.body) == json.loads(retry.body)


def test_standalone_failure_before_the_write_releases_the_key(claimed, monkeypatch):
    main, db = claimed
    punch_in = main.PunchIn(emp_code="EMP0042", punched_at=ist_ms(2026, 7, 6, 9, 0))

    class Down:
        def __getattr__(self, name):
            def fail(*a, **k):
                from pymongo.errors import AutoReconnect
                raise AutoReconnect("down")
            return fail if name == "insert_one" else getattr(db.attendance_logs, name)

    monkeypatch.setattr(main, "logs", Down())
    from pymongo.errors import AutoReconnect
    with pytest.raises(AutoReconnect):
        keyed(main, "f", "POST /attendance/punch-in", punch_in, 201, lambda: main.punch_in_op(punch_in))
    assert db.idempotency_keys.count_documents({}) == 0  # nothing committed: the retry simply runs
    monkeypatch.setattr(main, "logs", db.attendance_logs)
    assert keyed(main, "f", "POST /attendance/punch-in", punch_in, 201, lambda: main.punch_in_op(punch_in))["emp_code"] == "EMP0042"


def test_stamps_never_reach_api_output(api, mongo):
    make_employee(api, "EMP0001")
    punch(api, "k-out", punched_at=ist_ms(2026, 7, 6, 9, 0))
    mongo.attendance_logs.update_one({}, {"$set": {"_idem": [{"k": "x", "snap": {}}]}})  # a stamp left behind
    for body in (api.get("/attendance").json()["items"][0], api.get("/attendance/EMP0001/2026-07-06").json()):
        assert "_idem" not in body
