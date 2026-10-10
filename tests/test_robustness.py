"""The service must never answer 5xx (or drop the connection) because of what a client sends or how a seeded document
looks, and concurrent writers must never leave a record inconsistent."""
import threading
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal

import pytest
from bson.decimal128 import Decimal128
from bson.int64 import Int64

from tests.conftest import ist_ms, make_employee


def utc(*args):
    return datetime(*args, tzinfo=timezone.utc)


@pytest.fixture()
def nasty(api, mongo):
    """Documents that are legal for MongoDB but odd for the contract: missing fields, other numeric types, NaN,
    null history, malformed shifts and dates."""
    mongo.employees.insert_many([
        {"emp_code": "EMP0001", "name": "ok", "email": "a@b.c", "department": "Eng", "shift_start": "09:30", "shift_end": "18:30",
         "joined_on": "2026-01-01", "created_at": utc(2026, 1, 1)},
        {"emp_code": "EMP0002", "name": "no shift", "department": "Eng", "joined_on": "2026-01-01"},
        {"emp_code": "EMP0003", "name": "odd shift", "department": "Eng", "shift_start": "9:30", "shift_end": "6pm", "joined_on": "2026-01-01"},
        {"emp_code": "EMP0004", "name": "no department", "shift_start": "09:30", "shift_end": "18:30", "joined_on": "2026-01-01"},
        {"emp_code": "EMP0005", "name": "no joined_on", "department": "Ops", "shift_start": "22:00", "shift_end": "06:00"},
    ])
    mongo.attendance_logs.insert_many([
        {"emp_code": "EMP0001", "date": "2026-07-06", "status": "PRESENT", "punch_in": utc(2026, 7, 6, 4), "punch_out": utc(2026, 7, 6, 13),
         "work_hours": Decimal128("9.00"), "late_minutes": Int64(12), "overtime_minutes": 0.0},
        {"emp_code": "EMP0001", "date": "2026-07-07", "status": "PRESENT", "punch_in": utc(2026, 7, 7, 4), "punch_out": None,
         "work_hours": float("nan"), "late_minutes": 15.0, "history": None},
        {"emp_code": "EMP0001", "date": "2026-07-08", "status": "WFH", "punch_in": utc(2026, 7, 8, 4), "work_hours": "8.5", "half_day": None,
         "history": [{"at": utc(2026, 7, 9), "by": "x"}, {"changes": {"punch_in": {"from": utc(2026, 7, 8, 5)}}}, "junk"]},
        {"emp_code": "EMP0002", "date": "2026-07-06", "status": "PRESENT", "punch_in": utc(2026, 7, 6, 4)},
        {"emp_code": "EMP0003", "date": "2026-07-06", "status": "PRESENT", "punch_in": utc(2026, 7, 6, 4)},
        {"emp_code": "EMP0004", "date": "2026-07-06", "status": "ON_DUTY", "punch_in": utc(2026, 7, 6, 4), "late_minutes": 5},
        {"emp_code": "EMP0005", "date": "2026-07-06", "status": "LEAVE"},
        {"emp_code": "EMP0001", "date": "2026-07-1", "status": "PRESENT"},  # malformed date inside the month's range
        {"emp_code": "EMP0001", "date": "2026-07-10", "punch_in": utc(2026, 7, 10, 4)},  # no status
    ])
    return api


READS = [
    ("/employees", {}), ("/attendance", {}), ("/attendance", {"emp_code": "EMP0001"}),
    ("/attendance/EMP0001/2026-07-07", {}), ("/attendance/EMP0001/2026-07-08", {}), ("/attendance/EMP0001/2026-07-10", {}),
    ("/analytics/employees/EMP0001/monthly", {"month": "2026-07"}), ("/analytics/employees/EMP0005/monthly", {"month": "2026-07"}),
    ("/analytics/departments/summary", {"month": "2026-07"}), ("/analytics/leaderboard/late", {"month": "2026-07"}),
    ("/analytics/departments/Eng/trend", {"from": "2026-07-01", "to": "2026-07-31"}),
    ("/analytics/employees/EMP0001/calendar", {"month": "2026-07"}), ("/analytics/exceptions", {"date": "2026-07-06"}),
    ("/employees/EMP0002", {}), ("/departments", {}),
]


@pytest.mark.parametrize("path, params", READS)
def test_nasty_documents_never_break_reads(nasty, path, params):
    r = nasty.get(path, params=params)
    assert r.status_code < 500, r.text
    assert nasty.get("/health").status_code == 200  # and the keep-alive connection is still usable


def test_nasty_documents_are_read_with_the_contract_types(nasty):
    rec = nasty.get("/attendance/EMP0001/2026-07-06").json()
    assert rec["work_hours"] == 9.0 and rec["late_minutes"] == 12 and isinstance(rec["late_minutes"], int)
    rec = nasty.get("/attendance/EMP0001/2026-07-07").json()
    assert rec["work_hours"] is None and rec["history"] == [] and rec["half_day"] is False
    summary = nasty.get("/analytics/departments/summary", params={"month": "2026-07"}).json()["items"]
    assert [d["department"] for d in summary] == ["Eng"]  # no department / no joined_on: not counted, no crash


def test_nasty_documents_never_break_writes(nasty):
    for code in ("EMP0002", "EMP0003", "EMP0005"):
        assert nasty.post("/attendance/punch-in", json={"emp_code": code, "punched_at": ist_ms(2026, 7, 14, 9, 0)}).status_code == 201
        assert nasty.post("/attendance/punch-out", json={"emp_code": code, "punched_at": ist_ms(2026, 7, 14, 18, 0)}).status_code == 200
    body = {"reason": "fixing it", "regularized_by": "hr"}
    r = nasty.patch("/attendance/EMP0001/2026-07-07", json={**body, "status": "WFH"})  # history stored as null
    assert r.status_code == 200 and len(r.json()["history"]) == 1
    assert nasty.patch("/attendance/EMP0001/2026-07-08", json={**body, "status": "ABSENT"}).status_code == 200
    assert nasty.patch("/attendance/EMP0001/2026-07-10", json={**body, "punch_out": ist_ms(2026, 7, 10, 18, 0)}).status_code == 422
    assert nasty.patch("/attendance/EMP0001/2026-07-10", json={**body, "status": "PRESENT"}).status_code == 200


@pytest.mark.parametrize("path", ["/employees", "/attendance"])
@pytest.mark.parametrize("page", [10**6, 2**62, 10**20])
def test_pages_past_the_end_are_empty_not_errors(api, path, page):
    make_employee(api, "EMP0001")
    body = api.get(path, params={"page": page, "page_size": 100}).json()
    assert body["items"] == [] and body["page"] == page and body["total"] == (1 if path == "/employees" else 0)
    assert api.get("/admin/explain/attendance_list", params={"page": page}).status_code == 200


@pytest.mark.parametrize("method, path, kwargs", [
    ("GET", "/analytics/employees/EMP0001/monthly", {"params": {"month": "0000-01"}}),  # year 0 does not exist
    ("GET", "/analytics/departments/%F0%9F%98%80/trend", {"params": {"from": "2026-07-01", "to": "2026-07-02"}}),
    ("POST", "/attendance", {"content": b"\xff\xfe", "headers": {"content-type": "application/json"}}),  # 405
    ("POST", "/employees", {"content": b"\xff\xfe\x00", "headers": {"content-type": "application/json"}}),  # not UTF-8
    ("POST", "/attendance/punch-in", {"json": {"emp_code": "EMP0001", "punched_at": 10**30}}),
    ("POST", "/attendance/punch-in", {"json": {"emp_code": {"$ne": None}}}),  # operator injection attempt
    ("POST", "/attendance/punch-in", {"content": b"[1,2", "headers": {"content-type": "application/json"}}),
])
def test_hostile_requests_get_4xx(api, method, path, kwargs):
    make_employee(api, "EMP0001")
    r = api.request(method, path, **kwargs)
    assert 400 <= r.status_code < 500, (r.status_code, r.text)
    assert api.get("/health").status_code == 200


def test_extreme_but_valid_dates_work(api):
    """`format: date` allows 0001-01-01 .. 9999-12-31; nothing may overflow at either end."""
    make_employee(api, "EMP0001", dept="Eng", joined_on="0001-01-01")
    trend = api.get("/analytics/departments/Eng/trend", params={"from": "9999-12-01", "to": "9999-12-31"}).json()["items"]
    assert len(trend) == 31 and trend[-1]["date"] == "9999-12-31" and trend[-1]["headcount"] == 1
    trend = api.get("/analytics/departments/Eng/trend", params={"from": "0001-01-01", "to": "0001-01-07"}).json()["items"]
    assert [row["date"] for row in trend][0] == "0001-01-01" and len(trend) == 7
    calendar = api.get("/analytics/employees/EMP0001/calendar", params={"month": "9999-12"}).json()
    assert len(calendar["days"]) == 31 and calendar["days"][-1]["date"] == "9999-12-31"
    assert api.get("/analytics/employees/EMP0001/monthly", params={"month": "9999-12"}).json()["working_days"] == 23
    assert api.get("/analytics/departments/summary", params={"month": "0001-01"}).json()["items"][0]["headcount"] == 1
    assert api.get("/attendance", params={"date_from": "0001-01-01", "date_to": "9999-12-31"}).status_code == 200
    assert api.get("/analytics/exceptions", params={"date": "9999-12-31"}).status_code == 200


def test_undecodable_body_is_422_and_405_lists_every_method(api):
    r = api.post("/employees", content=b"\xff\xfe\x00", headers={"content-type": "application/json"})
    assert r.status_code == 422 and r.json()["detail"][0]["loc"] == ["body"]
    r = api.request("OPTIONS", "/employees")
    assert r.status_code == 405 and set(r.headers["allow"].split(", ")) == {"GET", "POST"}


def test_body_size_is_capped_even_when_chunked(api):
    assert api.post("/attendance/punch-in", content=b"x" * 100_000, headers={"content-type": "application/json"}).status_code == 413
    chunks = (b"x" * 1000 for _ in range(100))
    assert api.post("/attendance/punch-in", content=chunks, headers={"content-type": "application/json"}).status_code == 413
    assert api.get("/health").status_code == 200


def test_punch_out_racing_a_correction_never_stores_stale_work_hours(api, mongo):
    """Punch-out reads the record, then writes work hours computed from its punch_in. A correction that moves punch_in
    in between must not leave work hours computed from the old value (it did, in 27 of 60 runs, before the fix)."""
    make_employee(api, "EMP0001")
    for day in range(1, 21):
        api.post("/attendance/punch-in", json={"emp_code": "EMP0001", "punched_at": ist_ms(2026, 7, day, 10, 0)})
        barrier = threading.Barrier(2)

        def punch_out(barrier=barrier, day=day):
            barrier.wait()
            api.post("/attendance/punch-out", json={"emp_code": "EMP0001", "punched_at": ist_ms(2026, 7, day, 18, 0)})

        def correct(barrier=barrier, day=day):
            barrier.wait()
            api.patch(f"/attendance/EMP0001/2026-07-{day:02d}",
                      json={"punch_in": ist_ms(2026, 7, day, 9, 0), "reason": "late fix", "regularized_by": "hr"})

        threads = [threading.Thread(target=punch_out), threading.Thread(target=correct)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        rec = mongo.attendance_logs.find_one({"emp_code": "EMP0001", "date": f"2026-07-{day:02d}"})
        if rec.get("punch_out") is not None:
            seconds = int((rec["punch_out"] - rec["punch_in"]).total_seconds())
            expected = float((Decimal(seconds) / Decimal(3600)).quantize(Decimal("0.01"), ROUND_HALF_UP))
            assert rec["work_hours"] == expected, (day, rec["punch_in"], rec["work_hours"])


def test_large_department_fallback_gives_identical_answers(api, mongo, monkeypatch):
    import app.main as main  # the pipeline builders are pure; run them in-process against the test database
    mongo.employees.insert_many([{"emp_code": f"EMP{i:04d}", "name": str(i), "department": "Eng" if i % 3 else "Ops",
                                  "joined_on": "2026-01-01"} for i in range(1, 31)])
    mongo.attendance_logs.insert_many([{"emp_code": f"EMP{i:04d}", "date": f"2026-07-{d:02d}", "status": "PRESENT",
                                        "late_minutes": (i * d) % 7, "work_hours": 8.0} for i in range(1, 31) for d in range(1, 15)])
    monkeypatch.setattr(main, "db", mongo.client[mongo.name])
    monkeypatch.setattr(main, "employees", mongo.employees)
    small = (list(mongo.attendance_logs.aggregate(main.department_summary_pipeline("2026-07", "Eng"))),
             list(mongo.attendance_logs.aggregate(main.late_leaderboard_pipeline("2026-07", 5, "Eng"))))
    monkeypatch.setattr(main, "DEPARTMENT_IN_LIMIT", 0)
    large = (list(mongo.attendance_logs.aggregate(main.department_summary_pipeline("2026-07", "Eng"))),
             list(mongo.attendance_logs.aggregate(main.late_leaderboard_pipeline("2026-07", 5, "Eng"))))
    assert small == large and small[0][0]["headcount"] == 20


def test_transient_database_errors_are_503_not_500():
    """Found in a chaos drill: while mongod shut down, a write failed with ShutdownInProgress (code 91, labelled
    RetryableWriteError). That is an outage (503, retry) and not a failed operation (500)."""
    from pymongo.errors import AutoReconnect, DuplicateKeyError, OperationFailure, WriteConcernError

    from app.main import is_transient

    drill = WriteConcernError("Replication is being shut down", 91, {"code": 91, "errorLabels": ["RetryableWriteError"]})
    assert is_transient(drill)
    assert is_transient(OperationFailure("primary stepped down", 189, {"code": 189}))
    assert is_transient(AutoReconnect("connection reset"))
    assert not is_transient(OperationFailure("bad value", 2, {"code": 2}))
    assert not is_transient(DuplicateKeyError("duplicate", 11000, {"code": 11000}))


def test_made_up_http_methods_can_not_grow_the_metrics(api):
    api.request("X-1", "/health")
    before = {r["route"] for r in api.get("/admin/metrics").json()["routes"]}
    for i in range(50):
        api.request(f"X{i}", "/health")
    after = {r["route"] for r in api.get("/admin/metrics").json()["routes"]}
    assert after - before <= {"GET /admin/metrics"} and not any(r.startswith("X") for r in after)


def test_explain_validates_every_declared_parameter(api):
    """Found by contract fuzzing: a malformed date for a parameter the explained endpoint does not use was ignored."""
    assert api.get("/admin/explain/attendance_list", params={"to": ""}).status_code == 422
    assert api.get("/admin/explain/late_leaderboard", params={"month": "2026-07", "date_from": "2026-13-01"}).status_code == 422
    assert api.get("/admin/explain/attendance_list", params={"to": "2026-07-31"}).status_code == 200


def test_malformed_joined_on_never_breaks_the_monthly_summary(api, mongo):
    """Found by a requirements audit: a seeded joined_on that is not a valid date string made the monthly
    summary a 500. It is now read like a missing one: working days count from the start of the month."""
    from datetime import datetime, timezone
    bad = ["2026/01/05", datetime(2026, 1, 5, tzinfo=timezone.utc), 20260105, "2026-13-45", None]
    mongo.employees.insert_many([{"emp_code": f"EMP{i:04d}", "name": "x", "department": "Eng", "shift_start": "09:30",
                                  "shift_end": "18:30", "joined_on": value} for i, value in enumerate(bad, 1)])
    for i in range(1, len(bad) + 1):
        r = api.get(f"/analytics/employees/EMP{i:04d}/monthly", params={"month": "2026-07"})
        assert r.status_code == 200 and r.json()["working_days"] == 23, (bad[i - 1], r.text)


def test_seeded_employee_without_a_shift_reports_the_default_it_works_on(api, mongo):
    """Employee requires shift_start / shift_end as strings; the contract's defaults are 09:30 / 18:30, which is also
    the shift the rules use for such an employee."""
    mongo.employees.insert_one({"emp_code": "EMP0001", "name": "x", "email": "x@y.io", "department": "Eng", "joined_on": "2026-01-01"})
    body = api.get("/employees", params={"department": "Eng"}).json()["items"][0]
    assert (body["shift_start"], body["shift_end"]) == ("09:30", "18:30")
    r = api.post("/attendance/punch-in", json={"emp_code": "EMP0001", "punched_at": ist_ms(2026, 7, 6, 9, 45)})
    assert r.json()["late_minutes"] == 15  # computed against 09:30


@pytest.mark.parametrize("query", [
    "/employees?page=2&page=x", "/employees?page=x&page=2",
    "/analytics/leaderboard/late?month=2026-07&limit=5&limit=1.0",
    "/analytics/departments/summary?month=2026-07&month=2026-08",
    "/attendance?status=PRESENT&status=LEAVE",
    "/analytics/departments/Eng/trend?from=2026-07-01&to=2026-07-02&to=2026-07-03",
])
def test_a_parameter_given_twice_is_422(api, query):
    """Found by contract fuzzing: FastAPI silently keeps the last of repeated values (?month=2026-07&month=2026-08
    answered for August). Every contract parameter is a single value."""
    r = api.get(query)
    assert r.status_code == 422 and r.json()["detail"][0]["msg"] == "must be given once"


@pytest.mark.parametrize("query", ["/employees?page=1.0", "/employees?page_size=1e1", "/analytics/leaderboard/late?month=2026-07&limit=2.0"])
def test_query_integers_must_be_written_as_integers(api, query):
    assert api.get(query).status_code == 422


def test_ordinary_queries_are_unaffected(api):
    make_employee(api, "EMP0001")
    assert api.get("/employees?page=1&page_size=10&department=Eng").json()["total"] == 1
    assert api.get("/analytics/leaderboard/late?month=2026-07&limit=3").status_code == 200


@pytest.mark.parametrize("literal", ["NaN", "Infinity", "-Infinity"])
def test_non_finite_json_numbers_are_422_not_500(api, literal):
    """Found by a request-oddity probe: Python's JSON parser accepts NaN / Infinity, pydantic rejects them, and the
    default 422 handler then failed to serialize the echoed input (a 500)."""
    make_employee(api, "EMP0001")
    for path, body in (("/attendance/punch-in", '{"emp_code":"EMP0001","punched_at":%s}'),
                       ("/attendance/punch-out", '{"emp_code":"EMP0001","punched_at":%s}'),
                       ("/attendance/EMP0001/2026-07-06", '{"punch_in":%s,"reason":"check it","regularized_by":"x"}'),
                       ("/employees", '{"emp_code":"EMP0002","name":%s,"email":"a@b.cd","department":"Eng","joined_on":"2026-01-01"}')):
        method = "PATCH" if path.count("/") == 3 and "punch" not in path else "POST"
        r = api.request(method, path, content=body % literal, headers={"Content-Type": "application/json"})
        assert r.status_code == 422 and isinstance(r.json()["detail"], list), (path, r.status_code, r.text)
