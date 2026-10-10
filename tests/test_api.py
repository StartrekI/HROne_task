"""Contract tests over HTTP: status codes, R1 - R10, concurrency, regularization, analytics edge cases, explain."""
import threading
from datetime import datetime, timezone

import pytest

from tests.conftest import ist_ms, make_employee


def parallel(n, fn):
    results = []
    threads = [threading.Thread(target=lambda: results.append(fn())) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return sorted(results)


def punch_in(api, code, ms, **extra):
    return api.post("/attendance/punch-in", json={"emp_code": code, "punched_at": ms, **extra})


def punch_out(api, code, ms):
    return api.post("/attendance/punch-out", json={"emp_code": code, "punched_at": ms})


def patch(api, code, day, **body):
    body.setdefault("reason", "biometric glitch")
    body.setdefault("regularized_by", "hr.admin")
    return api.patch(f"/attendance/{code}/{day}", json=body)


# ---- employees -------------------------------------------------------------------------------------------------
def test_employee_create_shape_and_duplicates(api):
    body = make_employee(api, "EMP0001")
    assert isinstance(body["created_at"], int) and body["created_at"] > 100_000_000_000 and "_id" not in body
    assert api.post("/employees", json={**body, "created_at": None}).status_code == 409


@pytest.mark.parametrize("bad", [{"emp_code": "E1"}, {"email": "nope"}, {"shift_start": "24:00"}, {"joined_on": "2026-02-30"},
                                 {"shift_start": "09:00", "shift_end": "09:00"}, {"name": ""}, {"department": 7}])
def test_employee_validation(api, bad):
    body = {"emp_code": "EMP0002", "name": "n", "email": "a@b.co", "department": "D", "joined_on": "2026-01-01", **bad}
    assert api.post("/employees", json=body).status_code == 422


def test_concurrent_employee_create(api):
    body = {"emp_code": "EMP0003", "name": "n", "email": "a@b.co", "department": "D", "joined_on": "2026-01-01"}
    assert parallel(10, lambda: api.post("/employees", json=body).status_code) == [201] + [409] * 9


def test_employee_pagination_filter_and_sort(api):
    for i in range(12):
        make_employee(api, f"EMP{100 - i:04d}", dept="Sales" if i % 2 else "Eng")
    page = api.get("/employees", params={"department": "Sales", "page": 2, "page_size": 4}).json()
    assert page["total"] == 6 and [e["emp_code"] for e in page["items"]] == ["EMP0097", "EMP0099"]
    assert api.get("/employees", params={"page": 0}).status_code == 422
    assert api.get("/employees", params={"page_size": 101}).status_code == 422


# ---- punch-in / punch-out --------------------------------------------------------------------------------------
def test_punch_in_contract(api):
    make_employee(api, "EMP0001")
    r = punch_in(api, "EMP0001", ist_ms(2026, 7, 6, 9, 40, 1, extra_ms=900))
    body = r.json()
    assert r.status_code == 201 and "id" not in body
    assert body == {"emp_code": "EMP0001", "date": "2026-07-06", "status": "PRESENT", "punch_in": ist_ms(2026, 7, 6, 9, 40, 1),
                    "punch_out": None, "work_hours": None, "late_minutes": 10, "overtime_minutes": 0, "half_day": False, "history": []}
    assert punch_in(api, "EMP0001", ist_ms(2026, 7, 6, 11, 0)).status_code == 409
    assert punch_in(api, "EMP9999", ist_ms(2026, 7, 6, 11, 0)).status_code == 404


@pytest.mark.parametrize("value", [1783312500, 1783312500000.0, "1783312500000", True, 4102444800001])
def test_punch_in_rejects_non_millisecond_instants(api, value):
    make_employee(api, "EMP0001")
    assert punch_in(api, "EMP0001", value).status_code == 422


def test_punch_in_rejects_non_presence_status(api):
    make_employee(api, "EMP0001")
    assert punch_in(api, "EMP0001", ist_ms(2026, 7, 6, 9, 0), status="LEAVE").status_code == 422


def test_concurrent_punch_in_and_punch_out(api):
    make_employee(api, "EMP0001")
    assert parallel(10, lambda: punch_in(api, "EMP0001", ist_ms(2026, 7, 6, 9, 0)).status_code) == [201] + [409] * 9
    assert parallel(10, lambda: punch_out(api, "EMP0001", ist_ms(2026, 7, 6, 18, 0)).status_code) == [200] + [409] * 9


def test_overnight_shift_round_trip(api):
    make_employee(api, "EMP0005", shift_start="22:00", shift_end="06:00")
    assert punch_in(api, "EMP0005", ist_ms(2026, 7, 6, 21, 55)).json()["date"] == "2026-07-06"
    body = punch_out(api, "EMP0005", ist_ms(2026, 7, 7, 6, 40)).json()
    assert (body["date"], body["work_hours"], body["overtime_minutes"], body["half_day"]) == ("2026-07-06", 8.75, 40, False)


def test_punch_out_rules(api):
    make_employee(api, "EMP0001")
    assert punch_out(api, "EMP0001", ist_ms(2026, 7, 6, 18, 0)).status_code == 404
    punch_in(api, "EMP0001", ist_ms(2026, 7, 6, 9, 40))
    assert punch_out(api, "EMP0001", ist_ms(2026, 7, 7, 10, 0)).status_code == 422  # more than 24 h
    body = punch_out(api, "EMP0001", ist_ms(2026, 7, 6, 13, 10, 18)).json()
    assert body["work_hours"] == 3.51 and body["half_day"] is True  # 3.505 h rounds half-up


# ---- list ------------------------------------------------------------------------------------------------------
def test_attendance_list(api):
    for code in ("EMP0002", "EMP0001"):
        make_employee(api, code)
        for day in (6, 7, 8):
            punch_in(api, code, ist_ms(2026, 7, day, 9, 0))
    page = api.get("/attendance", params={"page_size": 4}).json()
    assert page["total"] == 6
    assert [(r["date"], r["emp_code"]) for r in page["items"]] == [
        ("2026-07-08", "EMP0001"), ("2026-07-08", "EMP0002"), ("2026-07-07", "EMP0001"), ("2026-07-07", "EMP0002")]
    filtered = api.get("/attendance", params={"emp_code": "EMP0001", "date_from": "2026-07-07"}).json()
    assert filtered["total"] == 2
    assert api.get("/attendance", params={"date_from": "2026-07-09", "date_to": "2026-07-01"}).status_code == 422
    assert api.get("/attendance", params={"status": "LATE"}).status_code == 422


# ---- regularization --------------------------------------------------------------------------------------------
def test_regularize_appends_history_with_changed_fields_only(api, mongo):
    make_employee(api, "EMP0001")
    punch_in(api, "EMP0001", ist_ms(2026, 7, 7, 10, 5))
    punch_out(api, "EMP0001", ist_ms(2026, 7, 7, 18, 30))
    body = patch(api, "EMP0001", "2026-07-07", punch_in=ist_ms(2026, 7, 7, 9, 28)).json()
    entry = body["history"][0]
    assert body["late_minutes"] == 0 and set(entry["changes"]) == {"punch_in", "late_minutes", "work_hours"}
    assert entry["changes"]["punch_in"] == {"from": ist_ms(2026, 7, 7, 10, 5), "to": ist_ms(2026, 7, 7, 9, 28)}
    assert entry["by"] == "hr.admin" and isinstance(entry["at"], int)
    stored = mongo.attendance_logs.find_one({"emp_code": "EMP0001"})["history"][0]["changes"]["punch_in"]["to"]
    assert isinstance(stored, datetime)  # BSON date inside the nested object, not an int

    body = patch(api, "EMP0001", "2026-07-07", status="LEAVE").json()
    assert body["punch_in"] is None and body["work_hours"] is None and len(body["history"]) == 2


@pytest.mark.parametrize("change", [
    {"status": "ABSENT", "punch_in": ist_ms(2026, 7, 7, 9, 5)},
    {"punch_in": ist_ms(2026, 7, 8, 9, 0)},
    {"punch_out": ist_ms(2026, 7, 7, 8, 0)},
    {"punch_in": None},
    {"status": "PRESENT"},
    {"reason": "x", "status": "WFH"},
], ids=["times-with-absent", "punch-in-other-date", "out-before-in", "explicit-null", "no-change", "short-reason"])
def test_regularize_validation(api, change):
    make_employee(api, "EMP0001")
    punch_in(api, "EMP0001", ist_ms(2026, 7, 7, 9, 0))
    assert patch(api, "EMP0001", "2026-07-07", **change).status_code == 422


def test_regularize_not_found(api):
    make_employee(api, "EMP0001")
    assert patch(api, "EMP0001", "2026-07-07", status="WFH").status_code == 404
    assert patch(api, "EMP9999", "2026-07-07", status="WFH").status_code == 404


def test_concurrent_regularizations_never_lose_history(api, mongo):
    make_employee(api, "EMP0001")
    punch_in(api, "EMP0001", ist_ms(2026, 7, 7, 9, 0))
    minutes = iter(range(100))
    lock = threading.Lock()

    def edit():
        with lock:
            m = next(minutes)
        return patch(api, "EMP0001", "2026-07-07", punch_out=ist_ms(2026, 7, 7, 17, 0) + m * 60_000).status_code

    codes = parallel(8, edit)
    history = mongo.attendance_logs.find_one({"emp_code": "EMP0001"})["history"]
    assert set(codes) <= {200, 409} and len(history) == codes.count(200)


# ---- analytics -------------------------------------------------------------------------------------------------
def seed(mongo, employees, logs):
    mongo.employees.insert_many([{"shift_start": "09:30", "shift_end": "18:30", "email": "x@y.z", "joined_on": "2026-01-01",
                                  "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc), **e} for e in employees])
    if logs:
        mongo.attendance_logs.insert_many(logs)


def log(code, day, status="PRESENT", late=0, wh=8.0, half=False, **extra):
    return {"emp_code": code, "date": day, "status": status, "late_minutes": late, "work_hours": wh, "half_day": half,
            "overtime_minutes": 0, **extra}


def test_leaderboard_ties_share_rank_and_survive_the_cutoff(api, mongo):
    seed(mongo, [{"emp_code": c, "name": c, "department": d} for c, d in
                 [("EMP0001", "Eng"), ("EMP0002", "Eng"), ("EMP0003", "Ops"), ("EMP0004", "Eng")]],
         [log("EMP0001", "2026-07-06", late=50), log("EMP0002", "2026-07-06", late=30), log("EMP0003", "2026-07-06", late=30),
          log("EMP0004", "2026-07-06", late=10), log("EMP7777", "2026-07-06", late=99)])  # unknown employee is ignored
    rows = api.get("/analytics/leaderboard/late", params={"month": "2026-07", "limit": 2}).json()["items"]
    assert [(r["rank"], r["emp_code"]) for r in rows] == [(1, "EMP0001"), (2, "EMP0002"), (2, "EMP0003")]
    rows = api.get("/analytics/leaderboard/late", params={"month": "2026-07", "limit": 4}).json()["items"]
    assert [r["rank"] for r in rows] == [1, 2, 2, 4]
    rows = api.get("/analytics/leaderboard/late", params={"month": "2026-07", "department": "Eng", "limit": 2}).json()["items"]
    assert [(r["rank"], r["emp_code"]) for r in rows] == [(1, "EMP0001"), (2, "EMP0002")]


def test_monthly_and_summary_edge_cases(api, mongo):
    seed(mongo, [{"emp_code": "EMP0001", "name": "a", "department": "Eng"},
                 {"emp_code": "EMP0002", "name": "b", "department": "Eng", "joined_on": "2026-07-15"},   # mid-month joiner
                 {"emp_code": "EMP0003", "name": "c", "department": "Eng"},                              # no logs at all
                 {"emp_code": "EMP0004", "name": "d", "department": "Eng", "joined_on": "2026-08-03"}],  # joins later
         [log("EMP0001", "2026-07-06", late=12, wh=9.12), log("EMP0001", "2026-07-07", wh=3.5, half=True),
          log("EMP0001", "2026-07-11", late=20, wh=9.08),                                  # Saturday: late counts, presence not
          log("EMP0001", "2026-07-08", status="LEAVE", wh=None),
          {"emp_code": "EMP0002", "date": "2026-07-16", "status": "ON_DUTY", "work_hours": 8.0, "late_minutes": 0}])  # legacy
    monthly = api.get("/analytics/employees/EMP0001/monthly", params={"month": "2026-07"}).json()
    assert monthly == {"emp_code": "EMP0001", "month": "2026-07", "working_days": 23, "present_days": 1.5, "leave_days": 1,
                       "late_count": 2, "total_late_minutes": 32, "total_overtime_minutes": 0, "attendance_pct": 6.52}
    joiner = api.get("/analytics/employees/EMP0002/monthly", params={"month": "2026-07"}).json()
    assert joiner["working_days"] == 13 and joiner["present_days"] == 1 and joiner["attendance_pct"] == 7.69
    later = api.get("/analytics/employees/EMP0004/monthly", params={"month": "2026-07"}).json()
    assert later["working_days"] == 0 and later["attendance_pct"] is None

    (eng,) = api.get("/analytics/departments/summary", params={"month": "2026-07"}).json()["items"]
    assert eng == {"department": "Eng", "headcount": 3, "present_days": 2.5, "avg_work_hours": 7.43, "late_count": 2,
                   "total_late_minutes": 32, "leave_count": 1, "on_duty_count": 1}


def test_trend_fills_every_day_and_windows_inside_range(api, mongo):
    seed(mongo, [{"emp_code": "EMP0001", "name": "a", "department": "Eng"},
                 {"emp_code": "EMP0002", "name": "b", "department": "Eng", "joined_on": "2026-07-08"}],
         [log("EMP0001", "2026-07-06"), log("EMP0001", "2026-07-07", half=True), log("EMP0002", "2026-07-08", late=15),
          log("EMP0001", "2026-07-11")])  # Saturday record
    items = api.get("/analytics/departments/Eng/trend", params={"from": "2026-07-06", "to": "2026-07-12"}).json()["items"]
    assert [i["date"] for i in items] == [f"2026-07-{d:02d}" for d in range(6, 13)]
    assert [i["headcount"] for i in items] == [1, 1, 2, 2, 2, 2, 2]
    assert [i["attendance_rate"] for i in items] == [1.0, 0.5, 0.5, 0.0, 0.0, None, None]
    assert [i["moving_avg_7d"] for i in items] == [1.0, 0.75, 0.6667, 0.5, 0.4, 0.4, 0.4]
    assert items[5]["present_count"] == 1 and items[2]["late_count"] == 1
    assert api.get("/analytics/departments/Nope/trend", params={"from": "2026-07-06", "to": "2026-07-12"}).status_code == 404
    assert api.get("/analytics/departments/Eng/trend", params={"from": "2026-07-12", "to": "2026-07-06"}).status_code == 422


@pytest.mark.parametrize("endpoint, params", [
    ("attendance_list", {}), ("attendance_list", {"status": "LEAVE"}),
    ("employee_monthly", {"emp_code": "EMP0001", "month": "2026-07"}),
    ("department_summary", {"month": "2026-07"}), ("late_leaderboard", {"month": "2026-07", "department": "Eng"}),
    ("department_trend", {"department": "Eng", "from": "2026-07-01", "to": "2026-07-31"}),
])
def test_explain_uses_indexes(api, mongo, endpoint, params):
    seed(mongo, [{"emp_code": "EMP0001", "name": "a", "department": "Eng"}], [log("EMP0001", "2026-07-06", late=5)])
    r = api.get(f"/admin/explain/{endpoint}", params=params)
    assert r.status_code == 200 and "IXSCAN" in r.text and "COLLSCAN" not in r.text


def test_explain_requires_parameters(api):
    assert api.get("/admin/explain/department_trend", params={"department": "Eng"}).status_code == 422


# ---- beyond the contract ---------------------------------------------------------------------------------------
def test_calendar_and_exceptions(api, mongo):
    seed(mongo, [{"emp_code": "EMP0001", "name": "a", "department": "Eng", "joined_on": "2026-07-02"},
                 {"emp_code": "EMP0002", "name": "b", "department": "Eng"}],
         [log("EMP0001", "2026-07-06", late=15), log("EMP0001", "2026-07-07", late=20), log("EMP0001", "2026-07-08"),
          log("EMP0001", "2026-07-09", late=11, punch_in=datetime(2026, 7, 9, 4, 0, tzinfo=timezone.utc), punch_out=None)])
    cal = api.get("/analytics/employees/EMP0001/calendar", params={"month": "2026-07"}).json()
    days = {d["date"]: d for d in cal["days"]}
    assert len(cal["days"]) == 31 and days["2026-07-01"]["day_type"] == "NOT_JOINED"
    assert days["2026-07-03"]["day_type"] == "NO_RECORD" and days["2026-07-04"]["day_type"] == "WEEKEND"
    assert cal["summary"]["longest_late_streak"] == 2 and cal["summary"]["records"] == 4

    exc = api.get("/analytics/exceptions", params={"date": "2026-07-09"}).json()
    assert exc["summary"]["headcount"] == 2 and exc["summary"]["present"] == 1
    assert {i["emp_code"]: i["issues"] for i in exc["items"]} == {"EMP0001": ["LATE", "MISSING_PUNCH_OUT"], "EMP0002": ["NO_RECORD"]}


def test_single_reads_headers_and_metrics(api):
    make_employee(api, "EMP0001")
    punch_in(api, "EMP0001", ist_ms(2026, 7, 6, 9, 0))
    assert api.get("/employees/EMP0001").json()["emp_code"] == "EMP0001"
    assert api.get("/attendance/EMP0001/2026-07-06").json()["date"] == "2026-07-06"
    assert api.get("/attendance/EMP0001/2026-07-07").status_code == 404
    r = api.get("/health", headers={"X-Request-ID": "trace-123"})
    assert r.headers["x-request-id"] == "trace-123" and r.headers["server-timing"].startswith("app;dur=")
    routes = {m["route"] for m in api.get("/admin/metrics").json()["routes"]}
    assert "POST /attendance/punch-in" in routes
    assert api.get("/dashboard").headers["content-type"].startswith("text/html")
