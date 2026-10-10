"""The report cache (replica sets) must never change an answer: after every kind of write, from the API or straight
into MongoDB, the very next report equals a fresh computation. It must also actually hit when nothing changed."""
import re
import threading
from datetime import date, datetime, timezone

import pytest

import app.main as main
from tests.conftest import ist_ms, make_employee


@pytest.fixture()
def cached(api, mongo):
    if "replicaSet" not in main.MONGO_URI and not mongo.client.admin.command("hello").get("setName"):
        pytest.skip("the report cache needs a replica set (change streams)")
    for _ in range(100):
        if "report_cache_active 1" in api.get("/metrics").text:
            return api
        threading.Event().wait(0.05)
    pytest.fail("report cache watcher did not start")


def counter(api, name):
    return int(re.search(rf"^{name} (\d+)$", api.get("/metrics").text, re.M).group(1))


def utc(y, mo, d, h, mi):
    return datetime(y, mo, d, h, mi, tzinfo=timezone.utc)


def log(code, day, status="PRESENT", late=0, hours=8.0, half=False):
    present = status in ("PRESENT", "WFH", "ON_DUTY")
    return {"emp_code": code, "date": day, "status": status, "punch_in": utc(2026, 1, 1, 4, 0) if present else None,
            "punch_out": None, "late_minutes": late, "work_hours": hours if present else None, "overtime_minutes": 0,
            "half_day": half, "history": []}


def expected(mongo):
    """Every cached report, computed fresh straight from MongoDB with the app's own pipelines."""
    out = {}
    for month in ("2026-06", "2026-07"):
        out[f"summary {month}"] = list(mongo.attendance_logs.aggregate(main.department_summary_pipeline(month, None)))
        out[f"leaderboard {month}"] = list(mongo.attendance_logs.aggregate(main.late_leaderboard_pipeline(month, 10, None)))
        working = main.working_days(date.fromisoformat(f"{month}-01"), date.fromisoformat(main.month_bounds(month)[1]))
        out[f"monthly {month}"] = list(mongo.attendance_logs.aggregate(main.employee_monthly_pipeline("EMP0001", month, working)))[0]
    for dept in ("Eng", "Ops"):
        pipeline = main.department_trend_pipeline(dept, date(2026, 6, 20), date(2026, 7, 10))
        out[f"trend {dept}"] = list(mongo.employees.aggregate(pipeline))
    return out


def served(api):
    out = {}
    for month in ("2026-06", "2026-07"):
        out[f"summary {month}"] = api.get("/analytics/departments/summary", params={"month": month}).json()["items"]
        out[f"leaderboard {month}"] = api.get("/analytics/leaderboard/late", params={"month": month, "limit": 10}).json()["items"]
        out[f"monthly {month}"] = api.get("/analytics/employees/EMP0001/monthly", params={"month": month}).json()
    for dept in ("Eng", "Ops"):
        r = api.get(f"/analytics/departments/{dept}/trend", params={"from": "2026-06-20", "to": "2026-07-10"})
        out[f"trend {dept}"] = r.json()["items"] if r.status_code == 200 else r.status_code
    return out


def test_every_kind_of_write_is_seen_by_the_very_next_report(cached, mongo):
    api = cached
    for i, dept in ((1, "Eng"), (2, "Eng"), (3, "Ops")):
        make_employee(api, f"EMP000{i}", dept=dept, joined_on="2026-01-01")
    mongo.attendance_logs.insert_many([log("EMP0001", "2026-06-29", late=12), log("EMP0002", "2026-07-01", late=30),
                                       log("EMP0003", "2026-07-02", status="LEAVE")])
    assert served(api) == expected(mongo)
    hits = counter(api, "report_cache_hits_total")
    assert served(api) == expected(mongo)  # nothing changed: served from the cache
    assert counter(api, "report_cache_hits_total") >= hits + 7

    june_id = mongo.attendance_logs.find_one({"date": "2026-06-29"})["_id"]
    july_id = mongo.attendance_logs.find_one({"date": "2026-07-01"})["_id"]
    fix = {"reason": "drill", "regularized_by": "hr"}
    writes = [
        ("external insert, July", lambda: mongo.attendance_logs.insert_one(log("EMP0001", "2026-07-03", late=45))),
        ("external update, June", lambda: mongo.attendance_logs.update_one({"_id": june_id}, {"$set": {"late_minutes": 50}})),
        ("date moved June -> July", lambda: mongo.attendance_logs.update_one({"_id": june_id}, {"$set": {"date": "2026-07-06"}})),
        ("date moved back", lambda: mongo.attendance_logs.update_one({"_id": june_id}, {"$set": {"date": "2026-06-29"}})),
        ("replace", lambda: mongo.attendance_logs.replace_one({"_id": july_id}, log("EMP0002", "2026-06-30", late=5))),
        ("delete", lambda: mongo.attendance_logs.delete_one({"_id": july_id})),
        ("malformed date", lambda: mongo.attendance_logs.insert_one(log("EMP0002", "2026-07-07T00:00", late=7))),
        ("employee moves department", lambda: mongo.employees.update_one({"emp_code": "EMP0002"}, {"$set": {"department": "Ops"}})),
        ("employee joins later", lambda: mongo.employees.update_one({"emp_code": "EMP0003"}, {"$set": {"joined_on": "2026-07-15"}})),
        ("API punch-in", lambda: api.post("/attendance/punch-in", json={"emp_code": "EMP0003", "punched_at": ist_ms(2026, 6, 22, 10, 0)})),
        ("API punch-out", lambda: api.post("/attendance/punch-out", json={"emp_code": "EMP0003", "punched_at": ist_ms(2026, 6, 22, 12, 0)})),
        ("API correction", lambda: api.patch("/attendance/EMP0003/2026-06-22", json={**fix, "status": "ABSENT"})),
        ("API keyed punch-in (transaction)", lambda: api.post("/attendance/punch-in", headers={"Idempotency-Key": "c-1"},
                                                          json={"emp_code": "EMP0001", "punched_at": ist_ms(2026, 7, 8, 9, 55)})),
    ]
    for what, write in writes:
        write()
        assert served(api) == expected(mongo), what

    with mongo.client.start_session() as session:  # an external multi-document transaction
        with session.start_transaction():
            mongo.attendance_logs.insert_one(log("EMP0002", "2026-07-09", late=20), session=session)
            mongo.attendance_logs.update_one({"_id": june_id}, {"$set": {"late_minutes": 1}}, session=session)
    assert served(api) == expected(mongo), "transaction"


def test_a_write_in_one_month_keeps_the_other_months_cached(cached, mongo):
    api = cached
    make_employee(api, "EMP0001")
    mongo.attendance_logs.insert_one(log("EMP0001", "2026-06-10", late=15))
    june = {"month": "2026-06"}
    first = api.get("/analytics/departments/summary", params=june).json()
    hits = counter(api, "report_cache_hits_total")
    mongo.attendance_logs.insert_one(log("EMP0001", "2026-07-10", late=99))  # July only
    assert api.get("/analytics/departments/summary", params=june).json() == first
    assert counter(api, "report_cache_hits_total") == hits + 1
    mongo.attendance_logs.update_one({"date": "2026-06-10"}, {"$set": {"late_minutes": 16}})  # June: recomputed
    assert api.get("/analytics/departments/summary", params=june).json()["items"][0]["total_late_minutes"] == 16


def test_read_your_writes_under_a_concurrent_writer(cached, mongo):
    """Each acknowledged write must be visible to every report that starts after it (linearizability)."""
    api = cached
    make_employee(api, "EMP0001")
    mongo.attendance_logs.insert_one(log("EMP0001", "2026-07-01", late=1))
    acknowledged, stop, failures = [1], threading.Event(), []

    def writer():
        value = 1
        while not stop.is_set():
            value += 1
            mongo.attendance_logs.update_one({"date": "2026-07-01"}, {"$set": {"late_minutes": value}})
            acknowledged[0] = value

    def reader():
        while not stop.is_set():
            floor = acknowledged[0]
            items = api.get("/analytics/leaderboard/late", params={"month": "2026-07"}).json()["items"]
            if items[0]["total_late_minutes"] < floor:
                failures.append((floor, items[0]["total_late_minutes"]))

    threads = [threading.Thread(target=writer)] + [threading.Thread(target=reader) for _ in range(6)]
    for t in threads:
        t.start()
    threading.Event().wait(4)
    stop.set()
    for t in threads:
        t.join()
    assert not failures, failures[:5]
