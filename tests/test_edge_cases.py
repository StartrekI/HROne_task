"""Edge cases chosen to make an implementation fail: one test per place where a plausible-looking implementation
gives a wrong answer. Every expected value is worked out by hand from the contract text (openapi.yaml R1 - R10 and
the endpoint descriptions), not taken from an oracle or from the app. Calendar: 2026-07-01 is a Wednesday."""
from datetime import datetime, timezone

import pytest

from tests.conftest import ist_ms, make_employee

FIX = {"reason": "edge case", "regularized_by": "hr"}


def punch_in(api, code, ms, **extra):
    return api.post("/attendance/punch-in", json={"emp_code": code, "punched_at": ms, **extra})


def punch_out(api, code, ms):
    return api.post("/attendance/punch-out", json={"emp_code": code, "punched_at": ms})


def utc(y, mo, d, h=0, mi=0):
    return datetime(y, mo, d, h, mi, tzinfo=timezone.utc)


def seed_log(mongo, code, day, status="PRESENT", late=0, hours=8.0, half=False, overtime=0):
    present = status in ("PRESENT", "WFH", "ON_DUTY")
    mongo.attendance_logs.insert_one({
        "emp_code": code, "date": day, "status": status, "punch_in": utc(2026, 7, 1, 4) if present else None,
        "punch_out": None, "work_hours": hours if present else None, "late_minutes": late, "overtime_minutes": overtime,
        "half_day": half, "history": []})


# --------------------------------------------------------------------------- R1 attendance date, R2 late
@pytest.mark.parametrize("when, date, late", [
    ((2026, 7, 7, 6, 0, 0, 0), "2026-07-07", 0),      # exactly shift_end: not "earlier than" it, so the same day
    ((2026, 7, 8, 5, 59, 59, 999), "2026-07-07", 479),  # earlier than shift_end: the shift that started at 22:00 the day before
    ((2026, 7, 7, 22, 10, 0, 999), "2026-07-07", 0),  # truncated to 22:10:00 = exactly 10:00 after start: not late
    ((2026, 7, 7, 22, 10, 1, 0), "2026-07-07", 10),   # strictly more than 10:00: minutes since shift_start
    ((2026, 7, 7, 21, 55, 0, 0), "2026-07-07", 0),    # before the shift starts
])
def test_overnight_shift_attendance_date_and_late(api, when, date, late):
    make_employee(api, "EMP0001", shift_start="22:00", shift_end="06:00")
    r = punch_in(api, "EMP0001", ist_ms(*when))
    assert r.status_code == 201 and (r.json()["date"], r.json()["late_minutes"]) == (date, late)


@pytest.mark.parametrize("shift, when, date, late", [
    (("09:30", "18:30"), (2026, 7, 7, 23, 59, 59, 999), "2026-07-07", 869),  # 14:29:59 after 09:30
    (("09:30", "18:30"), (2026, 7, 8, 0, 0, 0, 0), "2026-07-08", 0),          # a new IST day, before the shift
    (("00:00", "08:00"), (2026, 7, 7, 0, 10, 1, 0), "2026-07-07", 10),        # end > start: not an overnight shift
    (("00:00", "08:00"), (2026, 7, 6, 23, 50, 0, 0), "2026-07-06", 1430),     # its own IST date, 23:50 after 00:00
])
def test_day_shift_attendance_date_and_late(api, shift, when, date, late):
    make_employee(api, "EMP0001", shift_start=shift[0], shift_end=shift[1])
    r = punch_in(api, "EMP0001", ist_ms(*when))
    assert r.status_code == 201 and (r.json()["date"], r.json()["late_minutes"]) == (date, late)


def test_instants_are_truncated_before_storing_and_returning(api, mongo):
    make_employee(api, "EMP0001")
    r = punch_in(api, "EMP0001", ist_ms(2026, 7, 7, 9, 40, 0, 900))
    assert r.json()["punch_in"] == ist_ms(2026, 7, 7, 9, 40) and r.json()["late_minutes"] == 0
    assert mongo.attendance_logs.find_one()["punch_in"].microsecond == 0


# --------------------------------------------------------------------------- R3 overtime, R4 work hours, R5 half day
@pytest.mark.parametrize("out, overtime", [((18, 59, 59), 0), ((19, 0, 0), 30), ((19, 0, 59), 30), ((17, 0, 0), 0)])
def test_overtime_threshold(api, out, overtime):
    make_employee(api, "EMP0001")
    punch_in(api, "EMP0001", ist_ms(2026, 7, 7, 9, 30))
    r = punch_out(api, "EMP0001", ist_ms(2026, 7, 7, *out))
    assert r.status_code == 200 and r.json()["overtime_minutes"] == overtime


def test_overnight_overtime_and_hours(api):
    make_employee(api, "EMP0001", shift_start="22:00", shift_end="06:00")
    punch_in(api, "EMP0001", ist_ms(2026, 7, 6, 21, 55))
    r = punch_out(api, "EMP0001", ist_ms(2026, 7, 7, 6, 40))
    assert (r.json()["date"], r.json()["overtime_minutes"], r.json()["work_hours"]) == ("2026-07-06", 40, 8.75)


@pytest.mark.parametrize("seconds, hours, half", [
    (16182, 4.5, False),    # 4.495 h: half-up -> 4.50, and the ROUNDED value is not < 4.50
    (16181, 4.49, True),    # 4.49472 h
    (16199, 4.5, False),    # 4.49972 h
    (16146, 4.49, True),    # 4.485 h: half-up 4.49; half-to-even would give 4.48
    (4050, 1.13, True),     # 1.125 h: half-up 1.13; half-to-even would give 1.12
    (1, 0.0, True),
    (32400, 9.0, False),
])
def test_work_hours_half_up_and_half_day_on_the_rounded_value(api, seconds, hours, half):
    make_employee(api, "EMP0001")
    start = ist_ms(2026, 7, 7, 9, 0)
    punch_in(api, "EMP0001", start)
    r = punch_out(api, "EMP0001", start + seconds * 1000)
    assert r.status_code == 200 and (r.json()["work_hours"], r.json()["half_day"]) == (hours, half)


def test_punch_out_window(api):
    make_employee(api, "EMP0001")
    start = ist_ms(2026, 7, 6, 9, 0, 0, 100)
    punch_in(api, "EMP0001", start)
    assert punch_out(api, "EMP0001", ist_ms(2026, 7, 6, 9, 0, 0, 900)).status_code == 422  # same second once truncated
    assert punch_out(api, "EMP0001", ist_ms(2026, 7, 7, 9, 0, 1)).status_code == 422       # 24 h + 1 s
    r = punch_out(api, "EMP0001", ist_ms(2026, 7, 7, 9, 0))                                 # exactly 24 h is allowed
    assert r.status_code == 200 and (r.json()["work_hours"], r.json()["overtime_minutes"]) == (24.0, 870)


def test_punch_out_closes_the_latest_punch_in_before_it_only(api):
    make_employee(api, "EMP0001")
    punch_in(api, "EMP0001", ist_ms(2026, 7, 6, 9, 0))
    punch_out(api, "EMP0001", ist_ms(2026, 7, 6, 18, 0))
    punch_in(api, "EMP0001", ist_ms(2026, 7, 7, 9, 0))
    # Latest punch-in at or before 20:00 on the 6th is the 6th, already closed: 409, not "close the 7th".
    assert punch_out(api, "EMP0001", ist_ms(2026, 7, 6, 20, 0)).status_code == 409
    assert punch_out(api, "EMP0001", ist_ms(2026, 7, 6, 8, 0)).status_code == 404  # no punch-in before it
    assert punch_out(api, "EMP0999", ist_ms(2026, 7, 6, 8, 0)).status_code == 404  # unknown employee
    assert punch_out(api, "EMP0001", ist_ms(2026, 7, 7, 18, 0)).status_code == 200


# --------------------------------------------------------------------------- PATCH: recompute, history, rules
def test_patch_history_lists_exactly_the_fields_that_changed(api):
    make_employee(api, "EMP0001")
    punch_in(api, "EMP0001", ist_ms(2026, 7, 7, 9, 30))
    punch_out(api, "EMP0001", ist_ms(2026, 7, 7, 18, 30))
    r = api.patch("/attendance/EMP0001/2026-07-07", json={**FIX, "status": "WFH"})
    assert r.json()["history"][0]["changes"] == {"status": {"from": "PRESENT", "to": "WFH"}}  # derived values unchanged

    make_employee(api, "EMP0002")
    punch_in(api, "EMP0002", ist_ms(2026, 7, 7, 10, 5))
    r = api.patch("/attendance/EMP0002/2026-07-07", json={**FIX, "punch_out": ist_ms(2026, 7, 7, 19, 10)})
    entry = r.json()["history"][0]
    assert entry["changes"] == {"punch_out": {"from": None, "to": ist_ms(2026, 7, 7, 19, 10)},
                                "work_hours": {"from": None, "to": 9.08}, "overtime_minutes": {"from": 0, "to": 40}}
    assert entry["by"] == "hr" and entry["reason"] == "edge case" and isinstance(entry["at"], int)
    assert r.json()["late_minutes"] == 35  # untouched by a punch_out-only correction


def test_patch_from_absent_to_present_recomputes_everything(api, mongo):
    make_employee(api, "EMP0001")
    seed_log(mongo, "EMP0001", "2026-07-07", status="ABSENT")
    url = "/attendance/EMP0001/2026-07-07"
    assert api.patch(url, json={**FIX, "status": "PRESENT"}).status_code == 422  # presence needs a punch_in
    r = api.patch(url, json={**FIX, "status": "PRESENT", "punch_in": ist_ms(2026, 7, 7, 9, 45), "punch_out": ist_ms(2026, 7, 7, 13, 0)})
    body = r.json()
    assert (body["late_minutes"], body["work_hours"], body["half_day"], body["overtime_minutes"]) == (15, 3.25, True, 0)
    assert set(body["history"][0]["changes"]) == {"status", "punch_in", "punch_out", "work_hours", "late_minutes", "half_day"}


def test_patch_rules(api):
    make_employee(api, "EMP0001")
    punch_in(api, "EMP0001", ist_ms(2026, 7, 7, 9, 30))
    url = "/attendance/EMP0001/2026-07-07"
    assert api.patch(url, json={**FIX, "punch_in": ist_ms(2026, 7, 8, 9, 30)}).status_code == 422  # another attendance date
    assert api.patch(url, json={**FIX, "status": "ABSENT", "punch_in": ist_ms(2026, 7, 7, 9, 0)}).status_code == 422
    assert api.patch(url, json={**FIX, "punch_in": None}).status_code == 422  # may be omitted, not null
    assert api.patch(url, json={**FIX, "punch_out": ist_ms(2026, 7, 7, 9, 0)}).status_code == 422  # before punch_in
    assert api.patch(url, json={**FIX, "status": "PRESENT"}).status_code == 422  # changes nothing
    assert api.patch(url, json={"status": "WFH", "reason": "four", "regularized_by": "hr"}).status_code == 422  # reason < 5
    assert api.patch("/attendance/EMP0999/2026-07-07", json={**FIX, "status": "WFH"}).status_code == 404
    assert api.patch("/attendance/EMP0001/2026-07-08", json={**FIX, "status": "WFH"}).status_code == 404
    assert api.patch("/attendance/EMP0001/2026-02-30", json={**FIX, "status": "WFH"}).status_code == 422
    r = api.patch(url, json={**FIX, "status": "LEAVE"})
    assert {k: r.json()[k] for k in ("punch_in", "punch_out", "work_hours", "late_minutes", "half_day")} == \
        {"punch_in": None, "punch_out": None, "work_hours": None, "late_minutes": 0, "half_day": False}


# --------------------------------------------------------------------------- monthly (R7, R8)
@pytest.mark.parametrize("joined, month, working", [
    ("2026-07-11", "2026-07", 15),  # joined on a Saturday: counted from the next Monday
    ("2026-07-31", "2026-07", 1),
    ("2026-08-01", "2026-07", 0),
    ("2020-01-01", "2028-02", 21),  # leap February
])
def test_monthly_working_days(api, joined, month, working):
    make_employee(api, "EMP0001", joined_on=joined)
    body = api.get("/analytics/employees/EMP0001/monthly", params={"month": month}).json()
    assert body["working_days"] == working
    if working == 0:
        assert body["attendance_pct"] is None


def test_monthly_half_up_percentage_and_weekend_records(api, mongo):
    make_employee(api, "EMP0001", joined_on="2026-07-10")  # 16 working days in July
    seed_log(mongo, "EMP0001", "2026-07-14", half=True, hours=4.0)              # Tue: 0.5 present
    seed_log(mongo, "EMP0001", "2026-07-11", late=20, overtime=45)              # Sat: not present, but late and overtime count
    seed_log(mongo, "EMP0001", "2026-07-15", status="LEAVE")
    body = api.get("/analytics/employees/EMP0001/monthly", params={"month": "2026-07"}).json()
    # 0.5 / 16 * 100 = 3.125 -> 3.13 half-up (banker's rounding would give 3.12)
    assert body == {"emp_code": "EMP0001", "month": "2026-07", "working_days": 16, "present_days": 0.5, "leave_days": 1,
                    "late_count": 1, "total_late_minutes": 20, "total_overtime_minutes": 45, "attendance_pct": 3.13}


# --------------------------------------------------------------------------- department summary (R8, R9)
def test_summary_average_over_records_and_headcount(api, mongo):
    make_employee(api, "EMP0001", dept="Eng")
    make_employee(api, "EMP0002", dept="Eng")                         # no logs: still in the headcount
    make_employee(api, "EMP0003", dept="Eng", joined_on="2026-08-01")  # joins later: not in July's headcount
    make_employee(api, "EMP0004", dept="Later", joined_on="2026-08-01")  # the whole department joins later: omitted
    seed_log(mongo, "EMP0001", "2026-07-06", hours=9.12)
    seed_log(mongo, "EMP0001", "2026-07-07", hours=9.13)
    seed_log(mongo, "EMP0003", "2026-07-08", hours=9.12)
    seed_log(mongo, "EMP0004", "2026-07-08", hours=1.0)
    mongo.attendance_logs.insert_one({"emp_code": "EMP0001", "date": "2026-07-08", "status": "WFH",  # open record
                                      "punch_in": utc(2026, 7, 8, 4), "punch_out": None, "work_hours": None, "late_minutes": 0})
    items = api.get("/analytics/departments/summary", params={"month": "2026-07"}).json()["items"]
    # (9.12 + 9.13 + 9.12) / 3 = 9.1233 -> 9.12; present 4 (the open record counts as present, not in the average)
    assert items == [{"department": "Eng", "headcount": 2, "present_days": 4.0, "avg_work_hours": 9.12, "late_count": 0,
                      "total_late_minutes": 0, "leave_count": 0, "on_duty_count": 0}]
    assert api.get("/analytics/departments/summary", params={"month": "2026-07", "department": "Later"}).json()["items"] == []


def test_summary_average_counts_only_present_records_with_a_value(api, mongo):
    """Found by the mutation campaign (no test noticed when the present-status condition was dropped): a stray
    work_hours on an ABSENT / LEAVE record must not enter the average ("present-status AND have a work_hours value")."""
    make_employee(api, "EMP0001", dept="Eng")
    seed_log(mongo, "EMP0001", "2026-07-06", hours=8.0)
    mongo.attendance_logs.insert_many([
        {"emp_code": "EMP0001", "date": "2026-07-07", "status": "ABSENT", "punch_in": None, "punch_out": None, "work_hours": 2.0,
         "late_minutes": 0, "overtime_minutes": 0, "half_day": False, "history": []},
        {"emp_code": "EMP0001", "date": "2026-07-08", "status": "LEAVE", "work_hours": 1.0, "late_minutes": 0}])
    item = api.get("/analytics/departments/summary", params={"month": "2026-07"}).json()["items"][0]
    assert (item["avg_work_hours"], item["present_days"], item["leave_count"]) == (8.0, 1.0, 1)


def test_summary_average_tie_rounds_half_up(api, mongo):
    make_employee(api, "EMP0001", dept="Eng")
    seed_log(mongo, "EMP0001", "2026-07-06", hours=9.12)
    seed_log(mongo, "EMP0001", "2026-07-07", hours=9.13)
    items = api.get("/analytics/departments/summary", params={"month": "2026-07"}).json()["items"]
    assert items[0]["avg_work_hours"] == 9.13  # 9.125 -> 9.13 half-up (float or banker's rounding would give 9.12)


# --------------------------------------------------------------------------- leaderboard
def test_leaderboard_competition_ranking_and_limit_after_ranking(api, mongo):
    for code, dept in (("EMP0001", "Eng"), ("EMP0002", "Sales"), ("EMP0003", "Eng"), ("EMP0004", "Eng"), ("EMP0005", "Eng")):
        make_employee(api, code, dept=dept)
    for code, late in (("EMP0001", 50), ("EMP0003", 40), ("EMP0002", 40), ("EMP0004", 30), ("EMP0999", 100)):  # EMP0999: unknown
        seed_log(mongo, code, "2026-07-06", late=late)
    seed_log(mongo, "EMP0005", "2026-07-06", late=0)

    def board(**params):
        items = api.get("/analytics/leaderboard/late", params={"month": "2026-07", **params}).json()["items"]
        return [(i["rank"], i["emp_code"]) for i in items]

    assert board(limit=1) == [(1, "EMP0001")]
    assert board(limit=2) == [(1, "EMP0001"), (2, "EMP0002"), (2, "EMP0003")]  # the tie at the cutoff is kept, by emp_code
    assert board(limit=3) == [(1, "EMP0001"), (2, "EMP0002"), (2, "EMP0003")]  # rank 3 is skipped
    assert board(limit=4) == [(1, "EMP0001"), (2, "EMP0002"), (2, "EMP0003"), (4, "EMP0004")]
    assert board(limit=10, department="Sales") == [(1, "EMP0002")]  # ranked inside the department
    assert api.get("/analytics/leaderboard/late", params={"month": "2026-07", "limit": 51}).status_code == 422


# --------------------------------------------------------------------------- trend
def test_trend_rows_rates_and_moving_average(api, mongo):
    make_employee(api, "EMP0001", dept="Ops", joined_on="2026-07-01")
    make_employee(api, "EMP0002", dept="Ops", joined_on="2026-07-08")
    seed_log(mongo, "EMP0001", "2026-07-04")              # Saturday: present_count 1, rate null
    seed_log(mongo, "EMP0001", "2026-07-06")
    seed_log(mongo, "EMP0001", "2026-07-07", half=True)   # 0.5
    seed_log(mongo, "EMP0001", "2026-07-09", late=12)
    seed_log(mongo, "EMP0002", "2026-07-09")
    rows = api.get("/analytics/departments/Ops/trend", params={"from": "2026-07-04", "to": "2026-07-10"}).json()["items"]
    assert [(r["date"], r["is_working_day"], r["headcount"], r["present_count"], r["late_count"], r["attendance_rate"], r["moving_avg_7d"])
            for r in rows] == [
        ("2026-07-04", False, 1, 1.0, 0, None, None),
        ("2026-07-05", False, 1, 0.0, 0, None, None),
        ("2026-07-06", True, 1, 1.0, 0, 1.0, 1.0),
        ("2026-07-07", True, 1, 0.5, 0, 0.5, 0.75),
        ("2026-07-08", True, 2, 0.0, 0, 0.0, 0.5),      # a working day without records: rate 0.0, not null
        ("2026-07-09", True, 2, 2.0, 1, 1.0, 0.625),
        ("2026-07-10", True, 2, 0.0, 0, 0.0, 0.5),
    ]


def test_trend_moving_average_window_is_the_current_row_and_six_before(api, mongo):
    """Over 12 days the window must drop old rows: 7 rows, not 8 and not all."""
    make_employee(api, "EMP0001", dept="Ops", joined_on="2026-07-01")
    for day in ("2026-07-06", "2026-07-07"):  # Mon, Tue present; every later working day absent (no record)
        seed_log(mongo, "EMP0001", day)
    rows = api.get("/analytics/departments/Ops/trend", params={"from": "2026-07-06", "to": "2026-07-17"}).json()["items"]
    # rates: 06 1, 07 1, 08 0, 09 0, 10 0, 11 -, 12 -, 13 0, 14 0, 15 0, 16 0, 17 0
    # 13th: window 07..13 = rates 1,0,0,0,0 -> 0.2; 14th: window 08..14 = 0,0,0,0,0 -> 0.0 (the 7th has left it)
    by_day = {r["date"]: r["moving_avg_7d"] for r in rows}
    assert (by_day["2026-07-10"], by_day["2026-07-12"], by_day["2026-07-13"], by_day["2026-07-14"]) == (0.4, 0.4, 0.2, 0.0)


def test_trend_four_decimal_half_up_and_range_rules(api, mongo):
    for i in (1, 2, 3):
        make_employee(api, f"EMP000{i}", dept="Ops")
    seed_log(mongo, "EMP0001", "2026-07-06")
    seed_log(mongo, "EMP0001", "2026-07-07")
    seed_log(mongo, "EMP0002", "2026-07-07")
    rows = api.get("/analytics/departments/Ops/trend", params={"from": "2026-07-06", "to": "2026-07-07"}).json()["items"]
    assert [(r["attendance_rate"], r["moving_avg_7d"]) for r in rows] == [(0.3333, 0.3333), (0.6667, 0.5)]
    trend = "/analytics/departments/Ops/trend"
    assert len(api.get(trend, params={"from": "2026-07-01", "to": "2026-09-30"}).json()["items"]) == 92
    assert api.get(trend, params={"from": "2026-07-01", "to": "2026-10-01"}).status_code == 422  # 93 days
    assert api.get(trend, params={"from": "2026-07-02", "to": "2026-07-01"}).status_code == 422
    assert len(api.get(trend, params={"from": "2026-07-04", "to": "2026-07-04"}).json()["items"]) == 1
    assert api.get("/analytics/departments/Nope/trend", params={"from": "2026-07-01", "to": "2026-07-02"}).status_code == 404


# --------------------------------------------------------------------------- input validation and pagination
def test_punched_at_may_be_omitted_but_not_null(api):
    """Omitted means now ("Defaults to now"); the schema types it as a non-nullable integer, so null is 422."""
    make_employee(api, "EMP0001")
    assert api.post("/attendance/punch-in", json={"emp_code": "EMP0001", "punched_at": None}).status_code == 422
    assert api.post("/attendance/punch-out", json={"emp_code": "EMP0001", "punched_at": None}).status_code == 422
    assert api.post("/attendance/punch-in", json={"emp_code": "EMP0001"}).status_code == 201


@pytest.mark.parametrize("value", [99_999_999_999, 4_102_444_800_001, 1783312500000.0, "1783312500000", True, 1783312500])
def test_instants_that_are_not_epoch_milliseconds_are_422(api, value):
    make_employee(api, "EMP0001")
    assert api.post("/attendance/punch-in", json={"emp_code": "EMP0001", "punched_at": value}).status_code == 422


@pytest.mark.parametrize("change", [{"emp_code": "EMP123"}, {"emp_code": "EMP1234567"}, {"emp_code": "emp0001"},
                                    {"shift_start": "09:30", "shift_end": "09:30"}, {"joined_on": "2026-02-30"},
                                    {"shift_start": "9:30"}, {"shift_end": "24:00"}, {"email": "no-at-sign"}, {"name": ""}])
def test_invalid_employees_are_422(api, change):
    body = {"emp_code": "EMP0001", "name": "A", "email": "a@x.io", "department": "Eng", "joined_on": "2026-01-01", **change}
    assert api.post("/employees", json=body).status_code == 422


def test_pagination_bounds_and_filtered_totals(api, mongo):
    for i in range(1, 4):
        make_employee(api, f"EMP000{i}", dept="Eng" if i < 3 else "Ops")
    for params, status in (({"page_size": 100}, 200), ({"page_size": 101}, 422), ({"page_size": 0}, 422), ({"page": 0}, 422)):
        assert api.get("/employees", params=params).status_code == status
    body = api.get("/employees", params={"department": "Eng", "page_size": 1, "page": 2}).json()
    assert (body["total"], [e["emp_code"] for e in body["items"]], body["page"], body["page_size"]) == (2, ["EMP0002"], 2, 1)
    assert api.get("/employees", params={"department": "Eng", "page": 9}).json()["items"] == []
    seed_log(mongo, "EMP0001", "2026-07-06")
    seed_log(mongo, "EMP0002", "2026-07-06", status="LEAVE")
    seed_log(mongo, "EMP0001", "2026-07-07")
    body = api.get("/attendance", params={"date_from": "2026-07-06", "date_to": "2026-07-06"}).json()
    assert body["total"] == 2 and [(r["date"], r["emp_code"]) for r in body["items"]] == [("2026-07-06", "EMP0001"), ("2026-07-06", "EMP0002")]
    body = api.get("/attendance").json()
    assert [(r["date"], r["emp_code"]) for r in body["items"]] == [("2026-07-07", "EMP0001"), ("2026-07-06", "EMP0001"), ("2026-07-06", "EMP0002")]
    assert api.get("/attendance", params={"date_from": "2026-07-07", "date_to": "2026-07-06"}).status_code == 422
    assert api.get("/attendance", params={"date_from": "2026-02-30"}).status_code == 422
    assert api.get("/attendance", params={"status": "present"}).status_code == 422
