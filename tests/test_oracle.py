"""Differential test: every analytics endpoint against an independent pure-Python oracle written from the contract
text, on random datasets that contain every awkward case the contract names (ties at the cutoff, employees without
logs, weekend records, mid-month and later joiners, legacy records, logs of unknown employees, empty departments).

The oracle shares no code with the app: it works on the raw documents with Decimal arithmetic."""
import calendar
import os
import random
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal

import pytest

PRESENCE = ("PRESENT", "WFH", "ON_DUTY")
DEPARTMENTS = ("Eng", "Sales", "Ops", "Support")
MONTHS = ("2026-06", "2026-07", "2026-08")


def half_up(value, places):
    return None if value is None else float(Decimal(value).quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP))


def build_dataset(seed: int):
    rnd = random.Random(seed)
    employees = []
    for i in range(1, 41):
        joined = rnd.choice(["2025-11-03", "2026-01-15", "2026-06-17", "2026-07-15", "2026-07-18", "2026-08-03", "2026-12-01", None])
        doc = {"emp_code": f"EMP{i:04d}", "name": f"N{i}", "email": f"e{i}@x.io", "department": rnd.choice(DEPARTMENTS),
               "shift_start": "09:30", "shift_end": "18:30", "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc)}
        if joined:
            doc["joined_on"] = joined
        employees.append(doc)
    employees.append({"emp_code": "EMP0900", "name": "Later", "department": "Empty", "joined_on": "2027-01-01"})
    codes = [e["emp_code"] for e in employees if e["department"] != "Empty"]
    silent = set(rnd.sample(codes, 5))  # employees with no logs at all
    logs = []
    first = date(2026, 6, 1)
    for code in [c for c in codes if c not in silent] + ["EMP7777"]:  # EMP7777 does not exist
        for offset in range(92):
            if rnd.random() < 0.3:
                continue  # a day with no record at all
            day = (first + timedelta(days=offset)).isoformat()
            status = rnd.choices(["PRESENT", "WFH", "ON_DUTY", "ABSENT", "LEAVE"], [6, 2, 1, 1, 1])[0]
            doc = {"emp_code": code, "date": day, "status": status}
            if status in PRESENCE:
                hours = rnd.choice([None, 3.5, 4.49, 4.5, 8.0, 9.12, 9.125, 1.005, 7.33])
                doc.update(late_minutes=rnd.choice([0, 0, 0, 15, 30, 45]),  # few distinct values: many ties
                           work_hours=hours, overtime_minutes=rnd.choice([0, 0, 30, 45]),
                           half_day=hours is not None and hours < 4.5)
            else:
                doc.update(work_hours=None, late_minutes=0, overtime_minutes=0, half_day=False)
            legacy = rnd.random()
            if legacy < 0.1:
                doc.pop("half_day")
            elif legacy < 0.15 and doc["late_minutes"] == 0:
                doc.pop("late_minutes")
            elif legacy > 0.3:
                doc["history"] = []
            logs.append(doc)
    return employees, logs


class Oracle:
    def __init__(self, employees, logs):
        self.employees = {e["emp_code"]: e for e in employees}
        self.logs = logs

    @staticmethod
    def bounds(month):
        y, m = map(int, month.split("-"))
        return f"{month}-01", f"{month}-{calendar.monthrange(y, m)[1]:02d}"

    @staticmethod
    def present(log):
        if log["status"] not in PRESENCE:
            return Decimal(0)
        return Decimal("0.5") if log.get("half_day") is True else Decimal(1)

    @staticmethod
    def weekday(day):
        return date.fromisoformat(day).weekday() < 5

    def joined_by(self, emp, day):
        return emp.get("joined_on") is not None and emp["joined_on"] <= day

    def monthly(self, code, month):
        start, end = self.bounds(month)
        logs = [l for l in self.logs if l["emp_code"] == code and start <= l["date"] <= end]
        joined = self.employees[code].get("joined_on")
        first = max(date.fromisoformat(start), date.fromisoformat(joined)) if joined else date.fromisoformat(start)
        last = date.fromisoformat(end)
        working = sum(1 for i in range((last - first).days + 1) if (first + timedelta(days=i)).weekday() < 5) if first <= last else 0
        present = sum((self.present(l) for l in logs if self.weekday(l["date"])), Decimal(0))
        return {"emp_code": code, "month": month, "working_days": working, "present_days": half_up(present, 2),
                "leave_days": sum(l["status"] == "LEAVE" for l in logs),
                "late_count": sum(l.get("late_minutes", 0) > 0 for l in logs),
                "total_late_minutes": sum(l.get("late_minutes", 0) for l in logs),
                "total_overtime_minutes": sum(l.get("overtime_minutes", 0) for l in logs),
                "attendance_pct": None if working == 0 else half_up(present / working * 100, 2)}

    def summary(self, month, department=None):
        start, end = self.bounds(month)
        rows = []
        for dept in sorted({e["department"] for e in self.employees.values()}):
            if department is not None and dept != department:
                continue
            members = [e for e in self.employees.values() if e["department"] == dept]
            headcount = sum(self.joined_by(e, end) for e in members)
            if headcount == 0:
                continue
            codes = {e["emp_code"] for e in members}
            logs = [l for l in self.logs if l["emp_code"] in codes and start <= l["date"] <= end]
            hours = [Decimal(str(l["work_hours"])) for l in logs if l["status"] in PRESENCE and l.get("work_hours") is not None]
            rows.append({"department": dept, "headcount": headcount,
                         "present_days": half_up(sum((self.present(l) for l in logs if self.weekday(l["date"])), Decimal(0)), 2),
                         "avg_work_hours": half_up(sum(hours) / len(hours), 2) if hours else None,
                         "late_count": sum(l.get("late_minutes", 0) > 0 for l in logs),
                         "total_late_minutes": sum(l.get("late_minutes", 0) for l in logs),
                         "leave_count": sum(l["status"] == "LEAVE" for l in logs),
                         "on_duty_count": sum(l["status"] == "ON_DUTY" for l in logs)})
        return rows

    def leaderboard(self, month, limit, department=None):
        start, end = self.bounds(month)
        totals = {}
        for l in self.logs:
            emp = self.employees.get(l["emp_code"])
            if emp is None or not start <= l["date"] <= end or l.get("late_minutes", 0) <= 0:
                continue
            if department is not None and emp["department"] != department:
                continue
            t = totals.setdefault(l["emp_code"], [0, 0])
            t[0] += l["late_minutes"]
            t[1] += 1
        ordered = sorted(totals.items(), key=lambda kv: (-kv[1][0], kv[0]))
        rows = []
        for code, (minutes, count) in ordered:
            rank = 1 + sum(1 for _, (other, _) in ordered if other > minutes)  # standard competition ranking
            if rank <= limit:
                emp = self.employees[code]
                rows.append({"rank": rank, "emp_code": code, "name": emp["name"], "department": emp["department"],
                             "total_late_minutes": minutes, "late_count": count})
        return rows

    def trend(self, department, start, end):
        members = [e for e in self.employees.values() if e["department"] == department]
        codes = {e["emp_code"] for e in members}
        rows, rates = [], []
        first, last = date.fromisoformat(start), date.fromisoformat(end)
        for i in range((last - first).days + 1):
            day = (first + timedelta(days=i)).isoformat()
            headcount = sum(self.joined_by(e, day) for e in members)
            logs = [l for l in self.logs if l["date"] == day and l["emp_code"] in codes]
            present = sum((self.present(l) for l in logs), Decimal(0))
            working = self.weekday(day)
            rate = half_up(present / headcount, 4) if working and headcount > 0 else None
            rates.append(rate)
            window = [Decimal(str(r)) for r in rates[-7:] if r is not None]
            rows.append({"date": day, "is_working_day": working, "headcount": headcount, "present_count": float(present),
                         "late_count": sum(l.get("late_minutes", 0) > 0 for l in logs), "attendance_rate": rate,
                         "moving_avg_7d": half_up(sum(window) / len(window), 4) if window else None})
        return rows


def numbers_as_float(value):
    if isinstance(value, list):
        return [numbers_as_float(v) for v in value]
    if isinstance(value, dict):
        return {k: numbers_as_float(v) for k, v in value.items()}
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return value


def assert_same(got, expected, what):
    got, expected = numbers_as_float(got), numbers_as_float(expected)
    if got != expected:
        diff = next(((g, e) for g, e in zip(got, expected) if g != e), (len(got), len(expected))) if isinstance(got, list) else (got, expected)
        pytest.fail(f"{what}: first difference got={diff[0]} expected={diff[1]}")


@pytest.mark.parametrize("seed", range(1, 1 + int(os.environ.get("ORACLE_SEEDS", "4"))))  # ORACLE_SEEDS=40 locally
def test_analytics_match_the_oracle(api, mongo, seed):
    employees, logs = build_dataset(seed)
    mongo.employees.insert_many([dict(e) for e in employees])
    mongo.attendance_logs.insert_many([dict(l) for l in logs])
    oracle = Oracle(employees, logs)
    rnd = random.Random(seed)
    codes = [e["emp_code"] for e in employees]

    for month in MONTHS:
        for code in rnd.sample(codes, 8):
            got = api.get(f"/analytics/employees/{code}/monthly", params={"month": month}).json()
            assert_same(got, oracle.monthly(code, month), f"monthly {code} {month}")
        for department in (None, *DEPARTMENTS, "Empty"):
            params = {"month": month, **({"department": department} if department else {})}
            assert_same(api.get("/analytics/departments/summary", params=params).json()["items"],
                        oracle.summary(month, department), f"summary {params}")
            for limit in (1, 3, 10):
                got = api.get("/analytics/leaderboard/late", params={**params, "limit": limit}).json()["items"]
                assert_same(got, oracle.leaderboard(month, limit, department), f"leaderboard {params} limit={limit}")

    for department in DEPARTMENTS:
        for start, end in (("2026-07-01", "2026-07-31"), ("2026-06-10", "2026-09-09"), ("2026-07-04", "2026-07-04"),
                           ("2026-08-25", "2026-09-15")):
            got = api.get(f"/analytics/departments/{department}/trend", params={"from": start, "to": end}).json()["items"]
            assert_same(got, oracle.trend(department, start, end), f"trend {department} {start}..{end}")
    assert_same(api.get("/analytics/departments/Empty/trend", params={"from": "2026-07-01", "to": "2026-07-03"}).json()["items"],
                oracle.trend("Empty", "2026-07-01", "2026-07-03"), "trend of a department nobody has joined")
