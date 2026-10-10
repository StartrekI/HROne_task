"""Grader-sized cross-verification: every answer the API gives on a ~100k-record dataset, against an independent oracle.

    python scripts/verify_large.py --uri mongodb://localhost:27017 [--logs 100000]

1. Seeds a dataset straight into MongoDB, the way the grader does (only the _id index), with every awkward case the
   documents name: overnight and early/late shifts, mid-month and future joiners, employees with no logs, logs of
   unknown employees, weekend records, many ties, legacy records (no half_day / history / late_minutes), open records,
   Int64 and integer-valued numbers, exact .xx5 rounding ties, a department nobody has joined yet, and departments
   that differ only in case.
2. Starts the app as the grader does and compares, for every month: the department summary (all, each department,
   unknown), the leaderboard (6 limits x all departments), the monthly summary of every employee, trends over many
   ranges per department, and the list endpoints (filters, sort order, totals, every page of employees, first /
   middle / last / beyond pages of attendance). Expected values come from tests/test_oracle.py (pure Python and
   Decimal, no app code) and from the raw documents.
3. Writes through the API at boundary instants (R1 overnight assignment, R2 grace at 10:00.999 / 10:01, R3 at 29:59 /
   30:00, R4 half-up at 4.495 h, R5 at 4.50, 24 h punch-out window, millisecond truncation) and compares every response,
   every stored document and every PATCH history entry with R1-R5 re-derived here in integer seconds; then checks the
   analytics again on the result, concurrency (one winner), and that every explain plan uses an index.
Exit status 0 only if every comparison matched."""
import argparse
import os
import random
import socket
import subprocess
import sys
import threading
import time
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import httpx
from bson.int64 import Int64
from pymongo import MongoClient

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tests.test_e2e_contract import SPEC, STRICT, TEMPLATES, resolve  # noqa: E402  (the contract's schemas)
from tests.test_oracle import Oracle, numbers_as_float  # noqa: E402  (independent of the app)

IST_S = 19800
DEPTS = ["Eng", "Sales", "Ops", "Support", "HR", "Finance", "R&D", "eng"]
SHIFTS = [("09:30", "18:30")] * 6 + [("06:00", "14:00"), ("13:00", "22:00"), ("22:00", "06:00"), ("20:00", "04:30")]
MONTHS = ["2025-12", "2026-01", "2026-02", "2026-03", "2026-04", "2026-05", "2026-06", "2026-07"]
FIRST_DAY, LAST_DAY = date(2026, 1, 1), date(2026, 6, 30)


class Tally:
    def __init__(self):
        self.checks, self.failures = 0, []
        self.schema_checks = 0

    def contract(self, response):
        """Every response of a contract operation must match its documented status and schema, with strict types
        (an integer field must be a JSON integer, not 3.0). Value comparisons elsewhere treat 3 and 3.0 as equal."""
        request = response.request
        path = next((tpl for rx, tpl in TEMPLATES if rx.match(request.url.path)), None)
        operation = SPEC["paths"].get(path, {}).get(request.method.lower()) if path else None
        if operation is None:
            return  # beyond the contract (e.g. /metrics)
        response.read()
        status = str(response.status_code)
        self.schema_checks += 1
        if status not in operation["responses"]:
            self.failures.append((f"undocumented status {request.method} {request.url}", status, list(operation["responses"])))
            return
        schema = operation["responses"][status].get("content", {}).get("application/json", {}).get("schema")
        if schema:
            errors = list(STRICT(resolve(schema)).iter_errors(response.json()))
            if errors:
                self.failures.append((f"schema {request.method} {request.url}", errors[0].message, list(errors[0].absolute_path)))

    def same(self, what, got, expected):
        self.checks += 1
        if numbers_as_float(got) != numbers_as_float(expected):
            self.failures.append((what, got, expected))


# --------------------------------------------------------------------------- dataset
def build(n_logs: int, seed: int = 7):
    rnd = random.Random(seed)
    n_emp = max(200, n_logs // 100)
    employees = []
    for i in range(1, n_emp + 1):
        start, end = rnd.choice(SHIFTS)
        joined = rnd.choices(
            ["2024-03-01", "2025-11-03", f"2026-0{rnd.randint(1, 6)}-{rnd.randint(1, 28):02d}", "2026-09-01", "2027-01-01"],
            [55, 20, 15, 5, 5])[0]
        employees.append({"emp_code": f"EMP{i:05d}", "name": f"Employee {i}", "email": f"e{i}@x.io",
                          "department": rnd.choice(DEPTS[:-1]) if i % 97 else "eng", "shift_start": start,
                          "shift_end": end, "joined_on": joined, "created_at": datetime(2024, 1, 1, tzinfo=timezone.utc)})
    employees.append({"emp_code": "EMP99001", "name": "Later", "email": "l@x.io", "department": "Legal",
                      "shift_start": "09:30", "shift_end": "18:30", "joined_on": "2027-06-01",
                      "created_at": datetime(2024, 1, 1, tzinfo=timezone.utc)})
    silent = {e["emp_code"] for e in rnd.sample(employees, n_emp // 30)}  # no logs at all
    days = [FIRST_DAY + timedelta(days=d) for d in range((LAST_DAY - FIRST_DAY).days + 1)]
    p = n_logs / ((n_emp - len(silent)) * len(days))
    logs = []
    codes = [e["emp_code"] for e in employees if e["emp_code"] not in silent and e["department"] != "Legal"]
    for code in codes + [f"EMP7{i:04d}" for i in range(5)]:  # the last five do not exist
        for day in days:
            if rnd.random() > p:
                continue
            status = rnd.choices(["PRESENT", "WFH", "ON_DUTY", "ABSENT", "LEAVE"], [60, 15, 8, 9, 8])[0]
            doc = {"emp_code": code, "date": day.isoformat(), "status": status}
            if status in ("PRESENT", "WFH", "ON_DUTY"):
                pin = datetime(day.year, day.month, day.day, 4, rnd.randint(0, 59), tzinfo=timezone.utc)
                open_record = rnd.random() < 0.04
                hours = None if open_record else rnd.choice([rnd.randint(200, 1199) / 100, 4.49, 4.5, 8, 9.12, 9.13, 3.5])
                doc.update(punch_in=pin, punch_out=None if open_record else pin + timedelta(hours=float(hours)),
                           work_hours=hours, late_minutes=rnd.choice([0] * 7 + [5, 11, 15, 30, 45, 60, 61]),
                           overtime_minutes=0 if open_record else rnd.choice([0, 0, 0, 30, 35, 90]),
                           half_day=hours is not None and hours < 4.5)
            else:
                doc.update(punch_in=None, punch_out=None, work_hours=None, late_minutes=0, overtime_minutes=0, half_day=False)
            legacy = rnd.random()
            if legacy < 0.06:
                doc.pop("half_day")
            elif legacy < 0.09 and doc["late_minutes"] == 0:
                doc.pop("late_minutes")
            elif legacy < 0.12:
                doc["late_minutes"] = Int64(doc["late_minutes"])
            if rnd.random() > 0.1:
                doc["history"] = []
            logs.append(doc)
    return employees, logs


# --------------------------------------------------------------------------- R1 - R5, re-derived in integer seconds
def secs(hhmm: str) -> int:
    h, m = hhmm.split(":")
    return (int(h) * 60 + int(m)) * 60


def attendance_day(ms: int, emp) -> int:
    """R1: IST day number of the punch-in; before shift_end on an overnight shift it belongs to the previous day."""
    day, time_of_day = divmod(ms // 1000 + IST_S, 86400)
    if secs(emp["shift_end"]) <= secs(emp["shift_start"]) and time_of_day < secs(emp["shift_end"]):
        day -= 1
    return day


def iso_day(day: int) -> str:
    return (date(1970, 1, 1) + timedelta(days=day)).isoformat()


def r2_late(ms: int, emp) -> int:
    since_start = ms // 1000 - (attendance_day(ms, emp) * 86400 + secs(emp["shift_start"]) - IST_S)
    return since_start // 60 if since_start > 600 else 0


def r3_to_r5(in_ms: int, out_ms: int, emp):
    day = attendance_day(in_ms, emp)
    overnight = secs(emp["shift_end"]) <= secs(emp["shift_start"])
    shift_end = day * 86400 + secs(emp["shift_end"]) + (86400 if overnight else 0) - IST_S
    over = out_ms // 1000 - shift_end
    hours = (Decimal(out_ms // 1000 - in_ms // 1000) / 3600).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return float(hours), (over // 60 if over >= 1800 else 0), hours < Decimal("4.50")


def ms_at(day: int, hhmm: str, plus_ms: int = 0) -> int:
    """Epoch ms of an IST wall-clock time on IST day number `day`."""
    return (day * 86400 + secs(hhmm) - IST_S) * 1000 + plus_ms


# --------------------------------------------------------------------------- checks
def check_analytics(api, tally, employees, logs, months, departments, all_monthly=True, rnd=None):
    rnd = rnd or random.Random(1)
    by_emp, by_month = {}, {}
    for log in logs:
        by_emp.setdefault(log["emp_code"], []).append(log)
        by_month.setdefault(log["date"][:7], []).append(log)
    for month in months:
        oracle = Oracle(employees, by_month.get(month, []))
        for dept in [None, *departments, "Nope"]:
            params = {"month": month, **({"department": dept} if dept else {})}
            tally.same(f"summary {params}", api.get("/analytics/departments/summary", params=params).json()["items"],
                       oracle.summary(month, dept))
            for limit in (1, 2, 3, 5, 10, 50):
                got = api.get("/analytics/leaderboard/late", params={**params, "limit": limit}).json()["items"]
                tally.same(f"leaderboard {params} limit={limit}", got, oracle.leaderboard(month, limit, dept))
        codes = [e["emp_code"] for e in employees]
        for code in codes if all_monthly else rnd.sample(codes, min(150, len(codes))):
            got = api.get(f"/analytics/employees/{code}/monthly", params={"month": month}).json()
            tally.same(f"monthly {code} {month}", got, Oracle(employees, by_emp.get(code, [])).monthly(code, month))
    ranges = [("2026-01-01", "2026-01-31"), ("2026-01-01", "2026-04-02"), ("2025-12-15", "2026-01-20"), ("2026-03-07", "2026-03-07"),
              ("2026-03-09", "2026-03-09"), ("2026-06-15", "2026-07-20"), ("2026-02-23", "2026-03-15"), ("2026-04-01", "2026-06-30")]
    for dept in departments:
        members = {e["emp_code"] for e in employees if e["department"] == dept}
        dept_logs = [log for log in logs if log["emp_code"] in members]
        for start, end in ranges:
            subset = [log for log in dept_logs if start <= log["date"] <= end]
            got = api.get(f"/analytics/departments/{dept}/trend", params={"from": start, "to": end}).json()["items"]
            tally.same(f"trend {dept} {start}..{end}", got, Oracle(employees, subset).trend(dept, start, end))


def record_out(doc):
    ms = lambda v: None if v is None else int(v.replace(tzinfo=timezone.utc).timestamp() * 1000)  # noqa: E731
    return {"emp_code": doc["emp_code"], "date": doc["date"], "status": doc["status"], "punch_in": ms(doc.get("punch_in")),
            "punch_out": ms(doc.get("punch_out")), "work_hours": doc.get("work_hours"),
            "late_minutes": int(doc.get("late_minutes", 0)), "overtime_minutes": int(doc.get("overtime_minutes", 0)),
            "half_day": doc.get("half_day") is True, "history": doc.get("history") or []}


def check_lists(api, tally, employees, logs, rnd):
    ordered = sorted(employees, key=lambda e: e["emp_code"])
    seen, page = [], 1
    while True:
        body = api.get("/employees", params={"page": page, "page_size": 100}).json()
        tally.same(f"employees total page {page}", body["total"], len(ordered))
        if not body["items"]:
            break
        seen += [e["emp_code"] for e in body["items"]]
        page += 1
    tally.same("employees: every page in emp_code order", seen, [e["emp_code"] for e in ordered])
    for dept in DEPTS:
        expected = [e["emp_code"] for e in ordered if e["department"] == dept]
        body = api.get("/employees", params={"department": dept, "page_size": 100}).json()
        tally.same(f"employees {dept}", ([e["emp_code"] for e in body["items"]], body["total"]), (expected[:100], len(expected)))

    known = [log for log in logs]
    order = sorted(known, key=lambda d: d["emp_code"])
    order = sorted(order, key=lambda d: d["date"], reverse=True)  # date desc, then emp_code asc (stable sort)
    sample_codes = rnd.sample(sorted({log["emp_code"] for log in logs}), 15)
    filters = [{}, {"status": "LEAVE"}, {"status": "ON_DUTY"}, {"date_from": "2026-03-01", "date_to": "2026-03-31"},
               {"date_from": "2026-06-30"}, {"date_to": "2026-01-01"}, {"date_from": "2026-02-10", "date_to": "2026-02-10", "status": "PRESENT"},
               *({"emp_code": c} for c in sample_codes), {"emp_code": sample_codes[0], "status": "ABSENT"}, {"emp_code": "EMP00000"}]
    for f in filters:
        match = [d for d in order if all((d[k] == v) if k in ("emp_code", "status") else
                                         (d["date"] >= v if k == "date_from" else d["date"] <= v) for k, v in f.items())]
        size = rnd.choice([7, 20, 100])
        last = max(1, -(-len(match) // size))
        for page in sorted({1, max(1, last // 2), last, last + 1}):
            body = api.get("/attendance", params={**f, "page": page, "page_size": size}).json()
            expected = [record_out(d) for d in match[(page - 1) * size:page * size]]
            tally.same(f"attendance {f} page {page}/{last} size {size}", (body["items"], body["total"]), (expected, len(match)))


def check_writes(api, tally, db, rnd):
    """Writes through the API at the rules' boundaries; responses, stored documents and history versus R1-R5."""
    shifts = [("09:30", "18:30"), ("22:00", "06:00"), ("20:00", "04:30"), ("06:00", "14:00"), ("13:00", "22:00")]
    emps = []
    for i in range(1, 61):
        start, end = shifts[i % len(shifts)]
        emp = {"emp_code": f"EMP8{i:05d}", "name": f"V{i}", "email": f"v{i}@x.io", "department": "Verify",
               "shift_start": start, "shift_end": end, "joined_on": "2026-08-01"}
        r = api.post("/employees", json=emp)
        tally.same(f"create {emp['emp_code']}", r.status_code, 201)
        emps.append(emp)
    tally.same("duplicate create", api.post("/employees", json=emps[0]).status_code, 409)
    in_offsets = [-3_600_000, -1, 0, 599_999, 600_000, 600_999, 601_000, 601_500, 2_759_999, 10_800_000]
    out_from_in = [16_182_000, 16_181_999, 16_199_000, 16_200_000, 32_400_000, 86_400_000]
    first = (date(2026, 8, 3) - date(1970, 1, 1)).days
    for n, emp in enumerate(emps):
        day = first + n % 20
        in_ms = ms_at(day, emp["shift_start"], in_offsets[n % len(in_offsets)])
        if emp["shift_end"] <= emp["shift_start"] and n % 3 == 0:  # overnight: a punch-in after midnight, before shift end
            in_ms = ms_at(day + 1, "01:15", 123)
        r = api.post("/attendance/punch-in", json={"emp_code": emp["emp_code"], "punched_at": in_ms, "status": "WFH" if n % 4 == 0 else "PRESENT"})
        expected = {"date": iso_day(attendance_day(in_ms, emp)), "punch_in": in_ms // 1000 * 1000, "punch_out": None,
                    "work_hours": None, "late_minutes": r2_late(in_ms, emp), "overtime_minutes": 0, "half_day": False, "history": []}
        got = r.json()
        if r.status_code != 201:
            tally.same(f"punch-in {emp['emp_code']} {in_ms}", r.text, expected)
            continue
        tally.same(f"punch-in {emp['emp_code']} {in_ms}", (r.status_code, {k: got.get(k) for k in expected}), (201, expected))
        tally.same("second punch-in same day", api.post("/attendance/punch-in", json={"emp_code": emp["emp_code"], "punched_at": in_ms + 60_000}).status_code, 409)
        if n % 10 == 9:
            bad = api.post("/attendance/punch-out", json={"emp_code": emp["emp_code"], "punched_at": in_ms + 86_401_000})
            tally.same("punch-out after 24 h", bad.status_code, 422)
        out_ms = in_ms + out_from_in[n % len(out_from_in)] + rnd.randint(0, 999)
        if n % 7 == 3:  # around the overtime threshold: 29:59 and 30:00 after shift end
            end_day = attendance_day(in_ms, emp) + (1 if emp["shift_end"] <= emp["shift_start"] else 0)
            out_ms = ms_at(end_day, emp["shift_end"], 1_799_999 if n % 2 else 1_800_000)
        r = api.post("/attendance/punch-out", json={"emp_code": emp["emp_code"], "punched_at": out_ms})
        if out_ms // 1000 - in_ms // 1000 > 86400 or out_ms // 1000 <= in_ms // 1000:
            tally.same("punch-out outside the window", r.status_code, 422)
            continue
        hours, over, half = r3_to_r5(in_ms, out_ms, emp)
        got = r.json()
        tally.same(f"punch-out {emp['emp_code']}", (r.status_code, got.get("work_hours"), got.get("overtime_minutes"), got.get("half_day"), got.get("punch_out")),
                   (200, hours, over, half, out_ms // 1000 * 1000))
        if r.status_code != 200:
            continue
        tally.same("second punch-out", api.post("/attendance/punch-out", json={"emp_code": emp["emp_code"], "punched_at": out_ms + 1000}).status_code, 409)
        stored = db.attendance_logs.find_one({"emp_code": emp["emp_code"], "date": got["date"]})
        tally.same("stored document", record_out(stored), {k: got[k] for k in record_out(stored)})

        # PATCH: move the punch-in on the same attendance date; derived fields recomputed, one history entry.
        new_in = ms_at(attendance_day(in_ms, emp), emp["shift_start"], 660_000 if n % 2 else 0)
        before = record_out(stored)
        r = api.patch(f"/attendance/{emp['emp_code']}/{got['date']}", json={"punch_in": new_in, "reason": "verification", "regularized_by": "verify"})
        if r.status_code == 422:  # punch_out more than 24 h after or before the new punch-in, or nothing changes
            window_broken = (out_ms // 1000 - new_in // 1000 > 86400) or out_ms // 1000 <= new_in // 1000 or new_in // 1000 == in_ms // 1000
            tally.same(f"patch rejected only when the window rule says so: {r.text[:200]} in={in_ms} new_in={new_in} out={out_ms} {emp}",
                       window_broken, True)
            continue
        hours, over, half = r3_to_r5(new_in, out_ms, emp)
        after = {**before, "punch_in": new_in, "late_minutes": r2_late(new_in, emp), "work_hours": hours, "overtime_minutes": over, "half_day": half}
        changes = {f: {"from": before[f], "to": after[f]} for f in
                   ("status", "punch_in", "punch_out", "work_hours", "late_minutes", "overtime_minutes", "half_day") if before[f] != after[f]}
        body = r.json()
        entry = body["history"][-1] if body.get("history") else {}
        tally.same(f"patch {emp['emp_code']}", (r.status_code, {k: body.get(k) for k in ("punch_in", "late_minutes", "work_hours", "overtime_minutes", "half_day")},
                                                 numbers_as_float(entry.get("changes")), entry.get("by"), entry.get("reason")),
                   (200, {k: after[k] for k in ("punch_in", "late_minutes", "work_hours", "overtime_minutes", "half_day")},
                    numbers_as_float(changes), "verify", "verification"))
        # ABSENT clears the times; every derived field that changed is in the history entry
        r = api.patch(f"/attendance/{emp['emp_code']}/{got['date']}", json={"status": "ABSENT", "reason": "verification", "regularized_by": "verify"})
        absent = {"status": "ABSENT", "punch_in": None, "punch_out": None, "work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
        changes = {f: {"from": after[f], "to": absent[f]} for f in absent if after[f] != absent[f]}
        body = r.json()
        tally.same(f"patch ABSENT {emp['emp_code']}", (r.status_code, {k: body.get(k) for k in absent}, numbers_as_float(body["history"][-1]["changes"]), len(body["history"])),
                   (200, absent, numbers_as_float(changes), 2))
        tally.same("patch that changes nothing", api.patch(f"/attendance/{emp['emp_code']}/{got['date']}",
                                                           json={"status": "ABSENT", "reason": "verification", "regularized_by": "verify"}).status_code, 422)
    tally.same("punch-in unknown employee", api.post("/attendance/punch-in", json={"emp_code": "NOPE", "punched_at": ms_at(first, "09:00")}).status_code, 404)
    tally.same("patch unknown date", api.patch("/attendance/EMP800001/2030-01-01", json={"status": "WFH", "reason": "verification", "regularized_by": "v"}).status_code, 404)
    tally.same("seconds instead of ms", api.post("/attendance/punch-in", json={"emp_code": "EMP800001", "punched_at": 1786000000}).status_code, 422)


def check_concurrency(api, tally):
    api.post("/employees", json={"emp_code": "EMP900001", "name": "C", "email": "c@x.io", "department": "Verify", "joined_on": "2026-08-01"})
    when = ms_at((date(2026, 9, 1) - date(1970, 1, 1)).days, "09:31")
    for path, body, winner in (("/attendance/punch-in", {"emp_code": "EMP900001", "punched_at": when}, 201),
                               ("/attendance/punch-out", {"emp_code": "EMP900001", "punched_at": when + 30_000_000}, 200)):
        barrier, codes = threading.Barrier(20), []

        def go(barrier=barrier, codes=codes, path=path, body=body):
            with httpx.Client(base_url=api.base_url, timeout=30) as client:
                barrier.wait()
                codes.append(client.post(path, json=body).status_code)
        threads = [threading.Thread(target=go) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        tally.same(f"20 parallel {path}", sorted(codes), sorted([winner] + [409] * 19))
    results, barrier = [], threading.Barrier(10)

    def fix(i):
        with httpx.Client(base_url=api.base_url, timeout=30) as client:
            barrier.wait()
            results.append(client.patch("/attendance/EMP900001/2026-09-01", json={"punch_in": when - 60_000 * (i + 1), "reason": f"parallel {i}", "regularized_by": "v"}).status_code)
    threads = [threading.Thread(target=fix, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    history = api.get("/attendance", params={"emp_code": "EMP900001"}).json()["items"][0]["history"]
    tally.same("10 parallel corrections: no lost history", (len(history), set(results) <= {200, 409}), (results.count(200), True))


def check_explain(api, tally, code):
    for endpoint, params in [("attendance_list", {}), ("attendance_list", {"emp_code": code, "page": 3}), ("attendance_list", {"status": "LEAVE", "date_from": "2026-02-01"}),
                             ("employee_monthly", {"emp_code": code, "month": "2026-03"}), ("department_summary", {"month": "2026-03"}),
                             ("department_summary", {"month": "2026-03", "department": "Ops"}), ("late_leaderboard", {"month": "2026-03", "limit": 5}),
                             ("late_leaderboard", {"month": "2026-03", "department": "Eng"}), ("department_trend", {"department": "Eng", "from": "2026-01-01", "to": "2026-04-01"})]:
        text = api.get(f"/admin/explain/{endpoint}", params=params).text
        tally.same(f"explain {endpoint} {params}", ("IXSCAN" in text, "COLLSCAN" in text), (True, False))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", default=os.environ.get("MONGO_URI", "mongodb://localhost:27017"))
    parser.add_argument("--db", default="hr_verify")
    parser.add_argument("--logs", type=int, default=100_000)
    args = parser.parse_args()
    t0 = time.time()
    employees, logs = build(args.logs)
    client = MongoClient(args.uri, tz_aware=True)
    client.drop_database(args.db)
    db = client[args.db]
    db.employees.insert_many([dict(e) for e in employees])
    db.attendance_logs.insert_many([dict(log) for log in logs])
    print(f"seeded {len(employees)} employees, {len(logs)} logs in {time.time() - t0:.0f} s")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    app = subprocess.Popen([sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(port), "--log-level", "warning"], cwd=ROOT,
                           env={**os.environ, "MONGO_URI": args.uri, "MONGO_DB": args.db, "SLOW_REQUEST_MS": "100000"})
    tally, rnd = Tally(), random.Random(3)
    try:
        with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=60, event_hooks={"response": [tally.contract]}) as api:
            started = time.time()
            while True:
                try:
                    if api.get("/health").status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                time.sleep(0.1)
            print(f"ready in {time.time() - started:.1f} s")
            for step, run in [("analytics (twice: the second pass may come from the report cache)", lambda: [check_analytics(api, tally, employees, logs, MONTHS, DEPTS + ["Legal"], all_monthly=k == 0) for k in (0, 1)]),
                              ("lists and pagination", lambda: check_lists(api, tally, employees, logs, rnd)),
                              ("writes at the rules' boundaries", lambda: check_writes(api, tally, db, rnd)),
                              ("concurrency", lambda: check_concurrency(api, tally))]:
                before, t = tally.checks, time.time()
                try:
                    run()
                except Exception as exc:  # a crash is a difference too: report it with everything found so far
                    tally.failures.append((f"{step} crashed", repr(exc), "no crash"))
                print(f"{step}: {tally.checks - before} comparisons, {time.time() - t:.0f} s")
            # analytics again, on everything that is in the database now (seeded + written through the API)
            raw_emps = list(db.employees.find({}, {"_id": 0}))
            raw_logs = list(db.attendance_logs.find({}, {"_id": 0}))
            before = tally.checks
            check_analytics(api, tally, raw_emps, raw_logs, ["2026-03", "2026-08", "2026-09"], ["Verify", "Eng"], all_monthly=False)
            print(f"analytics after the writes: {tally.checks - before} comparisons")
            check_explain(api, tally, logs[0]["emp_code"])
    finally:
        app.terminate()
        app.wait(10)
        client.drop_database(args.db)
    print(f"{tally.checks} comparisons and {tally.schema_checks} responses validated against the contract's schemas (strict types), "
          f"{len(tally.failures)} differences")
    for what, got, expected in tally.failures[:10]:
        print(f"  DIFF {what}\n    got      {str(got)[:400]}\n    expected {str(expected)[:400]}")
    sys.exit(1 if tally.failures else 0)


if __name__ == "__main__":
    main()
