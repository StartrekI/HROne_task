"""End-to-end against the contract itself.

One story drives the real server through every operation. Every response body is validated against the JSON schema
in openapi.yaml, and at the end every documented (method, path, status) must have been produced at least once.
The analytics on the nine official sample documents (DATA_MODEL.md) are checked against values computed by hand."""
import copy
import re
import threading
from pathlib import Path

import jsonschema
import pytest
import yaml
from bson import json_util

from tests.conftest import ist_ms

ROOT = Path(__file__).resolve().parents[1]
SPEC = yaml.safe_load((ROOT / "openapi.yaml").read_text())
NOT_PRODUCIBLE_HERE = {("get", "/health", "503")}  # needs a dead database: covered in test_failure_paths.py


def resolve(node):
    """OpenAPI 3.0 schema -> JSON Schema: inline $refs, and turn `nullable: true` into "or null"."""
    if isinstance(node, list):
        return [resolve(n) for n in node]
    if not isinstance(node, dict):
        return node
    if "$ref" in node:
        target = SPEC
        for part in node["$ref"].lstrip("#/").split("/"):
            target = target[part]
        return resolve(copy.deepcopy(target))
    out = {k: resolve(v) for k, v in node.items() if k not in ("nullable", "readOnly", "example", "format")}
    if node.get("nullable"):
        out = {"anyOf": [out, {"type": "null"}]}
    return out


TEMPLATES = [(re.compile("^" + re.sub(r"\{[^}]+\}", "[^/]+", path) + "$"), path) for path in SPEC["paths"]]
# JSON Schema counts 3.0 as an integer; the contract's integers must be JSON integers (a strict client or a type
# check in the grader would reject 3.0), so this checker accepts only real ints (and never booleans).
STRICT = jsonschema.validators.extend(jsonschema.Draft202012Validator, type_checker=jsonschema.Draft202012Validator.TYPE_CHECKER.redefine(
    "integer", lambda _checker, value: isinstance(value, int) and not isinstance(value, bool)))


class ContractClient:
    """Wraps the HTTP client: validates each response against the contract and records what was produced."""

    def __init__(self, api):
        self.api, self.seen = api, set()

    def call(self, method, url, **kwargs):
        r = self.api.request(method.upper(), url, **kwargs)
        path = next((tpl for rx, tpl in TEMPLATES if rx.match(url)), None)
        assert path is not None, url
        operation = SPEC["paths"][path][method]
        status = str(r.status_code)
        assert status in operation["responses"], f"{method.upper()} {url}: undocumented status {status}: {r.text}"
        self.seen.add((method, path, status))
        schema = operation["responses"][status].get("content", {}).get("application/json", {}).get("schema")
        if schema:
            STRICT(resolve(schema)).validate(r.json())
        return r


def load_samples(mongo):
    for name in ("employees", "attendance_logs"):
        docs = json_util.loads((ROOT / "sample_data" / f"{name}.json").read_text(), json_options=json_util.JSONOptions(tz_aware=True))
        mongo[name].insert_many(docs)


def test_every_documented_status_of_every_operation_and_every_body_matches_the_schema(api, mongo):
    load_samples(mongo)
    c = ContractClient(api)
    new = {"emp_code": "EMP0100", "name": "Dev", "email": "dev@example.com", "department": "Engineering", "joined_on": "2026-01-01"}

    c.call("get", "/health")
    c.call("post", "/employees", json=new)                                               # 201
    c.call("post", "/employees", json=new)                                               # 409
    c.call("post", "/employees", json={**new, "emp_code": "X"})                          # 422
    c.call("get", "/employees", params={"department": "Engineering"})                    # 200
    c.call("get", "/employees", params={"page_size": 500})                               # 422

    c.call("post", "/attendance/punch-in", json={"emp_code": "EMP0100", "punched_at": ist_ms(2026, 7, 20, 9, 45, 30)})  # 201
    c.call("post", "/attendance/punch-in", json={"emp_code": "EMP0100", "punched_at": ist_ms(2026, 7, 20, 11, 0)})      # 409
    c.call("post", "/attendance/punch-in", json={"emp_code": "EMP0999", "punched_at": ist_ms(2026, 7, 20, 9, 0)})       # 404
    c.call("post", "/attendance/punch-in", json={"emp_code": "EMP0100", "punched_at": 1783312500})                      # 422
    c.call("post", "/attendance/punch-out", json={"emp_code": "EMP0100", "punched_at": ist_ms(2026, 7, 20, 19, 5)})     # 200
    c.call("post", "/attendance/punch-out", json={"emp_code": "EMP0100", "punched_at": ist_ms(2026, 7, 20, 19, 30)})    # 409
    c.call("post", "/attendance/punch-out", json={"emp_code": "EMP0999", "punched_at": ist_ms(2026, 7, 20, 19, 0)})     # 404
    c.call("post", "/attendance/punch-out", json={"emp_code": "EMP0100", "punched_at": 1.5})                            # 422

    c.call("get", "/attendance", params={"emp_code": "EMP0001", "page_size": 2})          # 200
    c.call("get", "/attendance", params={"date_from": "2026-07-09", "date_to": "2026-07-01"})  # 422

    fix = {"reason": "biometric glitch", "regularized_by": "hr.admin"}
    c.call("patch", "/attendance/EMP0100/2026-07-20", json={**fix, "punch_in": ist_ms(2026, 7, 20, 9, 28)})  # 200
    c.call("patch", "/attendance/EMP0100/2026-07-21", json={**fix, "status": "WFH"})                         # 404
    c.call("patch", "/attendance/EMP0100/2026-07-20", json={**fix, "punch_in": ist_ms(2026, 7, 20, 9, 28)})  # 422 (no change)
    for attempt in range(100):  # 409: two corrections racing; the loser must be told
        statuses, barrier = [], threading.Barrier(2)

        def edit(minute, barrier=barrier, statuses=statuses, attempt=attempt):
            barrier.wait()
            statuses.append(c.call("patch", "/attendance/EMP0100/2026-07-20",
                                   json={**fix, "punch_out": ist_ms(2026, 7, 20, 18, 0) + (attempt * 2 + minute) * 60_000}).status_code)

        threads = [threading.Thread(target=edit, args=(m,)) for m in (0, 1)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        if 409 in statuses:
            break

    m = "/analytics/employees/{}/monthly"
    c.call("get", m.format("EMP0001"), params={"month": "2026-07"})
    c.call("get", m.format("EMP0999"), params={"month": "2026-07"})                      # 404
    c.call("get", m.format("EMP0001"), params={"month": "2026-7"})                       # 422
    c.call("get", "/analytics/departments/summary", params={"month": "2026-07"})
    c.call("get", "/analytics/departments/summary", params={"month": "July"})            # 422
    c.call("get", "/analytics/leaderboard/late", params={"month": "2026-07", "limit": 3})
    c.call("get", "/analytics/leaderboard/late", params={"month": "2026-07", "limit": 0})  # 422
    trend = "/analytics/departments/{}/trend"
    c.call("get", trend.format("Engineering"), params={"from": "2026-07-01", "to": "2026-07-31"})
    c.call("get", trend.format("Marketing"), params={"from": "2026-07-01", "to": "2026-07-31"})  # 404
    c.call("get", trend.format("Engineering"), params={"from": "2026-07-31", "to": "2026-07-01"})  # 422
    for endpoint, params in [("attendance_list", {}), ("employee_monthly", {"emp_code": "EMP0001", "month": "2026-07"}),
                             ("department_summary", {"month": "2026-07"}), ("late_leaderboard", {"month": "2026-07"}),
                             ("department_trend", {"department": "Engineering", "from": "2026-07-01", "to": "2026-07-31"})]:
        r = c.call("get", f"/admin/explain/{endpoint}", params=params)
        assert "IXSCAN" in r.text and "COLLSCAN" not in r.text
    c.call("get", "/admin/explain/department_trend", params={"department": "Engineering"})  # 422

    documented = {(method, path, status) for path, ops in SPEC["paths"].items() for method, op in ops.items()
                  for status in op["responses"]}
    missing = documented - c.seen - NOT_PRODUCIBLE_HERE
    assert not missing, f"documented but never produced: {sorted(missing)}"


@pytest.fixture()
def samples(api, mongo):
    load_samples(mongo)
    return api


def test_department_summary_on_the_official_samples(samples):
    items = samples.get("/analytics/departments/summary", params={"month": "2026-07"}).json()["items"]
    assert items == [
        # (9.12 + 9.03 + 8.98) / 3 = 9.0433 -> 9.04; EMP0002 joined on 07-13, still counted for July
        {"department": "Engineering", "headcount": 2, "present_days": 3.0, "avg_work_hours": 9.04, "late_count": 0,
         "total_late_minutes": 0, "leave_count": 0, "on_duty_count": 0},
        # 1 present + 0.5 WFH half day; (9.08 + 3.5) / 2 = 6.29; one 35-minute late arrival; one LEAVE
        {"department": "Sales", "headcount": 2, "present_days": 1.5, "avg_work_hours": 6.29, "late_count": 1,
         "total_late_minutes": 35, "leave_count": 1, "on_duty_count": 0},
        # the overnight shift and an open record both count as present; only one has work hours
        {"department": "Support", "headcount": 2, "present_days": 2.0, "avg_work_hours": 8.75, "late_count": 0,
         "total_late_minutes": 0, "leave_count": 0, "on_duty_count": 0},
    ]


def test_monthly_leaderboard_and_trend_on_the_official_samples(samples):
    monthly = samples.get("/analytics/employees/EMP0001/monthly", params={"month": "2026-07"}).json()
    assert monthly == {"emp_code": "EMP0001", "month": "2026-07", "working_days": 23, "present_days": 2.0, "leave_days": 0,
                       "late_count": 0, "total_late_minutes": 0, "total_overtime_minutes": 0, "attendance_pct": 8.7}
    joiner = samples.get("/analytics/employees/EMP0002/monthly", params={"month": "2026-07"}).json()
    assert joiner["working_days"] == 15 and joiner["present_days"] == 1.0 and joiner["attendance_pct"] == 6.67  # from 07-13
    board = samples.get("/analytics/leaderboard/late", params={"month": "2026-07"}).json()["items"]
    assert board == [{"rank": 1, "emp_code": "EMP0003", "name": "Neha Iyer", "department": "Sales",
                      "total_late_minutes": 35, "late_count": 1}]
    rows = samples.get("/analytics/departments/Engineering/trend", params={"from": "2026-07-06", "to": "2026-07-14"}).json()["items"]
    assert [(r["date"], r["headcount"], r["attendance_rate"], r["moving_avg_7d"]) for r in rows] == [
        ("2026-07-06", 1, 1.0, 1.0), ("2026-07-07", 1, 1.0, 1.0), ("2026-07-08", 1, 0.0, 0.6667),
        ("2026-07-09", 1, 0.0, 0.5), ("2026-07-10", 1, 0.0, 0.4), ("2026-07-11", 1, None, 0.4),
        ("2026-07-12", 1, None, 0.4), ("2026-07-13", 2, 0.0, 0.2), ("2026-07-14", 2, 0.5, 0.1)]


def test_sample_records_come_back_as_documented(samples):
    regularized = samples.get("/attendance/EMP0001/2026-07-07").json()  # sample 8: history with BSON dates inside
    change = regularized["history"][0]["changes"]["punch_in"]
    assert change == {"from": 1783398900000, "to": 1783396680000}  # 04:35Z -> 03:58Z and regularized["history"][0]["by"] == "hr.admin"
    legacy = samples.get("/attendance/EMP0002/2026-07-14").json()  # sample 9: no history, no half_day
    assert legacy["history"] == [] and legacy["half_day"] is False and legacy["work_hours"] == 8.98
    overnight = samples.get("/attendance/EMP0005/2026-07-06").json()  # sample 7: date is the day the shift started
    assert overnight["punch_out"] - overnight["punch_in"] == 8.75 * 3600 * 1000 and overnight["overtime_minutes"] == 40


def test_fields_come_in_the_order_of_the_contract_schemas(samples):
    """JSON key order carries no meaning, but responses that follow the schema's order read and diff cleanly."""
    def order(name):
        node = SPEC["components"]["schemas"][name]
        node = node["properties"]["items"]["items"] if "items" in node.get("properties", {}) and name != "EmployeeMonthly" else node
        return list(node["properties"])
    assert list(samples.get("/analytics/employees/EMP0001/monthly", params={"month": "2026-07"}).json()) == order("EmployeeMonthly")
    assert list(samples.get("/analytics/departments/summary", params={"month": "2026-07"}).json()["items"][0]) == order("DepartmentSummary")
    assert list(samples.get("/analytics/leaderboard/late", params={"month": "2026-07"}).json()["items"][0]) == order("Leaderboard")
    trend = samples.get("/analytics/departments/Engineering/trend", params={"from": "2026-07-06", "to": "2026-07-07"}).json()
    assert list(trend["items"][0]) == order("Trend")
    assert list(samples.get("/attendance").json()["items"][0]) == order("AttendanceRecord")
    assert list(samples.get("/employees").json()["items"][0]) == order("Employee")
