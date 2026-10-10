"""Reproducible benchmark: seed a dataset, start the app, measure every read endpoint, then a concurrency sweep.

    python scripts/benchmark.py --uri mongodb://localhost:27017 --logs 100000 --clients 16,64,256

Numbers depend heavily on hardware; run it on the target machine
(and against the target MongoDB) before sizing. The load generator runs in --procs separate processes with one
event loop each, so it does not saturate before the server does (a threaded generator does: its own GIL capped an
earlier measurement and looked like a server collapse). Exits non-zero if any request failed.
"""
import argparse
import asyncio
import multiprocessing
import os
import random
import socket
import statistics
import subprocess
import sys
import time
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import httpx
from pymongo import MongoClient

ROOT = Path(__file__).resolve().parents[1]
DEPARTMENTS = ["Eng", "Sales", "Ops", "Support", "HR", "Finance"]
MONTH, TREND = "2026-02", {"from": "2026-02-01", "to": "2026-02-28"}


def seed(uri: str, db_name: str, n_logs: int):
    """Writes directly into MongoDB without indexes, like an external loader would; the app builds them at startup."""
    db = MongoClient(uri, tz_aware=True)[db_name]
    for name in ("employees", "attendance_logs", "idempotency_keys"):
        db.drop_collection(name)
    rnd = random.Random(42)
    n_emp = max(1000, n_logs // 100)
    emps = [{"emp_code": f"EMP{i:06d}", "name": f"Employee {i}", "email": f"e{i}@x.io", "department": rnd.choice(DEPARTMENTS),
             "shift_start": "22:00" if i % 10 == 0 else "09:30", "shift_end": "06:00" if i % 10 == 0 else "18:30",
             "joined_on": rnd.choice(["2025-01-06", "2025-11-03", "2026-03-16"]),
             "created_at": datetime(2025, 1, 1, tzinfo=timezone.utc)} for i in range(1, n_emp + 1)]
    db.employees.insert_many(emps)
    days = [date(2026, 1, 1) + timedelta(days=i) for i in range(n_logs // n_emp + 1)]
    batch, written = [], 0
    for e in emps:
        for d in days:
            if written >= n_logs:
                break
            status = rnd.choices(["PRESENT", "WFH", "ON_DUTY", "ABSENT", "LEAVE"], [6, 2, 1, 1, 1])[0]
            doc = {"emp_code": e["emp_code"], "date": d.isoformat(), "status": status, "history": []}
            if status in ("PRESENT", "WFH", "ON_DUTY"):
                pin = datetime(d.year, d.month, d.day, 4, rnd.randint(0, 59), tzinfo=timezone.utc)
                hours = round(rnd.uniform(3, 10), 2)
                doc.update(punch_in=pin, punch_out=pin + timedelta(hours=hours), work_hours=hours, half_day=hours < 4.5,
                           late_minutes=rnd.choice([0, 0, 0, 12, 25, 40]), overtime_minutes=rnd.choice([0, 0, 35]))
            else:
                doc.update(punch_in=None, punch_out=None, work_hours=None, late_minutes=0, overtime_minutes=0, half_day=False)
            batch.append(doc)
            written += 1
            if len(batch) == 20000:
                db.attendance_logs.insert_many(batch, ordered=False)
                batch = []
    if batch:
        db.attendance_logs.insert_many(batch, ordered=False)
    return [e["emp_code"] for e in emps]


def requests_for(codes, rnd, write_ratio=0.0):
    """The endpoint mix of an HR app: mostly per-employee reads, some list pages, some analytics; optionally a share
    of corrections to records of the very month the reports read (the worst case for the report cache: each one
    that changes something invalidates it)."""
    if rnd.random() < write_ratio:
        day = f"2026-02-{rnd.randint(1, 28):02d}"
        body = {"status": rnd.choice(["PRESENT", "WFH", "ON_DUTY"]), "reason": "benchmark", "regularized_by": "bench"}
        return f"/attendance/{rnd.choice(codes)}/{day}", body
    k, code, dept = rnd.random(), rnd.choice(codes), rnd.choice(DEPARTMENTS)
    if k < 0.40:
        return "/attendance", {"emp_code": code}
    if k < 0.55:
        return "/attendance", {"page": rnd.randint(1, 100)}
    if k < 0.65:
        return "/attendance/feed", {"limit": 50}
    if k < 0.80:
        return f"/analytics/employees/{code}/monthly", {"month": MONTH}
    if k < 0.90:
        return f"/analytics/departments/{dept}/trend", TREND
    if k < 0.96:
        return "/analytics/leaderboard/late", {"month": MONTH, "department": dept}
    return "/analytics/departments/summary", {"month": MONTH}


async def drive(base, clients, total, codes, proc_seed, write_ratio):
    rnd = random.Random(proc_seed)
    work = iter([requests_for(codes, rnd, write_ratio) for _ in range(total)])
    latencies, statuses = {"light": [], "analytics": [], "write": []}, Counter()
    limits = httpx.Limits(max_connections=clients + 4, max_keepalive_connections=clients + 4)
    async with httpx.AsyncClient(base_url=base, timeout=120, limits=limits) as client:
        async def one_client():
            for path, params in work:
                t = time.perf_counter()
                try:
                    if "reason" in params:
                        statuses[(await client.patch(path, json=params)).status_code] += 1
                    else:
                        statuses[(await client.get(path, params=params)).status_code] += 1
                except httpx.HTTPError as exc:
                    statuses[f"{type(exc).__name__}: {exc}"[:80]] += 1
                kind = ("write" if "reason" in params else
                        "analytics" if path.startswith(("/analytics/departments", "/analytics/leaderboard")) else "light")
                latencies[kind].append((time.perf_counter() - t) * 1000)
        await asyncio.gather(*[one_client() for _ in range(clients)])
    return latencies, statuses


def worker(args):
    return asyncio.run(drive(*args))


def sweep(base, codes, clients, procs, write_ratio=0.0):
    per_proc = max(1, clients // procs)
    total = max(200, per_proc * 20)
    with multiprocessing.Pool(procs) as pool:
        start = time.perf_counter()
        results = pool.map(worker, [(base, per_proc, total, codes, i, write_ratio) for i in range(procs)])
        elapsed = time.perf_counter() - start
    statuses = sum((st for _, st in results), Counter())
    by_kind = {kind: sorted(x for lat, _ in results for x in lat[kind]) for kind in ("light", "analytics", "write")}

    def q(values, p):
        return values[min(len(values) - 1, int(len(values) * p))] if values else float("nan")
    everything = sorted(by_kind["light"] + by_kind["analytics"] + by_kind["write"])
    rows = {"all": everything, **{k: v for k, v in by_kind.items() if v}}
    return len(everything) / elapsed, {k: (q(v, 0.5), q(v, 0.95), q(v, 0.99)) for k, v in rows.items()}, dict(statuses)


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", default=os.environ.get("MONGO_URI", "mongodb://localhost:27017"))
    parser.add_argument("--db", default="hr_benchmark")
    parser.add_argument("--logs", type=int, default=100_000)
    parser.add_argument("--clients", default="16,64,256")
    parser.add_argument("--procs", type=int, default=max(1, (os.cpu_count() or 2) // 2))
    parser.add_argument("--runs", type=int, default=30, help="sequential runs per endpoint")
    parser.add_argument("--write-ratio", type=float, default=0.0, help="share of corrections in the concurrent mix")
    parser.add_argument("--base", help="benchmark an already running app on --db, seeded by an earlier run (no reseed)")
    args = parser.parse_args()

    print(f"machine: {os.cpu_count()} cores, python {sys.version.split()[0]}; dataset: {args.logs} attendance records")
    if args.base is None:
        t = time.perf_counter()
        codes = seed(args.uri, args.db, args.logs)
        print(f"seeded in {time.perf_counter() - t:.1f} s")
    else:  # reseeding would drop the running app's indexes
        codes = MongoClient(args.uri)[args.db].employees.distinct("emp_code")
    app = None
    base = args.base
    if base is None:
        port = free_port()
        base = f"http://127.0.0.1:{port}"
        app = subprocess.Popen([sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(port), "--log-level", "warning"],
                               cwd=ROOT, env={**os.environ, "MONGO_URI": args.uri, "MONGO_DB": args.db})
        t = time.perf_counter()
        while True:
            try:
                if httpx.get(base + "/health").status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            if time.perf_counter() - t > 120:
                sys.exit("app not ready")
            time.sleep(0.1)
        print(f"cold start (index builds included): {time.perf_counter() - t:.1f} s")
    failed = False
    try:
        rnd = random.Random(7)
        endpoints = [
            ("GET /attendance?emp_code", "/attendance", lambda: {"emp_code": rnd.choice(codes)}),
            ("GET /attendance?page=500", "/attendance", lambda: {"page": 500}),
            ("GET /attendance/feed", "/attendance/feed", lambda: {"limit": 100}),
            ("GET /employees?department", "/employees", lambda: {"department": rnd.choice(DEPARTMENTS)}),
            ("monthly", None, lambda: {"month": MONTH}),
            ("department summary", "/analytics/departments/summary", lambda: {"month": MONTH}),
            ("late leaderboard", "/analytics/leaderboard/late", lambda: {"month": MONTH}),
            ("department trend (28 days)", "/analytics/departments/Eng/trend", lambda: TREND),
        ]
        print(f"\n{'endpoint (sequential)':<30}{'p50 ms':>8}{'p95 ms':>8}")
        with httpx.Client(base_url=base, timeout=120) as client:
            for name, path, params in endpoints:
                times = []
                for _ in range(args.runs):
                    url = path or f"/analytics/employees/{rnd.choice(codes)}/monthly"
                    t = time.perf_counter()
                    r = client.get(url, params=params())
                    times.append((time.perf_counter() - t) * 1000)
                    failed |= r.status_code != 200
                times.sort()
                print(f"{name:<30}{statistics.median(times):>8.1f}{times[int(len(times) * 0.95) - 1]:>8.1f}")
        print(f"\nmixed workload, {args.procs} generator processes; latency in ms (light = reads and lists, "
              "analytics = company / department reports)")
        print(f"{'clients':>8}{'req/s':>8}  {'class':<10}{'p50':>8}{'p95':>8}{'p99':>8}  statuses")
        for clients in map(int, args.clients.split(",")):
            rps, rows, statuses = sweep(base, codes, clients, args.procs, args.write_ratio)
            # 422: the correction changes nothing, or the record is ABSENT / LEAVE (no punch-in); 404: no record that day
            failed |= not set(statuses) <= {200, 404, 409, 422}
            for i, (kind, (p50, p95, p99)) in enumerate(rows.items()):
                head = f"{clients:>8}{rps:>8.0f}" if i == 0 else " " * 16
                print(f"{head}  {kind:<10}{p50:>8.0f}{p95:>8.0f}{p99:>8.0f}  {statuses if i == 0 else ''}")
    finally:
        if app is not None:
            app.terminate()
            app.wait(10)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
