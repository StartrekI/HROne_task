"""Failover drill: concurrent writes through the app while the replica set's primary is stepped down, then killed.

    python scripts/failover_drill.py --uri "mongodb://h1,h2,h3/?replicaSet=rs" [--seconds 40]

Needs a replica set of at least three members that this machine can signal (the killed member is restarted with
its own command line). What it checks, after two elections under load:
- durability: every write the app acknowledged (201/200) is in the database, with the acknowledged values
  (the app writes with w="majority", so an election must not roll any of them back);
- exactly-once with retries: every write is retried with the same Idempotency-Key until it gets a final answer,
  so each punch-in must end as 201, never as 409 "already punched in";
- no 500 at any point (an election is 503 + Retry-After, which the client retries);
- the service recovers: the last seconds of the run succeed;
- reports stay exact: after the elections, every report the app serves (from its change-stream cache, which had to
  follow the primary through both elections) equals a fresh computation.
Exit status 0 when every check passes."""
import argparse
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from pymongo import MongoClient

ROOT = Path(__file__).resolve().parents[1]
IST = timezone(timedelta(hours=5, minutes=30))
FINAL = {200, 201, 404, 409, 422}


def ms(day: datetime, hour: int, minute: int) -> int:
    return int(day.replace(hour=hour, minute=minute).timestamp() * 1000)


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def primary_of(uri: str):
    with MongoClient(uri, serverSelectionTimeoutMS=20000) as c:
        host = c.admin.command("hello")["primary"]
    with MongoClient(host, directConnection=True) as c:
        return host, c.admin.command("serverStatus")["pid"], c.admin.command("getCmdLineOpts")["argv"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", required=True)
    parser.add_argument("--db", default="hr_failover_drill")
    parser.add_argument("--seconds", type=float, default=40)
    parser.add_argument("--workers", type=int, default=16)
    args = parser.parse_args()

    MongoClient(args.uri).drop_database(args.db)
    port = free_port()
    app = subprocess.Popen([sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(port), "--log-level", "warning"],
                           cwd=ROOT, env={**os.environ, "MONGO_URI": args.uri, "MONGO_DB": args.db})
    base = f"http://127.0.0.1:{port}"
    for _ in range(100):
        try:
            if httpx.get(base + "/health").status_code == 200:
                break
        except httpx.HTTPError:
            pass
        time.sleep(0.2)
    else:
        sys.exit("app not ready")

    codes = [f"EMP{i:04d}" for i in range(1, args.workers + 1)]
    for code in codes:
        assert httpx.post(base + "/employees", json={"emp_code": code, "name": code, "email": f"{code}@x.io",
                                                     "department": "Ops", "joined_on": "2020-01-01"}).status_code == 201

    lock = threading.Lock()
    statuses, timeline = Counter(), []           # every HTTP answer, and (t, status) for the recovery check
    acked_in, acked_out, wrong = {}, {}, []      # (emp, date) -> punched_at the app acknowledged
    started, stop = time.time(), threading.Event()

    def send(client, path, body):
        """Retry with the same Idempotency-Key until a final answer, like a well-behaved client during an election."""
        key = uuid.uuid4().hex
        while True:
            try:
                r = client.post(path, json=body, headers={"Idempotency-Key": key})
                status = r.status_code
            except httpx.HTTPError:
                status = "conn"
            with lock:
                statuses[status] += 1
                timeline.append((time.time() - started, status))
            if status in FINAL or status == 500:
                return r
            time.sleep(0.2)

    def worker(code):
        day = datetime(2024, 1, 1, tzinfo=IST)
        with httpx.Client(base_url=base, timeout=30) as client:
            while not stop.is_set():
                day += timedelta(days=1)
                date = day.strftime("%Y-%m-%d")
                p_in, p_out = ms(day, 9, 30), ms(day, 18, 45)
                r = send(client, "/attendance/punch-in", {"emp_code": code, "punched_at": p_in})
                if r.status_code != 201:
                    wrong.append(("punch-in", code, date, r.status_code, r.text))
                    continue
                acked_in[(code, date)] = p_in
                r = send(client, "/attendance/punch-out", {"emp_code": code, "punched_at": p_out})
                if r.status_code != 200:
                    wrong.append(("punch-out", code, date, r.status_code, r.text))
                    continue
                acked_out[(code, date)] = p_out

    threads = [threading.Thread(target=worker, args=(c,)) for c in codes]
    for t in threads:
        t.start()

    third = args.seconds / 4
    time.sleep(third)
    host, _, _ = primary_of(args.uri)
    print(f"[{time.time() - started:5.1f}s] graceful: replSetStepDown on {host}")
    with MongoClient(host, directConnection=True) as c:
        try:
            c.admin.command("replSetStepDown", 30)
        except Exception as exc:  # the step-down closes our own connection
            print("   ", type(exc).__name__)
    time.sleep(third)
    host, pid, argv = primary_of(args.uri)
    print(f"[{time.time() - started:5.1f}s] crash: SIGKILL the new primary {host} (pid {pid})")
    os.kill(pid, signal.SIGKILL)
    time.sleep(third / 2)
    print(f"[{time.time() - started:5.1f}s] restart {host}")
    subprocess.run(argv, check=True, stdout=subprocess.DEVNULL)
    time.sleep(args.seconds - 2.5 * third)
    stop.set()
    for t in threads:
        t.join()

    os.environ.update(MONGO_URI=args.uri, MONGO_DB=args.db)
    sys.path.insert(0, str(ROOT))
    import app.main as m  # the app's own pipeline builders, run directly against MongoDB

    months = sorted({date[:7] for _, date in acked_in})
    report_diffs = []
    with MongoClient(args.uri, tz_aware=True) as direct:
        logs = direct[args.db].attendance_logs
        for month in months:
            for _ in range(2):  # the second read is served from the cache
                served = httpx.get(f"{base}/analytics/departments/summary", params={"month": month}).json()["items"]
                board = httpx.get(f"{base}/analytics/leaderboard/late", params={"month": month}).json()["items"]
                if served != list(logs.aggregate(m.department_summary_pipeline(month, None))):
                    report_diffs.append(("summary", month))
                if board != list(logs.aggregate(m.late_leaderboard_pipeline(month, 10, None))):
                    report_diffs.append(("leaderboard", month))
    metrics = httpx.get(f"{base}/metrics").text
    app.terminate()
    app.wait(10)

    with MongoClient(args.uri, w="majority", readConcernLevel="majority") as c:
        stored = {(d["emp_code"], d["date"]): d for d in c[args.db].attendance_logs.find()}
    lost_in = [k for k, v in acked_in.items() if k not in stored or int(stored[k]["punch_in"].replace(tzinfo=timezone.utc).timestamp() * 1000) != v]
    lost_out = [k for k, v in acked_out.items() if k not in stored or stored[k].get("punch_out") is None
                or int(stored[k]["punch_out"].replace(tzinfo=timezone.utc).timestamp() * 1000) != v]
    tail = [s for t, s in timeline if t > args.seconds - 3]
    checks = {
        "acknowledged punch-ins all stored": not lost_in,
        "acknowledged punch-outs all stored": not lost_out,
        "every retried write ended as success (exactly once)": not wrong,
        "no 500": statuses[500] == 0,
        "recovered: last 3 s all succeeded": bool(tail) and all(s in (200, 201) for s in tail),
        f"reports exact after the elections ({len(months)} months, cache {'on' if 'report_cache_active 1' in metrics else 'OFF'})":
            not report_diffs,
    }
    print(f"writes acknowledged: {len(acked_in)} punch-ins, {len(acked_out)} punch-outs; HTTP answers: {dict(statuses)}")
    for name, ok in checks.items():
        print(("PASS " if ok else "FAIL ") + name)
    for w in wrong[:5]:
        print("   ", w)
    sys.exit(0 if all(checks.values()) else 1)


if __name__ == "__main__":
    main()
