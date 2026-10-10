"""Crash drill: SIGKILL the app process again and again while clients write with Idempotency-Keys.

    python scripts/crash_drill.py --uri mongodb://localhost:27017 [--kills 4]

Works on a standalone server too, which has no transactions: there the write stamps its key on the document it
writes, and this drill checks that it holds when the process dies at any point of a request. Clients retry every
write with the same key (after a lost connection, a 503, or a 409 "still in progress") until a final answer. Checks:
every write ends as success exactly once (never 409 "already punched in"), every acknowledged write is stored with
the acknowledged values, and no 500. Exit status 0 when every check passes."""
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", required=True)
    parser.add_argument("--db", default="hr_crash_drill")
    parser.add_argument("--kills", type=int, default=4)
    parser.add_argument("--workers", type=int, default=16)
    args = parser.parse_args()

    MongoClient(args.uri).drop_database(args.db)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    env = {**os.environ, "MONGO_URI": args.uri, "MONGO_DB": args.db}

    def start():
        proc = subprocess.Popen([sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(port), "--log-level", "error"],
                                cwd=ROOT, env=env)
        for _ in range(200):
            try:
                if httpx.get(base + "/health").status_code == 200:
                    return proc
            except httpx.HTTPError:
                pass
            time.sleep(0.05)
        sys.exit("app not ready")

    app = start()
    codes = [f"EMP{i:04d}" for i in range(1, args.workers + 1)]
    for code in codes:
        httpx.post(base + "/employees", json={"emp_code": code, "name": code, "email": f"{code}@x.io",
                                              "department": "Ops", "joined_on": "2020-01-01"})

    lock, stop = threading.Lock(), threading.Event()
    statuses, acked, wrong = Counter(), {}, []

    def send(client, method, path, body):
        key = uuid.uuid4().hex
        while True:
            try:
                r = client.request(method, path, json=body, headers={"Idempotency-Key": key})
                status = r.status_code
            except httpx.HTTPError:
                r, status = None, "conn"
            with lock:
                statuses[status] += 1
            if r is not None and (status in (200, 201, 404, 422, 500) or (status == 409 and "in progress" not in r.text)):
                return r
            time.sleep(0.1)

    def worker(code):
        day = datetime(2024, 1, 1, tzinfo=IST)
        with httpx.Client(base_url=base, timeout=30) as client:
            while not stop.is_set():
                day += timedelta(days=1)
                date = day.strftime("%Y-%m-%d")
                p_in = int(day.replace(hour=9, minute=40).timestamp() * 1000)
                p_out = int(day.replace(hour=18, minute=50).timestamp() * 1000)
                p_fix = int(day.replace(hour=9, minute=25).timestamp() * 1000)
                steps = [("POST", "/attendance/punch-in", {"emp_code": code, "punched_at": p_in}, 201, "punch_in", p_in),
                         ("POST", "/attendance/punch-out", {"emp_code": code, "punched_at": p_out}, 200, "punch_out", p_out),
                         ("PATCH", f"/attendance/{code}/{date}", {"punch_in": p_fix, "reason": "drill", "regularized_by": "hr"},
                          200, "punch_in", p_fix)]
                for method, path, body, expected, field, value in steps:
                    r = send(client, method, path, body)
                    if r.status_code != expected:
                        wrong.append((method, path, r.status_code, r.text[:120]))
                        break
                    if r.json()[field] != value:
                        wrong.append((method, path, "wrong answer", r.text[:120]))
                        break
                    with lock:
                        acked[(code, date, field)] = value
                        if field == "punch_in" and method == "PATCH":
                            acked[(code, date, "history")] = 1

    threads = [threading.Thread(target=worker, args=(c,)) for c in codes]
    for t in threads:
        t.start()
    for i in range(args.kills):
        time.sleep(4)
        os.kill(app.pid, signal.SIGKILL)
        app.wait()
        print(f"kill {i + 1}: app SIGKILLed, restarting")
        app = start()
    time.sleep(4)
    stop.set()
    for t in threads:
        t.join()
    app.terminate()
    app.wait(10)

    db = MongoClient(args.uri, tz_aware=True)[args.db]
    stored = {(d["emp_code"], d["date"]): d for d in db.attendance_logs.find()}
    ms = lambda dt: int(dt.timestamp() * 1000)  # noqa: E731
    lost = []
    for (code, date, field), value in acked.items():
        doc = stored.get((code, date))
        if doc is None:
            lost.append((code, date, field))
        elif field == "history":
            if len(doc.get("history", [])) != 1:
                lost.append((code, date, "history length", len(doc.get("history", []))))
        elif doc.get(field) is None or ms(doc[field]) != value:  # the last acknowledged value is the stored one
            lost.append((code, date, field, value))
    duplicates = len(stored) != db.attendance_logs.count_documents({})
    leftover = db.attendance_logs.count_documents({"_idem.k": {"$exists": True}})
    checks = {
        "every retried write ended as success, exactly once": not wrong,
        "every acknowledged write stored (one history entry per correction)": not lost and not duplicates,
        "no 500": statuses[500] == 0,
    }
    print(f"acknowledged writes: {len(acked)}; HTTP answers: {dict(statuses)}; stamps left by killed requests: {leftover}")
    for name, ok in checks.items():
        print(("PASS " if ok else "FAIL ") + name)
    for w in (wrong + lost)[:5]:
        print("   ", w)
    sys.exit(0 if all(checks.values()) else 1)


if __name__ == "__main__":
    main()
