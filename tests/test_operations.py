"""Operational behaviour: liveness vs readiness, Prometheus metrics, optional metrics token, overload shedding,
security headers."""
import base64
import hashlib
import re

import httpx
import pytest

import app.main as main


@pytest.fixture()
def inproc():
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app, raise_app_exceptions=False), base_url="http://app")


def test_liveness_and_prometheus_metrics(api):
    assert api.get("/livez").json() == {"status": "ok"}
    api.get("/employees")
    r = api.get("/metrics")
    assert r.status_code == 200 and r.headers["content-type"].startswith("text/plain")
    text = r.text
    assert "# TYPE http_requests_total counter" in text and "# TYPE http_request_duration_seconds summary" in text
    assert re.search(r'http_requests_total\{method="GET",route="/employees",status="2xx"\} \d+', text)
    for line in text.splitlines():  # every sample line is "name{labels} number" or "name number"
        if line and not line.startswith("#"):
            assert re.fullmatch(r'[a-z_]+(\{.*\})? -?\d+(\.\d+)?', line), line


@pytest.mark.anyio
async def test_metrics_token_when_configured(inproc, monkeypatch):
    monkeypatch.setattr(main, "METRICS_TOKEN", "s3cret")
    async with inproc as client:
        assert (await client.get("/metrics")).status_code == 401
        assert (await client.get("/admin/metrics", headers={"Authorization": "Bearer nope"})).status_code == 401
        assert (await client.get("/metrics", headers={"Authorization": "Bearer s3cret"})).status_code == 200
        assert (await client.get("/livez")).status_code == 200  # nothing else needs the token


@pytest.mark.anyio
async def test_overload_is_shed_but_probes_still_answer(inproc, monkeypatch):
    monkeypatch.setattr(main, "MAX_IN_FLIGHT", 0)
    async with inproc as client:
        r = await client.get("/employees")
        assert r.status_code == 503 and r.headers["retry-after"] == "1"
        assert (await client.get("/livez")).status_code == 200


def test_security_headers_and_a_csp_that_matches_the_page(api):
    assert api.get("/employees").headers["x-content-type-options"] == "nosniff"
    r = api.get("/dashboard")
    csp = r.headers["content-security-policy"]
    script = re.search(r"<script>(.*?)</script>", r.text, re.S)[1]
    digest = base64.b64encode(hashlib.sha256(script.encode()).digest()).decode()
    assert f"'sha256-{digest}'" in csp and "frame-ancestors 'none'" in csp and "'unsafe-inline'" not in csp.split("script-src")[1].split(";")[0]
    assert r.headers["x-frame-options"] == "DENY"


def test_feed_walks_every_record_exactly_once_even_while_records_are_added(api):
    from tests.conftest import ist_ms, make_employee
    for i in range(5):
        make_employee(api, f"EMP{i:04d}")
        for day in range(1, 9):
            api.post("/attendance/punch-in", json={"emp_code": f"EMP{i:04d}", "punched_at": ist_ms(2026, 7, day, 9, 0)})
    expected = [(r["date"], r["emp_code"]) for r in api.get("/attendance", params={"page_size": 100}).json()["items"]]
    seen, cursor, added = [], None, False
    while True:
        page = api.get("/attendance/feed", params={"limit": 7, **({"after": cursor} if cursor else {})}).json()
        seen += [(r["date"], r["emp_code"]) for r in page["items"]]
        if not added:  # records added behind the cursor's position do not disturb the walk
            api.post("/attendance/punch-in", json={"emp_code": "EMP0000", "punched_at": ist_ms(2026, 7, 31, 9, 0)})
            added = True
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert seen == expected and len(set(seen)) == 40
    only_one = api.get("/attendance/feed", params={"emp_code": "EMP0003", "date_from": "2026-07-03"}).json()
    assert [r["date"] for r in only_one["items"]] == [f"2026-07-0{d}" for d in range(8, 2, -1)] and only_one["next_cursor"] is None
    assert api.get("/attendance/feed", params={"after": "not-a-cursor"}).status_code == 422
    assert api.get("/attendance/feed", params={"limit": 501}).status_code == 422


@pytest.mark.anyio
async def test_api_keys_when_configured(inproc, monkeypatch):
    monkeypatch.setattr(main, "API_KEYS", (b"k1", b"k2"))
    async with inproc as client:
        r = await client.get("/departments")
        assert r.status_code == 401 and r.headers["www-authenticate"] == "ApiKey"
        assert (await client.get("/departments", headers={"X-API-Key": "nope"})).status_code == 401
        assert (await client.get("/health")).status_code in (200, 503)  # probes stay open
        assert (await client.get("/livez")).status_code == 200
        assert (await client.get("/dashboard")).status_code == 200  # the page asks for the key itself
        monkeypatch.setattr(main, "run_pipeline", lambda *a, **k: [])
        assert (await client.get("/departments", headers={"X-API-Key": "k2"})).status_code == 200


@pytest.mark.anyio
async def test_rate_limit_per_client(inproc, monkeypatch):
    monkeypatch.setattr(main, "RATE_LIMITER", main.RateLimiter(per_minute=3))
    monkeypatch.setattr(main, "RATE_LIMIT_PER_MINUTE", 3)
    async with inproc as client:
        codes = [(await client.get("/livez")).status_code for _ in range(3)]  # probes are never limited
        codes += [(await client.get("/metrics")).status_code for _ in range(4)]
        assert codes == [200] * 6 + [429]
        r = await client.get("/metrics")
        assert r.status_code == 429 and int(r.headers["retry-after"]) >= 1 and r.headers["ratelimit-remaining"] == "0"
        # Without API keys configured, a made-up key is not an identity: it can not buy a fresh bucket.
        assert (await client.get("/metrics", headers={"X-API-Key": "made-up"})).status_code == 429
        monkeypatch.setattr(main, "API_KEYS", (b"k1",))
        assert (await client.get("/metrics", headers={"X-API-Key": "k1"})).status_code == 200  # a real key: own bucket


def test_rate_limiter_memory_is_bounded():
    limiter = main.RateLimiter(per_minute=10, max_clients=100)
    for i in range(1000):
        limiter.take(f"client-{i}")
    assert len(limiter._buckets) == 100


def test_trace_context_and_restart_metric(api):
    trace = "4bf92f3577b34da6a3ce929d0e0e4736"
    r = api.get("/livez", headers={"traceparent": f"00-{trace}-00f067aa0ba902b7-01"})
    assert r.headers["traceparent"].startswith(f"00-{trace}-") and r.headers["x-request-id"] == trace
    fresh = api.get("/livez").headers["traceparent"]
    assert re.fullmatch(r"00-[0-9a-f]{32}-[0-9a-f]{16}-01", fresh)
    assert re.search(r"^process_start_time_seconds \d+\.\d+$", api.get("/metrics").text, re.M)


@pytest.mark.anyio
async def test_reports_queue_in_their_own_lane_and_never_block_reads(inproc, monkeypatch):
    """Under load everything used to wait in one queue: a cheap read behind company-wide reports."""
    import asyncio
    import threading

    import anyio

    monkeypatch.setattr(main, "_analytics_limiter", main.CapacityLimiter(1))
    release, running, peak = threading.Event(), [0], [0]
    lock = threading.Lock()

    def pipeline(coll, _pipeline):
        if coll is main.logs:  # the department summary: hold it until released
            with lock:
                running[0] += 1
                peak[0] = max(peak[0], running[0])
            release.wait(5)
            with lock:
                running[0] -= 1
        return []

    monkeypatch.setattr(main, "run_pipeline", pipeline)
    async with inproc as client:
        reports = [asyncio.create_task(client.get("/analytics/departments/summary", params={"month": "2026-07"})) for _ in range(3)]
        await anyio.sleep(0.2)
        r = await asyncio.wait_for(client.get("/departments"), 2)  # served while every report slot is taken
        assert r.status_code == 200 and peak[0] == 1
        release.set()
        assert [x.status_code for x in await asyncio.gather(*reports)] == [200] * 3
    assert peak[0] == 1  # the lane bounds concurrent reports
