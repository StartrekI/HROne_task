"""Pure business rules (R1 - R5), no database needed."""
from datetime import date, datetime, timedelta, timezone

import pytest

from app.main import attendance_date, compute_late_minutes, compute_overtime, compute_work_hours, ms_to_dt, working_days
from tests.conftest import ist_ms

DAY = {"shift_start": "09:30", "shift_end": "18:30"}
NIGHT = {"shift_start": "22:00", "shift_end": "06:00"}


@pytest.mark.parametrize("hms, late", [((9, 40, 0), 0), ((9, 40, 1), 10), ((10, 15, 59), 45), ((9, 0, 0), 0), ((9, 41, 0), 11)])
def test_r2_grace_is_strictly_more_than_ten_minutes(hms, late):
    ts = ms_to_dt(ist_ms(2026, 7, 6, *hms))
    assert compute_late_minutes(ts, date(2026, 7, 6), DAY) == late


def test_r1_truncates_milliseconds():
    assert ms_to_dt(ist_ms(2026, 7, 6, 9, 40, 0, extra_ms=999)) == ms_to_dt(ist_ms(2026, 7, 6, 9, 40, 0))


@pytest.mark.parametrize("hms, expected", [((21, 55), "2026-07-06"), ((0, 30), "2026-07-05"), ((5, 59), "2026-07-05"), ((6, 0), "2026-07-06")])
def test_r1_overnight_attendance_date(hms, expected):
    assert attendance_date(ms_to_dt(ist_ms(2026, 7, 6, *hms)), NIGHT).isoformat() == expected


def test_r1_ist_date_not_utc_date():
    # 02:00 IST on the 10th is still the 9th in UTC
    assert attendance_date(ms_to_dt(ist_ms(2026, 7, 10, 2, 0)), DAY) == date(2026, 7, 10)


def test_r2_overnight_late_after_midnight():
    assert compute_late_minutes(ms_to_dt(ist_ms(2026, 7, 7, 0, 30)), date(2026, 7, 6), NIGHT) == 150


@pytest.mark.parametrize("out, ot", [((18, 59, 59), 29), ((19, 0, 0), 30), ((18, 35, 0), 0)])
def test_r3_overtime_threshold(out, ot):
    minutes = compute_overtime(ms_to_dt(ist_ms(2026, 7, 6, *out)), date(2026, 7, 6), DAY)
    assert minutes == (ot if ot >= 30 else 0)


def test_r3_overnight_shift_ends_next_day():
    assert compute_overtime(ms_to_dt(ist_ms(2026, 7, 7, 6, 40)), date(2026, 7, 6), NIGHT) == 40


@pytest.mark.parametrize("seconds, hours", [(12618, 3.51), (450, 0.13), (16200, 4.5), (32400, 9.0)])
def test_r4_half_up_rounding(seconds, hours):
    start = datetime(2026, 7, 6, 4, 0, tzinfo=timezone.utc)
    assert compute_work_hours(start, start + timedelta(seconds=seconds)) == hours


def test_r7_working_days():
    assert working_days(date(2026, 7, 1), date(2026, 7, 31)) == 23
    assert working_days(date(2026, 7, 15), date(2026, 7, 31)) == 13
    assert working_days(date(2026, 8, 1), date(2026, 7, 31)) == 0
