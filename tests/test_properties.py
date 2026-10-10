"""Property-based tests (Hypothesis) for R1 - R5.

Each rule is checked against an independent formulation that uses plain integer arithmetic on epoch seconds (IST is
UTC + 19800 s, a day is 86400 s) instead of datetime and time zones, over thousands of random instants and shifts."""
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal

from hypothesis import given, settings
from hypothesis import strategies as st

from app.main import attendance_date, compute_late_minutes, compute_overtime, compute_work_hours, derived_fields, ms_to_dt

IST_OFFSET = 19800
DAY = 86400
EPOCH = date(1970, 1, 1)

instants_ms = st.integers(min_value=1_600_000_000_000, max_value=2_000_000_000_000)  # 2020 - 2033
minutes_of_day = st.integers(min_value=0, max_value=24 * 60 - 1)


@st.composite
def shifts(draw):
    start, end = draw(minutes_of_day), draw(minutes_of_day)
    if start == end:
        end = (end + 1) % (24 * 60)
    return {"shift_start": f"{start // 60:02d}:{start % 60:02d}", "shift_end": f"{end // 60:02d}:{end % 60:02d}"}, start, end


@st.composite
def punches(draw):
    """(employee, punch ms). Half the time the punch is placed within two minutes of an instant where a rule flips:
    shift start, the end of the 10:00 grace period, or shift end. Uniform instants alone almost never hit those."""
    emp, start, end = draw(shifts())
    if draw(st.booleans()):
        return emp, start, end, draw(instants_ms)
    day_index = draw(st.integers(min_value=18_500, max_value=23_000))  # 2020 - 2032
    anchor = draw(st.sampled_from([start * 60, start * 60 + 600, end * 60]))
    offset = draw(st.integers(min_value=-120, max_value=120))
    ms_part = draw(st.integers(min_value=0, max_value=999))
    return emp, start, end, (day_index * DAY + anchor + offset - IST_OFFSET) * 1000 + ms_part


def ist_day_and_second(epoch_s: int) -> tuple[int, int]:
    """(days since 1970-01-01 in IST, seconds since IST midnight)."""
    local = epoch_s + IST_OFFSET
    return local // DAY, local % DAY


def shift_start_epoch(day_index: int, start_min: int) -> int:
    return day_index * DAY + start_min * 60 - IST_OFFSET


@settings(max_examples=3000, deadline=None)
@given(punches())
def test_r1_attendance_date(punch):
    emp, start, end, ms = punch
    day_index, second = ist_day_and_second(ms // 1000)
    overnight = end <= start
    expected = day_index - 1 if overnight and second < end * 60 else day_index
    assert attendance_date(ms_to_dt(ms), emp) == EPOCH + timedelta(days=expected)


@settings(max_examples=3000, deadline=None)
@given(instants_ms, st.integers(min_value=0, max_value=999))
def test_r1_milliseconds_never_matter(ms, extra):
    base = ms - ms % 1000
    assert ms_to_dt(base + extra) == ms_to_dt(base)


@settings(max_examples=3000, deadline=None)
@given(punches())
def test_r2_late_minutes(punch_case):
    emp, start, _, ms = punch_case
    punch = ms_to_dt(ms)
    day = attendance_date(punch, emp)
    elapsed = ms // 1000 - shift_start_epoch((day - EPOCH).days, start)
    expected = elapsed // 60 if elapsed > 600 else 0  # strictly more than 10:00 after shift_start
    assert compute_late_minutes(punch, day, emp) == expected


@settings(max_examples=3000, deadline=None)
@given(punches(), st.one_of(st.integers(min_value=1, max_value=24 * 3600), st.integers(min_value=16_150, max_value=16_250)))
def test_r3_r4_r5_after_punch_out(punch_case, worked_s):
    emp, start, end, ms = punch_case  # the second worked_s range brackets 4.50 h, where half_day flips
    punch_in = ms_to_dt(ms)
    punch_out = punch_in + timedelta(seconds=worked_s)
    day = attendance_date(punch_in, emp)
    derived = derived_fields("PRESENT", punch_in, punch_out, day, emp)

    hours = float((Decimal(worked_s) / Decimal(3600)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    assert derived["work_hours"] == compute_work_hours(punch_in, punch_out) == hours
    assert derived["half_day"] == (hours < 4.5)

    end_epoch = shift_start_epoch((day - EPOCH).days, end) + (DAY if end <= start else 0)
    over = (int(punch_out.timestamp()) - end_epoch) // 60
    assert derived["overtime_minutes"] == compute_overtime(punch_out, day, emp) == (over if over >= 30 else 0)


@settings(max_examples=500, deadline=None)
@given(instants_ms, shifts(), st.sampled_from(["ABSENT", "LEAVE"]))
def test_non_presence_has_no_derived_values(ms, shift, status):
    emp, _, _ = shift
    punch = ms_to_dt(ms)
    assert derived_fields(status, punch, punch + timedelta(hours=8), attendance_date(punch, emp), emp) == {
        "work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}


def test_known_examples_from_the_contract():
    emp = {"shift_start": "09:30", "shift_end": "18:30"}
    ist = timezone(timedelta(hours=5, minutes=30))
    for hms, late in (((9, 40, 0), 0), ((9, 40, 1), 10), ((10, 15, 59), 45)):
        punch = datetime(2026, 7, 6, *hms, tzinfo=ist).astimezone(timezone.utc)
        assert compute_late_minutes(punch, date(2026, 7, 6), emp) == late


@settings(max_examples=2000, deadline=None)
@given(shifts(), st.integers(min_value=18_500, max_value=23_000), st.integers(min_value=-180, max_value=180))
def test_r3_overtime_threshold_edge(shift, day_index, offset):
    """Punch-outs within three minutes of shift end + 30:00, where overtime starts to count."""
    emp, start, end = shift
    punch_in = ms_to_dt(shift_start_epoch(day_index, start) * 1000)
    end_epoch = shift_start_epoch(day_index, end) + (DAY if end <= start else 0)
    punch_out = ms_to_dt((end_epoch + 1800 + offset) * 1000)
    day = attendance_date(punch_in, emp)
    over = (1800 + offset) // 60
    assert compute_overtime(punch_out, day, emp) == (over if over >= 30 else 0)
