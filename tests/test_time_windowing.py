from datetime import datetime, timedelta, timezone

from runtime.async_processor import floor_time_to_window


def test_floor_time_to_window_exact_boundary():
    ws = timedelta(seconds=5)
    ts = datetime(2026, 1, 1, 12, 0, 10, tzinfo=timezone.utc)
    out = floor_time_to_window(ts, ws)
    assert out == ts


def test_floor_time_to_window_rounds_down():
    ws = timedelta(seconds=5)
    ts = datetime(2026, 1, 1, 12, 0, 12, tzinfo=timezone.utc)
    out = floor_time_to_window(ts, ws)
    assert out == datetime(2026, 1, 1, 12, 0, 10, tzinfo=timezone.utc)


def test_floor_time_to_window_handles_naive_as_utc():
    ws = timedelta(seconds=10)
    ts = datetime(2026, 1, 1, 0, 0, 9)  # naive
    out = floor_time_to_window(ts, ws)
    assert out.tzinfo == timezone.utc
    assert out == datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
