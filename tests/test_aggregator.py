from datetime import datetime, timedelta, timezone

from core.models import Event, EventSource, EventType
from runtime.async_processor import (
    WindowBatch,
    agg_sensor_avg,
    agg_log_levels,
    agg_feed_actions,
    aggregate_batch,
)


def mk_event(ts, source, payload):
    return Event(
        id="x",
        source=source,
        event_type=EventType.RAW,
        timestamp=ts,
        payload=payload,
    )


def test_agg_sensor_avg():
    ts = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    events = [
        mk_event(ts, EventSource.SENSOR, {"value": 10}),
        mk_event(ts, EventSource.SENSOR, {"value": 20}),
    ]
    out = agg_sensor_avg(events)
    assert out["aggregation"] == "avg"
    assert out["metric"] == "sensor.value"
    assert out["value"] == 15


def test_agg_log_levels():
    ts = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    events = [
        mk_event(ts, EventSource.LOG, {"level": "INFO"}),
        mk_event(ts, EventSource.LOG, {"level": "ERROR"}),
        mk_event(ts, EventSource.LOG, {"level": "INFO"}),
    ]
    out = agg_log_levels(events)
    assert out["aggregation"] == "count_by_level"
    assert out["levels"]["INFO"] == 2
    assert out["levels"]["ERROR"] == 1


def test_agg_feed_actions_success_rate():
    ts = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    events = [
        mk_event(ts, EventSource.FEED, {"action": "login", "success": True}),
        mk_event(ts, EventSource.FEED, {"action": "login", "success": False}),
        mk_event(ts, EventSource.FEED, {"action": "click", "success": True}),
    ]
    out = agg_feed_actions(events)
    assert out["aggregation"] == "count_by_action"
    assert out["actions"]["login"] == 2
    assert out["actions"]["click"] == 1
    assert abs(out["success_rate"] - (2 / 3)) < 1e-9


def test_aggregate_batch_emits_per_source_aggregates():
    start = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(seconds=5)

    events = [
        mk_event(start, EventSource.SENSOR, {"value": 10}),
        mk_event(start, EventSource.LOG, {"level": "INFO"}),
        mk_event(start, EventSource.FEED, {"action": "login", "success": True}),
    ]

    batch = WindowBatch(start=start, end=end, events=events)
    out = aggregate_batch(batch)

    sources = {e.source for e in out}
    assert sources == {EventSource.SENSOR, EventSource.LOG, EventSource.FEED}
