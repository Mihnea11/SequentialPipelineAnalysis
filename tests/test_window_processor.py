from dataclasses import replace
from datetime import datetime, timedelta, timezone

from core.models import Event, EventSource, EventType
from runtime.async_processor import AsyncTumblingWindowProcessor


def mk_event(ts: datetime, source=EventSource.SENSOR, payload=None) -> Event:
    return Event(
        id="e",
        source=source,
        event_type=EventType.RAW,
        timestamp=ts,
        payload=payload or {"value": 1},
    )


def test_tumbling_window_emits_batch_on_window_change():
    proc = AsyncTumblingWindowProcessor(window_size=timedelta(seconds=5))

    t0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 1, 1, 12, 0, 4, tzinfo=timezone.utc)
    t2 = datetime(2026, 1, 1, 12, 0, 5, tzinfo=timezone.utc)

    assert proc.push(mk_event(t0)) is None
    assert proc.push(mk_event(t1)) is None

    batch = proc.push(mk_event(t2))
    assert batch is not None
    assert batch.start == datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert batch.end == datetime(2026, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
    assert len(batch.events) == 2


def test_flush_returns_last_batch():
    proc = AsyncTumblingWindowProcessor(window_size=timedelta(seconds=5))
    t0 = datetime(2026, 1, 1, 12, 0, 1, tzinfo=timezone.utc)
    t1 = datetime(2026, 1, 1, 12, 0, 2, tzinfo=timezone.utc)

    proc.push(mk_event(t0))
    proc.push(mk_event(t1))

    last = proc.flush()
    assert last is not None
    assert len(last.events) == 2

    assert proc.flush() is None


def test_predicates_filter_events_out():
    def pred(ev: Event) -> bool:
        return ev.payload.get("value", 0) >= 10

    proc = AsyncTumblingWindowProcessor(
        window_size=timedelta(seconds=5),
        predicates=[pred],
    )

    t0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    ev1 = mk_event(t0, payload={"value": 1})
    ev2 = mk_event(t0, payload={"value": 10})

    assert proc.push(ev1) is None
    assert proc.push(ev2) is None


def test_mappers_transform_events():
    def mapper(ev: Event) -> Event:
        return replace(ev, payload={"value": ev.payload["value"] * 2})

    proc = AsyncTumblingWindowProcessor(
        window_size=timedelta(seconds=5),
        mappers=[mapper],
    )

    t0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    proc.push(mk_event(t0, payload={"value": 3}))

    last = proc.flush()
    assert last is not None
    assert last.events[0].payload["value"] == 6
