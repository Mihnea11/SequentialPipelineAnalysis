import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, List, Optional

from core.models import Event, EventSource
from metrics.collector import MetricsCollector
from pipeline.aggregation import aggregate_window

Predicate = Callable[[Event], bool]
Mapper = Callable[[Event], Event]


def floor_time_to_window(ts: datetime, window_size: timedelta) -> datetime:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    window_seconds = int(window_size.total_seconds())
    epoch_seconds = int(ts.timestamp())
    floored = (epoch_seconds // window_seconds) * window_seconds
    return datetime.fromtimestamp(floored, tz=timezone.utc)


@dataclass
class WindowBatch:
    start: datetime
    end: datetime
    events: List[Event]


class AsyncTumblingWindowProcessor:
    def __init__(
        self,
        window_size: timedelta,
        predicates: Optional[List[Predicate]] = None,
        mappers: Optional[List[Mapper]] = None,
    ):
        self.window_size = window_size
        self.predicates = predicates or []
        self.mappers = mappers or []

        self._current_start: Optional[datetime] = None
        self._current_events: List[Event] = []

    def _apply_pipeline(self, event: Event) -> Optional[Event]:
        for p in self.predicates:
            if not p(event):
                return None
        for m in self.mappers:
            event = m(event)
        return event

    def push(self, event: Event) -> Optional[WindowBatch]:
        event = self._apply_pipeline(event)
        if event is None:
            return None

        ws = floor_time_to_window(event.timestamp, self.window_size)

        if self._current_start is None:
            self._current_start = ws
            self._current_events = [event]
            return None

        if ws == self._current_start:
            self._current_events.append(event)
            return None

        batch = WindowBatch(
            start=self._current_start,
            end=self._current_start + self.window_size,
            events=self._current_events,
        )

        self._current_start = ws
        self._current_events = [event]
        return batch

    def flush(self) -> Optional[WindowBatch]:
        if self._current_start is None or not self._current_events:
            return None

        batch = WindowBatch(
            start=self._current_start,
            end=self._current_start + self.window_size,
            events=self._current_events,
        )

        self._current_start = None
        self._current_events = []
        return batch


# -------------------------
# Aggregators
# -------------------------

def agg_sensor_avg(events: List[Event]) -> dict:
    values = [e.payload["value"] for e in events if isinstance(e.payload, dict) and "value" in e.payload]
    if not values:
        return {"aggregation": "avg", "metric": "sensor.value", "value": None}
    return {"aggregation": "avg", "metric": "sensor.value", "value": sum(values) / len(values)}


def agg_log_levels(events: List[Event]) -> dict:
    counts: dict[str, int] = {}
    for e in events:
        level = "UNKNOWN"
        if isinstance(e.payload, dict):
            level = e.payload.get("level", "UNKNOWN")
        counts[level] = counts.get(level, 0) + 1
    return {"aggregation": "count_by_level", "levels": counts}


def agg_feed_actions(events: List[Event]) -> dict:
    counts: dict[str, int] = {}
    ok = 0
    for e in events:
        action = "UNKNOWN"
        success = None
        if isinstance(e.payload, dict):
            action = e.payload.get("action", "UNKNOWN")
            success = e.payload.get("success")
        counts[action] = counts.get(action, 0) + 1
        if success is True:
            ok += 1
    return {
        "aggregation": "count_by_action",
        "actions": counts,
        "success_rate": ok / len(events) if events else None,
    }


def aggregate_batch(batch: WindowBatch) -> List[Event]:
    out: List[Event] = []

    sensors = [e for e in batch.events if e.source == EventSource.SENSOR]
    logs = [e for e in batch.events if e.source == EventSource.LOG]
    feeds = [e for e in batch.events if e.source == EventSource.FEED]

    if sensors:
        out.append(
            aggregate_window(
                sensors,
                agg_sensor_avg,
                source=EventSource.SENSOR,
                window_start=batch.start,
                window_end=batch.end,
            )
        )

    if logs:
        out.append(
            aggregate_window(
                logs,
                agg_log_levels,
                source=EventSource.LOG,
                window_start=batch.start,
                window_end=batch.end,
            )
        )

    if feeds:
        out.append(
            aggregate_window(
                feeds,
                agg_feed_actions,
                source=EventSource.FEED,
                window_start=batch.start,
                window_end=batch.end,
            )
        )

    return out


# -------------------------
# Live runner
# -------------------------

async def run_live_aggregation(
    input_queue: "asyncio.Queue[Event]",
    output_queue: "asyncio.Queue[Event]",
    window_size: timedelta,
    stop_event: asyncio.Event,
    metrics: MetricsCollector | None = None,
) -> None:
    processor = AsyncTumblingWindowProcessor(window_size=window_size)

    try:
        while not stop_event.is_set():
            event = await input_queue.get()

            if metrics is not None:
                now = datetime.now(timezone.utc)
                ts = event.timestamp
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                latency_ms = (now - ts).total_seconds() * 1000.0
                metrics.record_processed(event.source.value, latency_ms)

            batch = processor.push(event)
            if batch is None:
                continue

            count_by_source = {"sensor": 0, "log": 0, "feed": 0}
            for e in batch.events:
                count_by_source[e.source.value] = count_by_source.get(e.source.value, 0) + 1

            t0 = time.perf_counter()
            aggs = aggregate_batch(batch)
            t1 = time.perf_counter()

            for agg in aggs:
                await output_queue.put(agg)
                if metrics is not None:
                    metrics.record_aggregated()

            if metrics is not None:
                metrics.record_window(
                    start=batch.start.isoformat(),
                    end=batch.end.isoformat(),
                    count_by_source=count_by_source,
                    aggregates_emitted=len(aggs),
                    aggregation_time_ms=(t1 - t0) * 1000.0,
                )
    finally:
        last = processor.flush()
        if last:
            count_by_source = {"sensor": 0, "log": 0, "feed": 0}
            for e in last.events:
                count_by_source[e.source.value] = count_by_source.get(e.source.value, 0) + 1

            t0 = time.perf_counter()
            aggs = aggregate_batch(last)
            t1 = time.perf_counter()

            for agg in aggs:
                await output_queue.put(agg)
                if metrics is not None:
                    metrics.record_aggregated()

            if metrics is not None:
                metrics.record_window(
                    start=last.start.isoformat(),
                    end=last.end.isoformat(),
                    count_by_source=count_by_source,
                    aggregates_emitted=len(aggs),
                    aggregation_time_ms=(t1 - t0) * 1000.0,
                )
