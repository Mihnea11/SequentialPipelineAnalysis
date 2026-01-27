from datetime import datetime
from typing import Callable, Dict, List

from core.models import (
    Event,
    EventType,
    EventSource,
)


Aggregator = Callable[[List[Event]], Dict]


def aggregate_window(
    events: List[Event],
    aggregator: Aggregator,
    source: EventSource,
) -> Event:
    if not events:
        raise ValueError("Cannot aggregate empty window")

    window_start = events[0].timestamp
    window_end = events[-1].timestamp

    payload = aggregator(events)

    payload["window"] = {
        "start": window_start.isoformat(),
        "end": window_end.isoformat(),
        "count": len(events),
    }

    return Event(
        source=source,
        event_type=EventType.AGGREGATED,
        payload=payload,
    )
