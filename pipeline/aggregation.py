from datetime import datetime
from typing import Callable, Dict, List, Optional

from core.models import Event, EventType, EventSource

Aggregator = Callable[[List[Event]], Dict]


def aggregate_window(
    events: List[Event],
    aggregator: Aggregator,
    source: EventSource,
    window_start: Optional[datetime] = None,
    window_end: Optional[datetime] = None,
) -> Event:
    if not events:
        raise ValueError("Cannot aggregate empty window")

    if window_start is None:
        window_start = events[0].timestamp
    if window_end is None:
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
