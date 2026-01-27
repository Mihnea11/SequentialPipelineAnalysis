from datetime import datetime, timedelta
from typing import Iterable, Iterator, List, Optional

from core.models import Event


def tumbling_window(
    events: Iterable[Event],
    window_size: timedelta,
) -> Iterator[List[Event]]:
    current_window_start: Optional[datetime] = None
    current_window_events: List[Event] = []

    for event in events:
        event_time = event.timestamp

        if current_window_start is None:
            current_window_start = event_time
            current_window_events.append(event)
            continue

        window_end = current_window_start + window_size

        if event_time < window_end:
            current_window_events.append(event)
        else:
            yield current_window_events

            current_window_start = event_time
            current_window_events = [event]

    if current_window_events:
        yield current_window_events
