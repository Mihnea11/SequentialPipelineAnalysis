from datetime import datetime, timedelta, timezone

from core.models import Event, EventSource, EventType
from pipeline.windowing import tumbling_window


def main():
    base_time = datetime(2026, 1, 27, 12, 0, 0, tzinfo=timezone.utc)

    events = [
        Event(source=EventSource.SENSOR, event_type=EventType.RAW,
              payload={"value": 1}, timestamp=base_time),

        Event(source=EventSource.SENSOR, event_type=EventType.RAW,
              payload={"value": 2}, timestamp=base_time + timedelta(seconds=2)),

        Event(source=EventSource.SENSOR, event_type=EventType.RAW,
              payload={"value": 3}, timestamp=base_time + timedelta(seconds=6)),

        Event(source=EventSource.SENSOR, event_type=EventType.RAW,
              payload={"value": 4}, timestamp=base_time + timedelta(seconds=7)),
    ]

    windows = tumbling_window(events, window_size=timedelta(seconds=5))

    for i, window in enumerate(windows, start=1):
        print(f"Window {i}:")
        for e in window:
            print("  ", e.payload)


if __name__ == "__main__":
    main()
