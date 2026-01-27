from datetime import datetime, timedelta, timezone

from core.models import Event, EventSource, EventType
from pipeline.windowing import tumbling_window
from pipeline.aggregation import aggregate_window


def avg_sensor_value(events):
    values = [e.payload["value"] for e in events]
    return {
        "metric": "temperature",
        "aggregation": "avg",
        "value": sum(values) / len(values),
    }


def main():
    base = datetime(2026, 1, 27, 12, 0, 0, tzinfo=timezone.utc)

    events = [
        Event(
            source=EventSource.SENSOR,
            event_type=EventType.RAW,
            payload={"value": 10},
            timestamp=base,
        ),
        Event(
            source=EventSource.SENSOR,
            event_type=EventType.RAW,
            payload={"value": 20},
            timestamp=base + timedelta(seconds=2),
        ),
        Event(
            source=EventSource.SENSOR,
            event_type=EventType.RAW,
            payload={"value": 30},
            timestamp=base + timedelta(seconds=6),
        ),
    ]

    windows = tumbling_window(events, timedelta(seconds=5))

    for window in windows:
        aggregated = aggregate_window(
            window,
            avg_sensor_value,
            source=EventSource.SENSOR,
        )
        print(aggregated)


if __name__ == "__main__":
    main()
