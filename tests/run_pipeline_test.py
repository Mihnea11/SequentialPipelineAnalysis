from core.models import Event, EventSource, EventType
from pipeline.operators import filter_events, map_events, run_pipeline


def is_sensor_event(event: Event) -> bool:
    return event.source == EventSource.SENSOR


def tag_high_temperature(event: Event) -> Event:
    value = event.payload.get("value", 0)
    if value > 25:
        return Event(
            source=event.source,
            event_type=event.event_type,
            payload=event.payload,
            tags={**event.tags, "alert": "high_temp"},
            correlation_id=event.correlation_id,
        )
    return event


def main():
    events = [
        Event(
            source=EventSource.SENSOR,
            event_type=EventType.RAW,
            payload={"value": 22},
        ),
        Event(
            source=EventSource.LOG,
            event_type=EventType.RAW,
            payload={"level": "INFO"},
        ),
        Event(
            source=EventSource.SENSOR,
            event_type=EventType.RAW,
            payload={"value": 28},
        ),
    ]

    pipeline = run_pipeline(
        events,
        [
            lambda evs: filter_events(evs, is_sensor_event),
            lambda evs: map_events(evs, tag_high_temperature),
        ],
    )

    for event in pipeline:
        print(event)


if __name__ == "__main__":
    main()
