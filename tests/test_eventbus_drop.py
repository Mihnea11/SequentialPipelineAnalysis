import asyncio
import pytest

from core.bus import EventBus
from core.models import Event, EventSource, EventType


@pytest.mark.asyncio
async def test_eventbus_drops_when_merged_queue_full():
    bus = EventBus(
        per_source_queue_size=1,
        merged_queue_size=1,
        drop_on_full=True,
        metrics=None,
        enable_per_source_queues=False,
    )

    ev1 = Event(id="1", source=EventSource.LOG, event_type=EventType.RAW, timestamp=None, payload={})
    ev2 = Event(id="2", source=EventSource.LOG, event_type=EventType.RAW, timestamp=None, payload={})

    ok1 = await bus.publish(ev1)
    ok2 = await bus.publish(ev2)

    assert ok1 is True
    assert ok2 is False
