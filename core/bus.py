import asyncio
from typing import Dict

from core.models import Event, EventSource


class EventBus:
    def __init__(
        self,
        per_source_queue_size: int = 100,
        merged_queue_size: int = 500,
        drop_on_full: bool = True,
    ):
        self.drop_on_full = drop_on_full

        self._source_queues: Dict[EventSource, asyncio.Queue[Event]] = {
            source: asyncio.Queue(maxsize=per_source_queue_size)
            for source in EventSource
        }

        self._merged_queue: asyncio.Queue[Event] = asyncio.Queue(
            maxsize=merged_queue_size
        )

    # -------------------------
    # INGESTION
    # -------------------------

    async def publish(self, event: Event) -> bool:
        source_queue = self._source_queues[event.source]

        try:
            source_queue.put_nowait(event)
            self._merged_queue.put_nowait(event)
            return True
        except asyncio.QueueFull:
            if not self.drop_on_full:
                await source_queue.put(event)
                await self._merged_queue.put(event)
                return True
            return False

    # -------------------------
    # CONSUMPTION
    # -------------------------

    def get_source_queue(self, source: EventSource) -> asyncio.Queue[Event]:
        return self._source_queues[source]

    def get_merged_queue(self) -> asyncio.Queue[Event]:
        return self._merged_queue

    # -------------------------
    # INTROSPECTION (for UI)
    # -------------------------

    def queue_sizes(self) -> Dict[str, int]:
        return {
            source.value: queue.qsize()
            for source, queue in self._source_queues.items()
        } | {"merged": self._merged_queue.qsize()}
