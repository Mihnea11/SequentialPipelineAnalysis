import asyncio
from typing import Dict, Optional

from core.models import Event, EventSource
from metrics.collector import MetricsCollector


class EventBus:
    def __init__(
        self,
        per_source_queue_size: int = 100,
        merged_queue_size: int = 500,
        drop_on_full: bool = True,
        metrics: Optional[MetricsCollector] = None,
    ):
        self.drop_on_full = drop_on_full
        self.metrics = metrics

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
        dropped = False

        try:
            source_queue.put_nowait(event)
            self._merged_queue.put_nowait(event)
        except asyncio.QueueFull:
            if not self.drop_on_full:
                await source_queue.put(event)
                await self._merged_queue.put(event)
            else:
                dropped = True

        if self.metrics is not None:
            self.metrics.record_ingest(
                source=event.source.value,
                dropped=dropped,
                queue_sizes=self.queue_sizes(),
            )

        return not dropped

    # -------------------------
    # CONSUMPTION
    # -------------------------

    def get_source_queue(self, source: EventSource) -> asyncio.Queue[Event]:
        return self._source_queues[source]

    def get_merged_queue(self) -> asyncio.Queue[Event]:
        return self._merged_queue

    # -------------------------
    # INTROSPECTION (for UI / METRICS)
    # -------------------------

    def queue_sizes(self) -> Dict[str, int]:
        return {
            source.value: queue.qsize()
            for source, queue in self._source_queues.items()
        } | {"merged": self._merged_queue.qsize()}
