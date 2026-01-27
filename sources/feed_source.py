import asyncio
import random
from datetime import datetime, timezone

from core.models import (
    Event,
    EventSource,
    EventType,
)
from sources.base import BaseSource


class FeedSource(BaseSource):
    def __init__(
        self,
        bus,
        stop_event: asyncio.Event,
        users: list[str],
        actions: list[str],
        resources: list[str],
        interval_range: tuple[float, float] = (2.0, 4.0),
    ):
        super().__init__(bus, stop_event)

        self.users = users
        self.actions = actions
        self.resources = resources
        self.interval_range = interval_range

    # -------------------------
    # INTERNAL BEHAVIOR
    # -------------------------

    def _generate_payload(self) -> dict:
        return {
            "user_id": random.choice(self.users),
            "action": random.choice(self.actions),
            "resource": random.choice(self.resources),
            "success": random.random() > 0.1,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def _next_interval(self) -> float:
        return random.uniform(*self.interval_range)

    # -------------------------
    # MAIN LOOP
    # -------------------------

    async def run(self) -> None:
        while not self.stop_event.is_set():
            payload = self._generate_payload()

            event = Event(
                source=EventSource.FEED,
                event_type=EventType.RAW,
                payload=payload,
                tags={
                    "action": payload["action"],
                    "success": str(payload["success"]),
                },
            )

            await self.bus.publish(event)
            await asyncio.sleep(self._next_interval())
