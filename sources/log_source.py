import asyncio
import random

from core.models import (
    Event,
    EventSource,
    EventType,
    LogPayload,
    LogLevel,
)
from sources.base import BaseSource


class LogSource(BaseSource):
    def __init__(
        self,
        bus,
        stop_event: asyncio.Event,
        service_name: str,
        host: str,
        base_interval: float = 1.5,
        burst_interval: float = 0.2,
        burst_probability: float = 0.1,
    ):
        super().__init__(bus, stop_event)

        self.service_name = service_name
        self.host = host

        self.base_interval = base_interval
        self.burst_interval = burst_interval
        self.burst_probability = burst_probability

        self._messages = {
            LogLevel.DEBUG: "Debugging internal state",
            LogLevel.INFO: "Operation completed successfully",
            LogLevel.WARNING: "Potential issue detected",
            LogLevel.ERROR: "Error while processing request",
            LogLevel.CRITICAL: "System failure",
        }

    # -------------------------
    # INTERNAL BEHAVIOR
    # -------------------------

    def _choose_log_level(self) -> LogLevel:
        return random.choices(
            population=list(LogLevel),
            weights=[0.4, 0.35, 0.15, 0.08, 0.02],
            k=1,
        )[0]

    def _choose_interval(self) -> float:
        if random.random() < self.burst_probability:
            return self.burst_interval
        return self.base_interval

    # -------------------------
    # MAIN LOOP
    # -------------------------

    async def run(self) -> None:
        while not self.stop_event.is_set():
            level = self._choose_log_level()

            payload = {
                "level": level.value,
                "message": self._messages[level],
                "service": self.service_name,
                "host": self.host,
            }

            event = Event(
                source=EventSource.LOG,
                event_type=EventType.RAW,
                payload=payload,
                tags={
                    "service": self.service_name,
                    "level": level.value,
                },
            )

            await self.bus.publish(event)
            await asyncio.sleep(self._choose_interval())
