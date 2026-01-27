from abc import ABC, abstractmethod
import asyncio

from core.bus import EventBus


class BaseSource(ABC):
    """
    Abstract base class for all event sources.
    """

    def __init__(self, bus: EventBus, stop_event: asyncio.Event):
        self.bus = bus
        self.stop_event = stop_event

    @abstractmethod
    async def run(self) -> None:
        """
        Main async loop of the source.
        Must exit cleanly when stop_event is set.
        """
        pass
