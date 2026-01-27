import asyncio
from typing import List

from core.bus import EventBus
from sources.base import BaseSource


class Supervisor:
    def __init__(self):
        self.bus = EventBus()
        self.stop_event = asyncio.Event()

        self._sources: List[BaseSource] = []
        self._tasks: List[asyncio.Task] = []

    # -------------------------
    # SOURCE REGISTRATION
    # -------------------------

    def register(self, source: BaseSource) -> None:
        self._sources.append(source)

    # -------------------------
    # LIFECYCLE MANAGEMENT
    # -------------------------

    def start(self) -> None:
        if self._tasks:
            return  # already running

        for source in self._sources:
            task = asyncio.create_task(source.run())
            task.add_done_callback(self._on_task_done)
            self._tasks.append(task)

    async def stop(self) -> None:
        self.stop_event.set()

        await asyncio.sleep(0.1)

        for task in self._tasks:
            task.cancel()

        self._tasks.clear()

    # -------------------------
    # ERROR VISIBILITY
    # -------------------------

    def _on_task_done(self, task: asyncio.Task) -> None:
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            print(f"[Supervisor] Source crashed: {exc}")
