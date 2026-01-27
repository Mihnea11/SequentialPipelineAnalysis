import asyncio

from core.bus import EventBus
from sources.log_source import LogSource


async def main():
    stop_event = asyncio.Event()
    bus = EventBus()

    log_source = LogSource(
        bus=bus,
        stop_event=stop_event,
        service_name="auth-service",
        host="node-1",
    )

    task = asyncio.create_task(log_source.run())
    merged_queue = bus.get_merged_queue()

    try:
        for _ in range(10):
            event = await merged_queue.get()
            print(event)
    finally:
        stop_event.set()
        await asyncio.sleep(0.1)
        task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
