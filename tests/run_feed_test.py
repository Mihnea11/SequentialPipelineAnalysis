import asyncio

from core.bus import EventBus
from sources.feed_source import FeedSource


async def main():
    stop_event = asyncio.Event()
    bus = EventBus()

    feed = FeedSource(
        bus=bus,
        stop_event=stop_event,
        users=["user-1", "user-2", "user-3"],
        actions=["login", "logout", "click", "purchase"],
        resources=["/home", "/dashboard", "/checkout"],
    )

    task = asyncio.create_task(feed.run())
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
