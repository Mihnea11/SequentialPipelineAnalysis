import asyncio

from core.bus import EventBus
from sources.sensor_source import SensorSource


async def main():
    stop_event = asyncio.Event()

    bus = EventBus(
        per_source_queue_size=50,
        merged_queue_size=100,
        drop_on_full=True,
    )

    sensor = SensorSource(
        bus=bus,
        stop_event=stop_event,
        sensor_id="sensor-1",
        metric="temperature",
        unit="°C",
        base_value=22.0,
        interval_seconds=1.0,
        location="lab-1",
    )

    sensor_task = asyncio.create_task(sensor.run())

    merged_queue = bus.get_merged_queue()

    try:
        for _ in range(10):
            event = await merged_queue.get()
            print(event)
    finally:
        stop_event.set()
        await asyncio.sleep(0.1)
        sensor_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
