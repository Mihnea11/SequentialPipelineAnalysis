import asyncio

from runtime.supervisor import Supervisor
from sources.sensor_source import SensorSource
from sources.log_source import LogSource
from sources.feed_source import FeedSource


async def main():
    supervisor = Supervisor()

    sensor = SensorSource(
        bus=supervisor.bus,
        stop_event=supervisor.stop_event,
        sensor_id="sensor-1",
        location="lab-1",
    )

    logs = LogSource(
        bus=supervisor.bus,
        stop_event=supervisor.stop_event,
        service_name="auth-service",
        host="node-1",
    )

    feed = FeedSource(
        bus=supervisor.bus,
        stop_event=supervisor.stop_event,
        users=["user-1", "user-2"],
        actions=["login", "logout", "click", "purchase"],
        resources=["/home", "/dashboard", "/checkout"],
    )

    supervisor.register(sensor)
    supervisor.register(logs)
    supervisor.register(feed)

    supervisor.start()

    merged_queue = supervisor.bus.get_merged_queue()

    try:
        for _ in range(20):
            event = await merged_queue.get()
            print(event)
    finally:
        await supervisor.stop()


if __name__ == "__main__":
    asyncio.run(main())
