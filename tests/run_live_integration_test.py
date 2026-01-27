import asyncio
from datetime import timedelta

from runtime.supervisor import Supervisor
from runtime.async_processor import run_live_aggregation
from sources.sensor_source import SensorSource
from sources.log_source import LogSource
from sources.feed_source import FeedSource


async def main():
    supervisor = Supervisor()

    sensor = SensorSource(
        bus=supervisor.bus,
        stop_event=supervisor.stop_event,
        sensor_id="sensor-1",
        interval_seconds=1.0,
        location="lab-1",
    )

    logs = LogSource(
        bus=supervisor.bus,
        stop_event=supervisor.stop_event,
        service_name="auth-service",
        host="node-1",
        base_interval=1.0,
        burst_interval=0.2,
        burst_probability=0.2,
    )

    feed = FeedSource(
        bus=supervisor.bus,
        stop_event=supervisor.stop_event,
        users=["user-1", "user-2", "user-3"],
        actions=["login", "logout", "click", "purchase"],
        resources=["/home", "/dashboard", "/checkout"],
        interval_range=(1.5, 3.0),
    )

    supervisor.register(sensor)
    supervisor.register(logs)
    supervisor.register(feed)

    supervisor.start()

    aggregated_queue: asyncio.Queue = asyncio.Queue()

    pipeline_task = asyncio.create_task(
    run_live_aggregation(
        input_queue=supervisor.bus.get_merged_queue(),
        output_queue=aggregated_queue,
        window_size=timedelta(seconds=5),
        stop_event=supervisor.stop_event,
        )
    )

    try:
        for _ in range(10):
            agg = await aggregated_queue.get()
            print(agg)
    finally:
        await supervisor.stop()
        pipeline_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
