import asyncio
from datetime import timedelta

from core.bus import EventBus
from metrics.collector import MetricsCollector
from runtime.async_processor import run_live_aggregation
from runtime.supervisor import Supervisor
from sources.feed_source import FeedSource
from sources.log_source import LogSource
from sources.sensor_source import SensorSource


async def print_metrics_periodically(metrics: MetricsCollector, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        await asyncio.sleep(2)
        snap = metrics.snapshot()
        print("\n--- METRICS SNAPSHOT ---")
        print(snap)


async def main() -> None:
    metrics = MetricsCollector()

    supervisor = Supervisor()
    supervisor.bus = EventBus(
        per_source_queue_size=10,
        merged_queue_size=30,
        drop_on_full=True,
        metrics=metrics,
        enable_per_source_queues=False,
    )

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
        base_interval=0.2,
        burst_interval=0.05,
        burst_probability=0.6,
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
            metrics=metrics,
        )
    )

    metrics_task = asyncio.create_task(
        print_metrics_periodically(metrics, supervisor.stop_event)
    )

    try:
        for _ in range(9):
            agg = await aggregated_queue.get()
            print("\nAGG:", agg)
    finally:
        await supervisor.stop()

        pipeline_task.cancel()
        metrics_task.cancel()

        try:
            await pipeline_task
        except asyncio.CancelledError:
            pass

        try:
            await metrics_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(main())
