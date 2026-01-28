import asyncio
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Optional

from core.bus import EventBus
from metrics.collector import MetricsCollector
from runtime.async_processor import run_live_aggregation
from runtime.supervisor import Supervisor
from sources.feed_source import FeedSource
from sources.log_source import LogSource
from sources.sensor_source import SensorSource


@dataclass(frozen=True)
class EngineConfig:
    stress_mode: bool = False

    per_source_queue_size: int = 10
    merged_queue_size: int = 30
 
    artificial_delay_ms: float = 0.0
    log_base_interval: float = 0.2
    log_burst_interval: float = 0.05
    log_burst_probability: float = 0.6


async def run_engine_for_ui(stop_thread_event, out_q, config: Optional[EngineConfig] = None) -> None:
    config = config or EngineConfig()

    metrics = MetricsCollector()

    if config.stress_mode:
        per_src = min(config.per_source_queue_size, 2)
        merged = min(config.merged_queue_size, 5)
        delay_ms = max(config.artificial_delay_ms, 30.0)
        log_base = min(config.log_base_interval, 0.06)
        log_burst = min(config.log_burst_interval, 0.01)
        log_prob = max(config.log_burst_probability, 0.9)
    else:
        per_src = config.per_source_queue_size
        merged = config.merged_queue_size
        delay_ms = config.artificial_delay_ms
        log_base = config.log_base_interval
        log_burst = config.log_burst_interval
        log_prob = config.log_burst_probability

    supervisor = Supervisor()
    supervisor.bus = EventBus(
        per_source_queue_size=per_src,
        merged_queue_size=merged,
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
        base_interval=log_base,
        burst_interval=log_burst,
        burst_probability=log_prob,
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

    aggregated_queue: asyncio.Queue[Any] = asyncio.Queue()

    _last_emit: dict[str, float] = {}

    def emit_event(ev: Any) -> None:
        src = getattr(ev, "source", None)
        if src is None and isinstance(ev, dict):
            src = ev.get("source", "unknown")
        src = str(src) if src is not None else "unknown"

        now = time.time()
        last = _last_emit.get(src, 0.0)
        if now - last < 0.10:
            return
        _last_emit[src] = now

        try:
            out_q.put_nowait({"type": "event", "ts": now, "data": ev})
        except Exception:
            pass

    async def on_batch_delay():
        if delay_ms > 0:
            await asyncio.sleep(delay_ms / 1000.0)

    pipeline_task = asyncio.create_task(
        run_live_aggregation(
            input_queue=supervisor.bus.get_merged_queue(),
            output_queue=aggregated_queue,
            window_size=timedelta(seconds=5),
            stop_event=supervisor.stop_event,
            metrics=metrics,
            on_event=emit_event,
            on_after_batch=on_batch_delay,
        )
    )

    async def stop_watcher():
        while not stop_thread_event.is_set():
            await asyncio.sleep(0.1)
        supervisor.stop_event.set()

    async def metrics_publisher():
        while not supervisor.stop_event.is_set():
            await asyncio.sleep(2)
            snap = metrics.snapshot()
            try:
                out_q.put_nowait({"type": "metrics", "ts": time.time(), "data": snap})
            except Exception:
                pass

    async def agg_forwarder():
        while not supervisor.stop_event.is_set():
            try:
                agg = await asyncio.wait_for(aggregated_queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            try:
                out_q.put_nowait({"type": "agg", "ts": time.time(), "data": agg})
            except Exception:
                pass

    stop_task = asyncio.create_task(stop_watcher())
    metrics_task = asyncio.create_task(metrics_publisher())
    forward_task = asyncio.create_task(agg_forwarder())

    try:
        await stop_task
    finally:
        await supervisor.stop()
        for t in (pipeline_task, metrics_task, forward_task):
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
