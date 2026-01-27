import asyncio
import random
from datetime import datetime, timezone

from core.models import (
    Event,
    EventSource,
    EventType,
    SensorPayload,
)
from sources.base import BaseSource


class SensorSource(BaseSource):
    def __init__(
        self,
        bus,
        stop_event: asyncio.Event,
        sensor_id: str,
        metric: str = "temperature",
        unit: str = "°C",
        base_value: float = 20.0,
        noise_std: float = 0.3,
        drift_per_minute: float = 0.01,
        anomaly_probability: float = 0.01,
        interval_seconds: float = 1.0,
        location: str | None = None,
    ):
        super().__init__(bus, stop_event)

        self.sensor_id = sensor_id
        self.metric = metric
        self.unit = unit

        self.base_value = base_value
        self.noise_std = noise_std
        self.drift_per_minute = drift_per_minute
        self.anomaly_probability = anomaly_probability
        self.interval_seconds = interval_seconds
        self.location = location

        self._current_drift = 0.0
        self._last_drift_update = datetime.now(timezone.utc)

    # -------------------------
    # INTERNAL BEHAVIOR
    # -------------------------

    def _update_drift(self) -> None:
        now = datetime.now(timezone.utc)
        elapsed_minutes = (now - self._last_drift_update).total_seconds() / 60.0
        self._current_drift += elapsed_minutes * self.drift_per_minute
        self._last_drift_update = now

    def _generate_value(self) -> float:
        noise = random.gauss(0, self.noise_std)
        value = self.base_value + self._current_drift + noise

        if random.random() < self.anomaly_probability:
            value += random.choice([-10, 10])

        return round(value, 3)

    # -------------------------
    # MAIN LOOP
    # -------------------------

    async def run(self) -> None:
        while not self.stop_event.is_set():
            self._update_drift()

            payload = SensorPayload(
                sensor_id=self.sensor_id,
                metric=self.metric,
                value=self._generate_value(),
                unit=self.unit,
                location=self.location,
            )

            event = Event(
                source=EventSource.SENSOR,
                event_type=EventType.RAW,
                payload=payload.__dict__,
                tags={
                    "metric": self.metric,
                    "sensor_id": self.sensor_id,
                },
            )

            await self.bus.publish(event)
            await asyncio.sleep(self.interval_seconds)
