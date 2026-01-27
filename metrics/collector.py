from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Optional


def _now_s() -> float:
    return time.time()


@dataclass
class RateMeter:
    window_seconds: float = 10.0
    _timestamps: Deque[float] = field(default_factory=deque)

    def mark(self, n: int = 1) -> None:
        t = _now_s()
        for _ in range(n):
            self._timestamps.append(t)
        self._trim(t)

    def _trim(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self._timestamps and self._timestamps[0] < cutoff:
            self._timestamps.popleft()

    def rate_per_sec(self) -> float:
        now = _now_s()
        self._trim(now)
        return len(self._timestamps) / self.window_seconds


@dataclass
class LatencyMeter:
    max_samples: int = 2000
    _samples_ms: Deque[float] = field(default_factory=deque)

    def add(self, latency_ms: float) -> None:
        self._samples_ms.append(latency_ms)
        while len(self._samples_ms) > self.max_samples:
            self._samples_ms.popleft()

    def snapshot(self) -> Dict[str, Optional[float]]:
        if not self._samples_ms:
            return {"avg_ms": None, "p50_ms": None, "p95_ms": None}
        xs = sorted(self._samples_ms)
        n = len(xs)
        avg = sum(xs) / n
        p50 = xs[int(0.50 * (n - 1))]
        p95 = xs[int(0.95 * (n - 1))]
        return {"avg_ms": avg, "p50_ms": p50, "p95_ms": p95}


@dataclass
class MetricsCollector:
    # ingestion
    ingested_total: int = 0
    ingested_by_source: Dict[str, int] = field(default_factory=dict)
    dropped_total: int = 0
    dropped_by_source: Dict[str, int] = field(default_factory=dict)

    # processing
    processed_total: int = 0
    aggregated_total: int = 0

    # rates
    ingest_rate: RateMeter = field(default_factory=lambda: RateMeter(window_seconds=10.0))
    process_rate: RateMeter = field(default_factory=lambda: RateMeter(window_seconds=10.0))
    aggregate_rate: RateMeter = field(default_factory=lambda: RateMeter(window_seconds=10.0))

    # latency: (processing time - event.timestamp)
    event_processing_latency: LatencyMeter = field(default_factory=lambda: LatencyMeter(max_samples=2000))

    # queue sizes (last observed)
    last_queue_sizes: Dict[str, int] = field(default_factory=dict)

    # -------- hooks --------

    def record_ingest(self, source: str, dropped: bool, queue_sizes: Dict[str, int]) -> None:
        self.ingested_total += 1
        self.ingested_by_source[source] = self.ingested_by_source.get(source, 0) + 1
        self.ingest_rate.mark()

        if dropped:
            self.dropped_total += 1
            self.dropped_by_source[source] = self.dropped_by_source.get(source, 0) + 1

        self.last_queue_sizes = dict(queue_sizes)

    def record_processed(self, latency_ms: float) -> None:
        self.processed_total += 1
        self.process_rate.mark()
        self.event_processing_latency.add(latency_ms)

    def record_aggregated(self) -> None:
        self.aggregated_total += 1
        self.aggregate_rate.mark()

    # -------- reporting --------

    def snapshot(self) -> Dict:
        return {
            "ingested_total": self.ingested_total,
            "ingested_by_source": dict(self.ingested_by_source),
            "dropped_total": self.dropped_total,
            "dropped_by_source": dict(self.dropped_by_source),
            "processed_total": self.processed_total,
            "aggregated_total": self.aggregated_total,
            "rates_eps": {
                "ingest": self.ingest_rate.rate_per_sec(),
                "process": self.process_rate.rate_per_sec(),
                "aggregate": self.aggregate_rate.rate_per_sec(),
            },
            "event_processing_latency_ms": self.event_processing_latency.snapshot(),
            "queue_sizes": dict(self.last_queue_sizes),
        }
