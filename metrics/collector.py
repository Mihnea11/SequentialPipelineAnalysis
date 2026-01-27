from __future__ import annotations

import time
import math
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, Optional


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
class WindowMetric:
    start: str
    end: str
    count_total: int
    count_by_source: Dict[str, int]
    aggregates_emitted: int
    aggregation_time_ms: float


@dataclass
class MetricsCollector:
    # ingestion
    ingested_total: int = 0
    ingested_by_source: Dict[str, int] = field(default_factory=dict)
    dropped_total: int = 0
    dropped_by_source: Dict[str, int] = field(default_factory=dict)

    # processing (global)
    processed_total: int = 0
    aggregated_total: int = 0

    # processing (per source)
    processed_by_source: Dict[str, int] = field(default_factory=dict)
    process_rate_by_source: Dict[str, RateMeter] = field(default_factory=dict)
    latency_by_source: Dict[str, LatencyMeter] = field(default_factory=dict)

    # rates (global)
    ingest_rate: RateMeter = field(default_factory=lambda: RateMeter(window_seconds=10.0))
    process_rate: RateMeter = field(default_factory=lambda: RateMeter(window_seconds=10.0))
    aggregate_rate: RateMeter = field(default_factory=lambda: RateMeter(window_seconds=10.0))

    # latency (global)
    event_processing_latency: LatencyMeter = field(default_factory=lambda: LatencyMeter(max_samples=2000))

    # queue sizes (last observed)
    last_queue_sizes: Dict[str, int] = field(default_factory=dict)

    # window metrics (rolling)
    window_max_samples: int = 200
    windows: Deque[WindowMetric] = field(default_factory=deque)

    # -------- internal helpers --------

    def _ensure_source(self, source: str) -> None:
        if source not in self.process_rate_by_source:
            self.process_rate_by_source[source] = RateMeter(window_seconds=10.0)
        if source not in self.latency_by_source:
            self.latency_by_source[source] = LatencyMeter(max_samples=2000)

    # -------- hooks --------

    def record_ingest(self, source: str, dropped: bool, queue_sizes: Dict[str, int]) -> None:
        self.ingested_total += 1
        self.ingested_by_source[source] = self.ingested_by_source.get(source, 0) + 1
        self.ingest_rate.mark()

        if dropped:
            self.dropped_total += 1
            self.dropped_by_source[source] = self.dropped_by_source.get(source, 0) + 1

        self.last_queue_sizes = dict(queue_sizes)

    def record_processed(self, source: str, latency_ms: float) -> None:
        self.processed_total += 1
        self.process_rate.mark()
        self.event_processing_latency.add(latency_ms)

        self._ensure_source(source)
        self.processed_by_source[source] = self.processed_by_source.get(source, 0) + 1
        self.process_rate_by_source[source].mark()
        self.latency_by_source[source].add(latency_ms)

    def record_aggregated(self) -> None:
        self.aggregated_total += 1
        self.aggregate_rate.mark()

    def record_window(
        self,
        start: str,
        end: str,
        count_by_source: Dict[str, int],
        aggregates_emitted: int,
        aggregation_time_ms: float,
    ) -> None:
        count_total = sum(count_by_source.values())

        self.windows.append(
            WindowMetric(
                start=start,
                end=end,
                count_total=count_total,
                count_by_source=dict(count_by_source),
                aggregates_emitted=aggregates_emitted,
                aggregation_time_ms=aggregation_time_ms,
            )
        )

        while len(self.windows) > self.window_max_samples:
            self.windows.popleft()

    # -------- reporting --------

    def _window_summary(self) -> Dict[str, Any]:
        if not self.windows:
            return {
                "last_window": None,
                "agg_time_ms": {"avg": None, "p50": None, "p95": None},
                "count_total": {"avg": None, "p50": None, "p95": None},
                "aggregates_emitted_avg": None,
            }

        agg_times = sorted(w.aggregation_time_ms for w in self.windows)
        counts = sorted(w.count_total for w in self.windows)
        n = len(self.windows)

        def p(xs, q):
            if not xs:
                return None
            idx = math.ceil(q * (len(xs) - 1))
            return xs[idx]

        last = self.windows[-1]

        return {
            "last_window": {
                "start": last.start,
                "end": last.end,
                "count_total": last.count_total,
                "count_by_source": dict(last.count_by_source),
                "aggregates_emitted": last.aggregates_emitted,
                "aggregation_time_ms": last.aggregation_time_ms,
            },
            "agg_time_ms": {
                "avg": sum(agg_times) / n,
                "p50": p(agg_times, 0.50),
                "p95": p(agg_times, 0.95),
            },
            "count_total": {
                "avg": sum(counts) / n,
                "p50": p(counts, 0.50),
                "p95": p(counts, 0.95),
            },
            "aggregates_emitted_avg": sum(w.aggregates_emitted for w in self.windows) / n,
        }

    def snapshot(self) -> Dict:
        per_source = {}
        for src in sorted(set(self.ingested_by_source.keys()) | set(self.processed_by_source.keys())):
            rate = self.process_rate_by_source[src].rate_per_sec() if src in self.process_rate_by_source else 0.0
            lat = self.latency_by_source[src].snapshot() if src in self.latency_by_source else {"avg_ms": None, "p50_ms": None, "p95_ms": None}
            per_source[src] = {
                "processed_total": self.processed_by_source.get(src, 0),
                "process_eps": rate,
                "latency_ms": lat,
            }

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
            "per_source_processing": per_source,
            "queue_sizes": dict(self.last_queue_sizes),
            "drop_ratio": (self.dropped_total / self.ingested_total) if self.ingested_total else 0.0,
            "window_metrics": self._window_summary(),
        }
