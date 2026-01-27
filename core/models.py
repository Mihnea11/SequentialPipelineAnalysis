from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional
import uuid


# -------------------------
# ENUMS (explicit semantics)
# -------------------------

class EventSource(str, Enum):
    LOG = "log"
    SENSOR = "sensor"
    FEED = "feed"


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class EventType(str, Enum):
    RAW = "raw"
    AGGREGATED = "aggregated"
    ALERT = "alert"


# -------------------------
# BASE EVENT
# -------------------------

@dataclass(frozen=True)
class Event:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    source: EventSource = EventSource.FEED
    event_type: EventType = EventType.RAW
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    payload: Dict[str, Any] = field(default_factory=dict)
    tags: Dict[str, str] = field(default_factory=dict)
    correlation_id: Optional[str] = None


# -------------------------
# SOURCE-SPECIFIC PAYLOADS
# -------------------------

@dataclass(frozen=True)
class LogPayload:
    level: LogLevel
    message: str
    service: str
    host: str


@dataclass(frozen=True)
class SensorPayload:
    sensor_id: str
    metric: str            
    value: float
    unit: str 
    location: Optional[str] = None


@dataclass(frozen=True)
class FeedPayload:
    user_id: str
    action: str
    resource: str
    metadata: Dict[str, Any] = field(default_factory=dict)


# -------------------------
# AGGREGATED / WINDOWED DATA
# -------------------------

@dataclass(frozen=True)
class WindowMetadata:
    window_start: datetime
    window_end: datetime
    event_count: int


@dataclass(frozen=True)
class AggregatedPayload:
    metric: str
    value: float
    aggregation: str
    window: WindowMetadata


# -------------------------
# METRICS & OBSERVABILITY
# -------------------------

@dataclass(frozen=True)
class ProcessingMetrics:
    received_at: datetime
    processed_at: datetime
    latency_ms: float
    dropped: bool = False


@dataclass(frozen=True)
class EventWithMetrics:
    event: Event
    metrics: ProcessingMetrics
