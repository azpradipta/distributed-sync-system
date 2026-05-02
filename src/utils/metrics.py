"""
Metrics collection for the Distributed Sync System.
Implements lightweight counters, gauges, and histograms (Prometheus-style).
"""
import time
from typing import Dict, List
from collections import defaultdict
from dataclasses import dataclass, field


# ── Metric Primitives ─────────────────────────────────────────────────────────

class Counter:
    """Monotonically increasing counter."""
    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        self._value: float = 0.0

    def increment(self, amount: float = 1.0) -> None:
        self._value += amount

    @property
    def value(self) -> float:
        return self._value

    def reset(self) -> None:
        self._value = 0.0


class Gauge:
    """Arbitrary value that can go up or down."""
    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        self._value: float = 0.0

    def set(self, value: float) -> None:
        self._value = value

    def increment(self, amount: float = 1.0) -> None:
        self._value += amount

    def decrement(self, amount: float = 1.0) -> None:
        self._value -= amount

    @property
    def value(self) -> float:
        return self._value


class Histogram:
    """Distribution of values with configurable buckets."""
    DEFAULT_BUCKETS = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]

    def __init__(self, name: str, description: str = "", buckets: List[float] = None):
        self.name = name
        self.description = description
        self.buckets = buckets or self.DEFAULT_BUCKETS
        self._bucket_counts: Dict[float, int] = {b: 0 for b in self.buckets}
        self._sum: float = 0.0
        self._count: int = 0

    def observe(self, value: float) -> None:
        self._count += 1
        self._sum += value
        for bucket in self.buckets:
            if value <= bucket:
                self._bucket_counts[bucket] += 1

    @property
    def count(self) -> int:
        return self._count

    @property
    def sum(self) -> float:
        return self._sum

    @property
    def avg(self) -> float:
        return self._sum / self._count if self._count > 0 else 0.0

    @property
    def p99(self) -> float:
        """Approximate 99th percentile from bucket data."""
        if self._count == 0:
            return 0.0
        target = self._count * 0.99
        cumulative = 0
        for bucket, cnt in self._bucket_counts.items():
            cumulative += cnt
            if cumulative >= target:
                return bucket
        return self.buckets[-1]


# ── Metrics Collector ─────────────────────────────────────────────────────────

class MetricsCollector:
    """Global metrics registry for all system components."""

    def __init__(self):
        self._counters: Dict[str, Counter] = {}
        self._gauges: Dict[str, Gauge] = {}
        self._histograms: Dict[str, Histogram] = {}
        self._start_time = time.time()
        self._init_metrics()

    def _init_metrics(self):
        # Lock metrics
        self.counter("lock_acquire_total", "Total lock acquisitions")
        self.counter("lock_release_total", "Total lock releases")
        self.counter("lock_timeout_total", "Total lock timeouts")
        self.counter("deadlock_detected_total", "Total deadlocks detected")
        self.gauge("active_locks", "Currently held locks")
        self.histogram("lock_wait_seconds", "Lock wait time distribution")

        # Queue metrics
        self.counter("messages_produced_total", "Total messages produced")
        self.counter("messages_consumed_total", "Total messages consumed")
        self.counter("messages_redelivered_total", "Total messages redelivered")
        self.gauge("queue_depth", "Current queue depth")
        self.histogram("message_processing_seconds", "Message processing time")

        # Cache metrics
        self.counter("cache_hits_total", "Total cache hits")
        self.counter("cache_misses_total", "Total cache misses")
        self.counter("cache_evictions_total", "Total cache evictions (LRU)")
        self.counter("cache_invalidations_total", "Total cache invalidations (MESI)")
        self.gauge("cache_size", "Current number of cache entries")

        # Raft metrics
        self.counter("raft_elections_total", "Total elections started")
        self.counter("raft_commits_total", "Total log entries committed")
        self.gauge("raft_term", "Current Raft term")
        self.gauge("raft_log_size", "Current Raft log size")

        # RPC metrics
        self.counter("rpc_calls_total", "Total RPC calls")
        self.counter("rpc_failures_total", "Total RPC failures")
        self.histogram("rpc_duration_seconds", "RPC call duration")

    # ── Factory methods ───────────────────────────────────────────────────────

    def counter(self, name: str, description: str = "") -> Counter:
        if name not in self._counters:
            self._counters[name] = Counter(name, description)
        return self._counters[name]

    def gauge(self, name: str, description: str = "") -> Gauge:
        if name not in self._gauges:
            self._gauges[name] = Gauge(name, description)
        return self._gauges[name]

    def histogram(self, name: str, description: str = "") -> Histogram:
        if name not in self._histograms:
            self._histograms[name] = Histogram(name, description)
        return self._histograms[name]

    # ── Reporting ─────────────────────────────────────────────────────────────

    def get_snapshot(self) -> dict:
        """Return full metrics snapshot as a serializable dict."""
        return {
            "uptime_seconds": round(time.time() - self._start_time, 2),
            "cache_hit_rate": self.cache_hit_rate(),
            "counters": {n: c.value for n, c in self._counters.items()},
            "gauges": {n: g.value for n, g in self._gauges.items()},
            "histograms": {
                n: {
                    "count": h.count,
                    "sum": round(h.sum, 6),
                    "avg_ms": round(h.avg * 1000, 3),
                    "p99_ms": round(h.p99 * 1000, 3),
                }
                for n, h in self._histograms.items()
            },
        }

    def cache_hit_rate(self) -> float:
        hits = self._counters.get("cache_hits_total", Counter("")).value
        misses = self._counters.get("cache_misses_total", Counter("")).value
        total = hits + misses
        return round(hits / total, 4) if total > 0 else 0.0


# Global singleton
metrics = MetricsCollector()
