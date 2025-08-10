"""
Metrics collection system for performance monitoring.
"""
import json
import time
import uuid
from dataclasses import asdict, dataclass

import psutil


@dataclass
class Metric:
    """Base metric data structure."""

    timestamp: float
    name: str
    value: float
    tags: dict[str, str] = None

    def to_dict(self) -> dict:
        data = asdict(self)
        if not data.get("tags"):
            data.pop("tags", None)
        return data


class MetricsSession:
    """Manages a metrics collection session."""

    def __init__(self, session_id: str = None, redis_client=None):
        self.id = session_id or str(uuid.uuid4())
        self.redis = redis_client
        self.start_time = time.time()
        self.key_prefix = f"metrics:{self.id}:"

    async def record(self, metric: Metric) -> bool:
        """Record a single metric."""
        if not self.redis:
            return False

        try:
            key = f"{self.key_prefix}data"
            await self.redis.rpush(key, json.dumps(metric.to_dict()))
            await self.redis.expire(key, 3600)
            return True
        except Exception:
            return False

    async def record_batch(self, metrics: list[Metric]) -> bool:
        """Record multiple metrics at once."""
        if not self.redis or not metrics:
            return False

        try:
            key = f"{self.key_prefix}data"
            pipeline = self.redis.pipeline()
            for metric in metrics:
                pipeline.rpush(key, json.dumps(metric.to_dict()))
            pipeline.expire(key, 3600)
            await pipeline.execute()
            return True
        except Exception:
            return False

    async def get_metrics(self) -> list[dict]:
        """Retrieve all metrics for this session."""
        if not self.redis:
            return []

        try:
            key = f"{self.key_prefix}data"
            data = await self.redis.lrange(key, 0, -1)
            return [json.loads(item) for item in data]
        except Exception:
            return []

    async def clear(self) -> int:
        """Clear all data for this session."""
        if not self.redis:
            return 0

        try:
            pattern = f"{self.key_prefix}*"
            cursor = 0
            deleted = 0

            while True:
                cursor, keys = await self.redis.scan(cursor, match=pattern)
                if keys:
                    deleted += await self.redis.delete(*keys)
                if cursor == 0:
                    break

            return deleted
        except Exception:
            return 0

    def compute_statistics(self, metrics: list[dict]) -> dict[str, dict[str, float]]:
        """Compute statistics from raw metrics."""
        if not metrics:
            return {}

        # Group metrics by name
        grouped = {}
        for m in metrics:
            name = m.get("name", "unknown")
            if name not in grouped:
                grouped[name] = []
            if isinstance(m.get("value"), int | float):
                grouped[name].append(m["value"])

        # Compute stats for each metric
        stats = {}
        for name, values in grouped.items():
            if not values:
                continue

            sorted_vals = sorted(values)
            n = len(sorted_vals)

            stats[name] = {
                "count": n,
                "sum": sum(values),
                "avg": sum(values) / n,
                "min": sorted_vals[0],
                "max": sorted_vals[-1],
            }

            # Add percentiles for larger datasets
            if n >= 10:
                for p in [50, 75, 90, 95, 99]:
                    idx = min(int(n * p / 100), n - 1)
                    stats[name][f"p{p}"] = sorted_vals[idx]

        return stats


class MetricsCollector:
    """Collects various system and application metrics."""

    def __init__(self):
        self.process = psutil.Process()

    def collect_system_metrics(self) -> list[Metric]:
        """Collect system-level metrics."""
        timestamp = time.time()
        metrics = []

        # CPU metrics
        metrics.append(Metric(timestamp=timestamp, name="cpu_percent", value=self.process.cpu_percent()))

        # Memory metrics
        mem_info = self.process.memory_info()
        metrics.extend(
            [
                Metric(timestamp=timestamp, name="memory_rss_mb", value=mem_info.rss / 1024 / 1024),
                Metric(timestamp=timestamp, name="memory_percent", value=self.process.memory_percent()),
            ]
        )

        # System-wide metrics
        metrics.extend(
            [
                Metric(timestamp=timestamp, name="system_cpu_percent", value=psutil.cpu_percent(interval=0)),
                Metric(timestamp=timestamp, name="system_memory_percent", value=psutil.virtual_memory().percent),
            ]
        )

        return metrics

    async def collect_postgres_metrics(self, session) -> list[Metric]:
        """Collect PostgreSQL metrics."""
        if not session:
            return []

        timestamp = time.time()
        metrics = []

        queries = [
            ("pg_connections_active", "SELECT count(*) FROM pg_stat_activity WHERE state = 'active'"),
            ("pg_connections_total", "SELECT count(*) FROM pg_stat_activity"),
            ("pg_database_size_mb", "SELECT pg_database_size(current_database()) / 1024.0 / 1024.0"),
        ]

        for name, query in queries:
            try:
                from sqlalchemy import text

                result = await session.execute(text(query))
                value = result.scalar()
                if value is not None:
                    metrics.append(Metric(timestamp=timestamp, name=name, value=float(value)))
            except Exception:
                pass

        return metrics

    def create_request_metric(self, endpoint: str, method: str, duration_ms: float, status: int) -> Metric:
        """Create a metric for an HTTP request."""
        return Metric(
            timestamp=time.time(),
            name="request_duration_ms",
            value=duration_ms,
            tags={
                "endpoint": endpoint,
                "method": method,
                "status": str(status),
            },
        )
