"""
Global registry for metrics components to avoid circular dependencies.
"""

from app.ops.stats.collector import MetricsCollector, MetricsSession


class MetricsRegistry:
    """Central registry for metrics components."""

    def __init__(self):
        self.current_session: MetricsSession | None = None
        self.collector = MetricsCollector()


# Global instance
metrics_registry = MetricsRegistry()
