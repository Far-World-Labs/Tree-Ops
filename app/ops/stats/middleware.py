import time
from collections.abc import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from app.ops.stats.registry import metrics_registry


class MetricsMiddleware(BaseHTTPMiddleware):
    """Capture metrics for all requests during active sessions."""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        session = metrics_registry.current_session
        if not session:
            # No active session, just pass through
            return await call_next(request)

        start_time = time.perf_counter()
        response = await call_next(request)
        duration_ms = (time.perf_counter() - start_time) * 1000

        # Collect metrics
        metrics = [
            metrics_registry.collector.create_request_metric(
                endpoint=request.url.path, method=request.method, duration_ms=duration_ms, status=response.status_code
            ),
            *metrics_registry.collector.collect_system_metrics(),
        ]

        await session.record_batch(metrics)

        return response
