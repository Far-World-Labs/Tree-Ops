from fastapi import APIRouter, HTTPException

from app.ops.stats.collector import MetricsSession
from app.ops.stats.redis_service import redis_service
from app.ops.stats.registry import metrics_registry

router = APIRouter()


@router.post("/start")
async def start_stats_session():
    """Start a new metrics collection session."""
    if not redis_service.connected:
        raise HTTPException(status_code=503, detail="Redis not available")

    # Clear previous session if exists
    if metrics_registry.current_session:
        await metrics_registry.current_session.clear()

    metrics_registry.current_session = MetricsSession(redis_client=redis_service.client)

    return {
        "session_id": metrics_registry.current_session.id,
        "status": "started",
    }


@router.post("/stop")
async def stop_stats_session():
    """Stop the current metrics collection session."""
    if not metrics_registry.current_session:
        raise HTTPException(status_code=400, detail="No active session")

    session_id = metrics_registry.current_session.id
    metrics_registry.current_session = None

    return {
        "session_id": session_id,
        "status": "stopped",
    }


@router.get("/results/{session_id}")
async def get_stats_results(session_id: str):
    """Get results for a specific session."""
    if not redis_service.connected:
        raise HTTPException(status_code=503, detail="Redis not available")

    session = MetricsSession(session_id=session_id, redis_client=redis_service.client)
    metrics = await session.get_metrics()

    if not metrics:
        raise HTTPException(status_code=404, detail="Session not found or no data")

    return {
        "session_id": session_id,
        "metrics": metrics,
        "statistics": session.compute_statistics(metrics),
        "total_metrics": len(metrics),
    }


@router.delete("/session/{session_id}")
async def clear_session(session_id: str):
    """Clear all data for a session."""
    if not redis_service.connected:
        raise HTTPException(status_code=503, detail="Redis not available")

    session = MetricsSession(session_id=session_id, redis_client=redis_service.client)
    deleted = await session.clear()

    return {
        "session_id": session_id,
        "deleted_keys": deleted,
    }
