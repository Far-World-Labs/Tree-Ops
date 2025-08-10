import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.lib.db.session import get_session
from app.lib.health import check_database_health
from app.middleware import RequestIDMiddleware, TimingMiddleware
from app.ops.routes.stats import router as stats_router
from app.ops.routes.tree import router as tree_router
from app.ops.stats.middleware import MetricsMiddleware
from app.ops.stats.redis_service import redis_service

settings = get_settings()

# Configure logging
logging.basicConfig(
    level=logging.INFO if not settings.debug else logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info(f"Starting {settings.app_name} in {settings.environment} mode")

    # Initialize Redis if configured
    if redis_service.enabled:
        try:
            await redis_service.connect()
            logger.info("Redis connected successfully")
        except RuntimeError as e:
            logger.error(f"Redis connection failed: {e}")
            # Application startup fails if Redis is configured but unavailable
            raise
    else:
        logger.info("Redis not configured, skipping connection")

    yield

    # Shutdown
    logger.info("Shutting down")
    await redis_service.close()


app = FastAPI(title=settings.app_name, debug=settings.debug, lifespan=lifespan)

# Add middleware
app.add_middleware(MetricsMiddleware)
app.add_middleware(TimingMiddleware)
app.add_middleware(RequestIDMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health_check(session: AsyncSession = Depends(get_session)):
    db_health = await check_database_health(session)

    # Get Redis health if configured
    checks = {"database": db_health}
    redis_health = await redis_service.health_check()
    if redis_health:
        checks.update(redis_health)

    # Overall status
    all_healthy = db_health["connected"] and (not redis_service.enabled or redis_service.connected)

    return {
        "status": "healthy" if all_healthy else "degraded",
        "environment": settings.environment,
        "checks": checks,
    }


# API routes
app.include_router(tree_router, prefix="/api/tree", tags=["tree"])
app.include_router(stats_router, prefix="/api/stats", tags=["stats"])
