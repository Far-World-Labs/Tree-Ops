import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.lib.db.session import get_session
from app.lib.health import check_database_health
from app.middleware import RequestIDMiddleware, TimingMiddleware
from app.ops.routes.tree import router as tree_router

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
    yield
    # Shutdown
    logger.info("Shutting down")


app = FastAPI(title=settings.app_name, debug=settings.debug, lifespan=lifespan)

# Add middleware
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
    return {
        "status": "healthy" if db_health["connected"] else "degraded",
        "environment": settings.environment,
        "checks": {"database": db_health},
    }


# API routes
app.include_router(tree_router, prefix="/api/tree", tags=["tree"])
