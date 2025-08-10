from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def check_database_health(session: AsyncSession) -> dict:
    """Check database connectivity and return health status."""
    try:
        # Simple query to verify database connection
        result = await session.execute(text("SELECT 1"))
        result.scalar()
        return {"database": "healthy", "connected": True}
    except Exception as e:
        return {"database": "unhealthy", "connected": False, "error": str(e)}
