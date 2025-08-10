import asyncio

import redis.asyncio as redis

from app.config import get_settings

settings = get_settings()


class RedisService:
    """Minimal Redis connection manager."""

    def __init__(self):
        self.client: redis.Redis | None = None
        self.enabled = bool(settings.redis_url)  # Redis is enabled if URL is configured
        self.connected = False

    async def connect(self, retry_count: int = 5, retry_delay: float = 1.0) -> bool:
        """
        Connect to Redis if configured.

        Args:
            retry_count: Number of connection attempts
            retry_delay: Delay between retries in seconds

        Returns:
            True if connected or Redis not configured, False if configured but failed
        """
        if not self.enabled:
            # Redis not configured, that's OK
            return True

        for attempt in range(retry_count):
            try:
                self.client = redis.from_url(settings.redis_url)
                await self.client.ping()
                self.connected = True
                return True
            except Exception as e:
                self.client = None
                self.connected = False
                if attempt < retry_count - 1:
                    await asyncio.sleep(retry_delay)
                else:
                    # Failed after all retries
                    raise RuntimeError(f"Failed to connect to Redis after {retry_count} attempts: {e}")

        return False

    async def close(self) -> None:
        """Close connection."""
        if self.client:
            await self.client.close()
            self.client = None
            self.connected = False

    async def health_check(self) -> dict:
        """Check Redis health for inclusion in health endpoint."""
        if not self.enabled:
            # Redis not configured, don't include in health check
            return {}

        try:
            if self.client:
                await self.client.ping()
                return {"redis": {"connected": True, "status": "healthy"}}
            else:
                return {"redis": {"connected": False, "status": "disconnected"}}
        except Exception as e:
            return {"redis": {"connected": False, "status": "unhealthy", "error": str(e)}}


# Global instance
redis_service = RedisService()
