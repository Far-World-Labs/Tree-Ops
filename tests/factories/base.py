"""Base factory configuration for async SQLAlchemy."""

import factory
from sqlalchemy.ext.asyncio import AsyncSession


class AsyncSQLAlchemyModelFactory(factory.Factory):
    """Base factory for async SQLAlchemy models."""

    class Meta:
        abstract = True

    @classmethod
    async def create_async(cls, session: AsyncSession, **kwargs):
        """Create and persist an instance asynchronously."""
        instance = cls.build(**kwargs)
        session.add(instance)
        await session.flush()
        return instance

    @classmethod
    async def create_batch_async(cls, session: AsyncSession, size: int, **kwargs):
        """Create multiple instances asynchronously."""
        instances = []
        for _ in range(size):
            instance = await cls.create_async(session, **kwargs)
            instances.append(instance)
        return instances
