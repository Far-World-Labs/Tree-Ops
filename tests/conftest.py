import asyncio
import logging
from collections.abc import AsyncGenerator, Generator

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool

from alembic import command
from alembic.config import Config
from app.lib.db.session import DATABASE_URL, get_session
from app.main import app

# Reduce logging noise during tests
logging.getLogger("asyncio").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
logging.getLogger("faker.factory").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

test_engine = create_async_engine(DATABASE_URL, poolclass=NullPool)


@pytest.fixture(scope="session")
def event_loop() -> Generator:
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="function")
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    # Run migrations instead of creating tables directly
    alembic_cfg = Config("alembic.ini")
    # Use synchronous URL for Alembic
    sync_url = DATABASE_URL.replace("+asyncpg", "")
    alembic_cfg.set_main_option("sqlalchemy.url", sync_url)

    # Run migrations
    command.downgrade(alembic_cfg, "base")
    command.upgrade(alembic_cfg, "head")

    async with AsyncSession(test_engine, expire_on_commit=False) as session:
        yield session
        await session.rollback()
        await session.close()


@pytest.fixture(scope="function")
async def client(db_session: AsyncSession) -> AsyncGenerator[AsyncClient, None]:
    async def override_get_session():
        yield db_session

    app.dependency_overrides[get_session] = override_get_session

    from httpx import ASGITransport

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac
    app.dependency_overrides.clear()
