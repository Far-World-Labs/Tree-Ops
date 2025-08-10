import asyncio
from collections.abc import AsyncGenerator, Generator

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool

from app.lib.db.base import Base
from app.lib.db.session import DATABASE_URL, get_session
from app.main import app
from app.ops.entities.tree_node import TreeNode
from tests.factories import TreeNodeFactory

test_engine = create_async_engine(DATABASE_URL, poolclass=NullPool)


@pytest.fixture(scope="session")
def event_loop() -> Generator:
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="function")
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    async with AsyncSession(test_engine) as session:
        yield session
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


class TreeBuilder:
    """Simple builder for test trees."""

    def __init__(self, db_session: AsyncSession):
        self.session = db_session

    async def root(self, label: str = "Root") -> TreeNode:
        """Create a root node."""
        node = await TreeNodeFactory.create_async(self.session, label=label)
        return node

    async def child(self, parent, label: str = None):
        """Create a child node."""
        return await TreeNodeFactory.create_async(
            self.session,
            label=label or f"{parent.label}-child",
            parent_id=parent.id,
            root_id=parent.root_id or parent.id,
            depth=parent.depth + 1,
        )


@pytest.fixture
def tree(db_session: AsyncSession) -> TreeBuilder:
    """Simple tree builder for tests."""
    return TreeBuilder(db_session)
