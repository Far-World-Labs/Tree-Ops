from dataclasses import dataclass

from sqlalchemy import cast, func, literal, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.ops.entities.tree_node import TreeNode
from app.ops.schemas import CreateNodeResponse


@dataclass
class CreateNodeCommand:
    label: str
    parent_id: int | None


async def find_root_nodes_ordered(session: AsyncSession) -> list[TreeNode]:
    stmt = select(TreeNode).where(TreeNode.parent_id.is_(None)).order_by(TreeNode.pos, TreeNode.id)

    result = await session.execute(stmt)
    return list(result.scalars().all())


async def get_next_position(session: AsyncSession, parent_id: int | None) -> int:
    if parent_id is None:
        where_clause = TreeNode.parent_id.is_(None)
    else:
        where_clause = TreeNode.parent_id == parent_id

    stmt = select(func.coalesce(func.max(TreeNode.pos), cast(literal(0), TreeNode.pos.type))).where(where_clause)

    result = await session.execute(stmt)
    max_pos = result.scalar_one()

    return max_pos + 1000


async def get_parent_root_id(session: AsyncSession, parent_id: int) -> int:
    stmt = select(TreeNode.root_id).where(TreeNode.id == parent_id)
    result = await session.execute(stmt)

    root_id = result.scalar_one_or_none()
    if root_id is None:
        raise ValueError(f"Parent node {parent_id} not found")

    return root_id


async def fetch_forest_json(session: AsyncSession, org_key: str) -> str:
    """Fetch entire forest as nested JSON using PostgreSQL function.

    This delegates all tree building to PostgreSQL's build_forest_json function,
    which recursively constructs the nested JSON structure. See migration
    1dde04ee4797 for implementation details and rationale.
    """
    query = text("SELECT build_forest_json(:org_key)")
    result = await session.execute(query, {"org_key": org_key})
    return result.scalar_one() or "[]"


class TreeService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def list_all_trees(self) -> str:
        return await fetch_forest_json(self.session, "default")

    async def insert_node(self, command: CreateNodeCommand) -> CreateNodeResponse:
        async with self.session.begin():
            next_pos = await get_next_position(self.session, command.parent_id)

            if command.parent_id is None:
                # root_id is NOT NULL but we don't know the ID until after insert
                root_id = 0
            else:
                root_id = await get_parent_root_id(self.session, command.parent_id)

            node = TreeNode(
                label=command.label,
                parent_id=command.parent_id,
                org_key="default",
                root_id=root_id,
                pos=next_pos,
            )

            self.session.add(node)
            await self.session.flush()

            if command.parent_id is None:
                node.root_id = node.id
                await self.session.flush()

        await self.session.refresh(node)

        return CreateNodeResponse(id=node.id, label=node.label, parentId=node.parent_id)
