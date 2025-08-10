from dataclasses import dataclass

from sqlalchemy import cast, func, literal, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.ops.entities.tree_node import TreeNode
from app.ops.schemas import BulkNodeRequest, CreateNodeResponse


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


async def fetch_forest_json(session: AsyncSession, org_id: str) -> str:
    """Fetch entire forest as nested JSON using PostgreSQL function.

    This delegates all tree building to PostgreSQL's build_forest_json function,
    which recursively constructs the nested JSON structure. See migration
    1dde04ee4797 for implementation details and rationale.
    """
    query = text("SELECT build_forest_json(:org_id)")
    result = await session.execute(query, {"org_id": org_id})
    return result.scalar_one() or "[]"


class TreeService:
    def __init__(self, session: AsyncSession, org_id: str | None = None):
        self.session = session
        self.org_id = org_id or "default"  # Default to "default" if not provided

    async def list_all_trees(self, format: str | None = None) -> str:
        """
        List all trees in the specified format.

        Args:
            format: Output format. Currently only "json" is supported.
                    Must be specified explicitly.

        Returns:
            Serialized JSON string of the forest structure, not an object.
            The string contains a JSON array of tree objects.

        Raises:
            ValueError: If format is not specified or is not "json"
        """
        if format != "json":
            raise ValueError("Format must be 'json'. Other formats not yet supported.")

        # Returns serialized JSON string, not an object structure
        return await fetch_forest_json(self.session, self.org_id)

    async def _get_next_position(self, parent_id: int | None) -> int:
        """Get next position for a node under the given parent in this org."""
        if parent_id is None:
            where_clause = (TreeNode.parent_id.is_(None)) & (TreeNode.org_id == self.org_id)
        else:
            where_clause = (TreeNode.parent_id == parent_id) & (TreeNode.org_id == self.org_id)

        stmt = select(func.coalesce(func.max(TreeNode.pos), cast(literal(0), TreeNode.pos.type))).where(where_clause)
        result = await self.session.execute(stmt)
        max_pos = result.scalar_one()
        return max_pos + 1000

    async def insert_node(self, command: CreateNodeCommand) -> CreateNodeResponse:
        async with self.session.begin():
            next_pos = await self._get_next_position(command.parent_id)

            if command.parent_id is None:
                # root_id is NOT NULL but we don't know the ID until after insert
                root_id = 0
            else:
                root_id = await get_parent_root_id(self.session, command.parent_id)

            node = TreeNode(
                label=command.label,
                parent_id=command.parent_id,
                org_id=self.org_id,
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

    async def bulk_insert_adjacency(self, nodes: list[BulkNodeRequest]) -> int:
        """
        Bulk insert nodes with client-provided IDs.

        WARNING: For testing/development only. Assumes client provides valid data:
        - Unique IDs
        - Valid parent references (parents exist before children in list)
        - Correct rootId values

        Returns number of nodes created.
        """
        if not nodes:
            return 0

        created_count = 0

        async with self.session.begin():
            # Track position counters per parent for this org
            position_counters: dict[int | None, int] = {}

            for node_data in nodes:
                node_id = node_data.id
                label = node_data.label
                parent_id = node_data.parent_id
                root_id = (
                    node_data.root_id if node_data.root_id is not None else (node_id if parent_id is None else None)
                )

                # Calculate position
                if parent_id not in position_counters:
                    position_counters[parent_id] = 1000
                else:
                    position_counters[parent_id] += 1000

                pos = position_counters[parent_id]

                # Create node with explicit ID
                node = TreeNode(
                    id=node_id,
                    label=label,
                    parent_id=parent_id,
                    root_id=root_id,
                    org_id=self.org_id,
                    pos=pos,
                )

                self.session.add(node)
                created_count += 1

            # Flush to commit all nodes
            await self.session.flush()

        return created_count

    async def delete_all_trees(self) -> None:
        """
        Delete all trees for the current org.

        WARNING: For testing/development only.
        """
        await self.session.execute(text("DELETE FROM tree_nodes WHERE org_id = :org_id"), {"org_id": self.org_id})
        await self.session.commit()
