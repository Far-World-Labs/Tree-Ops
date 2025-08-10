from dataclasses import dataclass

from lexorank.lexorank import lexorank
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.ops.entities.tree_node import TreeNode
from app.ops.schemas import CreateNodeResponse, TreeNodeResponse


@dataclass
class CreateNodeCommand:
    label: str
    parent_id: int | None


def calculate_rank_for_new_node(last_sibling_rank: str | None) -> str:
    """Pure function to calculate rank for a new node."""
    if last_sibling_rank:
        # Place after the last sibling
        rank = lexorank(last_sibling_rank, None)
    else:
        # First node at this level
        rank = lexorank("", "")

    # lexorank returns tuple (rank_string, success_bool)
    return rank[0] if isinstance(rank, tuple) else rank


async def find_root_nodes_ordered(session: AsyncSession) -> list[TreeNode]:
    result = await session.execute(
        select(TreeNode).where(TreeNode.parent_id.is_(None)).order_by(TreeNode.rank)  # Maintains consistent ordering
    )
    return list(result.scalars().all())


async def find_last_root_rank(session: AsyncSession) -> str | None:
    result = await session.execute(
        select(TreeNode.rank).where(TreeNode.parent_id.is_(None)).order_by(TreeNode.rank.desc()).limit(1)
    )
    return result.scalar_one_or_none()


class TreeService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def list_all_trees(self) -> list[TreeNodeResponse]:
        """List all trees as aggregated structures."""
        roots = await find_root_nodes_ordered(self.session)

        return [
            TreeNodeResponse(
                id=root.id,
                label=root.label,
                children=[],  # TODO: Load children recursively
            )
            for root in roots
        ]

    async def insert_node(self, command: CreateNodeCommand) -> CreateNodeResponse:
        """Insert a node at the specified position in the tree."""
        # Load data upfront (DDD style - separate data access from business logic)
        if command.parent_id is None:
            last_root_rank = await find_last_root_rank(self.session)
        else:
            # Simplified: child nodes not fully implemented
            last_root_rank = None

        rank = calculate_rank_for_new_node(last_root_rank)

        node = TreeNode(
            label=command.label,
            parent_id=command.parent_id,
            org_key="default",
            root_id=None,  # Set after flush for self-referential roots
            depth=0 if command.parent_id is None else 1,
            rank=rank,
        )

        self.session.add(node)
        await self.session.flush()  # Get the generated ID before commit

        # Self-referential update for root nodes
        if command.parent_id is None:
            node.root_id = node.id
            # Need to add to session again after modification
            self.session.add(node)

        await self.session.commit()
        await self.session.refresh(node)

        return CreateNodeResponse(id=node.id, label=node.label, parentId=node.parent_id)
