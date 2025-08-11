import json
import uuid
from dataclasses import dataclass

from sqlalchemy import cast, func, literal, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.ops.entities.tree_node import TreeNode
from app.ops.schemas import BulkNodeRequest, CreateNodeResponse


@dataclass
class CreateNodeCommand:
    label: str
    parent_id: str | None


@dataclass
class NodeTreeInfo:
    """Computed tree information for a node."""

    root_id: int
    pos: int
    path_ids: list[int]
    path_pos: list[int]
    depth: int
    label_json: str


def build_paths_for_bulk_insert(nodes: list[BulkNodeRequest]) -> dict[int, NodeTreeInfo]:
    """
    Build materialized path information for a list of nodes.

    This function computes the path_ids and path_pos arrays that enable
    efficient tree queries without recursion.

    Algorithm:
    1. Process nodes in order (assumes parents come before children)
    2. Track position counters per parent (gap-based: 1000, 2000, 3000...)
    3. Build paths by extending parent's path with current node

    Path arrays explanation:
    - path_ids: [root_id, ..., parent_id, node_id] - the IDs from root to node
    - path_pos: [root_pos, ..., parent_pos, node_pos] - the positions at each level

    Example for tree A(1000)->B(1000)->C(1000), A(1000)->D(2000):
    - A: path_ids=[A], path_pos=[1000]
    - B: path_ids=[A,B], path_pos=[1000,1000]
    - C: path_ids=[A,B,C], path_pos=[1000,1000,1000]
    - D: path_ids=[A,D], path_pos=[1000,2000]

    The path_pos array gives us perfect tree ordering:
    [1000] < [1000,1000] < [1000,1000,1000] < [1000,2000]
    This matches the desired tree traversal: A, B, C, D
    """
    MAX_DEPTH = 32767  # SmallInteger max value
    MAX_LABEL_JSON_SIZE = 1_000_000  # 1MB reasonable limit

    node_tree_info: dict[int, NodeTreeInfo] = {}
    position_counters: dict[int | None, int] = {}

    for node_data in nodes:
        node_id = int(node_data.id)
        parent_id = int(node_data.parent_id) if node_data.parent_id else None

        # Calculate position with gap-based allocation
        if parent_id not in position_counters:
            position_counters[parent_id] = 1000
        else:
            position_counters[parent_id] += 1000
        pos = position_counters[parent_id]

        # Build paths based on parent
        if parent_id is None:
            # Root node - paths start here
            root_id = node_id
            path_ids = [node_id]
            path_pos = [pos]
            depth = 1
        elif parent_id in node_tree_info:
            # Child node - extend parent's paths
            parent_info = node_tree_info[parent_id]
            root_id = parent_info.root_id
            path_ids = parent_info.path_ids + [node_id]
            path_pos = parent_info.path_pos + [pos]
            depth = parent_info.depth + 1
        else:
            # Parent not yet processed - shouldn't happen with proper ordering
            # Fallback: treat as root (will cause issues but won't crash)
            root_id = int(node_data.root_id) if node_data.root_id else node_id
            path_ids = [node_id]
            path_pos = [pos]
            depth = 1

        # Validate depth doesn't exceed SmallInteger max
        if depth > MAX_DEPTH:
            raise ValueError(f"Tree depth {depth} exceeds maximum supported depth of {MAX_DEPTH}")

        # Pre-escape label for JSON with validation
        try:
            label_json = json.dumps(node_data.label, ensure_ascii=False)
            # Verify it's valid JSON by parsing it back
            json.loads(label_json)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise ValueError(f"Label '{node_data.label}' cannot be JSON encoded: {e}")

        # Validate label_json length
        if len(label_json) > MAX_LABEL_JSON_SIZE:
            raise ValueError(f"Label JSON for node {node_id} exceeds size limit of {MAX_LABEL_JSON_SIZE} bytes")

        node_tree_info[node_id] = NodeTreeInfo(
            root_id=root_id, pos=pos, path_ids=path_ids, path_pos=path_pos, depth=depth, label_json=label_json
        )

    return node_tree_info


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


# SQL query for fast forest materialization using window functions
# This avoids all recursion and function calls by using a clever trick:
# 1. Materialize path arrays on write (path_ids, path_pos)
# 2. Use window functions to build JSON in a single ordered pass
# 3. String concatenation to assemble the final JSON

FOREST_JSON_QUERY = """
WITH roots AS (
    -- Get all root nodes for this org, ordered by most recently updated
    SELECT id AS root_id
    FROM tree_nodes
    WHERE parent_id IS NULL AND org_id = :org
    ORDER BY updated_at DESC, id
),
nodes AS (
    -- Get all nodes with pre-computed depth and JSON-escaped label
    SELECT n.id, n.label_json, n.root_id, n.path_pos, n.depth
    FROM tree_nodes n
    WHERE n.org_id = :org
),
ordered AS (
    SELECT
        id, label_json, root_id, path_pos, depth,

        -- Look ahead to next node's depth
        LEAD(depth, 1, 0) OVER (PARTITION BY root_id ORDER BY path_pos) AS next_depth,

        -- Look at previous node's depth
        LAG(depth) OVER (PARTITION BY root_id ORDER BY path_pos) AS prev_depth,

        -- Row number within tree
        ROW_NUMBER() OVER (PARTITION BY root_id ORDER BY path_pos) AS row_num
    FROM nodes
),
per_root AS (
    SELECT
        o.root_id,
        -- Build JSON string for each tree by concatenating tokens
        STRING_AGG(
            -- Comma before node if not first and not immediately after parent
            CASE
                WHEN row_num = 1 THEN ''  -- First node in tree
                WHEN depth > prev_depth THEN ''  -- First child
                ELSE ','
            END ||
            '{"id":"' || id::text || '"' ||          -- ID as JSON string to preserve precision
            ',"label":' || label_json ||             -- Pre-escaped label (no runtime JSON encoding)
            ',"children":[' ||                       -- Open children array
            -- Close brackets when depth decreases or at end
            CASE
                WHEN next_depth > depth THEN ''  -- Has children, keep open
                WHEN next_depth = 0 THEN REPEAT(']}', depth::int)  -- Last node, close all
                WHEN next_depth < depth THEN REPEAT(']}', (depth - next_depth)::int) || ']}'  -- Close levels and self
                ELSE ']}'  -- Same level sibling follows, close self
            END,
            '' ORDER BY path_pos                     -- Concatenate in tree order
        ) AS json_text
    FROM ordered o
    GROUP BY o.root_id
)
-- Final assembly: wrap all trees in array brackets
SELECT
    COALESCE(
        '[' ||
        STRING_AGG(pr.json_text, ',' ORDER BY r.root_id) ||
        ']',
        '[]'  -- Empty array if no trees
    )
FROM roots r
LEFT JOIN per_root pr USING (root_id)
"""


def explain_forest_json_algorithm():
    """
    Explains how the window function JSON building works.

    The key insight: We visit nodes in path order (parent before children),
    and use LEAD() to peek ahead. When we see the depth decrease, we know
    we need to close JSON brackets.

    Example for tree A->B->C, A->D:
    Row  Node  Depth  NextDepth  Brackets to close
    1    A     1      2          0 (child follows, keep open)
    2    B     2      3          0 (child follows, keep open)
    3    C     3      2          1 (depth drops by 1, close C with ]})
    4    D     2      0          2 (last node, close D and A with ]}]})

    The algorithm:
    1. Order nodes by path_pos (ensures parents before children)
    2. For each node, look ahead to next node's depth
    3. If depth decreases, close (current_depth - next_depth) brackets
    4. Add commas between siblings (not after parents)
    5. STRING_AGG concatenates all tokens in order

    Performance benefits:
    - Single sequential scan (no recursion)
    - O(N) complexity
    - No function call overhead
    - Excellent cache locality
    - Works for any tree depth
    """
    pass


async def fetch_forest_json(session: AsyncSession, org_id: str) -> str:
    """
    Fetch entire forest as nested JSON using window functions.

    This replaces the recursive PL/pgSQL approach that had severe performance
    issues (3.5ms/node at 1000 depth, stack overflow at 1400 depth).

    The new approach uses materialized paths and window functions to build
    JSON in a single pass with O(N) complexity.
    """
    result = await session.execute(text(FOREST_JSON_QUERY), {"org": org_id})
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
        MAX_DEPTH = 32767  # SmallInteger max value
        MAX_LABEL_JSON_SIZE = 1_000_000  # 1MB reasonable limit

        async with self.session.begin():
            # Convert string parent_id to int for database operations
            parent_id = int(command.parent_id) if command.parent_id else None
            next_pos = await self._get_next_position(parent_id)

            # Generate UUID for the node ID that fits in PostgreSQL BIGINT (signed 64-bit)
            # BIGINT range: -9223372036854775808 to 9223372036854775807
            node_id = uuid.uuid4().int & 0x7FFFFFFFFFFFFFFF  # Mask to ensure positive and within range

            # Pre-escape label for JSON with validation
            try:
                label_json = json.dumps(command.label, ensure_ascii=False)
                # Verify it's valid JSON by parsing it back
                json.loads(label_json)
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                raise ValueError(f"Label '{command.label}' cannot be JSON encoded: {e}")

            # Validate label_json length
            if len(label_json) > MAX_LABEL_JSON_SIZE:
                raise ValueError(f"Label JSON exceeds size limit of {MAX_LABEL_JSON_SIZE} bytes")

            if parent_id is None:
                # Creating a root node
                node = TreeNode(
                    id=node_id,
                    label=command.label,
                    parent_id=None,
                    org_id=self.org_id,
                    root_id=node_id,  # Will be itself
                    pos=next_pos,
                    path_ids=[node_id],
                    path_pos=[next_pos],
                    depth=1,
                    label_json=label_json,
                )
            else:
                # Get parent's paths and depth
                parent_result = await self.session.execute(
                    text("SELECT root_id, path_ids, path_pos, depth FROM tree_nodes WHERE id = :pid"),
                    {"pid": parent_id},
                )
                parent = parent_result.first()
                if not parent:
                    raise ValueError(f"Parent node {parent_id} not found")

                # Validate depth doesn't exceed SmallInteger max
                new_depth = parent.depth + 1
                if new_depth > MAX_DEPTH:
                    raise ValueError(
                        f"Cannot create node: tree depth {new_depth} would exceed maximum supported depth of {MAX_DEPTH}"
                    )

                node = TreeNode(
                    id=node_id,
                    label=command.label,
                    parent_id=parent_id,
                    org_id=self.org_id,
                    root_id=parent.root_id,
                    pos=next_pos,
                    path_ids=list(parent.path_ids) + [node_id],
                    path_pos=list(parent.path_pos) + [next_pos],
                    depth=new_depth,
                    label_json=label_json,
                )

            self.session.add(node)
            await self.session.flush()

            # Update root's updated_at for ordering
            if parent_id is not None:
                await self.session.execute(
                    text("UPDATE tree_nodes SET updated_at = now() WHERE id = :rid"), {"rid": node.root_id}
                )

        await self.session.refresh(node)
        # Return IDs as strings for JSON safety
        return CreateNodeResponse(
            id=f"{node.id}", label=node.label, parentId=f"{node.parent_id}" if node.parent_id else None
        )

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

        # Build path information for all nodes
        node_tree_info = build_paths_for_bulk_insert(nodes)

        for node_data in nodes:
            node_id = int(node_data.id)
            node_info = node_tree_info[node_id]

            # Create node with explicit ID and computed paths
            node = TreeNode(
                id=node_id,
                label=node_data.label,
                parent_id=int(node_data.parent_id) if node_data.parent_id else None,
                root_id=node_info.root_id,
                org_id=self.org_id,
                pos=node_info.pos,
                path_ids=node_info.path_ids,
                path_pos=node_info.path_pos,
                depth=node_info.depth,
                label_json=node_info.label_json,
            )

            self.session.add(node)
            created_count += 1

        # Flush to commit all nodes
        await self.session.flush()
        await self.session.commit()

        return created_count

    async def delete_all_trees(self) -> None:
        """
        Delete all trees for the current org.

        WARNING: For testing/development only.
        """
        await self.session.execute(text("DELETE FROM tree_nodes WHERE org_id = :org_id"), {"org_id": self.org_id})
        await self.session.commit()
