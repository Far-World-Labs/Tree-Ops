import json
import uuid
from dataclasses import dataclass

from sqlalchemy import cast, func, literal, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.ops.entities.tree_node import TreeNode
from app.ops.schemas import BulkNodeRequest, CloneNodeResponse, CreateNodeResponse, MoveNodeResponse


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


# SQL query for fast forest materialization using window functions
# Algorithm:
# 1. Order nodes by path_pos (ensures parents before children)
# 2. Use LEAD() to peek ahead at next node's depth
# 3. When depth decreases, close JSON brackets accordingly
# 4. Use STRING_AGG to concatenate all tokens in order
#
# Performance benefits:
# - Single sequential scan (no recursion)
# - O(N) complexity
# - No function call overhead
# - Works for any tree depth

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

    def _generate_clone_ids(self, nodes: list[TreeNode]) -> dict[int, int]:
        """
        Generate new IDs for cloning a subtree.

        Args:
            nodes: List of nodes to clone

        Returns:
            Dictionary mapping old IDs to new IDs
        """
        old_to_new = {}
        for node in nodes:
            new_id = uuid.uuid4().int & 0x7FFFFFFFFFFFFFFF
            old_to_new[node.id] = new_id
        return old_to_new

    async def delete_all_trees(self) -> None:
        """
        Delete all trees for the current org.

        WARNING: For testing/development only.
        """
        await self.session.execute(text("DELETE FROM tree_nodes WHERE org_id = :org_id"), {"org_id": self.org_id})
        await self.session.commit()

    async def move_node(self, source_id: str, target_id: str | None) -> MoveNodeResponse:
        """
        Move a node (and its subtree) to a new parent.

        Args:
            source_id: ID of the node to move
            target_id: ID of the new parent node (None for root level)

        Returns:
            MoveNodeResponse with success status and message
        """
        async with self.session.begin():
            source_node_id = int(source_id)
            target_parent_id = int(target_id) if target_id else None

            # Get source node
            source_stmt = select(TreeNode).where((TreeNode.id == source_node_id) & (TreeNode.org_id == self.org_id))
            source_result = await self.session.execute(source_stmt)
            source_node = source_result.scalar_one_or_none()

            if not source_node:
                return MoveNodeResponse(success=False, message=f"Source node {source_id} not found")

            # Validate target parent exists and isn't a descendant
            target_node = None
            if target_parent_id is not None:
                target_stmt = select(TreeNode).where(
                    (TreeNode.id == target_parent_id) & (TreeNode.org_id == self.org_id)
                )
                target_result = await self.session.execute(target_stmt)
                target_node = target_result.scalar_one_or_none()

                if not target_node:
                    return MoveNodeResponse(success=False, message=f"Target parent node {target_id} not found")

                # Check if target is a descendant of source
                if source_node_id in target_node.path_ids:
                    return MoveNodeResponse(success=False, message="Cannot move node to its own descendant")

            # Get next position under target parent
            next_pos = await self._get_next_position(target_parent_id)

            # TODO: Add max depth validation to prevent exceeding SmallInteger limit

            # Build new path information
            if target_parent_id is None:
                # Moving to root level
                new_root_id = source_node_id
                new_path_ids = [source_node_id]
                new_path_pos = [next_pos]
                new_depth = 1
            else:
                # Use target_node we already fetched during validation
                assert target_node is not None  # For type checker
                new_root_id = target_node.root_id
                new_path_ids = list(target_node.path_ids) + [source_node_id]
                new_path_pos = list(target_node.path_pos) + [next_pos]
                new_depth = target_node.depth + 1

            # Get all descendants
            descendants_stmt = select(TreeNode.id, TreeNode.path_ids, TreeNode.path_pos, TreeNode.depth).where(
                (TreeNode.org_id == self.org_id)
                & (text(":source_id = ANY(path_ids)").bindparams(source_id=source_node_id))
                & (TreeNode.id != source_node_id)
            )
            descendants_result = await self.session.execute(descendants_stmt)
            descendants = descendants_result.fetchall()

            # Update source node
            source_node.parent_id = target_parent_id
            source_node.root_id = new_root_id
            source_node.pos = next_pos
            source_node.path_ids = new_path_ids
            source_node.path_pos = new_path_pos
            source_node.depth = new_depth

            # Update descendants - prepare bulk update data
            if descendants:
                # Build update data for all descendants at once
                update_data = []
                for desc in descendants:
                    # Find where source appears in descendant's path
                    old_path_ids = list(desc.path_ids)
                    try:
                        source_idx = old_path_ids.index(source_node_id)
                    except ValueError:
                        # This shouldn't happen, but log it if it does
                        print(f"WARNING: Node {source_node_id} not found in descendant {desc.id} path: {old_path_ids}")
                        continue

                    # Rebuild paths from new parent path
                    new_desc_path_ids = new_path_ids + old_path_ids[source_idx + 1 :]
                    new_desc_path_pos = new_path_pos + list(desc.path_pos)[source_idx + 1 :]

                    update_data.append(
                        {
                            "desc_id": desc.id,
                            "new_root_id": new_root_id,
                            "new_path_ids": new_desc_path_ids,
                            "new_path_pos": new_desc_path_pos,
                            "new_depth": len(new_desc_path_ids),
                        }
                    )

                # Bulk update using executemany with raw SQL (safe and efficient)
                if update_data:
                    # Use executemany for bulk updates - safe and efficient
                    stmt = text(
                        """
                        UPDATE tree_nodes
                        SET root_id = :new_root_id,
                            path_ids = :new_path_ids,
                            path_pos = :new_path_pos,
                            depth = :new_depth
                        WHERE id = :desc_id AND org_id = :org_id
                    """
                    )

                    # Add org_id to each update record
                    update_params = [
                        {
                            "desc_id": data["desc_id"],
                            "new_root_id": data["new_root_id"],
                            "new_path_ids": data["new_path_ids"],
                            "new_path_pos": data["new_path_pos"],
                            "new_depth": data["new_depth"],
                            "org_id": self.org_id,
                        }
                        for data in update_data
                    ]

                    # Execute all updates in a single batch
                    await self.session.execute(stmt, update_params)

                # Option 2 (alternative): Single UPDATE with CASE statements
                # This would be even more efficient for very large sets
                # but is more complex to build dynamically

            # Update root timestamps
            if source_node.root_id != new_root_id and source_node.parent_id is not None:
                old_root_update = (
                    update(TreeNode).where(TreeNode.id == source_node.root_id).values(updated_at=func.now())
                )
                await self.session.execute(old_root_update)

            new_root_update = update(TreeNode).where(TreeNode.id == new_root_id).values(updated_at=func.now())
            await self.session.execute(new_root_update)

        return MoveNodeResponse(
            success=True, message=f"Successfully moved node {source_id} to parent {target_id if target_id else 'root'}"
        )

    async def clone_node(self, source_id: str, target_id: str | None) -> CloneNodeResponse:
        """
        Clone a node (and its entire subtree) to a new parent.

        Args:
            source_id: ID of the node to clone
            target_id: ID of the new parent node (None for root level)

        Returns:
            CloneNodeResponse with success status, message, and new node ID
        """
        async with self.session.begin():
            source_node_id = int(source_id)
            target_parent_id = int(target_id) if target_id else None

            # Get source node
            source_stmt = select(TreeNode).where((TreeNode.id == source_node_id) & (TreeNode.org_id == self.org_id))
            source_result = await self.session.execute(source_stmt)
            source_node = source_result.scalar_one_or_none()

            if not source_node:
                return CloneNodeResponse(success=False, message=f"Source node {source_id} not found", id=None)

            # Validate target parent exists
            target_node = None
            if target_parent_id is not None:
                target_stmt = select(TreeNode).where(
                    (TreeNode.id == target_parent_id) & (TreeNode.org_id == self.org_id)
                )
                target_result = await self.session.execute(target_stmt)
                target_node = target_result.scalar_one_or_none()

                if not target_node:
                    return CloneNodeResponse(
                        success=False, message=f"Target parent node {target_id} not found", id=None
                    )

            # Get all nodes in subtree (including source)
            subtree_stmt = select(TreeNode).where(
                (TreeNode.org_id == self.org_id)
                & (text(":source_id = ANY(path_ids)").bindparams(source_id=source_node_id))
            )
            subtree_result = await self.session.execute(subtree_stmt)
            subtree_nodes = subtree_result.scalars().all()

            # Generate new IDs for all nodes
            old_to_new = self._generate_clone_ids(subtree_nodes)
            new_source_id = old_to_new[source_node_id]

            # Get next position under target parent
            next_pos = await self._get_next_position(target_parent_id)

            # Build new root path information
            if target_parent_id is None:
                # Cloning to root level
                new_root_id = new_source_id
                new_root_path_ids = [new_source_id]
                new_root_path_pos = [next_pos]
            else:
                # Cloning under a parent
                assert target_node is not None
                new_root_id = target_node.root_id
                new_root_path_ids = list(target_node.path_ids) + [new_source_id]
                new_root_path_pos = list(target_node.path_pos) + [next_pos]

            # Sort nodes by depth to ensure parents are inserted before children
            sorted_nodes = sorted(subtree_nodes, key=lambda n: n.depth)

            # Build insert data for all new nodes
            insert_data = []
            for node in sorted_nodes:
                # Find position in original subtree
                source_idx = node.path_ids.index(source_node_id)
                relative_path = node.path_ids[source_idx:]
                relative_pos = node.path_pos[source_idx:]

                # Build new path with cloned IDs
                new_path_ids = new_root_path_ids[:-1]  # Parent path
                new_path_pos = new_root_path_pos[:-1]

                for i, old_id in enumerate(relative_path):
                    new_path_ids.append(old_to_new[old_id])
                    if i == 0:
                        new_path_pos.append(next_pos)  # Root of clone gets new position
                    else:
                        new_path_pos.append(relative_pos[i])  # Keep relative positions

                # Determine parent ID
                if node.id == source_node_id:
                    new_parent_id = target_parent_id
                else:
                    new_parent_id = old_to_new.get(node.parent_id)

                # Pre-escape label for JSON
                try:
                    label_json = json.dumps(node.label, ensure_ascii=False)
                    json.loads(label_json)  # Verify validity
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    raise ValueError(f"Label '{node.label}' cannot be JSON encoded: {e}")

                insert_data.append(
                    {
                        "id": old_to_new[node.id],
                        "label": node.label,
                        "parent_id": new_parent_id,
                        "org_id": self.org_id,
                        "root_id": new_root_id if target_parent_id else new_source_id,
                        "pos": next_pos if node.id == source_node_id else node.pos,
                        "path_ids": new_path_ids,
                        "path_pos": new_path_pos,
                        "depth": len(new_path_ids),
                        "label_json": label_json,
                    }
                )

            # Bulk insert all new nodes (sorted by depth ensures parents exist before children)
            if insert_data:
                stmt = text(
                    """
                    INSERT INTO tree_nodes
                    (id, label, parent_id, org_id, root_id, pos, path_ids, path_pos, depth, label_json)
                    VALUES
                    (:id, :label, :parent_id, :org_id, :root_id, :pos, :path_ids, :path_pos, :depth, :label_json)
                """
                )
                await self.session.execute(stmt, insert_data)

            # Update root timestamp if cloning under existing tree
            if target_parent_id is not None:
                root_update = update(TreeNode).where(TreeNode.id == new_root_id).values(updated_at=func.now())
                await self.session.execute(root_update)

        return CloneNodeResponse(
            success=True,
            message=f"Successfully cloned node {source_id} to parent {target_id if target_id else 'root'}",
            id=str(new_source_id),
        )
