"""Tree data generators for performance testing."""

from typing import Any


def linear_chain(start_id: int, depth: int, prefix: str = "D") -> list[dict[str, Any]]:
    """Generate a linear chain (deep tree)."""
    return [
        {
            "id": f"{start_id + i}",
            "label": f"{prefix}{i}",
            "parentId": f"{start_id + i - 1}" if i > 0 else None,
            "rootId": f"{start_id}",
        }
        for i in range(depth)
    ]


def star_tree(start_id: int, width: int, prefix: str = "W") -> list[dict[str, Any]]:
    """Generate a star topology (wide tree)."""
    nodes = [{"id": f"{start_id}", "label": f"{prefix}0", "parentId": None, "rootId": f"{start_id}"}]

    for i in range(1, width + 1):
        nodes.append(
            {
                "id": f"{start_id + i}",
                "label": f"{prefix}{i}",
                "parentId": f"{start_id}",
                "rootId": f"{start_id}",
            }
        )

    return nodes


def balanced_tree(start_id: int, total_nodes: int, branching: int = 3) -> list[dict[str, Any]]:
    """Generate a balanced tree with specified branching factor."""
    if total_nodes <= 0:
        return []

    nodes = []
    node_id = start_id
    nodes.append({"id": f"{node_id}", "label": f"B{0}", "parentId": None, "rootId": f"{start_id}"})

    if total_nodes == 1:
        return nodes

    # Use BFS to create balanced tree
    parent_queue = [f"{start_id}"]
    created = 1

    while created < total_nodes and parent_queue:
        parent_id = parent_queue.pop(0)

        for _ in range(branching):
            if created >= total_nodes:
                break

            node_id = start_id + created
            nodes.append(
                {
                    "id": f"{node_id}",
                    "label": f"B{created}",
                    "parentId": parent_id,
                    "rootId": f"{start_id}",
                }
            )
            parent_queue.append(f"{node_id}")
            created += 1

    return nodes
