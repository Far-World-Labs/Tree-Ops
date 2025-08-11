import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


def make_deep_tree_nodes(depth: int, base_id: int = 1):
    """Generate nodes for a linear tree of given depth."""
    nodes = []
    for i in range(depth):
        parent_id = f"{base_id + i - 1}" if i > 0 else None
        node_id = f"{base_id + i}"
        nodes.append({"id": node_id, "label": f"Level {i}", "parentId": parent_id, "rootId": f"{base_id}"})
    return nodes


def make_wide_tree_nodes(width: int, base_id: int = 1):
    """Generate nodes for a tree with many children."""
    nodes = [{"id": f"{base_id}", "label": "Root", "parentId": None, "rootId": f"{base_id}"}]
    for i in range(width):
        child_id = f"{base_id + i + 1}"
        nodes.append({"id": child_id, "label": f"Child {i+1}", "parentId": f"{base_id}", "rootId": f"{base_id}"})
    return nodes


@pytest.mark.asyncio
async def test_get_trees(client: AsyncClient, db_session: AsyncSession):
    await db_session.execute(text("TRUNCATE tree_nodes CASCADE"))

    response = await client.get("/api/tree")
    assert response.status_code == 200
    assert response.json() == []

    # Use bulk insert endpoint to properly populate path arrays
    nodes = [
        {"id": "1", "label": "Plan the perfect weekend trip to Portland", "parentId": None, "rootId": "1"},
        {"id": "2", "label": "Train squirrels to deliver mail", "parentId": None, "rootId": "2"},
    ]
    response = await client.post("/api/tree/bulk", json=nodes)
    assert response.status_code == 201

    response = await client.get("/api/tree")
    assert response.status_code == 200

    trees = response.json()
    assert len(trees) == 2
    labels = {tree["label"] for tree in trees}
    assert labels == {"Plan the perfect weekend trip to Portland", "Train squirrels to deliver mail"}


@pytest.mark.asyncio
async def test_create_root_node(client: AsyncClient):
    response = await client.post("/api/tree", json={"label": "Teach cats to use video chat", "parentId": None})
    assert response.status_code == 201

    data = response.json()
    assert data["label"] == "Teach cats to use video chat"
    assert data["parentId"] is None
    assert "id" in data


@pytest.mark.asyncio
async def test_empty_forest(client: AsyncClient, db_session: AsyncSession):
    await db_session.execute(text("TRUNCATE tree_nodes CASCADE"))

    response = await client.get("/api/tree")
    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.asyncio
async def test_simple_forest(client: AsyncClient, db_session: AsyncSession):
    """Two trees with flat children."""
    await db_session.execute(text("TRUNCATE tree_nodes CASCADE"))

    # Use bulk insert endpoint
    nodes = [
        # Root 1 and its children
        {"id": "1", "label": "Plan weekend trip", "parentId": None, "rootId": "1"},
        {"id": "2", "label": "Book flights", "parentId": "1", "rootId": "1"},
        {"id": "3", "label": "Reserve hotel", "parentId": "1", "rootId": "1"},
        {"id": "4", "label": "Pack luggage", "parentId": "1", "rootId": "1"},
        # Root 2 and its children
        {"id": "5", "label": "Launch product", "parentId": None, "rootId": "5"},
        {"id": "6", "label": "Write press release", "parentId": "5", "rootId": "5"},
        {"id": "7", "label": "Update website", "parentId": "5", "rootId": "5"},
    ]
    response = await client.post("/api/tree/bulk", json=nodes)
    assert response.status_code == 201

    response = await client.get("/api/tree")
    forest = response.json()

    # Two root trees
    assert len(forest) == 2

    # First tree: trip planning
    trip = forest[0]
    assert trip == {
        "id": "1",
        "label": "Plan weekend trip",
        "children": [
            {"id": "2", "label": "Book flights", "children": []},
            {"id": "3", "label": "Reserve hotel", "children": []},
            {"id": "4", "label": "Pack luggage", "children": []},
        ],
    }

    # Second tree: product launch
    launch = forest[1]
    assert launch == {
        "id": "5",
        "label": "Launch product",
        "children": [
            {"id": "6", "label": "Write press release", "children": []},
            {"id": "7", "label": "Update website", "children": []},
        ],
    }


@pytest.mark.asyncio
async def test_deep_tree(client: AsyncClient, db_session: AsyncSession):
    """Linear chain 5 levels deep."""
    await db_session.execute(text("TRUNCATE tree_nodes CASCADE"))

    depth = 5
    nodes = make_deep_tree_nodes(depth)
    response = await client.post("/api/tree/bulk", json=nodes)
    assert response.status_code == 201

    response = await client.get("/api/tree")

    assert response.json() == [
        {
            "id": "1",
            "label": "Level 0",
            "children": [
                {
                    "id": "2",
                    "label": "Level 1",
                    "children": [
                        {
                            "id": "3",
                            "label": "Level 2",
                            "children": [
                                {
                                    "id": "4",
                                    "label": "Level 3",
                                    "children": [{"id": "5", "label": "Level 4", "children": []}],
                                }
                            ],
                        }
                    ],
                }
            ],
        }
    ]


@pytest.mark.asyncio
async def test_wide_tree(client: AsyncClient, db_session: AsyncSession):
    """Single root with 10 direct children."""
    await db_session.execute(text("TRUNCATE tree_nodes CASCADE"))

    width = 10
    nodes = make_wide_tree_nodes(width)
    response = await client.post("/api/tree/bulk", json=nodes)
    assert response.status_code == 201

    response = await client.get("/api/tree")

    assert response.json() == [
        {
            "id": "1",
            "label": "Root",
            "children": [
                {"id": "2", "label": "Child 1", "children": []},
                {"id": "3", "label": "Child 2", "children": []},
                {"id": "4", "label": "Child 3", "children": []},
                {"id": "5", "label": "Child 4", "children": []},
                {"id": "6", "label": "Child 5", "children": []},
                {"id": "7", "label": "Child 6", "children": []},
                {"id": "8", "label": "Child 7", "children": []},
                {"id": "9", "label": "Child 8", "children": []},
                {"id": "10", "label": "Child 9", "children": []},
                {"id": "11", "label": "Child 10", "children": []},
            ],
        }
    ]


@pytest.mark.asyncio
async def test_multi_root_forest(client: AsyncClient, db_session: AsyncSession):
    await db_session.execute(text("TRUNCATE tree_nodes CASCADE"))

    num_roots = 5
    nodes = []
    for i in range(num_roots):
        root_id = f"{i + 1}"
        nodes.append({"id": root_id, "label": f"Root {i+1}", "parentId": None, "rootId": root_id})

        # Add 2 children per root
        child1_id = f"{num_roots + (i * 2) + 1}"
        child2_id = f"{num_roots + (i * 2) + 2}"
        nodes.append({"id": child1_id, "label": f"Root {i+1} - Task A", "parentId": root_id, "rootId": root_id})
        nodes.append({"id": child2_id, "label": f"Root {i+1} - Task B", "parentId": root_id, "rootId": root_id})

    response = await client.post("/api/tree/bulk", json=nodes)
    assert response.status_code == 201

    response = await client.get("/api/tree")
    assert response.status_code == 200

    forest = response.json()
    assert len(forest) == num_roots

    for i, tree in enumerate(forest):
        assert tree["label"] == f"Root {i+1}"


@pytest.mark.asyncio
async def test_complex_tree_structure(client: AsyncClient, db_session: AsyncSession):
    """Multi-level tree with varying depths."""
    await db_session.execute(text("TRUNCATE tree_nodes CASCADE"))

    nodes = [
        # Root
        {"id": "1", "label": "Master Plan", "parentId": None, "rootId": "1"},
        # Branch A
        {"id": "2", "label": "Research Phase", "parentId": "1", "rootId": "1"},
        {"id": "3", "label": "Literature Review", "parentId": "2", "rootId": "1"},
        {"id": "4", "label": "Expert Interviews", "parentId": "2", "rootId": "1"},
        {"id": "5", "label": "Data Collection", "parentId": "2", "rootId": "1"},
        {"id": "6", "label": "Survey Design", "parentId": "5", "rootId": "1"},
        {"id": "7", "label": "Run Surveys", "parentId": "5", "rootId": "1"},
        # Branch B
        {"id": "8", "label": "Implementation Phase", "parentId": "1", "rootId": "1"},
        {"id": "9", "label": "Build Prototype", "parentId": "8", "rootId": "1"},
        {"id": "10", "label": "Frontend", "parentId": "9", "rootId": "1"},
        {"id": "11", "label": "Backend", "parentId": "9", "rootId": "1"},
        {"id": "12", "label": "Testing", "parentId": "9", "rootId": "1"},
        {"id": "13", "label": "Documentation", "parentId": "8", "rootId": "1"},
    ]
    response = await client.post("/api/tree/bulk", json=nodes)
    assert response.status_code == 201

    response = await client.get("/api/tree")

    assert response.json() == [
        {
            "id": "1",
            "label": "Master Plan",
            "children": [
                {
                    "id": "2",
                    "label": "Research Phase",
                    "children": [
                        {"id": "3", "label": "Literature Review", "children": []},
                        {"id": "4", "label": "Expert Interviews", "children": []},
                        {
                            "id": "5",
                            "label": "Data Collection",
                            "children": [
                                {"id": "6", "label": "Survey Design", "children": []},
                                {"id": "7", "label": "Run Surveys", "children": []},
                            ],
                        },
                    ],
                },
                {
                    "id": "8",
                    "label": "Implementation Phase",
                    "children": [
                        {
                            "id": "9",
                            "label": "Build Prototype",
                            "children": [
                                {"id": "10", "label": "Frontend", "children": []},
                                {"id": "11", "label": "Backend", "children": []},
                                {"id": "12", "label": "Testing", "children": []},
                            ],
                        },
                        {"id": "13", "label": "Documentation", "children": []},
                    ],
                },
            ],
        }
    ]


@pytest.mark.asyncio
async def test_unbalanced_forest(client: AsyncClient, db_session: AsyncSession):
    await db_session.execute(text("TRUNCATE tree_nodes CASCADE"))

    nodes = [
        # Tiny tree (just root)
        {"id": "1", "label": "Quick Task", "parentId": None, "rootId": "1"},
        # Small tree (root + 2 children)
        {"id": "2", "label": "Small Project", "parentId": None, "rootId": "2"},
        {"id": "3", "label": "Step 1", "parentId": "2", "rootId": "2"},
        {"id": "4", "label": "Step 2", "parentId": "2", "rootId": "2"},
        # Deep tree (4 levels)
        {"id": "5", "label": "Deep Analysis", "parentId": None, "rootId": "5"},
        {"id": "6", "label": "Layer 1", "parentId": "5", "rootId": "5"},
        {"id": "7", "label": "Layer 2", "parentId": "6", "rootId": "5"},
        {"id": "8", "label": "Layer 3", "parentId": "7", "rootId": "5"},
        # Wide tree (root + 5 children)
        {"id": "9", "label": "Parallel Tasks", "parentId": None, "rootId": "9"},
        {"id": "10", "label": "Task A", "parentId": "9", "rootId": "9"},
        {"id": "11", "label": "Task B", "parentId": "9", "rootId": "9"},
        {"id": "12", "label": "Task C", "parentId": "9", "rootId": "9"},
        {"id": "13", "label": "Task D", "parentId": "9", "rootId": "9"},
        {"id": "14", "label": "Task E", "parentId": "9", "rootId": "9"},
    ]
    response = await client.post("/api/tree/bulk", json=nodes)
    assert response.status_code == 201

    response = await client.get("/api/tree")
    assert response.status_code == 200

    forest = response.json()
    assert len(forest) == 4

    labels = {t["label"] for t in forest}
    assert labels == {"Quick Task", "Small Project", "Deep Analysis", "Parallel Tasks"}


@pytest.mark.asyncio
async def test_forest_ordering(client: AsyncClient, db_session: AsyncSession):
    """Verify ordering by pos field for roots and children."""
    await db_session.execute(text("TRUNCATE tree_nodes CASCADE"))

    # Insert out of order to test sorting
    nodes = [
        {"id": "3", "label": "Third Root", "parentId": None, "rootId": "3"},
        {"id": "1", "label": "First Root", "parentId": None, "rootId": "1"},
        {"id": "2", "label": "Second Root", "parentId": None, "rootId": "2"},
        # Children also out of order
        {"id": "5", "label": "Child B", "parentId": "1", "rootId": "1"},
        {"id": "4", "label": "Child A", "parentId": "1", "rootId": "1"},
        {"id": "6", "label": "Child C", "parentId": "1", "rootId": "1"},
    ]
    response = await client.post("/api/tree/bulk", json=nodes)
    assert response.status_code == 201

    response = await client.get("/api/tree")

    # Children are ordered by insertion order in bulk request, not by ID
    assert response.json() == [
        {
            "id": "1",
            "label": "First Root",
            "children": [
                {"id": "5", "label": "Child B", "children": []},  # First child in bulk request
                {"id": "4", "label": "Child A", "children": []},  # Second child in bulk request
                {"id": "6", "label": "Child C", "children": []},  # Third child in bulk request
            ],
        },
        {"id": "2", "label": "Second Root", "children": []},
        {"id": "3", "label": "Third Root", "children": []},
    ]


@pytest.mark.asyncio
async def test_bulk_insert_simple_tree(client: AsyncClient, db_session: AsyncSession):
    """Test bulk insert with a simple tree structure."""
    # Clear any existing data
    await db_session.execute(text("TRUNCATE tree_nodes CASCADE"))
    await db_session.commit()

    # Create a simple tree with client-provided IDs
    nodes = [
        {"id": "100", "label": "root", "parentId": None, "rootId": "100"},
        {"id": "101", "label": "child1", "parentId": "100", "rootId": "100"},
        {"id": "102", "label": "child2", "parentId": "100", "rootId": "100"},
        {"id": "103", "label": "grandchild", "parentId": "101", "rootId": "100"},
    ]

    response = await client.post("/api/tree/bulk", json=nodes)
    assert response.status_code == 201
    assert response.json() == {"created": 4}

    # Verify the tree structure
    response = await client.get("/api/tree")
    assert response.status_code == 200
    trees = response.json()
    assert len(trees) == 1
    assert trees[0]["id"] == "100"
    assert trees[0]["label"] == "root"
    assert len(trees[0]["children"]) == 2

    # Verify child structure
    children = {c["id"]: c for c in trees[0]["children"]}
    assert "101" in children
    assert "102" in children
    assert len(children["101"]["children"]) == 1
    assert children["101"]["children"][0]["id"] == "103"


@pytest.mark.asyncio
async def test_bulk_insert_multiple_roots(client: AsyncClient, db_session: AsyncSession):
    """Test bulk insert with multiple root nodes."""
    # Clear any existing data
    await db_session.execute(text("TRUNCATE tree_nodes CASCADE"))
    await db_session.commit()

    nodes = [
        {"id": "200", "label": "root1", "parentId": None, "rootId": "200"},
        {"id": "201", "label": "child1", "parentId": "200", "rootId": "200"},
        {"id": "300", "label": "root2", "parentId": None, "rootId": "300"},
        {"id": "301", "label": "child2", "parentId": "300", "rootId": "300"},
    ]

    response = await client.post("/api/tree/bulk", json=nodes)
    assert response.status_code == 201
    assert response.json() == {"created": 4}

    # Verify we have two trees
    response = await client.get("/api/tree")
    assert response.status_code == 200
    trees = response.json()
    assert len(trees) == 2
    assert {t["id"] for t in trees} == {"200", "300"}


@pytest.mark.asyncio
async def test_bulk_insert_empty_list(client: AsyncClient, db_session: AsyncSession):
    """Test bulk insert with empty list."""
    response = await client.post("/api/tree/bulk", json=[])
    assert response.status_code == 201
    assert response.json() == {"created": 0}


@pytest.mark.asyncio
async def test_bulk_insert_large_tree(client: AsyncClient, db_session: AsyncSession):
    """Test bulk insert with a larger tree structure."""
    # Clear any existing data
    await db_session.execute(text("TRUNCATE tree_nodes CASCADE"))
    await db_session.commit()

    # Create a tree with 100 nodes
    nodes = []
    node_id = 1000

    # Create root
    nodes.append({"id": f"{node_id}", "label": f"node_{node_id}", "parentId": None, "rootId": f"{node_id}"})
    root_id = f"{node_id}"
    node_id += 1

    # Create 10 children of root
    level1_ids = []
    for i in range(10):
        nodes.append({"id": f"{node_id}", "label": f"node_{node_id}", "parentId": root_id, "rootId": root_id})
        level1_ids.append(f"{node_id}")
        node_id += 1

    # Create 9 children for each level 1 node (total 90 more nodes)
    for parent in level1_ids:
        for i in range(9):
            nodes.append({"id": f"{node_id}", "label": f"node_{node_id}", "parentId": parent, "rootId": root_id})
            node_id += 1

    assert len(nodes) == 101

    response = await client.post("/api/tree/bulk", json=nodes)
    assert response.status_code == 201
    assert response.json() == {"created": 101}

    # Verify the tree structure
    response = await client.get("/api/tree")
    assert response.status_code == 200
    trees = response.json()
    assert len(trees) == 1
    assert trees[0]["id"] == "1000"
    assert len(trees[0]["children"]) == 10

    # Verify grandchildren
    for child in trees[0]["children"]:
        assert len(child["children"]) == 9


@pytest.mark.asyncio
async def test_delete_org_trees(client: AsyncClient, db_session: AsyncSession):
    """Test deleting all trees for an org."""
    # Clear any existing data
    await db_session.execute(text("TRUNCATE tree_nodes CASCADE"))
    await db_session.commit()

    # Create trees for multiple orgs using bulk insert
    # Org1 trees
    nodes_org1 = [
        {"id": "1", "label": "Org1 Tree1", "parentId": None, "rootId": "1"},
        {"id": "2", "label": "Org1 Child1", "parentId": "1", "rootId": "1"},
        {"id": "3", "label": "Org1 Tree2", "parentId": None, "rootId": "3"},
    ]
    response = await client.post("/api/tree/bulk", json=nodes_org1, headers={"org-id": "org1"})
    assert response.status_code == 201

    # Org2 trees
    nodes_org2 = [
        {"id": "4", "label": "Org2 Tree1", "parentId": None, "rootId": "4"},
        {"id": "5", "label": "Org2 Child1", "parentId": "4", "rootId": "4"},
    ]
    response = await client.post("/api/tree/bulk", json=nodes_org2, headers={"org-id": "org2"})
    assert response.status_code == 201

    # Default org trees
    nodes_default = [
        {"id": "6", "label": "Default Tree1", "parentId": None, "rootId": "6"},
        {"id": "7", "label": "Default Child1", "parentId": "6", "rootId": "6"},
    ]
    response = await client.post("/api/tree/bulk", json=nodes_default)
    assert response.status_code == 201

    # Verify org1 has trees
    response = await client.get("/api/tree", headers={"org-id": "org1"})
    assert response.status_code == 200
    trees = response.json()
    assert len(trees) == 2

    # Delete org1 trees
    response = await client.delete("/api/tree", headers={"org-id": "org1"})
    assert response.status_code == 204

    # Verify org1 trees are gone
    response = await client.get("/api/tree", headers={"org-id": "org1"})
    assert response.status_code == 200
    assert response.json() == []

    # Verify org2 trees still exist
    response = await client.get("/api/tree", headers={"org-id": "org2"})
    assert response.status_code == 200
    trees = response.json()
    assert len(trees) == 1
    assert trees[0]["label"] == "Org2 Tree1"

    # Verify default org trees still exist
    response = await client.get("/api/tree")
    assert response.status_code == 200
    trees = response.json()
    assert len(trees) == 1
    assert trees[0]["label"] == "Default Tree1"
