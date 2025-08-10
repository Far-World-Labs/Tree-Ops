import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


def make_deep_tree_rows(depth: int):
    """Generate rows for a linear tree of given depth."""
    rows = []
    for i in range(depth):
        parent_id = i if i > 0 else "NULL"
        rows.append(f"({i+1}, 1, {parent_id}, 'default', 'Level {i}', 1000)")
    return ",\n".join(rows)


def make_wide_tree_rows(width: int):
    """Generate rows for a tree with many children."""
    rows = ["(1, 1, NULL, 'default', 'Root', 1000)"]
    for i in range(width):
        child_id = i + 2
        pos = (i + 1) * 1000
        rows.append(f"({child_id}, 1, 1, 'default', 'Child {i+1}', {pos})")
    return ",\n".join(rows)


@pytest.mark.asyncio
async def test_get_trees(client: AsyncClient, db_session: AsyncSession):
    await db_session.execute(text("TRUNCATE tree_nodes CASCADE"))

    response = await client.get("/api/tree")
    assert response.status_code == 200
    assert response.json() == []

    await db_session.execute(
        text(
            """
        INSERT INTO tree_nodes (id, root_id, parent_id, org_id, label, pos) VALUES
        (1, 1, NULL, 'default', 'Plan the perfect weekend trip to Portland', 1000),
        (2, 2, NULL, 'default', 'Train squirrels to deliver mail', 2000)
    """
        )
    )
    await db_session.commit()

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

    await db_session.execute(
        text(
            """
        INSERT INTO tree_nodes (id, root_id, parent_id, org_id, label, pos) VALUES
        -- Root 1 and its children
        (1, 1, NULL, 'default', 'Plan weekend trip', 1000),
        (2, 1, 1, 'default', 'Book flights', 1000),
        (3, 1, 1, 'default', 'Reserve hotel', 2000),
        (4, 1, 1, 'default', 'Pack luggage', 3000),

        -- Root 2 and its children
        (5, 5, NULL, 'default', 'Launch product', 1000),
        (6, 5, 5, 'default', 'Write press release', 1000),
        (7, 5, 5, 'default', 'Update website', 2000)
    """
        )
    )
    await db_session.commit()

    response = await client.get("/api/tree")
    forest = response.json()

    # Two root trees
    assert len(forest) == 2

    # First tree: trip planning
    trip = forest[0]
    assert trip == {
        "id": 1,
        "label": "Plan weekend trip",
        "children": [
            {"id": 2, "label": "Book flights", "children": []},
            {"id": 3, "label": "Reserve hotel", "children": []},
            {"id": 4, "label": "Pack luggage", "children": []},
        ],
    }

    # Second tree: product launch
    launch = forest[1]
    assert launch == {
        "id": 5,
        "label": "Launch product",
        "children": [
            {"id": 6, "label": "Write press release", "children": []},
            {"id": 7, "label": "Update website", "children": []},
        ],
    }


@pytest.mark.asyncio
async def test_deep_tree(client: AsyncClient, db_session: AsyncSession):
    """Linear chain 5 levels deep."""
    await db_session.execute(text("TRUNCATE tree_nodes CASCADE"))

    depth = 5
    rows = make_deep_tree_rows(depth)
    await db_session.execute(
        text(
            f"""
        INSERT INTO tree_nodes (id, root_id, parent_id, org_id, label, pos) VALUES
        {rows}
    """
        )
    )
    await db_session.commit()

    response = await client.get("/api/tree")

    assert response.json() == [
        {
            "id": 1,
            "label": "Level 0",
            "children": [
                {
                    "id": 2,
                    "label": "Level 1",
                    "children": [
                        {
                            "id": 3,
                            "label": "Level 2",
                            "children": [
                                {
                                    "id": 4,
                                    "label": "Level 3",
                                    "children": [{"id": 5, "label": "Level 4", "children": []}],
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
    rows = make_wide_tree_rows(width)
    await db_session.execute(
        text(
            f"""
        INSERT INTO tree_nodes (id, root_id, parent_id, org_id, label, pos) VALUES
        {rows}
    """
        )
    )
    await db_session.commit()

    response = await client.get("/api/tree")

    assert response.json() == [
        {
            "id": 1,
            "label": "Root",
            "children": [
                {"id": 2, "label": "Child 1", "children": []},
                {"id": 3, "label": "Child 2", "children": []},
                {"id": 4, "label": "Child 3", "children": []},
                {"id": 5, "label": "Child 4", "children": []},
                {"id": 6, "label": "Child 5", "children": []},
                {"id": 7, "label": "Child 6", "children": []},
                {"id": 8, "label": "Child 7", "children": []},
                {"id": 9, "label": "Child 8", "children": []},
                {"id": 10, "label": "Child 9", "children": []},
                {"id": 11, "label": "Child 10", "children": []},
            ],
        }
    ]


@pytest.mark.asyncio
async def test_multi_root_forest(client: AsyncClient, db_session: AsyncSession):
    await db_session.execute(text("TRUNCATE tree_nodes CASCADE"))

    num_roots = 5
    rows = []
    for i in range(num_roots):
        root_id = i + 1
        pos = (i + 1) * 1000
        rows.append(f"({root_id}, {root_id}, NULL, 'default', 'Root {i+1}', {pos})")

        # Add 2 children per root
        child1_id = num_roots + (i * 2) + 1
        child2_id = num_roots + (i * 2) + 2
        rows.append(f"({child1_id}, {root_id}, {root_id}, 'default', 'Root {i+1} - Task A', 1000)")
        rows.append(f"({child2_id}, {root_id}, {root_id}, 'default', 'Root {i+1} - Task B', 2000)")

    await db_session.execute(
        text(
            f"""
        INSERT INTO tree_nodes (id, root_id, parent_id, org_id, label, pos) VALUES
        {','.join(rows)}
    """
        )
    )
    await db_session.commit()

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

    await db_session.execute(
        text(
            """
        INSERT INTO tree_nodes (id, root_id, parent_id, org_id, label, pos) VALUES
        -- Root
        (1, 1, NULL, 'default', 'Master Plan', 1000),

        -- Branch A
        (2, 1, 1, 'default', 'Research Phase', 1000),
        (3, 1, 2, 'default', 'Literature Review', 1000),
        (4, 1, 2, 'default', 'Expert Interviews', 2000),
        (5, 1, 2, 'default', 'Data Collection', 3000),
        (6, 1, 5, 'default', 'Survey Design', 1000),
        (7, 1, 5, 'default', 'Run Surveys', 2000),

        -- Branch B
        (8, 1, 1, 'default', 'Implementation Phase', 2000),
        (9, 1, 8, 'default', 'Build Prototype', 1000),
        (10, 1, 9, 'default', 'Frontend', 1000),
        (11, 1, 9, 'default', 'Backend', 2000),
        (12, 1, 9, 'default', 'Testing', 3000),
        (13, 1, 8, 'default', 'Documentation', 2000)
    """
        )
    )
    await db_session.commit()

    response = await client.get("/api/tree")

    assert response.json() == [
        {
            "id": 1,
            "label": "Master Plan",
            "children": [
                {
                    "id": 2,
                    "label": "Research Phase",
                    "children": [
                        {"id": 3, "label": "Literature Review", "children": []},
                        {"id": 4, "label": "Expert Interviews", "children": []},
                        {
                            "id": 5,
                            "label": "Data Collection",
                            "children": [
                                {"id": 6, "label": "Survey Design", "children": []},
                                {"id": 7, "label": "Run Surveys", "children": []},
                            ],
                        },
                    ],
                },
                {
                    "id": 8,
                    "label": "Implementation Phase",
                    "children": [
                        {
                            "id": 9,
                            "label": "Build Prototype",
                            "children": [
                                {"id": 10, "label": "Frontend", "children": []},
                                {"id": 11, "label": "Backend", "children": []},
                                {"id": 12, "label": "Testing", "children": []},
                            ],
                        },
                        {"id": 13, "label": "Documentation", "children": []},
                    ],
                },
            ],
        }
    ]


@pytest.mark.asyncio
async def test_unbalanced_forest(client: AsyncClient, db_session: AsyncSession):
    await db_session.execute(text("TRUNCATE tree_nodes CASCADE"))

    await db_session.execute(
        text(
            """
        INSERT INTO tree_nodes (id, root_id, parent_id, org_id, label, pos) VALUES
        -- Tiny tree (just root)
        (1, 1, NULL, 'default', 'Quick Task', 1000),

        -- Small tree (root + 2 children)
        (2, 2, NULL, 'default', 'Small Project', 2000),
        (3, 2, 2, 'default', 'Step 1', 1000),
        (4, 2, 2, 'default', 'Step 2', 2000),

        -- Deep tree (4 levels)
        (5, 5, NULL, 'default', 'Deep Analysis', 3000),
        (6, 5, 5, 'default', 'Layer 1', 1000),
        (7, 5, 6, 'default', 'Layer 2', 1000),
        (8, 5, 7, 'default', 'Layer 3', 1000),

        -- Wide tree (root + 5 children)
        (9, 9, NULL, 'default', 'Parallel Tasks', 4000),
        (10, 9, 9, 'default', 'Task A', 1000),
        (11, 9, 9, 'default', 'Task B', 2000),
        (12, 9, 9, 'default', 'Task C', 3000),
        (13, 9, 9, 'default', 'Task D', 4000),
        (14, 9, 9, 'default', 'Task E', 5000)
    """
        )
    )
    await db_session.commit()

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

    await db_session.execute(
        text(
            """
        INSERT INTO tree_nodes (id, root_id, parent_id, org_id, label, pos) VALUES
        -- Insert out of order to test sorting
        (3, 3, NULL, 'default', 'Third Root', 3000),
        (1, 1, NULL, 'default', 'First Root', 1000),
        (2, 2, NULL, 'default', 'Second Root', 2000),

        -- Children also out of order
        (5, 1, 1, 'default', 'Child B', 2000),
        (4, 1, 1, 'default', 'Child A', 1000),
        (6, 1, 1, 'default', 'Child C', 3000)
    """
        )
    )
    await db_session.commit()

    response = await client.get("/api/tree")

    assert response.json() == [
        {
            "id": 1,
            "label": "First Root",
            "children": [
                {"id": 4, "label": "Child A", "children": []},
                {"id": 5, "label": "Child B", "children": []},
                {"id": 6, "label": "Child C", "children": []},
            ],
        },
        {"id": 2, "label": "Second Root", "children": []},
        {"id": 3, "label": "Third Root", "children": []},
    ]


@pytest.mark.asyncio
async def test_bulk_insert_simple_tree(client: AsyncClient, db_session: AsyncSession):
    """Test bulk insert with a simple tree structure."""
    # Clear any existing data
    await db_session.execute(text("TRUNCATE tree_nodes CASCADE"))
    await db_session.commit()

    # Create a simple tree with client-provided IDs
    nodes = [
        {"id": 100, "label": "root", "parentId": None, "rootId": 100},
        {"id": 101, "label": "child1", "parentId": 100, "rootId": 100},
        {"id": 102, "label": "child2", "parentId": 100, "rootId": 100},
        {"id": 103, "label": "grandchild", "parentId": 101, "rootId": 100},
    ]

    response = await client.post("/api/tree/bulk", json=nodes)
    assert response.status_code == 201
    assert response.json() == {"created": 4}

    # Verify the tree structure
    response = await client.get("/api/tree")
    assert response.status_code == 200
    trees = response.json()
    assert len(trees) == 1
    assert trees[0]["id"] == 100
    assert trees[0]["label"] == "root"
    assert len(trees[0]["children"]) == 2

    # Verify child structure
    children = {c["id"]: c for c in trees[0]["children"]}
    assert 101 in children
    assert 102 in children
    assert len(children[101]["children"]) == 1
    assert children[101]["children"][0]["id"] == 103


@pytest.mark.asyncio
async def test_bulk_insert_multiple_roots(client: AsyncClient, db_session: AsyncSession):
    """Test bulk insert with multiple root nodes."""
    # Clear any existing data
    await db_session.execute(text("TRUNCATE tree_nodes CASCADE"))
    await db_session.commit()

    nodes = [
        {"id": 200, "label": "root1", "parentId": None, "rootId": 200},
        {"id": 201, "label": "child1", "parentId": 200, "rootId": 200},
        {"id": 300, "label": "root2", "parentId": None, "rootId": 300},
        {"id": 301, "label": "child2", "parentId": 300, "rootId": 300},
    ]

    response = await client.post("/api/tree/bulk", json=nodes)
    assert response.status_code == 201
    assert response.json() == {"created": 4}

    # Verify we have two trees
    response = await client.get("/api/tree")
    assert response.status_code == 200
    trees = response.json()
    assert len(trees) == 2
    assert {t["id"] for t in trees} == {200, 300}


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
    nodes.append({"id": node_id, "label": f"node_{node_id}", "parentId": None, "rootId": node_id})
    root_id = node_id
    node_id += 1

    # Create 10 children of root
    level1_ids = []
    for i in range(10):
        nodes.append({"id": node_id, "label": f"node_{node_id}", "parentId": root_id, "rootId": root_id})
        level1_ids.append(node_id)
        node_id += 1

    # Create 9 children for each level 1 node (total 90 more nodes)
    for parent in level1_ids:
        for i in range(9):
            nodes.append({"id": node_id, "label": f"node_{node_id}", "parentId": parent, "rootId": root_id})
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
    assert trees[0]["id"] == 1000
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

    # Create trees for multiple orgs
    await db_session.execute(
        text(
            """
            INSERT INTO tree_nodes (id, root_id, parent_id, org_id, label, pos) VALUES
            -- Org1 trees
            (1, 1, NULL, 'org1', 'Org1 Tree1', 1000),
            (2, 1, 1, 'org1', 'Org1 Child1', 1000),
            (3, 3, NULL, 'org1', 'Org1 Tree2', 2000),

            -- Org2 trees
            (4, 4, NULL, 'org2', 'Org2 Tree1', 1000),
            (5, 4, 4, 'org2', 'Org2 Child1', 1000),

            -- Default org trees
            (6, 6, NULL, 'default', 'Default Tree1', 1000),
            (7, 6, 6, 'default', 'Default Child1', 1000)
        """
        )
    )
    await db_session.commit()

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
