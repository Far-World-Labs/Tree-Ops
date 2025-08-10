import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_get_trees(client: AsyncClient, tree):
    # Start empty
    response = await client.get("/api/tree")
    assert response.status_code == 200
    assert response.json() == []

    # Create test data
    await tree.root("Plan the perfect weekend trip to Portland")
    await tree.root("Train squirrels to deliver mail")

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
