from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.lib.db.session import get_session
from app.ops.schemas import (
    BulkNodeRequest,
    CloneNodeRequest,
    CloneNodeResponse,
    CreateNodeRequest,
    CreateNodeResponse,
    MoveNodeRequest,
    MoveNodeResponse,
)
from app.ops.services.tree_service import CreateNodeCommand, TreeService

settings = get_settings()

router = APIRouter()


@router.get("")
async def list_trees(session: AsyncSession = Depends(get_session), org_id: str | None = Header(None)):
    """Get all trees as aggregated structures with their children."""
    service = TreeService(session, org_id=org_id)
    forest_json = await service.list_all_trees(format="json")
    return Response(content=forest_json, media_type="application/json")


@router.post("", response_model=CreateNodeResponse, status_code=status.HTTP_201_CREATED)
async def insert_node(
    request: CreateNodeRequest, session: AsyncSession = Depends(get_session), org_id: str | None = Header(None)
):
    """
    Insert a new node at the specified position.
    Creates a root node if parentId is null, otherwise inserts as child of parent.
    """
    service = TreeService(session, org_id=org_id)
    command = CreateNodeCommand(label=request.label, parent_id=request.parentId)
    return await service.insert_node(command)


@router.post("/move", response_model=MoveNodeResponse, status_code=status.HTTP_200_OK)
async def move_node(
    request: MoveNodeRequest, session: AsyncSession = Depends(get_session), org_id: str | None = Header(None)
):
    """Move a node by source_id to target_id"""
    service = TreeService(session, org_id=org_id)
    try:
        await service.move_node(request.sourceId, request.targetId)
        return MoveNodeResponse(
            success=True,
            message=f"Successfully moved node {request.sourceId} to {'root' if request.targetId is None else f'parent {request.targetId}'}",
        )
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"{e}")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to move node: {e}")


@router.post("/clone", response_model=CloneNodeResponse, status_code=status.HTTP_201_CREATED)
async def clone_node(
    request: CloneNodeRequest, session: AsyncSession = Depends(get_session), org_id: str | None = Header(None)
):
    """Clone a node (and its subtree) to a new location"""
    service = TreeService(session, org_id=org_id)
    try:
        new_node_id = await service.clone_node(request.sourceId, request.targetId)
        return CloneNodeResponse(
            success=True,
            message=f"Successfully cloned node {request.sourceId} to {'root' if request.targetId is None else f'parent {request.targetId}'}",
            id=new_node_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"{e}")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to clone node: {e}")


@router.post("/bulk", status_code=status.HTTP_201_CREATED)
async def bulk_insert(
    nodes: list[BulkNodeRequest], session: AsyncSession = Depends(get_session), org_id: str | None = Header(None)
):
    """
    Bulk insert nodes with client-provided IDs.

    WARNING: This endpoint is for testing/development only, not for production use.
    Disabled in production environments.

    Expected format: [{"id": 1, "label": "root", "parentId": null, "rootId": 1}, ...]
    Client must ensure:
    - IDs are unique
    - Parent references exist before children
    - rootId is set correctly (same as id for roots, parent's rootId for children)
    """
    # Block in production
    if settings.environment == "production":
        raise HTTPException(status_code=403, detail="Bulk insert is disabled in production environments")

    service = TreeService(session, org_id=org_id)
    count = await service.bulk_insert_adjacency(nodes)

    return {"created": count}


@router.delete("", status_code=status.HTTP_204_NO_CONTENT)
async def delete_trees(session: AsyncSession = Depends(get_session), org_id: str | None = Header(None)):
    """
    Delete all trees for the org.

    WARNING: This endpoint is for testing/development only, not for production use.
    Disabled in production environments.
    """
    # Block in production
    if settings.environment == "production":
        raise HTTPException(status_code=403, detail="Tree deletion is disabled in production environments")

    service = TreeService(session, org_id=org_id)
    await service.delete_all_trees()

    return Response(status_code=status.HTTP_204_NO_CONTENT)
