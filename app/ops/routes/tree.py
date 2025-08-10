from fastapi import APIRouter, Depends, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.lib.db.session import get_session
from app.ops.schemas import CreateNodeRequest, CreateNodeResponse
from app.ops.services.tree_service import CreateNodeCommand, TreeService

router = APIRouter()


@router.get("")
async def list_trees(session: AsyncSession = Depends(get_session)):
    """Get all trees as aggregated structures with their children."""
    service = TreeService(session)
    forest_json = await service.list_all_trees()
    return Response(content=forest_json, media_type="application/json")


@router.post("", response_model=CreateNodeResponse, status_code=status.HTTP_201_CREATED)
async def insert_node(request: CreateNodeRequest, session: AsyncSession = Depends(get_session)):
    """
    Insert a new node at the specified position.
    Creates a root node if parentId is null, otherwise inserts as child of parent.
    """
    service = TreeService(session)
    command = CreateNodeCommand(label=request.label, parent_id=request.parentId)
    return await service.insert_node(command)
