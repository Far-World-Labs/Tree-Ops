from pydantic import BaseModel, Field


class CreateNodeRequest(BaseModel):
    label: str
    parentId: str | None = Field(None, description="ID of parent node")


class CreateNodeResponse(BaseModel):
    id: str
    label: str
    parentId: str | None


class TreeNodeResponse(BaseModel):
    id: str
    label: str
    children: list["TreeNodeResponse"] = []


class BulkNodeRequest(BaseModel):
    """Node for bulk insert operation."""

    id: str = Field(..., description="Node ID")
    label: str = Field(..., description="Node label")
    parentId: str | None = Field(None, description="Parent node ID")
    rootId: str | None = Field(None, description="Root node ID")

    class Config:
        # Accept either camelCase or snake_case
        populate_by_name = True

    @property
    def parent_id(self) -> str | None:
        """Get parent_id for internal use."""
        return self.parentId

    @property
    def root_id(self) -> str | None:
        """Get root_id for internal use."""
        return self.rootId


class MoveNodeRequest(BaseModel):
    """Request model for moving a node from one parent to another."""

    sourceId: str = Field(..., description="ID of the node to move")
    targetId: str | None = Field(None, description="ID of the target parent node (null for root level)")


class MoveNodeResponse(BaseModel):
    """Response model for node move operation."""

    success: bool = Field(..., description="Whether the move operation was successful")
    message: str = Field(..., description="Status message")


class CloneNodeRequest(BaseModel):
    """Request model for cloning a node and its subtree to a new location."""

    sourceId: str = Field(..., description="ID of the node to clone")
    targetId: str | None = Field(None, description="ID of the target parent node (null for root level)")


class CloneNodeResponse(BaseModel):
    """Response model for node clone operation."""

    success: bool = Field(..., description="Whether the clone operation was successful")
    message: str = Field(..., description="Status message")
    id: str | None = Field(None, description="ID of the newly created root of the cloned subtree")
