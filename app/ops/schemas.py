from pydantic import BaseModel, Field


class CreateNodeRequest(BaseModel):
    label: str
    parentId: int | None = Field(None, description="ID of parent node")


class CreateNodeResponse(BaseModel):
    id: int
    label: str
    parentId: int | None


class TreeNodeResponse(BaseModel):
    id: int
    label: str
    children: list["TreeNodeResponse"] = []


class BulkNodeRequest(BaseModel):
    """Node for bulk insert operation."""

    id: int = Field(..., description="Node ID")
    label: str = Field(..., description="Node label")
    parentId: int | None = Field(None, description="Parent node ID")
    rootId: int | None = Field(None, description="Root node ID")

    class Config:
        # Accept either camelCase or snake_case
        populate_by_name = True

    @property
    def parent_id(self) -> int | None:
        """Get parent_id for internal use."""
        return self.parentId

    @property
    def root_id(self) -> int | None:
        """Get root_id for internal use."""
        return self.rootId
