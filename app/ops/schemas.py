from pydantic import BaseModel


class CreateNodeRequest(BaseModel):
    label: str
    parentId: int | None


class CreateNodeResponse(BaseModel):
    id: int
    label: str
    parentId: int | None


class TreeNodeResponse(BaseModel):
    id: int
    label: str
    children: list["TreeNodeResponse"] = []
