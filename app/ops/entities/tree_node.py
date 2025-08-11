from datetime import datetime

from sqlalchemy import ARRAY, BigInteger, DateTime, ForeignKey, SmallInteger, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.lib.db.base import Base


class TreeNode(Base):
    __tablename__ = "tree_nodes"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    root_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    parent_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("tree_nodes.id", ondelete="CASCADE"), nullable=True, index=True
    )
    org_id: Mapped[str] = mapped_column(String, nullable=False, default="default", index=True)
    label: Mapped[str] = mapped_column(String, nullable=False)
    pos: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    path_ids: Mapped[list[int]] = mapped_column(ARRAY(BigInteger), nullable=False, default=[])
    path_pos: Mapped[list[int]] = mapped_column(ARRAY(BigInteger), nullable=False, default=[])
    depth: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=1)
    label_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    parent: Mapped["TreeNode | None"] = relationship("TreeNode", remote_side=[id], backref="children")

    def __repr__(self) -> str:
        return f"<TreeNode(id={self.id}, label={self.label}, parent_id={self.parent_id}, pos={self.pos})>"
