from datetime import datetime

from sqlalchemy import BigInteger, DateTime, ForeignKey, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.lib.db.base import Base


class TreeNode(Base):
    __tablename__ = "tree_node"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    org_key: Mapped[str] = mapped_column(String, nullable=False, default="default")
    root_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True, index=True)
    parent_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("tree_node.id", ondelete="CASCADE"), nullable=True, index=True
    )
    label: Mapped[str] = mapped_column(String, nullable=False)
    depth: Mapped[int] = mapped_column(Integer, nullable=False)
    rank: Mapped[str] = mapped_column(String, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    parent: Mapped["TreeNode | None"] = relationship("TreeNode", remote_side=[id], backref="children")

    def __repr__(self) -> str:
        return f"<TreeNode(id={self.id}, label={self.label}, parent_id={self.parent_id})>"
