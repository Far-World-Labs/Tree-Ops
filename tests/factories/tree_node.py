"""Factory for TreeNode model."""

from typing import Self

import factory
from factory import Faker, LazyAttribute, Sequence
from lexorank.lexorank import lexorank

from app.ops.entities.tree_node import TreeNode

from .base import AsyncSQLAlchemyModelFactory


class TreeNodeFactory(AsyncSQLAlchemyModelFactory):
    """Factory for creating TreeNode instances."""

    class Meta:
        model = TreeNode

    # Sensible defaults for a root node
    id = Sequence(lambda n: n + 1)
    label = Faker("company")
    org_key = "default"
    depth = 0
    parent_id = None
    root_id = LazyAttribute(lambda obj: obj.id if obj.parent_id is None else None)

    @factory.lazy_attribute
    def rank(self):
        """Generate a valid lexorank."""
        rank_value = lexorank("", "")
        return rank_value[0] if isinstance(rank_value, tuple) else rank_value

    @classmethod
    def root(cls, **kwargs) -> Self:
        """Create a root node with explicit defaults."""
        return cls.build(parent_id=None, depth=0, **kwargs)

    @classmethod
    def child_of(cls, parent: TreeNode, **kwargs) -> Self:
        """Create a child of a specific parent."""
        return cls.build(parent_id=parent.id, root_id=parent.root_id or parent.id, depth=parent.depth + 1, **kwargs)
