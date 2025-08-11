"""Initial schema with tree_nodes table

Revision ID: 1dde04ee4797
Revises:
Create Date: 2025-08-09 22:06:50.517422

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "1dde04ee4797"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    # Create tree_nodes table with proper schema
    op.create_table(
        "tree_nodes",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("root_id", sa.BigInteger(), nullable=False),
        sa.Column("parent_id", sa.BigInteger(), nullable=True),
        sa.Column("org_id", sa.String(), nullable=False, server_default="default"),
        sa.Column("label", sa.String(), nullable=False),
        sa.Column("pos", sa.BigInteger(), nullable=False),
        sa.Column("path_ids", sa.ARRAY(sa.BigInteger()), nullable=False, server_default="{}"),
        sa.Column("path_pos", sa.ARRAY(sa.BigInteger()), nullable=False, server_default="{}"),
        sa.Column("depth", sa.SmallInteger(), nullable=False, server_default="1"),
        sa.Column("label_json", sa.Text(), nullable=False, server_default="'{}'"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["parent_id"], ["tree_nodes.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )

    # Create optimized indexes for path-based queries
    op.create_index("ix_tree_nodes_root_pathpos", "tree_nodes", ["root_id", "path_pos"], unique=False)
    op.create_index("ix_tree_nodes_parent_pos", "tree_nodes", ["parent_id", "pos"], unique=False)
    op.create_index("ix_tree_nodes_root_updated", "tree_nodes", ["root_id", "updated_at"], unique=False)
    op.create_index("ix_tree_nodes_org_root", "tree_nodes", ["org_id", "root_id"], unique=False)

    # Add CHECK constraints for data integrity
    op.execute(
        """
        ALTER TABLE tree_nodes
        ADD CONSTRAINT depth_positive_and_bounded
        CHECK (depth > 0 AND depth <= 32767)
    """
    )

    op.execute(
        """
        ALTER TABLE tree_nodes
        ADD CONSTRAINT label_json_not_empty
        CHECK (label_json != '')
    """
    )

    # Note: We use window functions in the application layer to build JSON
    # instead of PL/pgSQL functions. This avoids recursion limits and provides
    # better performance for deep trees (>1000 nodes).


def downgrade() -> None:
    """Downgrade schema."""
    op.execute("ALTER TABLE tree_nodes DROP CONSTRAINT IF EXISTS label_json_not_empty")
    op.execute("ALTER TABLE tree_nodes DROP CONSTRAINT IF EXISTS depth_positive_and_bounded")
    op.execute("DROP INDEX IF EXISTS ix_tree_nodes_org_root")
    op.execute("DROP INDEX IF EXISTS ix_tree_nodes_root_updated")
    op.execute("DROP INDEX IF EXISTS ix_tree_nodes_parent_pos")
    op.execute("DROP INDEX IF EXISTS ix_tree_nodes_root_pathpos")
    op.execute("DROP TABLE IF EXISTS tree_nodes CASCADE")
