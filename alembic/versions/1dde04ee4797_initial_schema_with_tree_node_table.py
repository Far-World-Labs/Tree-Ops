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
        sa.Column("org_key", sa.String(), nullable=False, server_default="default"),
        sa.Column("label", sa.String(), nullable=False),
        sa.Column("pos", sa.BigInteger(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["parent_id"], ["tree_nodes.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )

    # Create indexes as specified in docs
    op.create_index(
        "ix_tree_nodes_root_parent_pos_id", "tree_nodes", ["root_id", "parent_id", "pos", "id"], unique=False
    )
    op.create_index("ix_tree_nodes_parent_pos", "tree_nodes", ["parent_id", "pos"], unique=False)
    op.create_index("ix_tree_nodes_root_updated", "tree_nodes", ["root_id", "updated_at"], unique=False)
    op.create_index("ix_tree_nodes_org_root", "tree_nodes", ["org_key", "root_id"], unique=False)

    # Create PL/pgSQL functions for building tree JSON
    #
    # Why we need PL/pgSQL functions instead of pure SQL:
    #
    # PostgreSQL's recursive CTEs have a fundamental limitation: they cannot use
    # aggregate functions (like jsonb_agg) in the recursive term. This prevents us
    # from building nested JSON structures directly in a single recursive CTE.
    #
    # Alternative approaches we considered:
    # 1. Fixed-depth nested queries - Works but limited to hardcoded depth (5-10 levels)
    # 2. Python-based tree building - Works but requires transferring all data and building in app
    # 3. Recursive CTE + post-processing - Complex and still can't aggregate properly
    # 4. String manipulation in SQL - Fragile and hard to maintain
    #
    # The PL/pgSQL recursive function solution:
    # - Handles arbitrary tree depth
    # - Builds JSON entirely in PostgreSQL (single round-trip)
    # - Returns properly nested JSON structure
    # - Clean and maintainable

    # Function to recursively build JSON for a single tree node and its descendants
    op.execute(
        """
        CREATE OR REPLACE FUNCTION build_tree_json(p_node_id BIGINT)
        RETURNS JSONB AS $$
        BEGIN
            RETURN (
                SELECT jsonb_build_object(
                    'id', t.id,
                    'label', t.label,
                    'children', COALESCE(
                        (
                            -- Recursively build JSON for each child
                            SELECT jsonb_agg(
                                build_tree_json(c.id) ORDER BY c.pos, c.id
                            )
                            FROM tree_nodes c
                            WHERE c.parent_id = t.id
                        ),
                        '[]'::jsonb
                    )
                )
                FROM tree_nodes t
                WHERE t.id = p_node_id
            );
        END;
        $$ LANGUAGE plpgsql STABLE;

        -- Function to build entire forest (all trees) for an organization
        CREATE OR REPLACE FUNCTION build_forest_json(p_org_key TEXT)
        RETURNS TEXT AS $$
        BEGIN
            RETURN (
                SELECT COALESCE(
                    jsonb_agg(
                        build_tree_json(t.id) ORDER BY t.pos, t.id
                    )::text,
                    '[]'
                )
                FROM tree_nodes t
                WHERE t.parent_id IS NULL
                  AND t.org_key = p_org_key
            );
        END;
        $$ LANGUAGE plpgsql STABLE;
    """
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.execute("DROP FUNCTION IF EXISTS build_forest_json(TEXT)")
    op.execute("DROP FUNCTION IF EXISTS build_tree_json(BIGINT)")
    op.execute("DROP INDEX IF EXISTS ix_tree_nodes_org_root")
    op.execute("DROP INDEX IF EXISTS ix_tree_nodes_root_updated")
    op.execute("DROP INDEX IF EXISTS ix_tree_nodes_parent_pos")
    op.execute("DROP INDEX IF EXISTS ix_tree_nodes_root_parent_pos_id")
    op.execute("DROP TABLE IF EXISTS tree_nodes CASCADE")
