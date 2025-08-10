CREATE TABLE tree_node (
  id           BIGSERIAL PRIMARY KEY,
  org_key      TEXT NOT NULL DEFAULT 'default',        -- single-tenant for now
  root_id      BIGINT NULL,                            -- denormalized for O(1) tree lookup
  parent_id    BIGINT NULL REFERENCES tree_node(id) ON DELETE CASCADE,
  label        TEXT NOT NULL,
  depth        INTEGER NOT NULL,                       -- denormalized for cheap filtering
  rank         TEXT NOT NULL,                          -- lexorank for ordering
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_node_parent    ON tree_node(parent_id);
CREATE INDEX idx_node_root      ON tree_node(root_id);
CREATE INDEX idx_node_org_root  ON tree_node(org_key, root_id);
CREATE INDEX idx_node_rank      ON tree_node(rank);
CREATE INDEX idx_root_order     ON tree_node(org_key, root_id, updated_at DESC);
