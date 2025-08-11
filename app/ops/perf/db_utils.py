"""Database utilities for performance testing."""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class DatabaseManager:
    """Manages database operations for performance testing."""

    def __init__(self):
        # Use environment variables directly
        import os

        self.conn_params = {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": int(os.getenv("POSTGRES_PORT", "5430")),
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
            "database": os.getenv("POSTGRES_DB", "tree-ops"),
        }

    def kill_queries(self, max_age_seconds: int = 10):
        """Kill all queries older than max_age_seconds, except our own."""
        conn = psycopg2.connect(**self.conn_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        try:
            # Terminate queries older than max_age
            cur.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s
                  AND pid != pg_backend_pid()
                  AND state != 'idle'
                  AND query NOT LIKE '%%pg_stat_activity%%'
                  AND query_start < NOW() - INTERVAL '%s seconds'
            """,
                (self.conn_params["database"], max_age_seconds),
            )

            terminated = cur.fetchall()
            count = len(terminated)

            if count > 0:
                print(f"  Terminated {count} runaway queries")

        finally:
            cur.close()
            conn.close()

    def reset_connections(self):
        """Reset all database connections except our own."""
        conn = psycopg2.connect(**self.conn_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        try:
            # Terminate all connections except ours
            cur.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s
                  AND pid != pg_backend_pid()
            """,
                (self.conn_params["database"],),
            )

            terminated = cur.fetchall()
            count = len(terminated)

            if count > 0:
                print(f"  Reset {count} database connections")

        finally:
            cur.close()
            conn.close()

    def clear_cache(self):
        """Clear PostgreSQL caches."""
        conn = psycopg2.connect(**self.conn_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        try:
            # Clear shared buffers (requires superuser)
            try:
                cur.execute("DISCARD ALL")
            except Exception:
                pass

            # Reset statistics
            try:
                cur.execute("SELECT pg_stat_reset()")
            except Exception:
                pass

        finally:
            cur.close()
            conn.close()

    def vacuum_analyze(self, table: str = "tree_nodes"):
        """Run VACUUM ANALYZE on table to update statistics."""
        conn = psycopg2.connect(**self.conn_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        try:
            cur.execute(f"VACUUM ANALYZE {table}")
            print(f"  Vacuumed and analyzed {table}")
        finally:
            cur.close()
            conn.close()

    def get_active_queries(self):
        """Get list of currently active queries."""
        conn = psycopg2.connect(**self.conn_params)
        cur = conn.cursor()

        try:
            cur.execute(
                """
                SELECT pid,
                       EXTRACT(epoch FROM (NOW() - query_start))::int as duration_seconds,
                       state,
                       LEFT(query, 100) as query_preview
                FROM pg_stat_activity
                WHERE datname = %s
                  AND pid != pg_backend_pid()
                  AND state != 'idle'
                ORDER BY query_start
            """,
                (self.conn_params["database"],),
            )

            return cur.fetchall()
        finally:
            cur.close()
            conn.close()

    def get_table_stats(self, table: str = "tree_nodes"):
        """Get table statistics including index usage."""
        conn = psycopg2.connect(**self.conn_params)
        cur = conn.cursor()

        try:
            # Get index and sequential scan counts
            cur.execute(
                """
                SELECT
                    seq_scan,
                    seq_tup_read,
                    idx_scan,
                    idx_tup_fetch
                FROM pg_stat_user_tables
                WHERE schemaname = 'public'
                  AND relname = %s
            """,
                (table,),
            )

            result = cur.fetchone()
            if result:
                return {
                    "seq_scans": result[0] or 0,
                    "seq_rows": result[1] or 0,
                    "idx_scans": result[2] or 0,
                    "idx_rows": result[3] or 0,
                }
            return {"seq_scans": 0, "seq_rows": 0, "idx_scans": 0, "idx_rows": 0}

        finally:
            cur.close()
            conn.close()

    def reset_stats(self):
        """Reset PostgreSQL statistics."""
        conn = psycopg2.connect(**self.conn_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        try:
            cur.execute("SELECT pg_stat_reset()")
        finally:
            cur.close()
            conn.close()

    def prepare_for_test(self):
        """Prepare database for performance testing."""
        print("Preparing database...")

        # Kill runaway queries (aggressive - anything over 0.5 seconds)
        self.kill_queries(max_age_seconds=0.5)

        # Clear caches
        self.clear_cache()

        # Update statistics
        self.vacuum_analyze()

        # Reset statistics for fresh measurements
        self.reset_stats()

        print("  Database ready for testing")
