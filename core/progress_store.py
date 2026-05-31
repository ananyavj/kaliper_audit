# core/progress_store.py
"""
Progress store — persists import progress to SQLite so it is visible
across processes (e.g. the amplitude_importer process and the ingestion
server/dashboard process can both read/write the same state).

Previously this used an in-memory dict (PROGRESS_STATE) which meant:
  - The ingestion server always saw "idle" because the importer ran in a
    separate process with its own memory space.
  - Concurrent writes from multiple threads had no synchronisation.

Both problems are solved by routing through SQLite, which serialises
writes natively and persists data to disk.
"""

from __future__ import annotations

from typing import Any

from core.storage import get_connection


def _ensure_table() -> None:
    """Create the import_progress table if it doesn't exist."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS import_progress (
            workspace_id TEXT PRIMARY KEY,
            status       TEXT NOT NULL DEFAULT 'idle',
            current      INTEGER NOT NULL DEFAULT 0,
            total        INTEGER NOT NULL DEFAULT 0,
            updated_at   TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.commit()
    conn.close()


def update_progress(workspace_id: str, status: str, current: int, total: int) -> None:
    """Upsert the import progress for a workspace (safe to call from any process)."""
    _ensure_table()
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO import_progress (workspace_id, status, current, total, updated_at)
        VALUES (?, ?, ?, ?, datetime('now'))
        ON CONFLICT(workspace_id) DO UPDATE SET
            status     = excluded.status,
            current    = excluded.current,
            total      = excluded.total,
            updated_at = excluded.updated_at
        """,
        (workspace_id, status, current, total),
    )
    conn.commit()
    conn.close()


def get_progress(workspace_id: str) -> dict[str, Any]:
    """Return the current progress for a workspace, or idle defaults."""
    _ensure_table()
    conn = get_connection()
    cur = conn.cursor()
    row = cur.execute(
        "SELECT status, current, total FROM import_progress WHERE workspace_id = ?",
        (workspace_id,),
    ).fetchone()
    conn.close()

    if not row:
        return {"status": "idle", "current": 0, "total": 0}
    return {
        "status": row["status"],
        "current": row["current"],
        "total": row["total"],
    }
