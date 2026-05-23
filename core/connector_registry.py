#core/connector_registry.py
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Optional, Any

from core.storage import get_connection


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Checkpoint table — tracks incremental sync state per connector
# ---------------------------------------------------------------------------

DEFAULT_LOOKBACK_HOURS = 24   # how far back to start on a brand-new connector


def initialize_checkpoint_table() -> None:
    """Create the connector_checkpoints table if it doesn't exist."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS connector_checkpoints (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            connector_id     INTEGER NOT NULL UNIQUE,
            last_imported_at TEXT NOT NULL,
            updated_at       TEXT NOT NULL,
            FOREIGN KEY (connector_id) REFERENCES connectors(id)
        )
        """
    )
    conn.commit()
    conn.close()


def get_checkpoint(connector_id: int) -> Optional[datetime]:
    """Return the last successfully imported timestamp for a connector, or None."""
    conn = get_connection()
    cur = conn.cursor()
    row = cur.execute(
        "SELECT last_imported_at FROM connector_checkpoints WHERE connector_id = ?",
        (connector_id,),
    ).fetchone()
    conn.close()
    if not row:
        return None
    return datetime.fromisoformat(row["last_imported_at"])


def set_checkpoint(connector_id: int, timestamp: datetime) -> None:
    """Upsert the checkpoint timestamp for a connector."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO connector_checkpoints (connector_id, last_imported_at, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(connector_id) DO UPDATE SET
            last_imported_at = excluded.last_imported_at,
            updated_at       = excluded.updated_at
        """,
        (
            connector_id,
            timestamp.isoformat(),
            utc_now(),
        ),
    )
    conn.commit()
    conn.close()


def get_or_init_checkpoint(connector_id: int) -> datetime:
    """
    Return the existing checkpoint, or create one starting DEFAULT_LOOKBACK_HOURS
    ago. This is the safe entry-point — always call this instead of get_checkpoint
    when you need a guaranteed start time.
    """
    existing = get_checkpoint(connector_id)
    if existing is not None:
        return existing
    fallback = datetime.now(timezone.utc) - timedelta(hours=DEFAULT_LOOKBACK_HOURS)
    set_checkpoint(connector_id, fallback)
    return fallback


def clear_checkpoint(connector_id: int) -> None:
    """Delete the checkpoint for a connector (forces a full re-import on next run)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM connector_checkpoints WHERE connector_id = ?",
        (connector_id,),
    )
    conn.commit()
    conn.close()


def initialize_connector_tables() -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS connectors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_id TEXT NOT NULL,
            workspace_id TEXT NOT NULL,

            connector_name TEXT NOT NULL,
            connector_type TEXT NOT NULL,

            is_active INTEGER NOT NULL DEFAULT 1,

            credentials_json TEXT NOT NULL,
            config_json TEXT NOT NULL,

            last_sync_at TEXT,
            created_at TEXT NOT NULL
        )
        """
    )

    # Checkpoints live alongside connectors — always create both together
    initialize_checkpoint_table()

    conn.commit()
    conn.close()


def register_connector(
    tenant_id: str,
    workspace_id: str,
    connector_name: str,
    connector_type: str,
    credentials: dict[str, Any],
    config: Optional[dict[str, Any]] = None,
    is_active: bool = True,
) -> int:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO connectors (
            tenant_id,
            workspace_id,
            connector_name,
            connector_type,
            is_active,
            credentials_json,
            config_json,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            tenant_id,
            workspace_id,
            connector_name,
            connector_type,
            1 if is_active else 0,
            json.dumps(credentials),
            json.dumps(config or {}),
            utc_now(),
        ),
    )

    connector_id = cur.lastrowid

    conn.commit()
    conn.close()

    return connector_id


def list_connectors(
    tenant_id: str,
    workspace_id: str,
) -> list[dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor()

    rows = cur.execute(
        """
        SELECT
            id, tenant_id, workspace_id, connector_name, connector_type,
            is_active, credentials_json, config_json, last_sync_at, created_at
        FROM connectors
        WHERE tenant_id = ? AND workspace_id = ?
        ORDER BY id DESC
        """,
        (tenant_id, workspace_id),
    ).fetchall()

    conn.close()

    return [
        {
            "id": row["id"],
            "tenant_id": row["tenant_id"],
            "workspace_id": row["workspace_id"],
            "connector_name": row["connector_name"],
            "connector_type": row["connector_type"],
            "is_active": bool(row["is_active"]),
            "credentials": json.loads(row["credentials_json"]),
            "config": json.loads(row["config_json"]),
            "last_sync_at": row["last_sync_at"],
            "created_at": row["created_at"],
        }
        for row in rows
    ]


def get_connector(
    connector_id: int,
) -> Optional[dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor()

    row = cur.execute(
        """
        SELECT
            id, tenant_id, workspace_id, connector_name, connector_type,
            is_active, credentials_json, config_json, last_sync_at, created_at
        FROM connectors
        WHERE id = ?
        """,
        (connector_id,),
    ).fetchone()

    conn.close()

    if not row:
        return None

    return {
        "id": row["id"],
        "tenant_id": row["tenant_id"],
        "workspace_id": row["workspace_id"],
        "connector_name": row["connector_name"],
        "connector_type": row["connector_type"],
        "is_active": bool(row["is_active"]),
        "credentials": json.loads(row["credentials_json"]),
        "config": json.loads(row["config_json"]),
        "last_sync_at": row["last_sync_at"],
        "created_at": row["created_at"],
    }

def update_connector_sync_time(
    connector_id: int,
) -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE connectors
        SET last_sync_at = ?
        WHERE id = ?
        """,
        (utc_now(), connector_id),
    )

    conn.commit()
    conn.close()


def activate_connector(connector_id: int) -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE connectors
        SET is_active = 1
        WHERE id = ?
        """,
        (connector_id,),
    )

    conn.commit()
    conn.close()


def deactivate_connector(connector_id: int) -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE connectors
        SET is_active = 0
        WHERE id = ?
        """,
        (connector_id,),
    )

    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Credential isolation — always use this instead of get_connector when the
# caller is a tenant-scoped context (ingestion server, sync service, API)
# ---------------------------------------------------------------------------

def get_connector_for_tenant(
    connector_id: int,
    tenant_id: str,
) -> Optional[dict[str, Any]]:
    """
    Fetch a connector only if it belongs to the given tenant.
    Returns None if the connector doesn't exist OR belongs to a different tenant.

    This is the credential isolation boundary — a tenant can never accidentally
    (or intentionally) access another tenant's connector credentials.
    """
    connector = get_connector(connector_id)
    if connector is None:
        return None
    if connector["tenant_id"] != tenant_id:
        return None
    return connector


def list_all_workspaces() -> list[dict[str, str]]:
    """
    Return all (tenant_id, workspace_id) pairs that have at least one connector.
    Used by the sync service for dynamic discovery instead of hardcoded pairs.
    """
    conn = get_connection()
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT DISTINCT tenant_id, workspace_id
        FROM connectors
        ORDER BY tenant_id, workspace_id
        """
    ).fetchall()
    conn.close()
    # Bug 6 fix: access columns by name via sqlite3.Row (set in get_connection),
    # not by index. Index access is fragile — adding a column silently breaks it.
    return [{"tenant_id": row["tenant_id"], "workspace_id": row["workspace_id"]} for row in rows]
