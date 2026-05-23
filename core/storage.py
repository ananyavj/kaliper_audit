#core/storage.py
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

DB_PATH = Path(__file__).resolve().parent.parent / "kaliper.db"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_connection(db_path: str | Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def initialize_db(db_path: str | Path = DB_PATH) -> None:
    conn = get_connection(db_path)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS tenants (
            tenant_id TEXT PRIMARY KEY,
            tenant_name TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS workspaces (
            workspace_id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            workspace_name TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_id TEXT NOT NULL,
            workspace_id TEXT NOT NULL,
            environment TEXT NOT NULL,
            mode TEXT NOT NULL,
            domain TEXT NOT NULL,
            confidence REAL NOT NULL,
            plan_version_id INTEGER,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            event_count INTEGER DEFAULT 0,
            issue_count INTEGER DEFAULT 0
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_id TEXT NOT NULL,
            workspace_id TEXT NOT NULL,
            run_id INTEGER,
            source TEXT,
            name TEXT NOT NULL,
            user_id TEXT,
            anonymous_id TEXT,
            timestamp TEXT NOT NULL,
            event_id TEXT NOT NULL,
            properties_json TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (run_id) REFERENCES runs(id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS issues (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_id TEXT NOT NULL,
            workspace_id TEXT NOT NULL,
            run_id INTEGER,
            event_id TEXT NOT NULL,
            event_name TEXT NOT NULL,
            issue_type TEXT NOT NULL,
            severity TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (run_id) REFERENCES runs(id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS plan_versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_id TEXT NOT NULL,
            workspace_id TEXT NOT NULL,
            version TEXT NOT NULL,
            plan_path TEXT NOT NULL,
            domain TEXT NOT NULL,
            plan_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)

    conn.commit()
    conn.close()


def ensure_tenant(
    tenant_id: str,
    tenant_name: str,
    db_path: str | Path = DB_PATH,
) -> None:
    conn = get_connection(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO tenants (tenant_id, tenant_name, created_at)
        VALUES (?, ?, ?)
        """,
        (tenant_id, tenant_name, _utc_now()),
    )
    conn.commit()
    conn.close()


def ensure_workspace(
    workspace_id: str,
    tenant_id: str,
    workspace_name: str,
    db_path: str | Path = DB_PATH,
) -> None:
    conn = get_connection(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO workspaces (workspace_id, tenant_id, workspace_name, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (workspace_id, tenant_id, workspace_name, _utc_now()),
    )
    conn.commit()
    conn.close()


def start_run(
    *,
    tenant_id: str,
    workspace_id: str,
    environment: str,
    mode: str,
    domain: str,
    confidence: float,
    plan_version_id: Optional[int] = None,
    db_path: str | Path = DB_PATH,
) -> int:
    conn = get_connection(db_path)
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO runs (
            tenant_id, workspace_id, environment, mode, domain, confidence,
            plan_version_id, started_at, event_count, issue_count
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0)
        """,
        (
            tenant_id,
            workspace_id,
            environment,
            mode,
            domain,
            confidence,
            plan_version_id,
            _utc_now(),
        ),
    )

    run_id = int(cur.lastrowid)
    conn.commit()
    conn.close()
    return run_id


def finish_run(
    run_id: int,
    event_count: int,
    issue_count: int,
    db_path: str | Path = DB_PATH,
) -> None:
    conn = get_connection(db_path)
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE runs
        SET ended_at = ?, event_count = ?, issue_count = ?
        WHERE id = ?
        """,
        (_utc_now(), event_count, issue_count, run_id),
    )

    conn.commit()
    conn.close()


def store_event(
    *,
    tenant_id: str,
    workspace_id: str,
    run_id: Optional[int],
    source: str,
    name: str,
    user_id: Optional[str],
    anonymous_id: Optional[str],
    timestamp: str,
    event_id: str,
    properties: dict[str, Any],
    raw_json: dict[str, Any],
    db_path: str | Path = DB_PATH,
) -> None:
    conn = get_connection(db_path)
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO events (
            tenant_id, workspace_id, run_id, source, name, user_id, anonymous_id,
            timestamp, event_id, properties_json, raw_json, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            tenant_id,
            workspace_id,
            run_id,
            source,
            name,
            user_id,
            anonymous_id,
            timestamp,
            event_id,
            json.dumps(properties),
            json.dumps(raw_json),
            _utc_now(),
        ),
    )

    conn.commit()
    conn.close()


def store_issue(
    *,
    tenant_id: str,
    workspace_id: str,
    run_id: Optional[int],
    event_id: str,
    event_name: str,
    issue_type: str,
    severity: str,
    message: str,
    db_path: str | Path = DB_PATH,
) -> None:
    conn = get_connection(db_path)
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO issues (
            tenant_id, workspace_id, run_id, event_id, event_name,
            issue_type, severity, message, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            tenant_id,
            workspace_id,
            run_id,
            event_id,
            event_name,
            issue_type,
            severity,
            message,
            _utc_now(),
        ),
    )

    conn.commit()
    conn.close()


def event_id_exists(
    event_id: str,
    tenant_id: str,
    workspace_id: str,
    db_path: str | Path = DB_PATH,
) -> bool:
    """
    Return True if an event with this event_id has already been stored for this
    tenant+workspace. Used by importers to skip duplicate events.
    """
    conn = get_connection(db_path)
    cur = conn.cursor()
    row = cur.execute(
        """
        SELECT 1 FROM events
        WHERE event_id = ? AND tenant_id = ? AND workspace_id = ?
        LIMIT 1
        """,
        (event_id, tenant_id, workspace_id),
    ).fetchone()
    conn.close()
    return row is not None


def store_plan_version(
    *,
    tenant_id: str,
    workspace_id: str,
    version: str,
    plan_path: str,
    domain: str,
    plan_json: str,
    db_path: str | Path = DB_PATH,
) -> int:
    conn = get_connection(db_path)
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO plan_versions (
            tenant_id, workspace_id, version, plan_path, domain, plan_json, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            tenant_id,
            workspace_id,
            version,
            plan_path,
            domain,
            plan_json,
            _utc_now(),
        ),
    )

    version_id = int(cur.lastrowid)
    conn.commit()
    conn.close()
    return version_id