#core/plan_registry.py
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from uuid import uuid4

from core.plan_analyzer import analyze_tracking_plan, PlanProfile
from core.plan_diff import PlanDiffResult, compare_tracking_plans
from core.plan_loader import load_tracking_plan
from core.plan_normalizer import normalize_specs
from core.schemas import TrackingEventSpec
from core.storage import get_connection, initialize_db, store_plan_version

DB_PATH = Path(__file__).resolve().parent.parent / "kaliper.db"


@dataclass
class RegisteredPlan:
    plan_version_id: int
    tenant_id: str
    workspace_id: str
    version: str
    plan_path: str
    domain: str
    created_at: str
    is_active: bool = False


def _utc_version_label(prefix: str = "plan") -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}-{timestamp}-{uuid4().hex[:8]}"


def _ensure_registry_tables() -> None:
    initialize_db()

    conn = get_connection(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS active_plan_versions (
            tenant_id TEXT NOT NULL,
            workspace_id TEXT NOT NULL,
            plan_version_id INTEGER NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (tenant_id, workspace_id)
        )
    """)

    conn.commit()
    conn.close()


def _serialize_specs(specs: list[TrackingEventSpec]) -> str:
    return json.dumps(
        {
            "events": [
                {
                    "name": spec.name,
                    "required_properties": spec.required_properties,
                    "property_types": spec.property_types,
                    "identity_required": spec.identity_required,
                    "allowed_previous_events": spec.allowed_previous_events,
                }
                for spec in specs
            ]
        }
    )


def _deserialize_specs(plan_json: str) -> list[TrackingEventSpec]:
    data = json.loads(plan_json)
    specs: list[TrackingEventSpec] = []

    for event in data.get("events", []):
        specs.append(
            TrackingEventSpec(
                name=event["name"],
                required_properties=event.get("required_properties", []),
                property_types=event.get("property_types", {}),
                identity_required=event.get("identity_required", False),
                allowed_previous_events=event.get("allowed_previous_events", []),
            )
        )

    return specs


def _analyze_specs(specs: list[TrackingEventSpec]) -> PlanProfile:
    normalized = normalize_specs(specs)
    return analyze_tracking_plan(normalized)


def register_plan_from_file(
    *,
    tenant_id: str,
    workspace_id: str,
    plan_path: str,
    version_prefix: str = "plan",
    make_active: bool = True,
) -> RegisteredPlan:
    _ensure_registry_tables()

    raw_specs = load_tracking_plan(plan_path)
    normalized_specs = normalize_specs(raw_specs)
    profile = analyze_tracking_plan(normalized_specs)

    version = _utc_version_label(version_prefix)
    plan_json = _serialize_specs(normalized_specs)

    plan_version_id = store_plan_version(
        tenant_id=tenant_id,
        workspace_id=workspace_id,
        version=version,
        plan_path=plan_path,
        domain=profile.domain,
        plan_json=plan_json,
        db_path=DB_PATH,
    )

    if make_active:
        set_active_plan_version(
            tenant_id=tenant_id,
            workspace_id=workspace_id,
            plan_version_id=plan_version_id,
        )

    return RegisteredPlan(
        plan_version_id=plan_version_id,
        tenant_id=tenant_id,
        workspace_id=workspace_id,
        version=version,
        plan_path=plan_path,
        domain=profile.domain,
        created_at=datetime.now(timezone.utc).isoformat(),
        is_active=make_active,
    )


def register_plan_version(
    *,
    tenant_id: str,
    workspace_id: str,
    version: str,
    plan_path: str,
    specs: list[TrackingEventSpec],
    make_active: bool = False,
) -> RegisteredPlan:
    _ensure_registry_tables()

    normalized_specs = normalize_specs(specs)
    profile = analyze_tracking_plan(normalized_specs)
    plan_json = _serialize_specs(normalized_specs)

    plan_version_id = store_plan_version(
        tenant_id=tenant_id,
        workspace_id=workspace_id,
        version=version,
        plan_path=plan_path,
        domain=profile.domain,
        plan_json=plan_json,
        db_path=DB_PATH,
    )

    if make_active:
        set_active_plan_version(
            tenant_id=tenant_id,
            workspace_id=workspace_id,
            plan_version_id=plan_version_id,
        )

    return RegisteredPlan(
        plan_version_id=plan_version_id,
        tenant_id=tenant_id,
        workspace_id=workspace_id,
        version=version,
        plan_path=plan_path,
        domain=profile.domain,
        created_at=datetime.now(timezone.utc).isoformat(),
        is_active=make_active,
    )


def get_latest_plan_version(
    *,
    tenant_id: str,
    workspace_id: str,
) -> Optional[dict]:
    _ensure_registry_tables()

    conn = get_connection(DB_PATH)
    cur = conn.cursor()

    row = cur.execute(
        """
        SELECT *
        FROM plan_versions
        WHERE tenant_id = ? AND workspace_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (tenant_id, workspace_id),
    ).fetchone()

    conn.close()
    return dict(row) if row else None


def get_plan_version_by_id(plan_version_id: int) -> Optional[dict]:
    _ensure_registry_tables()

    conn = get_connection(DB_PATH)
    cur = conn.cursor()

    row = cur.execute(
        """
        SELECT *
        FROM plan_versions
        WHERE id = ?
        """,
        (plan_version_id,),
    ).fetchone()

    conn.close()
    return dict(row) if row else None


def list_plan_versions(
    *,
    tenant_id: str,
    workspace_id: str,
    limit: int = 20,
) -> list[dict]:
    _ensure_registry_tables()

    conn = get_connection(DB_PATH)
    cur = conn.cursor()

    rows = cur.execute(
        """
        SELECT *
        FROM plan_versions
        WHERE tenant_id = ? AND workspace_id = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (tenant_id, workspace_id, limit),
    ).fetchall()

    conn.close()
    return [dict(row) for row in rows]


def set_active_plan_version(
    *,
    tenant_id: str,
    workspace_id: str,
    plan_version_id: int,
) -> None:
    _ensure_registry_tables()

    conn = get_connection(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO active_plan_versions (tenant_id, workspace_id, plan_version_id, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(tenant_id, workspace_id)
        DO UPDATE SET
            plan_version_id = excluded.plan_version_id,
            updated_at = excluded.updated_at
        """,
        (
            tenant_id,
            workspace_id,
            plan_version_id,
            datetime.now(timezone.utc).isoformat(),
        ),
    )

    conn.commit()
    conn.close()


def get_active_plan_version(
    *,
    tenant_id: str,
    workspace_id: str,
) -> Optional[dict]:
    _ensure_registry_tables()

    conn = get_connection(DB_PATH)
    cur = conn.cursor()

    row = cur.execute(
        """
        SELECT apv.plan_version_id, pv.*
        FROM active_plan_versions apv
        JOIN plan_versions pv
            ON pv.id = apv.plan_version_id
        WHERE apv.tenant_id = ? AND apv.workspace_id = ?
        """,
        (tenant_id, workspace_id),
    ).fetchone()

    conn.close()
    return dict(row) if row else None


def get_active_plan_bundle(
    *,
    tenant_id: str,
    workspace_id: str,
) -> Optional[dict]:
    active = get_active_plan_version(tenant_id=tenant_id, workspace_id=workspace_id)
    if not active:
        return None

    specs = normalize_specs(_deserialize_specs(active["plan_json"]))
    profile = analyze_tracking_plan(specs)

    return {
        "active_plan": active,
        "specs": specs,
        "profile": profile,
    }


def compare_with_latest(
    *,
    tenant_id: str,
    workspace_id: str,
    new_specs: list[TrackingEventSpec],
) -> PlanDiffResult:
    latest = get_latest_plan_version(tenant_id=tenant_id, workspace_id=workspace_id)

    if not latest:
        return PlanDiffResult()

    old_specs = _deserialize_specs(latest["plan_json"])
    normalized_new_specs = normalize_specs(new_specs)

    return compare_tracking_plans(old_specs, normalized_new_specs)


def register_and_compare(
    *,
    tenant_id: str,
    workspace_id: str,
    plan_path: str,
    version_prefix: str = "plan",
    make_active: bool = True,
) -> tuple[RegisteredPlan, PlanDiffResult]:
    _ensure_registry_tables()

    raw_specs = load_tracking_plan(plan_path)
    normalized_specs = normalize_specs(raw_specs)
    profile = analyze_tracking_plan(normalized_specs)

    diff = compare_with_latest(
        tenant_id=tenant_id,
        workspace_id=workspace_id,
        new_specs=normalized_specs,
    )

    version = _utc_version_label(version_prefix)
    plan_json = _serialize_specs(normalized_specs)

    plan_version_id = store_plan_version(
        tenant_id=tenant_id,
        workspace_id=workspace_id,
        version=version,
        plan_path=plan_path,
        domain=profile.domain,
        plan_json=plan_json,
        db_path=DB_PATH,
    )

    if make_active:
        set_active_plan_version(
            tenant_id=tenant_id,
            workspace_id=workspace_id,
            plan_version_id=plan_version_id,
        )

    plan_record = RegisteredPlan(
        plan_version_id=plan_version_id,
        tenant_id=tenant_id,
        workspace_id=workspace_id,
        version=version,
        plan_path=plan_path,
        domain=profile.domain,
        created_at=datetime.now(timezone.utc).isoformat(),
        is_active=make_active,
    )

    return plan_record, diff