#ingestion/ingestion_server.py
import json
import os
from dataclasses import asdict
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Any

from flask import Flask, request, jsonify
from flask_cors import CORS

from core.plan_registry import (
    get_active_plan_bundle,
    list_plan_versions,
    get_plan_version_by_id,
    get_active_plan_version,
)
from core.plan_diff import compare_tracking_plans
from core.plan_normalizer import normalize_specs
from core.state_store import StreamState
from core.schemas import IncomingEvent
from core.runtime_context import RuntimeContext
from core.storage import (
    initialize_db,
    ensure_tenant,
    ensure_workspace,
    get_connection,
    store_event,
    store_events_bulk,
    store_issue,
    store_issues_bulk,
    start_run,
    finish_run,
)
from core.detectors import detect_issues
from core.reporting import (
    get_run_history,
    get_trend_report,
    get_event_health,
    get_tracking_coverage,
    get_issue_type_breakdown,
)
from scorer import build_scorecard

app = Flask(__name__)
CORS(app)

# ---------------------------------------------------------------------------
# Tenant API key registry
# ---------------------------------------------------------------------------

def _load_api_keys() -> dict[str, str]:
    keys: dict[str, str] = {}
    prefix = "KALIPER_API_KEY_"
    for env_var, value in os.environ.items():
        if env_var.startswith(prefix) and value:
            tenant_id = env_var[len(prefix):]
            keys[value] = tenant_id
    return keys


API_KEYS: dict[str, str] = _load_api_keys()
AUTH_ENABLED: bool = len(API_KEYS) > 0

if not AUTH_ENABLED:
    print(
        "[kaliper] WARNING: No KALIPER_API_KEY_* env vars found. "
        "Running WITHOUT authentication. "
        "Set KALIPER_API_KEY_<tenant_id>=<secret> in .env to enable auth."
    )


def _resolve_tenant_from_request() -> tuple[str | None, str | None]:
    if not AUTH_ENABLED:
        return None, None

    api_key = (
        request.headers.get("X-Kaliper-API-Key")
        or request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    )
    if not api_key:
        return None, "Missing API key. Provide X-Kaliper-API-Key header."

    tenant_id = API_KEYS.get(api_key)
    if not tenant_id:
        return None, "Invalid API key."

    return tenant_id, None


def require_auth(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        tenant_id, err = _resolve_tenant_from_request()
        if err:
            return jsonify({"success": False, "message": err}), 401
        kwargs["authenticated_tenant_id"] = tenant_id
        return f(*args, **kwargs)
    return wrapper


BASE_DIR = Path(__file__).resolve().parent.parent

DEFAULT_CONTEXT = RuntimeContext(
    tenant_id=os.getenv("KALIPER_TENANT_ID", "tenant_demo"),
    workspace_id=os.getenv("KALIPER_WORKSPACE_ID", "ecommerce_workspace"),
    environment=os.getenv("KALIPER_ENVIRONMENT", "production"),
    source=os.getenv("KALIPER_SOURCE", "webhook"),
    tenant_name=os.getenv("KALIPER_TENANT_NAME", "Demo Tenant"),
    workspace_name=os.getenv("KALIPER_WORKSPACE_NAME", "Demo Workspace"),
)

initialize_db()
from core.connector_registry import initialize_connector_tables
initialize_connector_tables()

WORKSPACE_RUNTIMES: dict[str, dict[str, Any]] = {}


def _workspace_key(tenant_id: str, workspace_id: str, environment: str) -> str:
    return f"{tenant_id}:{workspace_id}:{environment}"


def _ensure_workspace_records(tenant_id: str, workspace_id: str, tenant_name: str = "Demo Tenant") -> None:
    ensure_tenant(tenant_id, tenant_name)
    workspace_name = workspace_id.replace("_", " ").title()
    ensure_workspace(workspace_id, tenant_id, workspace_name)


def _persist_run_counters(runtime: dict[str, Any]) -> None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE runs SET event_count = ?, issue_count = ? WHERE id = ?",
        (runtime["event_count"], runtime["issue_count"], runtime["run_id"]),
    )
    conn.commit()
    conn.close()


def _finalize_runtime(runtime: dict[str, Any]) -> None:
    finish_run(
        run_id=runtime["run_id"],
        event_count=runtime["event_count"],
        issue_count=runtime["issue_count"],
    )


def _load_or_refresh_runtime(
    tenant_id: str,
    workspace_id: str,
    environment: str,
) -> dict[str, Any]:
    key = _workspace_key(tenant_id, workspace_id, environment)

    bundle = get_active_plan_bundle(tenant_id=tenant_id, workspace_id=workspace_id)
    if bundle is None:
        raise ValueError(
            f"No active tracking plan for tenant='{tenant_id}', workspace='{workspace_id}'."
        )

    active_plan = bundle["active_plan"]
    specs = bundle["specs"]
    profile = bundle["profile"]
    active_plan_version_id = active_plan["plan_version_id"]

    cached = WORKSPACE_RUNTIMES.get(key)
    if cached and cached["active_plan_version_id"] == active_plan_version_id:
        return cached

    if cached:
        _finalize_runtime(cached)

    runtime = {
        "tenant_id": tenant_id,
        "workspace_id": workspace_id,
        "environment": environment,
        "profile": profile,
        "specs": specs,
        "state": StreamState(),
        "run_id": start_run(
            tenant_id=tenant_id,
            workspace_id=workspace_id,
            environment=environment,
            mode=profile.domain,
            domain=profile.domain,
            confidence=profile.confidence,
            plan_version_id=active_plan_version_id,
        ),
        "event_count": 0,
        "issue_count": 0,
        "active_plan_version_id": active_plan_version_id,
    }

    WORKSPACE_RUNTIMES[key] = runtime
    return runtime


def _parse_envelope(data: dict[str, Any]) -> tuple[RuntimeContext, dict[str, Any], str]:
    if "event" in data and isinstance(data["event"], dict):
        tenant_id = data.get("tenant_id", DEFAULT_CONTEXT.tenant_id)
        workspace_id = data.get("workspace_id", DEFAULT_CONTEXT.workspace_id)
        environment = data.get("environment", DEFAULT_CONTEXT.environment)
        source = data.get("source", DEFAULT_CONTEXT.source)
        event_data = data["event"]
    else:
        tenant_id = DEFAULT_CONTEXT.tenant_id
        workspace_id = DEFAULT_CONTEXT.workspace_id
        environment = DEFAULT_CONTEXT.environment
        source = DEFAULT_CONTEXT.source
        event_data = data

    context = RuntimeContext(
        tenant_id=tenant_id,
        workspace_id=workspace_id,
        environment=environment,
        source=source,
        tenant_name=DEFAULT_CONTEXT.tenant_name,
        workspace_name=workspace_id.replace("_", " ").title(),
    )
    return context, event_data, source


def payload_to_event(data: dict[str, Any]) -> IncomingEvent:
    required_fields = ["name", "timestamp", "properties", "event_id"]
    missing = [f for f in required_fields if f not in data]
    if missing:
        raise ValueError(f"Missing required fields: {missing}")
    if "user_id" not in data and "anonymous_id" not in data:
        raise ValueError("Either user_id or anonymous_id must be present.")
    if not isinstance(data["properties"], dict):
        raise ValueError("properties must be an object/dict.")

    return IncomingEvent(
        name=data["name"],
        user_id=data.get("user_id"),
        anonymous_id=data.get("anonymous_id"),
        timestamp=data["timestamp"],
        properties=data["properties"],
        event_id=data["event_id"],
    )


def _require_workspace_params() -> tuple[str, str] | tuple[None, None]:
    tenant_id = request.args.get("tenant_id")
    workspace_id = request.args.get("workspace_id")
    return tenant_id, workspace_id


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "success": True,
        "message": "Kaliper ingestion server is running.",
        "default_context": {
            "tenant_id": DEFAULT_CONTEXT.tenant_id,
            "workspace_id": DEFAULT_CONTEXT.workspace_id,
            "environment": DEFAULT_CONTEXT.environment,
        },
        "known_runtimes": list(WORKSPACE_RUNTIMES.keys()),
    })


@app.route("/upload-plan", methods=["POST"])
def upload_plan():
    tenant_id, workspace_id = _require_workspace_params()
    if not tenant_id:
        tenant_id = DEFAULT_CONTEXT.tenant_id
    if not workspace_id:
        workspace_id = DEFAULT_CONTEXT.workspace_id

    _ensure_workspace_records(tenant_id, workspace_id, DEFAULT_CONTEXT.tenant_name)

    activate_str = request.args.get("activate", "true").lower()
    make_active = activate_str == "true"

    from core.plan_registry import register_plan_from_file, register_plan_from_dict
    import tempfile
    import os

    file = request.files.get("file")
    data = request.get_json(silent=True)

    try:
        if file:
            filename = file.filename.lower()
            if filename.endswith(".json"):
                plan_data = json.load(file)
                registered = register_plan_from_dict(
                    tenant_id=tenant_id,
                    workspace_id=workspace_id,
                    plan_data=plan_data,
                    make_active=make_active,
                )
            elif filename.endswith(".xlsx") or filename.endswith(".xlsm"):
                fd, temp_path = tempfile.mkstemp(suffix=".xlsx")
                try:
                    with os.fdopen(fd, "wb") as f:
                        file.save(f)
                    registered = register_plan_from_file(
                        tenant_id=tenant_id,
                        workspace_id=workspace_id,
                        plan_path=temp_path,
                        make_active=make_active,
                    )
                finally:
                    os.remove(temp_path)
            else:
                return jsonify({"error": "Unsupported file type. Must be .json or .xlsx"}), 400
        elif data:
            registered = register_plan_from_dict(
                tenant_id=tenant_id,
                workspace_id=workspace_id,
                plan_data=data,
                make_active=make_active,
            )
        else:
            return jsonify({"error": "No JSON payload or file provided"}), 400

        # Clear any cached runtime for this workspace so the new plan takes effect
        if make_active:
            prefix = f"{tenant_id}:{workspace_id}:"
            keys_to_delete = [k for k in WORKSPACE_RUNTIMES if k.startswith(prefix)]
            for k in keys_to_delete:
                _finalize_runtime(WORKSPACE_RUNTIMES[k])
                del WORKSPACE_RUNTIMES[k]

        return jsonify({
            "success": True,
            "plan_version_id": registered.plan_version_id,
            "domain": registered.domain
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/plan-activate", methods=["POST"])
@require_auth
def activate_plan(authenticated_tenant_id: str | None = None):
    data = request.get_json(silent=True) or {}
    tenant_id = data.get("tenant_id", DEFAULT_CONTEXT.tenant_id)
    workspace_id = data.get("workspace_id", DEFAULT_CONTEXT.workspace_id)
    version_id = data.get("version_id")

    if not version_id:
        return jsonify({"error": "version_id is required"}), 400

    if authenticated_tenant_id and tenant_id != authenticated_tenant_id:
        return jsonify({"success": False, "message": "Tenant mismatch."}), 403

    conn = get_connection()
    cur = conn.cursor()
    try:
        row = cur.execute(
            "SELECT id FROM plan_versions WHERE id = ? AND tenant_id = ? AND workspace_id = ?",
            (version_id, tenant_id, workspace_id)
        ).fetchone()
        if not row:
            conn.close()
            return jsonify({"error": "Plan version not found."}), 404
    finally:
        conn.close()

    from core.plan_registry import set_active_plan_version
    set_active_plan_version(
        tenant_id=tenant_id,
        workspace_id=workspace_id,
        plan_version_id=int(version_id),
    )

    # Clear runtime cache so it reloads on next event
    prefix = f"{tenant_id}:{workspace_id}:"
    keys_to_delete = [k for k in WORKSPACE_RUNTIMES if k.startswith(prefix)]
    for k in keys_to_delete:
        _finalize_runtime(WORKSPACE_RUNTIMES[k])
        del WORKSPACE_RUNTIMES[k]

    return jsonify({"success": True})


@app.route("/ingest", methods=["POST"])
@require_auth
def ingest(authenticated_tenant_id: str | None = None):
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"success": False, "message": "No JSON payload received."}), 400

    try:
        context, event_data, source = _parse_envelope(data)

        if authenticated_tenant_id and context.tenant_id != authenticated_tenant_id:
            return jsonify({
                "success": False,
                "message": (
                    f"Tenant mismatch: API key belongs to '{authenticated_tenant_id}' "
                    f"but envelope claims '{context.tenant_id}'."
                ),
            }), 403

        _ensure_workspace_records(
            tenant_id=context.tenant_id,
            workspace_id=context.workspace_id,
            tenant_name=context.tenant_name,
        )
        runtime = _load_or_refresh_runtime(
            tenant_id=context.tenant_id,
            workspace_id=context.workspace_id,
            environment=context.environment,
        )
        event = payload_to_event(event_data)
    except ValueError as e:
        return jsonify({"success": False, "message": str(e)}), 400

    store_event(
        tenant_id=context.tenant_id,
        workspace_id=context.workspace_id,
        run_id=runtime["run_id"],
        source=source,
        name=event.name,
        user_id=event.user_id,
        anonymous_id=event.anonymous_id,
        timestamp=event.timestamp,
        event_id=event.event_id,
        properties=event.properties,
        raw_json=data,
    )

    issues = detect_issues(
        [event],
        runtime["specs"],
        enabled_checks=runtime["profile"].enabled_checks,
        state=runtime["state"],
        funnel_map=runtime["profile"].funnel_map,
        property_map=runtime["profile"].property_map,
    )

    for issue in issues:
        store_issue(
            tenant_id=context.tenant_id,
            workspace_id=context.workspace_id,
            run_id=runtime["run_id"],
            event_id=issue.event_id,
            event_name=issue.event_name,
            issue_type=issue.issue_type,
            severity=issue.severity,
            message=issue.message,
        )

    runtime["event_count"] += 1
    runtime["issue_count"] += len(issues)
    _persist_run_counters(runtime)

    if issues:
        print(f"  [{event.name}] {len(issues)} issue(s):")
        for issue in issues:
            print(f"    - [{issue.severity}] {issue.issue_type}: {issue.message}")
    else:
        print(f"  [{event.name}] ok")

    return jsonify({
        "success": True,
        "tenant_id": context.tenant_id,
        "workspace_id": context.workspace_id,
        "environment": context.environment,
        "domain": runtime["profile"].domain,
        "run_id": runtime["run_id"],
        "issues_detected": len(issues),
        "issues": [
            {
                "issue_type": i.issue_type,
                "severity": i.severity,
                "message": i.message,
                "event_id": i.event_id,
                "event_name": i.event_name,
            }
            for i in issues
        ],
    })


@app.route("/ingest-batch", methods=["POST"])
@require_auth
def ingest_batch(authenticated_tenant_id: str | None = None):
    data_list = request.get_json(silent=True)
    if not isinstance(data_list, list):
        return jsonify({"success": False, "message": "Expected a JSON list."}), 400

    if not data_list:
        return jsonify({"success": True, "issues_detected": 0})

    events_to_store = []
    issues_to_store = []
    
    total_issues = 0
    # Group by workspace_id to minimize loading runtimes
    runtime = None
    last_workspace_id = None

    for data in data_list:
        try:
            context, event_data, source = _parse_envelope(data)

            if authenticated_tenant_id and context.tenant_id != authenticated_tenant_id:
                # Just skip invalid ones in a batch
                continue

            if last_workspace_id != context.workspace_id:
                _ensure_workspace_records(
                    tenant_id=context.tenant_id,
                    workspace_id=context.workspace_id,
                    tenant_name=context.tenant_name,
                )
                runtime = _load_or_refresh_runtime(
                    tenant_id=context.tenant_id,
                    workspace_id=context.workspace_id,
                    environment=context.environment,
                )
                last_workspace_id = context.workspace_id
                
            event = payload_to_event(event_data)
        except ValueError:
            continue

        events_to_store.append({
            "tenant_id": context.tenant_id,
            "workspace_id": context.workspace_id,
            "run_id": runtime["run_id"],
            "source": source,
            "name": event.name,
            "user_id": event.user_id,
            "anonymous_id": event.anonymous_id,
            "timestamp": event.timestamp,
            "event_id": event.event_id,
            "properties": event.properties,
            "raw_json": data,
        })

        issues = detect_issues(
            [event],
            runtime["specs"],
            enabled_checks=runtime["profile"].enabled_checks,
            state=runtime["state"],
            funnel_map=runtime["profile"].funnel_map,
            property_map=runtime["profile"].property_map,
        )
        
        for issue in issues:
            issues_to_store.append({
                "tenant_id": context.tenant_id,
                "workspace_id": context.workspace_id,
                "run_id": runtime["run_id"],
                "event_id": issue.event_id,
                "event_name": issue.event_name,
                "issue_type": issue.issue_type,
                "severity": issue.severity,
                "message": issue.message,
            })
            
        runtime["event_count"] += 1
        runtime["issue_count"] += len(issues)
        total_issues += len(issues)

    if events_to_store:
        store_events_bulk(events_to_store)
    if issues_to_store:
        store_issues_bulk(issues_to_store)
        
    if runtime:
        _persist_run_counters(runtime)

    return jsonify({
        "success": True,
        "events_processed": len(events_to_store),
        "issues_detected": total_issues,
    })


# ---------------------------------------------------------------------------
# Events -- reads from DB
# ---------------------------------------------------------------------------

@app.route("/events", methods=["GET"])
def get_events():
    tenant_id = request.args.get("tenant_id")
    workspace_id = request.args.get("workspace_id")
    limit = int(request.args.get("limit", 100))
    run_id = request.args.get("run_id")

    conn = get_connection()
    cur = conn.cursor()

    params: list[Any] = []
    where_clauses = []

    if tenant_id:
        where_clauses.append("tenant_id = ?")
        params.append(tenant_id)
    if workspace_id:
        where_clauses.append("workspace_id = ?")
        params.append(workspace_id)
    if run_id:
        where_clauses.append("run_id = ?")
        params.append(int(run_id))

    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    params.append(limit)

    rows = cur.execute(
        f"""
        SELECT id, tenant_id, workspace_id, run_id, source, name,
               user_id, anonymous_id, timestamp, event_id,
               properties_json, created_at
        FROM events
        {where}
        ORDER BY id DESC
        LIMIT ?
        """,
        params,
    ).fetchall()
    conn.close()

    return jsonify([
        {
            "id": r["id"],
            "tenant_id": r["tenant_id"],
            "workspace_id": r["workspace_id"],
            "run_id": r["run_id"],
            "source": r["source"],
            "name": r["name"],
            "user_id": r["user_id"],
            "anonymous_id": r["anonymous_id"],
            "timestamp": r["timestamp"],
            "event_id": r["event_id"],
            "properties": json.loads(r["properties_json"] or "{}"),
            "created_at": r["created_at"],
        }
        for r in rows
    ])


# ---------------------------------------------------------------------------
# Issues -- reads from DB
# ---------------------------------------------------------------------------

@app.route("/issues", methods=["GET"])
def get_issues():
    tenant_id = request.args.get("tenant_id")
    workspace_id = request.args.get("workspace_id")
    limit = int(request.args.get("limit", 200))
    run_id = request.args.get("run_id")
    severity = request.args.get("severity")

    conn = get_connection()
    cur = conn.cursor()

    params: list[Any] = []
    where_clauses = []

    if tenant_id:
        where_clauses.append("tenant_id = ?")
        params.append(tenant_id)
    if workspace_id:
        where_clauses.append("workspace_id = ?")
        params.append(workspace_id)
    if run_id:
        where_clauses.append("run_id = ?")
        params.append(int(run_id))
    if severity:
        where_clauses.append("severity = ?")
        params.append(severity)

    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    params.append(limit)

    rows = cur.execute(
        f"""
        SELECT id, tenant_id, workspace_id, run_id, event_id, event_name,
               issue_type, severity, message, created_at
        FROM issues
        {where}
        ORDER BY id DESC
        LIMIT ?
        """,
        params,
    ).fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])


# ---------------------------------------------------------------------------
# Scorecard
# ---------------------------------------------------------------------------

@app.route("/scorecard", methods=["GET"])
def scorecard():
    tenant_id, workspace_id = _require_workspace_params()
    if not tenant_id or not workspace_id:
        return jsonify({"error": "tenant_id and workspace_id are required"}), 400

    run_id = request.args.get("run_id")
    try:
        sc = build_scorecard(
            tenant_id=tenant_id,
            workspace_id=workspace_id,
            run_id=int(run_id) if run_id else None,
        )
        return jsonify(sc.to_dict())
    except ValueError:
        # No runs yet — return an empty scorecard instead of a 500
        return jsonify({
            "tenant_id": tenant_id,
            "workspace_id": workspace_id,
            "run_id": None,
            "mode": None,
            "domain": None,
            "confidence": 0,
            "plan_version_id": None,
            "label": None,
            "started_at": None,
            "ended_at": None,
            "event_count": 0,
            "issue_count": 0,
            "health_score": 0,
            "grade": "-",
            "severity_counts": {},
            "issue_type_counts": {},
            "affected_events": [],
            "top_issue_types": [],
            "notes": ["No ingestion runs yet. Upload a tracking plan, register your connector, and start ingestion."],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/summary", methods=["GET"])
def summary():
    tenant_id, workspace_id = _require_workspace_params()
    if not tenant_id or not workspace_id:
        return jsonify({"error": "tenant_id and workspace_id are required"}), 400

    try:
        sc = build_scorecard(tenant_id=tenant_id, workspace_id=workspace_id)
        return jsonify({
            "tenant_id": sc.tenant_id,
            "workspace_id": sc.workspace_id,
            "run_id": sc.run_id,
            "health_score": sc.health_score,
            "grade": sc.grade,
            "event_count": sc.event_count,
            "issue_count": sc.issue_count,
            "severity_counts": sc.severity_counts,
            "top_issue_types": sc.top_issue_types,
        })
    except ValueError:
        return jsonify({
            "tenant_id": tenant_id,
            "workspace_id": workspace_id,
            "run_id": None,
            "health_score": 0,
            "grade": "-",
            "event_count": 0,
            "issue_count": 0,
            "severity_counts": {},
            "top_issue_types": [],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Reporting endpoints
# ---------------------------------------------------------------------------

@app.route("/runs", methods=["GET"])
def run_history():
    tenant_id, workspace_id = _require_workspace_params()
    if not tenant_id or not workspace_id:
        return jsonify({"error": "tenant_id and workspace_id are required"}), 400

    limit = int(request.args.get("limit", 20))
    try:
        return jsonify(get_run_history(tenant_id, workspace_id, limit=limit))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/trends", methods=["GET"])
def trends():
    tenant_id, workspace_id = _require_workspace_params()
    if not tenant_id or not workspace_id:
        return jsonify({"error": "tenant_id and workspace_id are required"}), 400

    limit = int(request.args.get("limit", 30))
    try:
        return jsonify(get_trend_report(tenant_id, workspace_id, limit=limit))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/event-health", methods=["GET"])
def event_health():
    tenant_id, workspace_id = _require_workspace_params()
    if not tenant_id or not workspace_id:
        return jsonify({"error": "tenant_id and workspace_id are required"}), 400

    run_id = request.args.get("run_id")
    try:
        return jsonify(get_event_health(
            tenant_id, workspace_id,
            run_id=int(run_id) if run_id else None,
        ))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/coverage", methods=["GET"])
def coverage():
    tenant_id, workspace_id = _require_workspace_params()
    if not tenant_id or not workspace_id:
        return jsonify({"error": "tenant_id and workspace_id are required"}), 400

    run_id = request.args.get("run_id")
    try:
        return jsonify(get_tracking_coverage(
            tenant_id, workspace_id,
            run_id=int(run_id) if run_id else None,
        ))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/issue-breakdown", methods=["GET"])
def issue_breakdown():
    tenant_id, workspace_id = _require_workspace_params()
    if not tenant_id or not workspace_id:
        return jsonify({"error": "tenant_id and workspace_id are required"}), 400

    limit_runs = int(request.args.get("limit_runs", 10))
    try:
        return jsonify(get_issue_type_breakdown(tenant_id, workspace_id, limit_runs=limit_runs))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Connector Configuration and UI Ingestion
# ---------------------------------------------------------------------------

@app.route("/config/connector", methods=["GET", "POST"])
@require_auth
def config_connector(authenticated_tenant_id: str | None = None):
    if request.method == "GET":
        tenant_id, workspace_id = _require_workspace_params()
        if not tenant_id: tenant_id = DEFAULT_CONTEXT.tenant_id
        if not workspace_id: workspace_id = DEFAULT_CONTEXT.workspace_id
        from core.connector_registry import list_connectors
        try:
            connectors = list_connectors(tenant_id=tenant_id, workspace_id=workspace_id)
            amplitude = next((c for c in connectors if c["connector_type"] == "amplitude" and c["is_active"]), None)
            if amplitude:
                return jsonify({"has_connector": True, "connector_name": amplitude.get("connector_name")})
            return jsonify({"has_connector": False}), 404
        except Exception as e:
            return jsonify({"has_connector": False, "error": str(e)}), 404
    tenant_id, workspace_id = _require_workspace_params()
    if not tenant_id: tenant_id = DEFAULT_CONTEXT.tenant_id
    if not workspace_id: workspace_id = DEFAULT_CONTEXT.workspace_id

    data = request.get_json(silent=True) or {}
    api_key = data.get("api_key")
    secret_key = data.get("secret_key")

    if not api_key or not secret_key:
        return jsonify({"error": "api_key and secret_key are required"}), 400

    from core.connector_registry import register_connector, list_connectors, deactivate_connector
    try:
        # Deactivate any existing active connector for this workspace before
        # registering a new one — prevents the same accumulation bug that
        # caused connectors 1-3 to pile up.
        existing = list_connectors(tenant_id=tenant_id, workspace_id=workspace_id)
        for c in existing:
            if c["connector_type"] == "amplitude" and c["is_active"]:
                deactivate_connector(c["id"])
        register_connector(
            tenant_id=tenant_id,
            workspace_id=workspace_id,
            connector_name=f"Amplitude - {workspace_id}",
            connector_type="amplitude",
            credentials={
                "api_key": api_key,
                "secret_key": secret_key,
            },
            config={
                "poll_interval_minutes": 1440,
                "environment": "production",
            },
            is_active=True,
        )
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/trigger-ingestion", methods=["POST"])
@require_auth
def trigger_ingestion(authenticated_tenant_id: str | None = None):
    tenant_id, workspace_id = _require_workspace_params()
    if not workspace_id: workspace_id = DEFAULT_CONTEXT.workspace_id

    # Proactively start a run if none is active in memory so the
    # dashboard can show the run ID while we wait for the first event.
    environment = DEFAULT_CONTEXT.environment
    key = _workspace_key(tenant_id, workspace_id, environment)
    if key not in WORKSPACE_RUNTIMES:
        try:
            _load_or_refresh_runtime(tenant_id, workspace_id, environment)
        except ValueError:
            pass # No active plan, ignore

    import threading
    def bg_task():
        try:
            import sys
            from pathlib import Path
            project_root = str(Path(__file__).resolve().parent.parent)
            if project_root not in sys.path:
                sys.path.insert(0, project_root)
                
            from core.connector_registry import list_connectors
            
            connectors = list_connectors(tenant_id, workspace_id)
            active_connectors = [c for c in connectors if c["is_active"]]
            
            if not active_connectors:
                print(f"No active connectors for workspace {workspace_id}")
                from core.progress_store import update_progress
                update_progress(workspace_id, "done", 1, 1)
                return
                
            for c in active_connectors:
                if c["connector_type"] == "amplitude":
                    from connectors.amplitude_importer import run_incremental_import
                    run_incremental_import(c)
                elif c["connector_type"] == "mixpanel":
                    from connectors.mixpanel_importer import run_incremental_import
                    run_incremental_import(c)
        except Exception as e:
            print(f"Background ingestion failed: {e}")
            from core.progress_store import update_progress
            update_progress(workspace_id, "error", 0, 0)

    t = threading.Thread(target=bg_task)
    t.start()
    return jsonify({"success": True, "message": "Ingestion started in background"})


@app.route("/ingestion-progress", methods=["GET"])
def ingestion_progress():
    tenant_id, workspace_id = _require_workspace_params()
    if not workspace_id: workspace_id = DEFAULT_CONTEXT.workspace_id
    
    from core.progress_store import get_progress
    progress = get_progress(workspace_id)
    return jsonify(progress)


# ---------------------------------------------------------------------------
# Workspaces list (for dashboard workspace switcher)
# ---------------------------------------------------------------------------

@app.route("/workspaces", methods=["GET"])
def list_workspaces():
    tenant_id = request.args.get("tenant_id", "tenant_demo")
    conn = get_connection()
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT workspace_id, workspace_name FROM workspaces WHERE tenant_id = ? ORDER BY workspace_id",
        (tenant_id,),
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


# ---------------------------------------------------------------------------
# Plan Explorer endpoints
# ---------------------------------------------------------------------------

def _serialize_spec(spec) -> dict[str, Any]:
    """Convert a TrackingEventSpec to a JSON-serialisable dict."""
    return {
        "name": spec.name,
        "required_properties": spec.required_properties,
        "property_types": spec.property_types,
        "identity_required": spec.identity_required,
        "allowed_previous_events": spec.allowed_previous_events,
    }


@app.route("/plan-versions", methods=["GET"])
def plan_versions():
    """
    GET /plan-versions?tenant_id=...&workspace_id=...&limit=20

    Returns a list of registered plan versions for the workspace,
    newest first, including which one is currently active.
    """
    tenant_id, workspace_id = _require_workspace_params()
    if not tenant_id or not workspace_id:
        return jsonify({"error": "tenant_id and workspace_id are required"}), 400

    limit = int(request.args.get("limit", 20))

    try:
        versions = list_plan_versions(
            tenant_id=tenant_id,
            workspace_id=workspace_id,
            limit=limit,
        )

        active = get_active_plan_version(tenant_id=tenant_id, workspace_id=workspace_id)
        active_id = active["plan_version_id"] if active else None

        result = []
        for v in versions:
            result.append({
                "id": v["id"],
                "version": v["version"],
                "domain": v["domain"],
                "plan_path": v["plan_path"],
                "created_at": v["created_at"],
                "is_active": v["id"] == active_id,
                "event_count": len(json.loads(v["plan_json"]).get("events", [])),
            })

        return jsonify({
            "versions": result,
            "active_plan_version_id": active_id,
            "total": len(result),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/plan-version/<int:version_id>", methods=["GET"])
def plan_version_detail(version_id: int):
    """
    GET /plan-version/<id>

    Returns the full event list for a specific plan version.
    """
    try:
        pv = get_plan_version_by_id(version_id)
        if not pv:
            return jsonify({"error": f"Plan version {version_id} not found"}), 404

        plan_data = json.loads(pv["plan_json"])
        events = plan_data.get("events", [])

        return jsonify({
            "id": pv["id"],
            "version": pv["version"],
            "domain": pv["domain"],
            "plan_path": pv["plan_path"],
            "created_at": pv["created_at"],
            "tenant_id": pv["tenant_id"],
            "workspace_id": pv["workspace_id"],
            "events": events,
            "event_count": len(events),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/plan-active", methods=["GET"])
def plan_active():
    """
    GET /plan-active?tenant_id=...&workspace_id=...

    Returns the active plan with full event list.
    """
    tenant_id, workspace_id = _require_workspace_params()
    if not tenant_id or not workspace_id:
        return jsonify({"error": "tenant_id and workspace_id are required"}), 400

    try:
        bundle = get_active_plan_bundle(tenant_id=tenant_id, workspace_id=workspace_id)
        if not bundle:
            return jsonify({"error": f"No active plan for workspace '{workspace_id}'"}), 404

        active_plan = bundle["active_plan"]
        specs = bundle["specs"]
        profile = bundle["profile"]

        return jsonify({
            "plan_version_id": active_plan["plan_version_id"],
            "version": active_plan["version"],
            "domain": active_plan["domain"],
            "plan_path": active_plan["plan_path"],
            "created_at": active_plan["created_at"],
            "confidence": profile.confidence,
            "enabled_checks": list(profile.enabled_checks),
            "events": [_serialize_spec(s) for s in specs],
            "event_count": len(specs),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/plan-diff", methods=["GET"])
def plan_diff():
    """
    GET /plan-diff?tenant_id=...&workspace_id=...&from_version=<id>&to_version=<id>

    Diffs two plan versions and returns added/removed/modified events,
    breaking changes, warnings, and a compatibility score.
    """
    tenant_id, workspace_id = _require_workspace_params()
    if not tenant_id or not workspace_id:
        return jsonify({"error": "tenant_id and workspace_id are required"}), 400

    from_version_id = request.args.get("from_version")
    to_version_id = request.args.get("to_version")

    if not from_version_id or not to_version_id:
        return jsonify({"error": "from_version and to_version are required"}), 400

    try:
        from_pv = get_plan_version_by_id(int(from_version_id))
        to_pv = get_plan_version_by_id(int(to_version_id))

        if not from_pv:
            return jsonify({"error": f"Plan version {from_version_id} not found"}), 404
        if not to_pv:
            return jsonify({"error": f"Plan version {to_version_id} not found"}), 404

        from core.plan_registry import _deserialize_specs
        old_specs = normalize_specs(_deserialize_specs(from_pv["plan_json"]))
        new_specs = normalize_specs(_deserialize_specs(to_pv["plan_json"]))

        diff = compare_tracking_plans(old_specs, new_specs)

        return jsonify({
            "from_version": {
                "id": from_pv["id"],
                "version": from_pv["version"],
                "created_at": from_pv["created_at"],
                "event_count": len(old_specs),
            },
            "to_version": {
                "id": to_pv["id"],
                "version": to_pv["version"],
                "created_at": to_pv["created_at"],
                "event_count": len(new_specs),
            },
            "added_events": diff.added_events,
            "removed_events": diff.removed_events,
            "breaking_changes": [
                {"change_type": c.change_type, "severity": c.severity, "message": c.message}
                for c in diff.breaking_changes
            ],
            "warnings": [
                {"change_type": c.change_type, "severity": c.severity, "message": c.message}
                for c in diff.warnings
            ],
            "modified_events": [
                {"change_type": c.change_type, "severity": c.severity, "message": c.message}
                for c in diff.modified_events
            ],
            "compatibility_score": diff.compatibility_score,
            "has_breaking_changes": len(diff.breaking_changes) > 0,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Clear / reset run
# ---------------------------------------------------------------------------

@app.route("/clear", methods=["POST"])
@require_auth  # Bug 8 fix: was missing auth entirely -- any caller could reset any workspace's
               # run state by POSTing arbitrary tenant_id/workspace_id in the body.
def clear_events(authenticated_tenant_id: str | None = None):
    data = request.get_json(silent=True) or {}
    tenant_id = data.get("tenant_id", DEFAULT_CONTEXT.tenant_id)
    workspace_id = data.get("workspace_id", DEFAULT_CONTEXT.workspace_id)
    environment = data.get("environment", DEFAULT_CONTEXT.environment)

    # Bug 8 fix: when auth is enabled, enforce that the caller can only clear
    # their own tenant's workspace — not any arbitrary tenant_id in the body.
    if authenticated_tenant_id and tenant_id != authenticated_tenant_id:
        return jsonify({
            "success": False,
            "message": (
                f"Tenant mismatch: API key belongs to '{authenticated_tenant_id}' "
                f"but request claims '{tenant_id}'."
            ),
        }), 403

    key = _workspace_key(tenant_id, workspace_id, environment)
    label = data.get("label")

    if key in WORKSPACE_RUNTIMES:
        _finalize_runtime(WORKSPACE_RUNTIMES[key])
        del WORKSPACE_RUNTIMES[key]

    new_run_id = None
    try:
        # Proactively load runtime which automatically calls start_run() 
        # so the dashboard sees the new run ID immediately, even before
        # events arrive.
        runtime = _load_or_refresh_runtime(tenant_id, workspace_id, environment)
        new_run_id = runtime["run_id"]
        
        if label:
            conn = get_connection()
            conn.execute("UPDATE runs SET label = ? WHERE id = ?", (label, new_run_id))
            conn.commit()
            conn.close()
    except ValueError:
        pass # No active plan

    return jsonify({
        "success": True,
        "message": "Run finalized and new run started." if new_run_id else "No active plan.",
        "tenant_id": tenant_id,
        "workspace_id": workspace_id,
        "environment": environment,
        "new_run_id": new_run_id,
    })


@app.route("/clear-history", methods=["POST"])
@require_auth
def clear_history(authenticated_tenant_id: str | None = None):
    data = request.get_json(silent=True) or {}
    tenant_id = data.get("tenant_id", DEFAULT_CONTEXT.tenant_id)
    workspace_id = data.get("workspace_id", DEFAULT_CONTEXT.workspace_id)
    environment = data.get("environment", DEFAULT_CONTEXT.environment)

    if authenticated_tenant_id and tenant_id != authenticated_tenant_id:
        return jsonify({
            "success": False,
            "message": "Tenant mismatch."
        }), 403

    from core.storage import clear_workspace_history
    clear_workspace_history(tenant_id, workspace_id)

    key = _workspace_key(tenant_id, workspace_id, environment)
    runtime = WORKSPACE_RUNTIMES.get(key)
    if runtime:
        _finalize_runtime(runtime)
        del WORKSPACE_RUNTIMES[key]

    return jsonify({"success": True})


# ---------------------------------------------------------------------------
# Webhook receiver routes (Segment, RudderStack, generic)
# ---------------------------------------------------------------------------
from connectors.webhook_receiver import register_webhook_routes
register_webhook_routes(app)


# ---------------------------------------------------------------------------
# Auth: tenant login for the dashboard
# ---------------------------------------------------------------------------

@app.route("/auth/login", methods=["POST"])
def auth_login():
    """
    POST /auth/login
    Body: {"tenant_id": "...", "api_key": "..."}

    Validates an API key against the tenant registry and returns the
    tenant_id + list of workspaces on success.
    Used by the dashboard login screen to bootstrap the session.

    When AUTH_ENABLED=False (no KALIPER_API_KEY_* env vars), any tenant_id
    is accepted without a key — development mode only.
    """
    data = request.get_json(silent=True) or {}
    tenant_id = (data.get("tenant_id") or "").strip()
    api_key   = (data.get("api_key")   or "").strip()

    if not tenant_id:
        return jsonify({"success": False, "message": "tenant_id is required."}), 400

    if AUTH_ENABLED:
        # Validate the key and confirm it belongs to the claimed tenant
        resolved = API_KEYS.get(api_key)
        if not resolved:
            return jsonify({"success": False, "message": "Invalid API key."}), 401
        if resolved != tenant_id:
            return jsonify({"success": False, "message": "API key does not belong to this tenant."}), 403
    # else: dev mode — accept any tenant_id without a key

    # Return workspaces so the dashboard can populate the switcher immediately
    conn = get_connection()
    cur  = conn.cursor()
    rows = cur.execute(
        "SELECT workspace_id, workspace_name FROM workspaces WHERE tenant_id = ? ORDER BY workspace_id",
        (tenant_id,),
    ).fetchall()
    conn.close()

    workspaces = [dict(r) for r in rows]

    return jsonify({
        "success":    True,
        "tenant_id":  tenant_id,
        "auth_mode":  "authenticated" if AUTH_ENABLED else "dev",
        "workspaces": workspaces,
    })


@app.route("/auth/me", methods=["GET"])
def auth_me():
    """
    GET /auth/me
    Header: X-Kaliper-API-Key: <key>  (or Authorization: Bearer <key>)

    Returns the tenant identity for a given API key.
    Used by the dashboard on page load to restore a session.
    """
    if not AUTH_ENABLED:
        # Dev mode — read tenant_id from query param, no key needed
        tenant_id = request.args.get("tenant_id", DEFAULT_CONTEXT.tenant_id)
        conn = get_connection()
        cur  = conn.cursor()
        rows = cur.execute(
            "SELECT workspace_id, workspace_name FROM workspaces WHERE tenant_id = ? ORDER BY workspace_id",
            (tenant_id,),
        ).fetchall()
        conn.close()
        return jsonify({
            "success":    True,
            "tenant_id":  tenant_id,
            "auth_mode":  "dev",
            "workspaces": [dict(r) for r in rows],
        })

    api_key = (
        request.headers.get("X-Kaliper-API-Key")
        or request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    )
    tenant_id = API_KEYS.get(api_key)
    if not tenant_id:
        return jsonify({"success": False, "message": "Invalid or missing API key."}), 401

    conn = get_connection()
    cur  = conn.cursor()
    rows = cur.execute(
        "SELECT workspace_id, workspace_name FROM workspaces WHERE tenant_id = ? ORDER BY workspace_id",
        (tenant_id,),
    ).fetchall()
    conn.close()

    return jsonify({
        "success":    True,
        "tenant_id":  tenant_id,
        "auth_mode":  "authenticated",
        "workspaces": [dict(r) for r in rows],
    })


if __name__ == "__main__":
    app.run(debug=True, port=5000)
