#core/reporting.py
"""
reporting.py
------------
Cross-run reporting, trend analysis, per-event health, and tracking coverage.

All functions query SQLite directly and return plain dicts so they can be
serialised to JSON and served by the ingestion server.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from typing import Any, Optional

from core.storage import get_connection


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rows(query: str, params: tuple = ()) -> list[dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor()
    rows = cur.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _one(query: str, params: tuple = ()) -> Optional[dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor()
    row = cur.execute(query, params).fetchone()
    conn.close()
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# 1. Run history
# ---------------------------------------------------------------------------

def get_run_history(
    tenant_id: str,
    workspace_id: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """
    Return the N most recent runs for a workspace, newest first.
    Includes a health_score computed from issue density so the UI
    can show a trend line without calling the full scorer per run.
    """
    runs = _rows(
        """
        SELECT id, mode, domain, confidence, plan_version_id,
               started_at, ended_at, event_count, issue_count
        FROM runs
        WHERE tenant_id = ? AND workspace_id = ? AND event_count > 0
        ORDER BY id DESC
        LIMIT ?
        """,
        (tenant_id, workspace_id, limit),
    )

    # Quick health score: 100 - weighted penalty - density penalty
    SEVERITY_W = {"critical": 18, "high": 10, "medium": 5, "low": 2}

    run_ids = [r["id"] for r in runs]
    if not run_ids:
        return []

    # Fetch severity breakdown for all runs in one query
    placeholders = ",".join("?" * len(run_ids))
    sev_rows = _rows(
        f"""
        SELECT run_id, severity, COUNT(*) as cnt
        FROM issues
        WHERE run_id IN ({placeholders})
        GROUP BY run_id, severity
        """,
        tuple(run_ids),
    )

    sev_by_run: dict[int, dict[str, int]] = defaultdict(dict)
    for row in sev_rows:
        sev_by_run[row["run_id"]][row["severity"].lower()] = row["cnt"]

    for run in runs:
        run_id = run["id"]
        sevs = sev_by_run.get(run_id, {})
        event_count = run["event_count"] or 0
        issue_count = run["issue_count"] or 0

        score = 100
        for sev, cnt in sevs.items():
            score -= SEVERITY_W.get(sev, 4) * cnt
        if event_count > 0:
            density = issue_count / event_count
            score -= min(20, int(density * 10))
        if sevs.get("critical", 0) > 0:
            score -= 5
        run["health_score"] = max(0, min(100, int(score)))
        run["severity_counts"] = sevs

    return runs


# ---------------------------------------------------------------------------
# 2. Cross-run trends
# ---------------------------------------------------------------------------

@dataclass
class TrendPoint:
    run_id: int
    started_at: str
    event_count: int
    issue_count: int
    health_score: int
    issue_rate: float   # issues / events


@dataclass
class TrendReport:
    tenant_id: str
    workspace_id: str
    run_count: int
    points: list[TrendPoint] = field(default_factory=list)
    avg_health_score: float = 0.0
    avg_issue_rate: float = 0.0
    trend_direction: str = "stable"   # "improving", "degrading", "stable"
    delta_health: float = 0.0         # latest health - oldest health in window


def get_trend_report(
    tenant_id: str,
    workspace_id: str,
    limit: int = 30,
) -> dict[str, Any]:
    runs = get_run_history(tenant_id, workspace_id, limit=limit)
    runs = list(reversed(runs))  # oldest first for trend calculation

    points = []
    for run in runs:
        event_count = run["event_count"] or 0
        issue_count = run["issue_count"] or 0
        issue_rate = round(issue_count / event_count, 4) if event_count > 0 else 0.0
        points.append(
            TrendPoint(
                run_id=run["id"],
                started_at=run["started_at"],
                event_count=event_count,
                issue_count=issue_count,
                health_score=run["health_score"],
                issue_rate=issue_rate,
            )
        )

    if not points:
        return asdict(TrendReport(
            tenant_id=tenant_id,
            workspace_id=workspace_id,
            run_count=0,
        ))

    avg_health = round(sum(p.health_score for p in points) / len(points), 1)
    avg_issue_rate = round(sum(p.issue_rate for p in points) / len(points), 4)

    delta = 0.0
    direction = "stable"
    if len(points) >= 2:
        delta = round(points[-1].health_score - points[0].health_score, 1)
        if delta >= 5:
            direction = "improving"
        elif delta <= -5:
            direction = "degrading"

    report = TrendReport(
        tenant_id=tenant_id,
        workspace_id=workspace_id,
        run_count=len(points),
        points=points,
        avg_health_score=avg_health,
        avg_issue_rate=avg_issue_rate,
        trend_direction=direction,
        delta_health=delta,
    )
    return asdict(report)


# ---------------------------------------------------------------------------
# 3. Per-event health
# ---------------------------------------------------------------------------

def get_event_health(
    tenant_id: str,
    workspace_id: str,
    run_id: Optional[int] = None,
) -> list[dict[str, Any]]:
    """
    Return one row per distinct event name with aggregated issue stats.
    If run_id is provided, scoped to that run; otherwise across all runs.
    """
    base_where = "tenant_id = ? AND workspace_id = ?"
    params: list[Any] = [tenant_id, workspace_id]

    if run_id is not None:
        base_where += " AND run_id = ?"
        params.append(run_id)

    # Total event occurrences per name
    event_counts = _rows(
        f"""
        SELECT name, COUNT(*) as total
        FROM events
        WHERE {base_where}
        GROUP BY name
        ORDER BY total DESC
        """,
        tuple(params),
    )

    # Issue counts per event name and severity
    issue_rows = _rows(
        f"""
        SELECT event_name, severity, COUNT(*) as cnt
        FROM issues
        WHERE {base_where}
        GROUP BY event_name, severity
        """,
        tuple(params),
    )

    # Build lookup: event_name -> {severity: count}
    issue_map: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in issue_rows:
        issue_map[row["event_name"]][row["severity"].lower()] += row["cnt"]

    SEVERITY_W = {"critical": 18, "high": 10, "medium": 5, "low": 2}

    result = []
    for row in event_counts:
        name = row["name"]
        total = row["total"]
        sevs = dict(issue_map.get(name, {}))
        total_issues = sum(sevs.values())

        score = 100
        for sev, cnt in sevs.items():
            score -= SEVERITY_W.get(sev, 4) * cnt
        if total > 0:
            score -= min(20, int((total_issues / total) * 10))
        if sevs.get("critical", 0) > 0:
            score -= 5
        score = max(0, min(100, int(score)))

        result.append({
            "event_name": name,
            "total_seen": total,
            "total_issues": total_issues,
            "severity_counts": sevs,
            "health_score": score,
            "issue_rate": round(total_issues / total, 4) if total > 0 else 0.0,
        })

    # Sort: most issues first, then alphabetical
    result.sort(key=lambda r: (-r["total_issues"], r["event_name"]))
    return result


# ---------------------------------------------------------------------------
# 4. Tracking coverage
# ---------------------------------------------------------------------------

def get_tracking_coverage(
    tenant_id: str,
    workspace_id: str,
    run_id: Optional[int] = None,
) -> dict[str, Any]:
    """
    Compare the registered tracking plan against events actually seen.
    Returns which plan events have been seen, which are missing, and
    which events arrived that are NOT in the plan (unknown events).
    """
    from core.plan_registry import get_active_plan_bundle

    bundle = get_active_plan_bundle(tenant_id=tenant_id, workspace_id=workspace_id)
    if not bundle:
        return {
            "error": f"No active tracking plan for workspace '{workspace_id}'.",
            "plan_events": [],
            "seen_events": [],
            "covered": [],
            "missing": [],
            "unknown": [],
            "coverage_pct": 0.0,
        }

    plan_event_names = {spec.name for spec in bundle["specs"]}

    base_where = "tenant_id = ? AND workspace_id = ?"
    params: list[Any] = [tenant_id, workspace_id]
    if run_id is not None:
        base_where += " AND run_id = ?"
        params.append(run_id)

    seen_rows = _rows(
        f"SELECT DISTINCT name FROM events WHERE {base_where}",
        tuple(params),
    )
    seen_event_names = {r["name"] for r in seen_rows}

    covered = sorted(plan_event_names & seen_event_names)
    missing = sorted(plan_event_names - seen_event_names)
    unknown = sorted(seen_event_names - plan_event_names)

    coverage_pct = (
        round(len(covered) / len(plan_event_names) * 100, 1)
        if plan_event_names else 0.0
    )

    return {
        "plan_event_count": len(plan_event_names),
        "seen_event_count": len(seen_event_names),
        "covered": covered,
        "missing": missing,
        "unknown": unknown,
        "coverage_pct": coverage_pct,
        "plan_version_id": bundle["active_plan"]["plan_version_id"],
        "domain": bundle["profile"].domain,
    }


# ---------------------------------------------------------------------------
# 5. Issue type breakdown across runs
# ---------------------------------------------------------------------------

def get_issue_type_breakdown(
    tenant_id: str,
    workspace_id: str,
    limit_runs: int = 10,
) -> list[dict[str, Any]]:
    """
    Aggregate issue type counts across the N most recent runs.
    Useful for identifying which rule is firing most.
    """
    run_rows = _rows(
        """
        SELECT id FROM runs
        WHERE tenant_id = ? AND workspace_id = ? AND event_count > 0
        ORDER BY id DESC
        LIMIT ?
        """,
        (tenant_id, workspace_id, limit_runs),
    )
    if not run_rows:
        return []

    run_ids = [r["id"] for r in run_rows]
    placeholders = ",".join("?" * len(run_ids))

    rows = _rows(
        f"""
        SELECT issue_type, severity, COUNT(*) as cnt
        FROM issues
        WHERE run_id IN ({placeholders})
        GROUP BY issue_type, severity
        ORDER BY cnt DESC
        """,
        tuple(run_ids),
    )

    return [
        {
            "issue_type": r["issue_type"],
            "severity": r["severity"],
            "count": r["cnt"],
        }
        for r in rows
    ]
