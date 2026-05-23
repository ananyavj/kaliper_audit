#scorer.py
from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional

from core.storage import get_connection


SEVERITY_WEIGHTS = {
    "critical": 18,
    "high": 10,
    "medium": 5,
    "low": 2,
}

GRADE_BANDS = [
    (90, "A"),
    (80, "B"),
    (70, "C"),
    (60, "D"),
    (0, "F"),
]


@dataclass
class RunScorecard:
    tenant_id: str
    workspace_id: str
    run_id: int
    mode: str
    domain: str
    confidence: float
    plan_version_id: Optional[int]
    started_at: str
    ended_at: Optional[str]
    event_count: int
    issue_count: int
    health_score: int
    grade: str
    severity_counts: dict[str, int] = field(default_factory=dict)
    issue_type_counts: dict[str, int] = field(default_factory=dict)
    affected_events: list[str] = field(default_factory=list)
    top_issue_types: list[dict[str, Any]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _get_latest_run_row(
    tenant_id: str,
    workspace_id: str,
    run_id: Optional[int] = None,
) -> Optional[dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor()

    if run_id is None:
        row = cur.execute(
            """
            SELECT *
            FROM runs
            WHERE tenant_id = ? AND workspace_id = ? AND event_count > 0
            ORDER BY id DESC
            LIMIT 1
            """,
            (tenant_id, workspace_id),
        ).fetchone()
    else:
        row = cur.execute(
            """
            SELECT *
            FROM runs
            WHERE tenant_id = ? AND workspace_id = ? AND id = ?
            """,
            (tenant_id, workspace_id, run_id),
        ).fetchone()

    conn.close()
    return dict(row) if row else None


def _get_issues_for_run(run_id: int) -> list[dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor()

    rows = cur.execute(
        """
        SELECT *
        FROM issues
        WHERE run_id = ?
        ORDER BY id ASC
        """,
        (run_id,),
    ).fetchall()

    conn.close()
    return [dict(row) for row in rows]


def _score_from_issues(event_count: int, issues: list[dict[str, Any]]) -> int:
    score = 100

    for issue in issues:
        severity = (issue.get("severity") or "").lower()
        score -= SEVERITY_WEIGHTS.get(severity, 4)

    if event_count > 0:
        density = len(issues) / event_count
        score -= min(20, int(round(density * 10)))

    if any((issue.get("severity") or "").lower() == "critical" for issue in issues):
        score -= 5

    return max(0, min(100, int(round(score))))


def _grade_from_score(score: int) -> str:
    for threshold, grade in GRADE_BANDS:
        if score >= threshold:
            return grade
    return "F"


def build_scorecard(
    tenant_id: str,
    workspace_id: str,
    run_id: Optional[int] = None,
) -> RunScorecard:
    run = _get_latest_run_row(tenant_id=tenant_id, workspace_id=workspace_id, run_id=run_id)
    if not run:
        raise ValueError(
            f"No run found for tenant_id='{tenant_id}', workspace_id='{workspace_id}'."
        )

    issues = _get_issues_for_run(int(run["id"]))

    severity_counts = Counter((issue.get("severity") or "unknown").lower() for issue in issues)
    issue_type_counts = Counter((issue.get("issue_type") or "unknown") for issue in issues)
    affected_events = sorted(
        {
            issue.get("event_name")
            for issue in issues
            if issue.get("event_name")
        }
    )

    health_score = _score_from_issues(int(run["event_count"] or 0), issues)
    grade = _grade_from_score(health_score)

    top_issue_types = [
        {"issue_type": issue_type, "count": count}
        for issue_type, count in issue_type_counts.most_common(5)
    ]

    notes: list[str] = []
    if run["issue_count"] == 0:
        notes.append("No issues detected in this run.")
    else:
        if severity_counts.get("critical", 0) > 0:
            notes.append("Critical issues were detected.")
        if severity_counts.get("high", 0) > 0:
            notes.append("High-severity issues require attention.")
        if int(run["event_count"] or 0) > 0 and len(issues) / int(run["event_count"]) > 0.5:
            notes.append("Issue density is high relative to total event volume.")

    return RunScorecard(
        tenant_id=run["tenant_id"],
        workspace_id=run["workspace_id"],
        run_id=int(run["id"]),
        mode=run["mode"],
        domain=run["domain"],
        confidence=float(run["confidence"]),
        plan_version_id=run["plan_version_id"],
        started_at=run["started_at"],
        ended_at=run["ended_at"],
        event_count=int(run["event_count"] or 0),
        issue_count=int(run["issue_count"] or 0),
        health_score=health_score,
        grade=grade,
        severity_counts=dict(severity_counts),
        issue_type_counts=dict(issue_type_counts),
        affected_events=affected_events,
        top_issue_types=top_issue_types,
        notes=notes,
    )


def render_scorecard(scorecard: RunScorecard) -> str:
    lines: list[str] = []

    lines.append("KALIPER QA SCORECARD")
    lines.append("=" * 22)
    lines.append(f"Tenant: {scorecard.tenant_id}")
    lines.append(f"Workspace: {scorecard.workspace_id}")
    lines.append(f"Run ID: {scorecard.run_id}")
    lines.append(f"Mode: {scorecard.mode}")
    lines.append(f"Domain: {scorecard.domain}")
    lines.append(f"Confidence: {scorecard.confidence:.2f}")
    lines.append(f"Plan version: {scorecard.plan_version_id}")
    lines.append(f"Started at: {scorecard.started_at}")
    lines.append(f"Ended at: {scorecard.ended_at or 'still open'}")
    lines.append("")
    lines.append(f"Events processed: {scorecard.event_count}")
    lines.append(f"Issues detected: {scorecard.issue_count}")
    lines.append(f"Health score: {scorecard.health_score}/100")
    lines.append(f"Grade: {scorecard.grade}")
    lines.append("")

    lines.append("Severity breakdown:")
    if scorecard.severity_counts:
        for severity in ["critical", "high", "medium", "low", "unknown"]:
            count = scorecard.severity_counts.get(severity)
            if count:
                lines.append(f"  - {severity}: {count}")
    else:
        lines.append("  - none")

    lines.append("")
    lines.append("Top issue types:")
    if scorecard.top_issue_types:
        for item in scorecard.top_issue_types:
            lines.append(f"  - {item['issue_type']}: {item['count']}")
    else:
        lines.append("  - none")

    lines.append("")
    lines.append("Affected events:")
    if scorecard.affected_events:
        for event_name in scorecard.affected_events:
            lines.append(f"  - {event_name}")
    else:
        lines.append("  - none")

    lines.append("")
    lines.append("Notes:")
    if scorecard.notes:
        for note in scorecard.notes:
            lines.append(f"  - {note}")
    else:
        lines.append("  - none")

    return "\n".join(lines)


def export_scorecard_json(scorecard: RunScorecard, output_path: str | Path) -> None:
    path = Path(output_path)
    path.write_text(json.dumps(scorecard.to_dict(), indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a Kaliper QA scorecard from SQLite.")
    parser.add_argument("--tenant", default="tenant_demo", help="Tenant ID")
    parser.add_argument("--workspace", default="ecommerce_workspace", help="Workspace ID")
    parser.add_argument("--run-id", type=int, default=None, help="Specific run ID")
    parser.add_argument("--json-out", default=None, help="Optional path to export JSON")
    args = parser.parse_args()

    scorecard = build_scorecard(
        tenant_id=args.tenant,
        workspace_id=args.workspace,
        run_id=args.run_id,
    )

    print(render_scorecard(scorecard))

    if args.json_out:
        export_scorecard_json(scorecard, args.json_out)
        print(f"\nSaved JSON scorecard to {args.json_out}")


if __name__ == "__main__":
    main()