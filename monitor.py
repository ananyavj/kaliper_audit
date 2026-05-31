# monitor.py
# ============================================================
# DEPRECATED — Early prototype for a continuous monitoring loop.
# Not imported anywhere, not wired into the dashboard.
# Kept for reference only.
# ============================================================
"""
Kaliper Continuous Monitor  —  Req 10
---------------------------------------
Always-on, time-driven monitoring loop.  Unlike the ingestion server
(which is request-driven — it only acts when events arrive) and the
scheduler (which pulls from connectors), the monitor periodically wakes
up and actively evaluates workspace health based on the data that is
already in SQLite.

What it does on each tick
--------------------------
For every active workspace:
  1. Reads the most recent run from the DB.
  2. Computes its scorecard via build_scorecard().
  3. Compares health_score against the configured WARNING_THRESHOLD and
     CRITICAL_THRESHOLD.
  4. Logs a structured alert if thresholds are breached.
  5. (Hook) Calls on_alert() — override this or replace with your own
     notification handler (Slack, PagerDuty, email, etc.).

Run it
------
    # Default: check every 60 seconds
    python monitor.py

    # Check every 5 minutes
    python monitor.py --interval 300

    # Run a single check pass and exit (useful for cron / testing)
    python monitor.py --once

    # Dry-run: compute but do not fire alerts
    python monitor.py --dry-run

Architecture
------------
Each workspace runs in its own daemon thread so a slow DB query for one
workspace never delays another.  The main thread just sleeps and watches
for Ctrl+C.

Thresholds (overridable via env vars)
--------------------------------------
KALIPER_MONITOR_WARNING_THRESHOLD   default 70   health_score < this → WARNING
KALIPER_MONITOR_CRITICAL_THRESHOLD  default 50   health_score < this → CRITICAL
KALIPER_MONITOR_INTERVAL_SECONDS    default 60   seconds between checks
"""

from __future__ import annotations

import argparse
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv

from core.connector_registry import list_all_workspaces, initialize_connector_tables
from core.storage import initialize_db, auto_close_idle_runs
from scorer import build_scorecard, RunScorecard

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

WARNING_THRESHOLD: int = int(os.getenv("KALIPER_MONITOR_WARNING_THRESHOLD", "70"))
CRITICAL_THRESHOLD: int = int(os.getenv("KALIPER_MONITOR_CRITICAL_THRESHOLD", "50"))
DEFAULT_INTERVAL_SECONDS: int = int(os.getenv("KALIPER_MONITOR_INTERVAL_SECONDS", "60"))
SLEEP_TICK_SECONDS: int = 1   # how often each thread checks its timer


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _log(workspace_key: str, msg: str) -> None:
    print(f"[{_utc_now()}] [monitor:{workspace_key}] {msg}")


def _log_main(msg: str) -> None:
    print(f"[{_utc_now()}] [monitor] {msg}")


# ---------------------------------------------------------------------------
# Alert hook
# ---------------------------------------------------------------------------

def on_alert(
    level: str,           # "WARNING" or "CRITICAL"
    scorecard: RunScorecard,
    workspace_key: str,
    dry_run: bool = False,
) -> None:
    """
    Called whenever a health threshold is breached.

    Replace or extend this function to send Slack messages, PagerDuty
    incidents, emails, or write to an alerts table.  The default
    implementation just prints a structured alert line.

    Parameters
    ----------
    level         : "WARNING" or "CRITICAL"
    scorecard     : full RunScorecard for the run that tripped the threshold
    workspace_key : "<tenant_id>:<workspace_id>" string
    dry_run       : if True, log but do not perform any external side effects
    """
    tag = "[DRY-RUN] " if dry_run else ""
    _log(
        workspace_key,
        f"{tag}ALERT [{level}] "
        f"health={scorecard.health_score}/100 grade={scorecard.grade} "
        f"issues={scorecard.issue_count} events={scorecard.event_count} "
        f"run_id={scorecard.run_id} "
        f"top_issues={[x['issue_type'] for x in scorecard.top_issue_types[:3]]}",
    )


# ---------------------------------------------------------------------------
# Single workspace check
# ---------------------------------------------------------------------------

def _check_workspace(
    tenant_id: str,
    workspace_id: str,
    dry_run: bool = False,
) -> None:
    """
    Pull the latest scorecard for a workspace and fire an alert if needed.
    Swallows all exceptions — one bad workspace never kills the monitor.
    """
    workspace_key = f"{tenant_id}:{workspace_id}"
    try:
        sc = build_scorecard(tenant_id=tenant_id, workspace_id=workspace_id)
    except ValueError:
        # No completed run yet — nothing to monitor
        return
    except Exception as exc:
        _log(workspace_key, f"scorecard error: {exc}")
        return

    if sc.health_score < CRITICAL_THRESHOLD:
        on_alert("CRITICAL", sc, workspace_key, dry_run=dry_run)
    elif sc.health_score < WARNING_THRESHOLD:
        on_alert("WARNING", sc, workspace_key, dry_run=dry_run)
    else:
        _log(workspace_key, f"OK health={sc.health_score}/100 grade={sc.grade} run_id={sc.run_id}")


# ---------------------------------------------------------------------------
# Per-workspace monitor thread
# ---------------------------------------------------------------------------

class WorkspaceMonitorThread(threading.Thread):
    """
    Daemon thread that checks one workspace on a fixed time interval.
    Uses a sleep-loop (not a fixed-schedule timer) so a slow check
    resets the clock from the end of the check, not the start.
    """

    def __init__(
        self,
        tenant_id: str,
        workspace_id: str,
        interval_seconds: int,
        stop_event: threading.Event,
        run_once: bool = False,
        dry_run: bool = False,
    ) -> None:
        super().__init__(daemon=True)
        self.tenant_id = tenant_id
        self.workspace_id = workspace_id
        self.interval_seconds = max(10, interval_seconds)  # hard floor: 10 s
        self._requested_interval_seconds = self.interval_seconds  # store for respawn
        self.stop_event = stop_event
        self.run_once = run_once
        self.dry_run = dry_run
        self.name = f"monitor-{tenant_id}-{workspace_id}"

    def run(self) -> None:
        workspace_key = f"{self.tenant_id}:{self.workspace_id}"
        _log(workspace_key, f"started — interval={self.interval_seconds}s")

        last_check_at: float = 0.0  # 0 = never checked; triggers immediately on first tick

        while not self.stop_event.is_set():
            now = time.monotonic()
            if now - last_check_at >= self.interval_seconds:
                _check_workspace(self.tenant_id, self.workspace_id, dry_run=self.dry_run)
                last_check_at = time.monotonic()
                if self.run_once:
                    break
            self.stop_event.wait(timeout=SLEEP_TICK_SECONDS)


# ---------------------------------------------------------------------------
# Workspace discovery
# ---------------------------------------------------------------------------

def _discover_workspaces() -> list[dict[str, Any]]:
    """
    Return all workspace records from the connector registry.
    Falls back to an empty list if the DB has no workspaces yet.
    """
    try:
        return list_all_workspaces()
    except Exception as exc:
        _log_main(f"workspace discovery error: {exc}")
        return []


# ---------------------------------------------------------------------------
# Main monitor loop
# ---------------------------------------------------------------------------

def run_monitor(
    interval_seconds: int = DEFAULT_INTERVAL_SECONDS,
    run_once: bool = False,
    dry_run: bool = False,
) -> None:
    initialize_db()
    initialize_connector_tables()

    workspaces = _discover_workspaces()
    if not workspaces:
        _log_main(
            "No workspaces found in the database.  "
            "Run setup_workspace_plans.py first, then restart the monitor."
        )
        return

    _log_main(
        f"{'[dry-run] ' if dry_run else ''}"
        f"Starting continuous monitor for {len(workspaces)} workspace(s). "
        f"Interval={interval_seconds}s  "
        f"WARNING<{WARNING_THRESHOLD}  CRITICAL<{CRITICAL_THRESHOLD}.  "
        + ("Running once then exiting." if run_once else "Press Ctrl+C to stop.")
    )

    stop_event = threading.Event()
    threads: list[WorkspaceMonitorThread] = []

    for ws in workspaces:
        t = WorkspaceMonitorThread(
            tenant_id=ws["tenant_id"],
            workspace_id=ws["workspace_id"],
            interval_seconds=interval_seconds,
            stop_event=stop_event,
            run_once=run_once,
            dry_run=dry_run,
        )
        threads.append(t)
        t.start()

    if run_once:
        for t in threads:
            t.join()
        _log_main("Single-pass check complete. Exiting.")
        return

    # Continuous mode — block until Ctrl+C
    try:
        while True:
            time.sleep(5)
            
            # Sweep for idle runs and close them automatically
            closed_count = auto_close_idle_runs(idle_minutes=15)
            if closed_count > 0:
                _log_main(f"Auto-closed {closed_count} idle run(s).")
                
            for i, t in enumerate(threads):
                if not t.is_alive():
                    _log_main(
                        f"WARNING: monitor thread '{t.name}' died unexpectedly. "
                        "Respawning it to resume coverage for that workspace."
                    )
                    new_thread = WorkspaceMonitorThread(
                        tenant_id=t.tenant_id,
                        workspace_id=t.workspace_id,
                        interval_seconds=t._requested_interval_seconds,  # already clamped; apply floor once
                        stop_event=t.stop_event,
                        run_once=t.run_once,
                        dry_run=t.dry_run,
                    )
                    threads[i] = new_thread
                    new_thread.start()
    except KeyboardInterrupt:
        _log_main("Ctrl+C received — stopping monitor threads...")
        stop_event.set()
        for t in threads:
            t.join(timeout=10)
            if t.is_alive():
                _log_main(f"Thread '{t.name}' did not stop in time — abandoned.")
        _log_main("Monitor stopped.")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Kaliper continuous health monitor")
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL_SECONDS,
        metavar="SECONDS",
        help=f"Seconds between health checks per workspace (default: {DEFAULT_INTERVAL_SECONDS}).",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one check pass across all workspaces then exit.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute health scores and log alerts but do not perform external notifications.",
    )
    args = parser.parse_args()

    run_monitor(
        interval_seconds=args.interval,
        run_once=args.once,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
