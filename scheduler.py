# scheduler.py
"""
Kaliper Scheduler
-----------------
Continuously syncs all active connectors at their configured polling intervals.

How it works
------------
1. On startup, read all active connectors from SQLite.
2. For each connector, determine its poll_interval_minutes from config
   (default: FALLBACK_POLL_INTERVAL_MINUTES).
3. Each connector runs in its own daemon thread with an independent timer,
   so a slow or erroring connector never delays another.
4. Errors inside a connector's sync are caught and logged — the scheduler
   itself never crashes due to a single connector failure.
5. On Ctrl+C, all threads are signalled to stop and the process exits cleanly.

Connector config field
----------------------
Set poll_interval_minutes in the connector's config_json to control frequency:

    {"environment": "production", "poll_interval_minutes": 15}

Supported values (minutes): 5, 15, 60, 1440 (daily)
If the field is missing, FALLBACK_POLL_INTERVAL_MINUTES is used.

Usage
-----
    # Run continuously (normal mode):
    python scheduler.py

    # Run every connector once immediately, then exit (useful for testing):
    python scheduler.py --once

    # Override poll interval for all connectors for this run:
    python scheduler.py --interval 5

    # Dry-run: print what would be synced without actually syncing:
    python scheduler.py --dry-run
"""

from __future__ import annotations

import argparse
import threading
import time
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv

from core.connector_registry import (
    initialize_connector_tables,
    list_all_workspaces,
    list_connectors,
    update_connector_sync_time,
)

load_dotenv()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FALLBACK_POLL_INTERVAL_MINUTES = 60   # used when poll_interval_minutes is absent from config
MIN_POLL_INTERVAL_MINUTES = 5         # guard against configs that are too aggressive
SYNC_DISPATCH_SLEEP_SECONDS = 1       # how often the per-connector timer thread wakes to check


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

def _utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _log(connector_id: int, connector_name: str, msg: str) -> None:
    print(f"[{_utc_now_str()}] [connector {connector_id} | {connector_name}] {msg}")


def _log_scheduler(msg: str) -> None:
    print(f"[{_utc_now_str()}] [scheduler] {msg}")


# ---------------------------------------------------------------------------
# Connector dispatch: route to the correct importer by connector_type
# ---------------------------------------------------------------------------

def _sync_connector(connector: dict[str, Any], dry_run: bool = False) -> None:
    """
    Run one incremental import cycle for a single connector.
    Dispatches to the correct importer module based on connector_type.
    All exceptions are caught here — the caller (timer thread) handles logging.
    """
    connector_type = connector["connector_type"].lower()

    if dry_run:
        _log(connector["id"], connector["connector_name"],
             f"[dry-run] would sync {connector_type} connector")
        return

    if connector_type == "amplitude":
        from connectors.amplitude_importer import run_incremental_import
        run_incremental_import(connector)

    elif connector_type == "mixpanel":
        from connectors.mixpanel_importer import run_incremental_import
        run_incremental_import(connector)

    else:
        raise ValueError(
            f"Unknown connector_type '{connector_type}'. "
            "Supported types: amplitude, mixpanel."
        )

    # Record sync time regardless of whether any events were found
    update_connector_sync_time(connector["id"])


# ---------------------------------------------------------------------------
# Per-connector timer thread
# ---------------------------------------------------------------------------

class ConnectorThread(threading.Thread):
    """
    Daemon thread that syncs one connector on its configured interval.

    The thread wakes every SYNC_DISPATCH_SLEEP_SECONDS and checks whether
    enough time has elapsed since the last sync. This approach (sleep-loop
    rather than a fixed schedule) means:
      - the interval resets from the END of the last sync, not the start,
        so a slow sync never causes an immediate follow-up
      - stopping is clean: the stop_event is checked on each wake
    """

    def __init__(
        self,
        connector: dict[str, Any],
        interval_minutes: int,
        stop_event: threading.Event,
        run_once: bool = False,
        dry_run: bool = False,
    ) -> None:
        super().__init__(daemon=True)
        self.connector = connector
        self._requested_interval_minutes = max(MIN_POLL_INTERVAL_MINUTES, interval_minutes)
        self.interval_seconds = self._requested_interval_minutes * 60
        self.stop_event = stop_event
        self.run_once = run_once
        self.dry_run = dry_run
        self.name = f"connector-{connector['id']}-{connector['connector_type']}"

    def run(self) -> None:
        connector_id = self.connector["id"]
        connector_name = self.connector["connector_name"]

        _log(connector_id, connector_name,
             f"started — interval={self.interval_seconds // 60}min "
             f"type={self.connector['connector_type']}")

        last_sync_at: float = -self.interval_seconds   # force immediate first sync

        while not self.stop_event.is_set():
            now = time.monotonic()
            elapsed = now - last_sync_at

            if elapsed >= self.interval_seconds:
                _log(connector_id, connector_name, "sync starting")
                try:
                    _sync_connector(self.connector, dry_run=self.dry_run)
                    _log(connector_id, connector_name, "sync complete")
                except Exception as exc:
                    # Log the error but keep running — one bad sync doesn't kill the thread
                    _log(connector_id, connector_name, f"sync ERROR: {exc}")

                last_sync_at = time.monotonic()

                if self.run_once:
                    break

            self.stop_event.wait(timeout=SYNC_DISPATCH_SLEEP_SECONDS)


# ---------------------------------------------------------------------------
# Connector discovery
# ---------------------------------------------------------------------------

def _discover_active_connectors() -> list[dict[str, Any]]:
    """
    Return all active connectors across all tenants and workspaces.
    Uses list_all_workspaces() + list_connectors() so no workspace is hardcoded.
    """
    active: list[dict[str, Any]] = []
    for ws in list_all_workspaces():
        connectors = list_connectors(
            tenant_id=ws["tenant_id"],
            workspace_id=ws["workspace_id"],
        )
        for c in connectors:
            if c["is_active"]:
                active.append(c)
    return active


def _poll_interval_for(connector: dict[str, Any], override: int | None) -> int:
    """
    Determine the polling interval (minutes) for a connector.
    Priority: CLI override > connector config > FALLBACK_POLL_INTERVAL_MINUTES.
    """
    if override is not None:
        return override
    config_interval = connector.get("config", {}).get("poll_interval_minutes")
    if isinstance(config_interval, (int, float)) and config_interval > 0:
        return int(config_interval)
    return FALLBACK_POLL_INTERVAL_MINUTES


# ---------------------------------------------------------------------------
# Main scheduler loop
# ---------------------------------------------------------------------------

def run_scheduler(
    run_once: bool = False,
    interval_override: int | None = None,
    dry_run: bool = False,
) -> None:
    initialize_connector_tables()

    connectors = _discover_active_connectors()
    if not connectors:
        _log_scheduler(
            "No active connectors found. Register a connector with "
            "register_amplitude_connector.py or register_mixpanel_connector.py, "
            "then restart the scheduler."
        )
        return

    _log_scheduler(
        f"{'[dry-run] ' if dry_run else ''}"
        f"Starting with {len(connectors)} active connector(s). "
        f"{'Running each once then exiting.' if run_once else 'Running continuously. Press Ctrl+C to stop.'}"
    )

    stop_event = threading.Event()
    threads: list[ConnectorThread] = []

    for connector in connectors:
        interval = _poll_interval_for(connector, interval_override)
        thread = ConnectorThread(
            connector=connector,
            interval_minutes=interval,
            stop_event=stop_event,
            run_once=run_once,
            dry_run=dry_run,
        )
        threads.append(thread)
        thread.start()

    if run_once:
        # Wait for all threads to finish naturally
        for thread in threads:
            thread.join()
        _log_scheduler("All connectors synced once. Exiting.")
        return

    # Continuous mode — block until Ctrl+C
    try:
        while True:
            time.sleep(5)

            # Check if any thread has died unexpectedly (shouldn't happen,
            # but good to surface it early rather than silently missing syncs)
            for i, thread in enumerate(threads):
                if not thread.is_alive():
                    _log_scheduler(
                        f"WARNING: thread '{thread.name}' is no longer alive. "
                        "Respawning it to resume coverage for that connector."
                    )
                    # Use _requested_interval_minutes (the clamped value stored on
                    # the original thread) so the floor is applied exactly once.
                    new_thread = ConnectorThread(
                        connector=thread.connector,
                        interval_minutes=thread._requested_interval_minutes,
                        stop_event=thread.stop_event,
                        run_once=thread.run_once,
                        dry_run=thread.dry_run,
                    )
                    threads[i] = new_thread
                    new_thread.start()

    except KeyboardInterrupt:
        _log_scheduler("Ctrl+C received — stopping all connector threads...")
        stop_event.set()

        # Give threads up to 10s to finish their current sync cleanly
        for thread in threads:
            thread.join(timeout=10)
            if thread.is_alive():
                _log_scheduler(
                    f"Thread '{thread.name}' did not stop within 10s — "
                    "it will be abandoned (daemon thread, exits with process)."
                )

        _log_scheduler("Scheduler stopped.")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Kaliper continuous connector scheduler"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run every active connector once immediately, then exit. Useful for testing.",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=None,
        metavar="MINUTES",
        help=(
            "Override poll interval (minutes) for all connectors in this run. "
            "Does not modify stored connector config. "
            f"Minimum: {MIN_POLL_INTERVAL_MINUTES} minutes."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover connectors and print what would be synced, but do not actually sync.",
    )
    args = parser.parse_args()

    run_scheduler(
        run_once=args.once,
        interval_override=args.interval,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
