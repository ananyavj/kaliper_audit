"""
mixpanel_importer.py
--------------------
Incremental importer for the Mixpanel Export API.

How it works
------------
1. Read the checkpoint (last successfully imported end-time) from SQLite.
   - First run on a connector: defaults to DEFAULT_LOOKBACK_DAYS ago.
2. Build a fetch window: checkpoint_time -> now, split into daily slices
   (Mixpanel Export API accepts date-level granularity, not hourly).
3. For each daily slice:
   - GET https://data.mixpanel.com/api/2.0/export with from_date/to_date.
   - Parse the newline-delimited JSON response.
   - Normalise each raw Mixpanel event -> Kaliper envelope format.
   - Skip events whose event_id already exists in the DB (idempotent).
   - Forward valid events to the local ingestion server.
4. Only after a slice is fully processed, advance the checkpoint.
   - On any slice error, checkpoint stays at last safe point.

Retry / rate-limit behaviour
-----------------------------
- 429 Too Many Requests: waits Retry-After header (or RATE_LIMIT_WAIT
  seconds as fallback), retries up to MAX_RETRIES times.
- Network errors (timeout, connection reset): exponential backoff
  starting at RETRY_BASE_WAIT seconds, up to MAX_RETRIES attempts.
- Any other non-200: hard failure for that slice.

Mixpanel credentials required in connector config:
  credentials.api_secret  -- Project Secret (for basic auth)
  credentials.project_id  -- (optional) used only for display/logging

Usage
-----
    python -m connectors.mixpanel_importer                   # first active Mixpanel connector
    python -m connectors.mixpanel_importer --connector-id 3
    python -m connectors.mixpanel_importer --connector-id 3 --reset-checkpoint
    python -m connectors.mixpanel_importer --connector-id 3 --lookback-days 7
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from dotenv import load_dotenv

from core.connector_registry import (
    get_connector,
    get_or_init_checkpoint,
    set_checkpoint,
    clear_checkpoint,
    initialize_connector_tables,
)
from core.storage import event_id_exists

load_dotenv()

INGEST_BATCH_URL = "http://127.0.0.1:5000/ingest-batch"

# Mixpanel Export API granularity is per-day
DAY_STEP = timedelta(days=1)

# Default lookback when no checkpoint exists
DEFAULT_LOOKBACK_DAYS = 3

# Retry / rate-limit settings
MAX_RETRIES = 4
RETRY_BASE_WAIT = 2.0      # seconds; doubled on each attempt
RATE_LIMIT_WAIT = 60.0     # fallback if Retry-After header is missing

# Mixpanel Export API base URL
EXPORT_URL = "https://data.mixpanel.com/api/2.0/export"


def _get_latest_open_run_id(tenant_id: str, workspace_id: str) -> int | None:
    from core.storage import get_connection

    conn = get_connection()
    cur = conn.cursor()
    row = cur.execute(
        """
        SELECT id
        FROM runs
        WHERE tenant_id = ? AND workspace_id = ? AND ended_at IS NULL
        ORDER BY id DESC
        LIMIT 1
        """,
        (tenant_id, workspace_id),
    ).fetchone()
    conn.close()
    return int(row["id"]) if row else None


# ---------------------------------------------------------------------------
# Auth -- Mixpanel uses HTTP Basic Auth with api_secret as the username
# ---------------------------------------------------------------------------

def _basic_auth(api_secret: str) -> tuple[str, str]:
    """Return (username, password) tuple for requests.get(auth=...)."""
    return (api_secret, "")


# ---------------------------------------------------------------------------
# Mixpanel Export API fetch (one daily slice) -- with retry + rate-limit
# ---------------------------------------------------------------------------

def _fetch_slice(
    api_secret: str,
    from_date: datetime,
    to_date: datetime,
) -> list[dict[str, Any]]:
    """
    Fetch one daily slice from the Mixpanel Export API.

    Returns a list of raw event dicts, or an empty list for empty responses.
    Retries on 429 and transient network errors.
    Raises RuntimeError on hard failures after retries are exhausted.
    """
    from_str = from_date.strftime("%Y-%m-%d")
    to_str = to_date.strftime("%Y-%m-%d")

    params = {
        "from_date": from_str,
        "to_date": to_str,
    }

    auth = _basic_auth(api_secret)
    wait = RETRY_BASE_WAIT

    for attempt in range(1, MAX_RETRIES + 2):
        try:
            response = requests.get(
                EXPORT_URL,
                params=params,
                auth=auth,
                timeout=120,
                stream=True,
            )
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            if attempt > MAX_RETRIES:
                raise RuntimeError(
                    f"Network error after {MAX_RETRIES} retries "
                    f"[{from_str}->{to_str}]: {exc}"
                ) from exc
            print(f"  [retry {attempt}/{MAX_RETRIES}] network error: {exc} -- waiting {wait:.0f}s")
            time.sleep(wait)
            wait *= 2
            continue

        if response.status_code == 429:
            retry_after = float(response.headers.get("Retry-After", RATE_LIMIT_WAIT))
            if attempt > MAX_RETRIES:
                raise RuntimeError(
                    f"Rate-limited by Mixpanel [{from_str}->{to_str}] "
                    f"after {MAX_RETRIES} retries."
                )
            print(f"  [rate-limit] 429 -- waiting {retry_after:.0f}s (attempt {attempt})")
            time.sleep(retry_after)
            continue

        if response.status_code == 400:
            # Mixpanel returns 400 for future dates or invalid params
            body = response.text[:300]
            raise RuntimeError(
                f"Mixpanel rejected request [{from_str}->{to_str}]: "
                f"400 {body}"
            )

        if response.status_code != 200:
            raise RuntimeError(
                f"Mixpanel export failed [{from_str}->{to_str}]: "
                f"{response.status_code} {response.text[:300]}"
            )

        # Parse newline-delimited JSON
        events: list[dict[str, Any]] = []
        for line in response.iter_lines():
            if isinstance(line, bytes):
                line = line.decode("utf-8", errors="replace")
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                print(f"  [warn] skipping malformed JSON line: {line[:80]}")

        return events

    raise RuntimeError(f"Exhausted retries for slice [{from_str}->{to_str}]")


# ---------------------------------------------------------------------------
# Derive a stable event_id from Mixpanel event fields
# ---------------------------------------------------------------------------

def _derive_event_id(raw: dict[str, Any]) -> str:
    """
    Mixpanel events don't always have a unique ID field.
    We derive a stable deterministic ID from (event_name, distinct_id, time)
    so that re-importing the same event is idempotent.

    If Mixpanel provides $insert_id, we use that directly.
    """
    props = raw.get("properties") or {}

    insert_id = props.get("$insert_id")
    if insert_id:
        return str(insert_id)

    # Build a stable hash from key fields
    event_name = raw.get("event", "")
    distinct_id = str(props.get("distinct_id", ""))
    event_time = str(props.get("time", ""))
    source_str = f"{event_name}|{distinct_id}|{event_time}"
    return "mp-" + hashlib.sha256(source_str.encode()).hexdigest()[:32]


# ---------------------------------------------------------------------------
# Normalise raw Mixpanel event -> Kaliper envelope format
# ---------------------------------------------------------------------------

def _normalize(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Convert a raw Mixpanel export event to the Kaliper IncomingEvent format.

    Mixpanel format:
      {
        "event": "Page Viewed",
        "properties": {
          "distinct_id": "user_123",
          "time": 1700000000,          # Unix timestamp (seconds)
          "$insert_id": "abc123",
          "page": "/home",
          ...
        }
      }
    """
    props = raw.get("properties") or {}
    event_name = raw.get("event") or props.get("event") or ""

    distinct_id = str(props.get("distinct_id", ""))

    # Convert Unix timestamp (seconds) to ISO 8601
    raw_time = props.get("time")
    if raw_time and isinstance(raw_time, (int, float)):
        try:
            ts = datetime.fromtimestamp(float(raw_time), tz=timezone.utc)
            timestamp = ts.isoformat()
        except (OSError, OverflowError, ValueError):
            timestamp = datetime.now(timezone.utc).isoformat()
    else:
        timestamp = datetime.now(timezone.utc).isoformat()

    # Separate user identity from other properties
    user_id = props.get("$user_id") or props.get("user_id")

    # Strip internal Mixpanel $ properties from forwarded event properties
    clean_props = {
        k: v for k, v in props.items()
        if not k.startswith("$") and k not in ("distinct_id", "time", "user_id")
    }

    event_id = _derive_event_id(raw)

    return {
        "name": event_name,
        "user_id": user_id,
        "anonymous_id": distinct_id or None,
        "timestamp": timestamp,
        "event_id": event_id,
        "properties": clean_props,
    }


# ---------------------------------------------------------------------------
# Send one normalised event to the local ingestion server -- with retry
# ---------------------------------------------------------------------------

def _send_batch(connector: dict[str, Any], events: list[dict[str, Any]]) -> None:
    if not events:
        return
        
    envelopes = [
        {
            "tenant_id": connector["tenant_id"],
            "workspace_id": connector["workspace_id"],
            "environment": connector["config"].get("environment", "production"),
            "source": "mixpanel",
            "event": e,
        }
        for e in events
    ]

    wait = RETRY_BASE_WAIT
    for attempt in range(1, MAX_RETRIES + 2):
        try:
            resp = requests.post(INGEST_BATCH_URL, json=envelopes, timeout=60)
            if resp.status_code == 200:
                return
            if 400 <= resp.status_code < 500:
                raise RuntimeError(
                    f"Ingestion rejected batch (client error): "
                    f"{resp.status_code} {resp.text[:200]}"
                )
            if attempt > MAX_RETRIES:
                raise RuntimeError(
                    f"Ingestion server error after {MAX_RETRIES} retries for batch: "
                    f"{resp.status_code} {resp.text[:200]}"
                )
            print(f"  [retry {attempt}/{MAX_RETRIES}] ingest {resp.status_code} -- waiting {wait:.0f}s")
            time.sleep(wait)
            wait *= 2
        except requests.exceptions.ConnectionError as exc:
            if attempt > MAX_RETRIES:
                raise RuntimeError(f"Ingestion server unreachable after {MAX_RETRIES} retries for batch") from exc
            print(f"  [retry {attempt}/{MAX_RETRIES}] ingest connection error -- waiting {wait:.0f}s")
            time.sleep(wait)
            wait *= 2
        except requests.exceptions.Timeout:
            if attempt > MAX_RETRIES:
                raise RuntimeError(f"Ingestion timed out after {MAX_RETRIES} retries for batch")
            print(f"  [retry {attempt}/{MAX_RETRIES}] ingest timeout -- waiting {wait:.0f}s")
            time.sleep(wait)
            wait *= 2


# ---------------------------------------------------------------------------
# Main incremental sync logic
# ---------------------------------------------------------------------------

def run_incremental_import(
    connector: dict[str, Any],
    lookback_days: int | None = None,
) -> None:
    """
    Run one incremental import cycle for the given Mixpanel connector.

    Args:
        connector:     Full connector dict from the registry.
        lookback_days: If set, overrides the stored checkpoint and starts
                       this many days in the past (does not persist the override).
    """
    connector_id = connector["id"]
    tenant_id = connector["tenant_id"]
    workspace_id = connector["workspace_id"]
    api_secret = connector["credentials"].get("api_secret", "")

    if not api_secret:
        raise RuntimeError(
            f"Connector {connector_id} is missing credentials.api_secret. "
            "Set it via register_mixpanel_connector.py."
        )

    # Step 1: determine fetch window
    if lookback_days is not None:
        checkpoint = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        print(f"  Lookback override: starting {lookback_days}d ago ({checkpoint.isoformat()})")
    else:
        # Checkpoint stores hours precision; snap to midnight for daily slicing
        checkpoint = get_or_init_checkpoint(connector_id)

    # Snap to start-of-day UTC; Mixpanel's export is per-calendar-day
    start_date = checkpoint.replace(hour=0, minute=0, second=0, microsecond=0)
    # End at yesterday midnight (today's data is still incoming)
    today_midnight = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = today_midnight  # exclusive upper bound

    if start_date >= end_date:
        print(f"Connector {connector_id}: checkpoint is current (today), nothing to import.")
        return

    total_days = (end_date - start_date).days
    print(f"\nConnector {connector_id} ({connector['connector_name']}) -- incremental Mixpanel import")
    print(f"  Tenant    : {tenant_id}")
    print(f"  Workspace : {workspace_id}")
    print(f"  From      : {start_date.date().isoformat()}")
    print(f"  To        : {(end_date - DAY_STEP).date().isoformat()}")
    print(f"  Slices    : {total_days} day(s)")

    total_fetched = 0
    total_skipped = 0
    total_sent = 0
    last_safe_checkpoint = start_date

    slice_start = start_date
    while slice_start < end_date:
        slice_end = slice_start + DAY_STEP

        try:
            # Bug 3 fix: was _fetch_slice(api_secret, slice_start, slice_start) --
            # passing slice_start as both from_date and to_date, producing a
            # zero-width (same-day) window for every slice and silently skipping
            # all events while still advancing the checkpoint.
            raw_events = _fetch_slice(api_secret, slice_start, slice_end)
        except RuntimeError as exc:
            print(f"\n  [error] {exc}")
            print(
                f"  Stopped at {slice_start.date()}. "
                f"Checkpoint NOT advanced past {last_safe_checkpoint.date()}."
            )
            break

        total_fetched += len(raw_events)
        slice_sent = 0
        slice_skipped = 0
        active_run_id = _get_latest_open_run_id(tenant_id, workspace_id)

        events_to_send = []
        for raw in raw_events:
            event = _normalize(raw)
            event_id = event["event_id"]

            # Skip events with no name (malformed Mixpanel data)
            if not event.get("name"):
                slice_skipped += 1
                continue

            # Idempotency check
            if event_id and active_run_id is not None and event_id_exists(
                event_id, tenant_id, workspace_id, run_id=active_run_id
            ):
                slice_skipped += 1
                continue

            events_to_send.append(event)
            slice_sent += 1

        try:
            _send_batch(connector, events_to_send)
        except RuntimeError as exc:
            print(f"  [warn] {exc} -- skipping batch")
            slice_skipped += len(events_to_send)
            slice_sent = 0

        total_sent += slice_sent
        total_skipped += slice_skipped

        # Advance checkpoint only after this slice is fully processed
        if lookback_days is None:
            set_checkpoint(connector_id, slice_end)
        last_safe_checkpoint = slice_end

        label = slice_start.strftime("%Y-%m-%d")
        print(
            f"  [{label}]  fetched={len(raw_events)}  "
            f"sent={slice_sent}  skipped={slice_skipped}"
        )

        slice_start = slice_end

    print(f"\nDone.")
    print(f"  Total fetched : {total_fetched}")
    print(f"  Total sent    : {total_sent}")
    print(f"  Total skipped : {total_skipped}  (duplicates, send errors, or malformed)")
    if lookback_days is None:
        print(f"  Checkpoint now: {last_safe_checkpoint.date().isoformat()}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Kaliper incremental Mixpanel importer"
    )
    parser.add_argument(
        "--connector-id",
        type=int,
        default=None,
        help="Connector ID to import. If not provided, defaults to the first active Mixpanel connector.",
    )
    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help="Clear the checkpoint and re-import from DEFAULT_LOOKBACK_DAYS ago.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="Override the checkpoint and start N days in the past (does not update the stored checkpoint).",
    )
    args = parser.parse_args()

    initialize_connector_tables()

    connector_id = args.connector_id
    if connector_id is None:
        from core.connector_registry import list_all_workspaces, list_connectors
        for ws in list_all_workspaces():
            connectors = list_connectors(
                tenant_id=ws["tenant_id"],
                workspace_id=ws["workspace_id"],
            )
            for c in connectors:
                if c["is_active"] and c["connector_type"].lower() == "mixpanel":
                    connector_id = c["id"]
                    break
            if connector_id:
                break

    if connector_id is None:
        print("No active Mixpanel connector found. Register one with register_mixpanel_connector.py.")
        return

    connector = get_connector(connector_id)
    if not connector:
        raise RuntimeError(f"Connector {connector_id} not found in database.")
    if not connector["is_active"]:
        raise RuntimeError(f"Connector {connector_id} is inactive.")
    if connector["connector_type"].lower() != "mixpanel":
        raise RuntimeError(
            f"Connector {connector_id} is type '{connector['connector_type']}', not 'mixpanel'."
        )

    if args.reset_checkpoint:
        clear_checkpoint(connector_id)
        print(f"Checkpoint cleared for connector {connector_id}.")

    run_incremental_import(connector, lookback_days=args.lookback_days)


if __name__ == "__main__":
    main()
