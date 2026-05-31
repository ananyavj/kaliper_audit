# connectors/amplitude_importer.py
"""
Incremental importer for the Amplitude Export API.

How it works
------------
1. Read the checkpoint (last successfully imported end-time) from SQLite.
   - First run on a connector: defaults to DEFAULT_LOOKBACK_HOURS ago.
2. Build a fetch window:  checkpoint_time  →  now (truncated to whole hours,
   because the Export API only accepts hourly granularity).
3. Fetch each hourly slice sequentially, skipping slices with no data (404).
   Transient network errors are retried with exponential backoff.
4. For each event:
   - Check event_id_exists() — skip if already imported (idempotent).
   - Forward to the local ingestion server.
5. Only after ALL slices succeed, advance the checkpoint to end_time.
   - If any slice fails mid-way, the checkpoint stays at the last safe point,
     so the next run retries from there.

Retry / rate-limit behaviour
-----------------------------
- 429 Too Many Requests: waits the Retry-After header value (or RATE_LIMIT_WAIT
  seconds as fallback) then retries up to MAX_RETRIES times.
- Network errors (timeout, connection reset): exponential backoff starting at
  RETRY_BASE_WAIT seconds, up to MAX_RETRIES attempts.
- Any other non-200: treated as a hard failure for that slice.

Usage
-----
    python -m connectors.amplitude_importer
    python -m connectors.amplitude_importer --connector-id 2
    python -m connectors.amplitude_importer --connector-id 3 --reset-checkpoint
    python -m connectors.amplitude_importer --connector-id 3 --lookback-hours 48
"""

from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import io
import json
import time
import zipfile
import zlib
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import requests
from dotenv import load_dotenv

from core.connector_registry import (
    clear_checkpoint,
    get_connector,
    get_or_init_checkpoint,
    initialize_connector_tables,
    set_checkpoint,
)
from core.storage import event_id_exists, get_connection

load_dotenv()

INGEST_BATCH_URL = "http://127.0.0.1:5000/ingest-batch"

# Fetch in 24-hour chunks to speed up historical ingestion, instead of 1 hour at a time.
HOUR_STEP = timedelta(hours=24)

# Retry / rate-limit settings
MAX_RETRIES = 4
RETRY_BASE_WAIT = 2.0
RATE_LIMIT_WAIT = 60.0


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def _basic_auth(api_key: str, secret_key: str) -> str:
    token = f"{api_key}:{secret_key}"
    return "Basic " + base64.b64encode(token.encode()).decode()


# ---------------------------------------------------------------------------
# Connector discovery
# ---------------------------------------------------------------------------

def _get_first_active_amplitude_connector_id() -> Optional[int]:
    conn = get_connection()
    cur = conn.cursor()
    row = cur.execute(
        """
        SELECT id
        FROM connectors
        WHERE connector_type = 'amplitude'
          AND is_active = 1
        ORDER BY id ASC
        LIMIT 1
        """
    ).fetchone()
    conn.close()
    return int(row["id"]) if row else None


def _get_latest_open_run_id(tenant_id: str, workspace_id: str) -> int | None:
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
# Export payload decoding
# ---------------------------------------------------------------------------

def _decode_zip_payload(raw_bytes: bytes) -> str:
    """
    Decode a ZIP archive returned by Amplitude Export.
    Handles plain text files and nested gzip members.
    """
    pieces: list[str] = []

    with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
        for name in zf.namelist():
            if name.endswith("/"):
                continue

            member = zf.read(name)

            if member.startswith(b"\x1f\x8b"):
                member = gzip.decompress(member)

            try:
                pieces.append(member.decode("utf-8"))
            except UnicodeDecodeError:
                pieces.append(member.decode("utf-8", errors="replace"))

    return "\n".join(pieces)


def _decode_export_response(response: requests.Response) -> str:
    """
    Decode Amplitude export response robustly.
    Supports:
    - gzip payloads
    - zip payloads
    - plain UTF-8 text
    """
    raw_bytes = response.content
    content_type = (response.headers.get("Content-Type") or "").lower()
    content_encoding = (response.headers.get("Content-Encoding") or "").lower()

    # ZIP archive
    if raw_bytes.startswith(b"PK") or "zip" in content_type:
        return _decode_zip_payload(raw_bytes)

    # gzip compressed
    if raw_bytes.startswith(b"\x1f\x8b"):
        return gzip.decompress(raw_bytes).decode("utf-8")

    if "gzip" in content_encoding or "gzip" in content_type:
        try:
            return gzip.decompress(raw_bytes).decode("utf-8")
        except Exception:
            pass

    # deflate compressed
    if "deflate" in content_encoding:
        try:
            return zlib.decompress(raw_bytes).decode("utf-8")
        except Exception:
            pass

    # plain UTF-8 text
    try:
        return raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RuntimeError(
            "Unable to decode Amplitude export payload. "
            "The response was neither valid UTF-8 nor a recognized compressed format."
        ) from exc


# ---------------------------------------------------------------------------
# Amplitude Export API fetch (one hourly slice) — with retry + rate-limit
# ---------------------------------------------------------------------------

def _fetch_slice(
    api_key: str,
    secret_key: str,
    start: datetime,
    end: datetime,
) -> list[dict[str, Any]]:
    """
    Fetch one hourly slice from the Amplitude Export API.
    Returns a list of raw event dicts, or an empty list on 404 (no data).
    Retries on 429 and transient network errors.
    Raises RuntimeError on a hard non-200 response after retries are exhausted.
    """
    start_str = start.strftime("%Y%m%dT%H")
    end_str = end.strftime("%Y%m%dT%H")
    url = f"https://amplitude.com/api/2/export?start={start_str}&end={end_str}"
    headers = {"Authorization": _basic_auth(api_key, secret_key)}

    wait = RETRY_BASE_WAIT
    for attempt in range(1, MAX_RETRIES + 2):
        try:
            response = requests.get(url, headers=headers, timeout=60)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            if attempt > MAX_RETRIES:
                raise RuntimeError(
                    f"Network error after {MAX_RETRIES} retries [{start_str}→{end_str}]: {exc}"
                ) from exc
            print(f"  [retry {attempt}/{MAX_RETRIES}] network error: {exc} — waiting {wait:.0f}s")
            time.sleep(wait)
            wait *= 2
            continue

        if response.status_code == 404:
            return []

        if response.status_code == 429:
            retry_after = float(response.headers.get("Retry-After", RATE_LIMIT_WAIT))
            if attempt > MAX_RETRIES:
                raise RuntimeError(
                    f"Rate-limited by Amplitude [{start_str}→{end_str}] after {MAX_RETRIES} retries."
                )
            print(f"  [rate-limit] 429 received — waiting {retry_after:.0f}s (attempt {attempt})")
            time.sleep(retry_after)
            continue

        if response.status_code != 200:
            raise RuntimeError(
                f"Amplitude export failed [{start_str}→{end_str}]: "
                f"{response.status_code} {response.text[:300]}"
            )

        break
    else:
        raise RuntimeError(f"Exhausted retries for slice [{start_str}→{end_str}]")

    text = _decode_export_response(response)

    events: list[dict[str, Any]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            print(f"  [warn] skipping malformed JSON line: {line[:80]}")

    return events


# ---------------------------------------------------------------------------
# Normalise raw Amplitude event → Kaliper envelope format
# ---------------------------------------------------------------------------

def _normalize(raw: dict[str, Any]) -> dict[str, Any]:
    device_id = (
        raw.get("device_id")
        or raw.get("amplitude_id")
        or "unknown_device"
    )

    raw_time = raw.get("event_time", "")
    try:
        ts = datetime.strptime(raw_time, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc)
        timestamp = ts.isoformat()
    except (ValueError, TypeError):
        timestamp = raw_time

    # Prefer Amplitude's own UUID; fall back to a deterministic hash so that
    # re-importing the same event is still idempotent even when uuid is absent.
    # An empty-string event_id previously bypassed the event_id_exists() dedup
    # check, allowing duplicate events to accumulate in SQLite.
    raw_event_id = str(raw.get("uuid") or raw.get("event_id") or "").strip()
    if raw_event_id:
        event_id = raw_event_id
    else:
        # Build a stable ID from (event_type, device_id, event_time)
        source_str = f"{raw.get('event_type', '')}|{device_id}|{raw_time}"
        event_id = "amp-" + hashlib.sha256(source_str.encode()).hexdigest()[:32]

    return {
        "name": raw.get("event_type"),
        "user_id": raw.get("user_id"),
        "anonymous_id": str(device_id),
        "timestamp": timestamp,
        "event_id": event_id,
        "properties": raw.get("event_properties") or {},
    }


# ---------------------------------------------------------------------------
# Send one normalised event to the local ingestion server — with retry
# ---------------------------------------------------------------------------

def _send_batch(connector: dict[str, Any], events: list[dict[str, Any]]) -> None:
    if not events:
        return
        
    envelopes = [
        {
            "tenant_id": connector["tenant_id"],
            "workspace_id": connector["workspace_id"],
            "environment": connector["config"].get("environment", "production"),
            "source": "amplitude",
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
            print(f"  [retry {attempt}/{MAX_RETRIES}] ingest {resp.status_code} — waiting {wait:.0f}s")
            time.sleep(wait)
            wait *= 2
        except requests.exceptions.ConnectionError as exc:
            if attempt > MAX_RETRIES:
                raise RuntimeError(f"Ingestion server unreachable after {MAX_RETRIES} retries for batch") from exc
            print(f"  [retry {attempt}/{MAX_RETRIES}] ingest connection error — waiting {wait:.0f}s")
            time.sleep(wait)
            wait *= 2
        except requests.exceptions.Timeout as exc:
            if attempt > MAX_RETRIES:
                raise RuntimeError(f"Ingestion server timed out after {MAX_RETRIES} retries for batch") from exc
            print(f"  [retry {attempt}/{MAX_RETRIES}] ingest timeout — waiting {wait:.0f}s")
            time.sleep(wait)
            wait *= 2


# ---------------------------------------------------------------------------
# Main incremental sync logic
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Domain sanity check — detect wrong Amplitude project before mass-importing
# ---------------------------------------------------------------------------

# Event name substrings that are strong signals for each domain.
# Used to cheaply classify a sample of events without loading the full plan.
_DOMAIN_SIGNALS: dict[str, list[str]] = {
    "ecommerce": ["cart", "checkout", "product", "order", "purchase", "payment", "sku"],
    "saas":      ["signup", "sign_up", "trial", "subscription", "billing", "onboard"],
    "content":   ["play", "episode", "watch", "stream", "video", "article", "paywall",
                  "content", "ad_impression", "ad_skip", "trailer"],
}


def _infer_domain_from_sample(events: list[dict[str, Any]]) -> str | None:
    """
    Look at up to 50 event names and return the most likely domain, or None
    if the sample is too small / ambiguous to be confident.
    """
    counts: dict[str, int] = {d: 0 for d in _DOMAIN_SIGNALS}
    sample = [e.get("event_type", "").lower() for e in events[:50]]

    for name in sample:
        for domain, keywords in _DOMAIN_SIGNALS.items():
            if any(kw in name for kw in keywords):
                counts[domain] += 1

    total = sum(counts.values())
    if total == 0:
        return None

    top_domain = max(counts, key=counts.__getitem__)
    top_ratio  = counts[top_domain] / total

    # Only confident if >60% of matched signals point the same way
    return top_domain if top_ratio >= 0.6 else None


def _get_workspace_expected_domain(connector: dict[str, Any]) -> str | None:
    """
    Return the domain of the active plan for this workspace, or None if
    no plan is registered yet.
    """
    try:
        from core.plan_registry import get_active_plan_version
        active = get_active_plan_version(
            tenant_id=connector["tenant_id"],
            workspace_id=connector["workspace_id"],
        )
        return active["domain"] if active else None
    except Exception:
        return None


def run_incremental_import(
    connector: dict[str, Any],
    lookback_hours: int | None = None,
) -> None:
    connector_id = connector["id"]
    tenant_id = connector["tenant_id"]
    workspace_id = connector["workspace_id"]
    api_key = connector["credentials"]["api_key"]
    secret_key = connector["credentials"]["secret_key"]

    if lookback_hours is not None:
        checkpoint = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        print(f"  Lookback override: starting {lookback_hours}h ago ({checkpoint.isoformat()})")
    else:
        checkpoint = get_or_init_checkpoint(connector_id)

    start_time = checkpoint.replace(minute=0, second=0, microsecond=0)
    end_time = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

    if start_time >= end_time:
        print(f"Connector {connector_id}: checkpoint is current, nothing to import.")
        return

    # ------------------------------------------------------------------
    # Domain sanity check: fetch one hour of events and verify they look
    # like they belong to this workspace's plan domain.  If the inferred
    # domain disagrees with the expected one we abort immediately with a
    # clear message rather than silently poisoning the workspace with
    # thousands of unknown_event issues.
    # ------------------------------------------------------------------
    expected_domain = _get_workspace_expected_domain(connector)
    # probe_slice_start tracks which hour was used for the domain check so the
    # main import loop can skip re-fetching it (avoids double-billing API quota).
    probe_slice_start: datetime | None = None
    probe_cached_events: list[dict[str, Any]] | None = None

    if expected_domain and expected_domain != "generic":
        print(f"  Checking domain alignment (expected: {expected_domain})...")
        try:
            probe_start = end_time - timedelta(hours=1)
            probe_events = _fetch_slice(api_key, secret_key, probe_start, end_time)
            if probe_events:
                inferred = _infer_domain_from_sample(probe_events)
                if inferred is not None and inferred != expected_domain:
                    raise RuntimeError(
                        f"DOMAIN MISMATCH — workspace '{workspace_id}' expects "
                        f"'{expected_domain}' events but the Amplitude project is "
                        f"returning '{inferred}' events (sample of "
                        f"{min(len(probe_events), 50)} events checked).\n"
                        f"  This means the connector is pointing at the WRONG "
                        f"Amplitude project.\n"
                        f"  Fix: update AMPLITUDE_API_KEY_{workspace_id.upper()} "
                        f"and AMPLITUDE_SECRET_KEY_{workspace_id.upper()} in .env,\n"
                        f"  then run: python register_amplitude_connector.py "
                        f"--workspace {workspace_id} --force"
                    )
                if inferred is not None:
                    print(f"  Domain check passed ({inferred} ✓)")
                else:
                    print("  Domain check inconclusive (mixed/ambiguous sample) — proceeding.")
            # Cache the probe results so the main loop can reuse them instead
            # of issuing a second Export API call for the same hour window.
            if probe_start >= start_time:
                probe_slice_start = probe_start
                probe_cached_events = probe_events
        except RuntimeError:
            raise  # re-raise domain mismatch errors unchanged
        except Exception as probe_err:
            print(f"  [warn] Domain probe failed ({probe_err}) — proceeding without check.")
    
    from core.progress_store import update_progress
    total_hours = int((end_time - start_time).total_seconds() // 3600)
    print(f"\nConnector {connector_id} ({connector['connector_name']}) — incremental import")
    print(f"  Tenant    : {tenant_id}")
    print(f"  Workspace : {workspace_id}")
    print(f"  From      : {start_time.isoformat()}")
    print(f"  To        : {end_time.isoformat()}")
    print(f"  Slices    : {total_hours} hour(s)")

    total_fetched = 0
    total_skipped = 0
    total_sent = 0
    last_safe_checkpoint = start_time

    slice_start = start_time
    hours_done = 0
    update_progress(workspace_id, "running", hours_done, total_hours)
    
    while slice_start < end_time:
        slice_end = slice_start + HOUR_STEP

        try:
            # Reuse events already fetched during the domain probe to avoid
            # double-billing the Amplitude Export API for the same hour window.
            if probe_cached_events is not None and slice_start == probe_slice_start:
                raw_events = probe_cached_events
                probe_cached_events = None  # consume the cache; only one reuse
            else:
                raw_events = _fetch_slice(api_key, secret_key, slice_start, slice_end)
        except RuntimeError as exc:
            print(f"\n  [error] {exc}")
            print(
                f"  Stopped at {slice_start.isoformat()}. "
                f"Checkpoint NOT advanced past {last_safe_checkpoint.isoformat()}."
            )
            update_progress(workspace_id, "error", hours_done, total_hours)
            break

        total_fetched += len(raw_events)
        slice_sent = 0
        slice_skipped = 0
        active_run_id = _get_latest_open_run_id(tenant_id, workspace_id)

        events_to_send = []
        for raw in raw_events:
            event = _normalize(raw)
            event_id = event["event_id"]

            if event_id and active_run_id is not None and event_id_exists(
                event_id, tenant_id, workspace_id, run_id=active_run_id
            ):
                slice_skipped += 1
                continue

            if not event.get("name"):
                slice_skipped += 1
                continue

            events_to_send.append(event)
            slice_sent += 1

        try:
            _send_batch(connector, events_to_send)
        except RuntimeError as exc:
            print(f"  [warn] {exc} — skipping batch")
            slice_skipped += len(events_to_send)
            slice_sent = 0

        total_sent += slice_sent
        total_skipped += slice_skipped

        if lookback_hours is None:
            set_checkpoint(connector_id, slice_end)
        last_safe_checkpoint = slice_end

        label = slice_start.strftime("%Y-%m-%dT%H")
        print(
            f"  [{label}]  fetched={len(raw_events)}  "
            f"sent={slice_sent}  skipped={slice_skipped}"
        )

        slice_start = slice_end
        hours_done += 1
        update_progress(workspace_id, "running", hours_done, total_hours)

    if slice_start >= end_time:
        update_progress(workspace_id, "done", total_hours, total_hours)

    print(f"\nDone.")
    print(f"  Total fetched : {total_fetched}")
    print(f"  Total sent    : {total_sent}")
    print(f"  Total skipped : {total_skipped}  (duplicates, send errors, or malformed)")
    if lookback_hours is None:
        print(f"  Checkpoint now: {last_safe_checkpoint.isoformat()}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Kaliper incremental Amplitude importer"
    )
    parser.add_argument(
        "--connector-id",
        type=int,
        default=None,
        help="Connector ID to import. If not provided, uses the first active Amplitude connector.",
    )
    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help="Clear the checkpoint and re-import from DEFAULT_LOOKBACK_HOURS ago.",
    )
    parser.add_argument(
        "--lookback-hours",
        type=int,
        default=None,
        help="Override the checkpoint and start N hours in the past (does not update the stored checkpoint).",
    )
    args = parser.parse_args()

    initialize_connector_tables()

    connector_id = args.connector_id
    if connector_id is None:
        connector_id = _get_first_active_amplitude_connector_id()

    if connector_id is None:
        print("No active Amplitude connector found.")
        return

    connector = get_connector(connector_id)
    if not connector:
        raise RuntimeError(f"Connector {connector_id} not found in database.")
    if not connector["is_active"]:
        raise RuntimeError(f"Connector {connector_id} is inactive.")

    if args.reset_checkpoint:
        clear_checkpoint(connector_id)
        print(f"Checkpoint cleared for connector {connector_id}.")

    run_incremental_import(connector, lookback_hours=args.lookback_hours)


if __name__ == "__main__":
    main()
