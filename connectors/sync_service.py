# connectors/sync_service.py
from __future__ import annotations

import argparse
import base64
import gzip
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
    list_connectors,
    set_checkpoint,
    update_connector_sync_time,
)
from core.storage import event_id_exists, get_connection  # Bug 2 fix: import directly

load_dotenv()

INGEST_URL = "http://127.0.0.1:5000/ingest"

HOUR_STEP = timedelta(hours=1)
MAX_RETRIES = 4
RETRY_BASE_WAIT = 2.0
RATE_LIMIT_WAIT = 60.0


def _basic_auth(api_key: str, secret_key: str) -> str:
    token = f"{api_key}:{secret_key}"
    return "Basic " + base64.b64encode(token.encode()).decode()


def _get_first_active_amplitude_connector_id() -> Optional[int]:
    # Bug 2 fix: was using initialize_connector_tables.__globals__["get_connection"]()
    # which is a fragile internal-attribute hack that breaks on any import refactor.
    # get_connection is now imported directly above.
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


def _decode_zip_payload(raw_bytes: bytes) -> str:
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
    raw_bytes = response.content
    content_type = (response.headers.get("Content-Type") or "").lower()
    content_encoding = (response.headers.get("Content-Encoding") or "").lower()

    if raw_bytes.startswith(b"PK") or "zip" in content_type:
        return _decode_zip_payload(raw_bytes)

    if raw_bytes.startswith(b"\x1f\x8b"):
        return gzip.decompress(raw_bytes).decode("utf-8")

    if "gzip" in content_encoding or "gzip" in content_type:
        try:
            return gzip.decompress(raw_bytes).decode("utf-8")
        except Exception:
            pass

    if "deflate" in content_encoding:
        try:
            return zlib.decompress(raw_bytes).decode("utf-8")
        except Exception:
            pass

    try:
        return raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RuntimeError(
            "Unable to decode Amplitude export payload. "
            "The response was neither valid UTF-8 nor a recognized compressed format."
        ) from exc


def _fetch_slice(
    api_key: str,
    secret_key: str,
    start: datetime,
    end: datetime,
) -> list[dict[str, Any]]:
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

    raise RuntimeError(f"Exhausted retries for slice [{start_str}→{end_str}]")


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

    return {
        "name": raw.get("event_type"),
        "user_id": raw.get("user_id"),
        "anonymous_id": str(device_id),
        "timestamp": timestamp,
        "event_id": str(raw.get("uuid") or raw.get("event_id") or ""),
        "properties": raw.get("event_properties") or {},
    }


def _send(connector: dict[str, Any], event: dict[str, Any]) -> None:
    envelope = {
        "tenant_id": connector["tenant_id"],
        "workspace_id": connector["workspace_id"],
        "environment": connector["config"].get("environment", "production"),
        "source": "amplitude",
        "event": event,
    }

    wait = RETRY_BASE_WAIT
    for attempt in range(1, MAX_RETRIES + 2):
        try:
            resp = requests.post(INGEST_URL, json=envelope, timeout=30)
            if resp.status_code == 200:
                return
            raise RuntimeError(
                f"Ingestion rejected event '{event.get('name')}': "
                f"{resp.status_code} {resp.text[:200]}"
            )
        except requests.exceptions.ConnectionError as exc:
            if attempt > MAX_RETRIES:
                raise RuntimeError(
                    f"Ingestion server unreachable after {MAX_RETRIES} retries "
                    f"for event '{event.get('name')}'"
                ) from exc
            print(f"  [retry {attempt}/{MAX_RETRIES}] ingest connection error — waiting {wait:.0f}s")
            time.sleep(wait)
            wait *= 2
        except requests.exceptions.Timeout as exc:
            if attempt > MAX_RETRIES:
                raise RuntimeError(
                    f"Ingestion server timed out after {MAX_RETRIES} retries "
                    f"for event '{event.get('name')}'"
                ) from exc
            print(f"  [retry {attempt}/{MAX_RETRIES}] ingest timeout — waiting {wait:.0f}s")
            time.sleep(wait)
            wait *= 2


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
    while slice_start < end_time:
        slice_end = slice_start + HOUR_STEP

        try:
            raw_events = _fetch_slice(api_key, secret_key, slice_start, slice_end)
        except RuntimeError as exc:
            print(f"\n  [error] {exc}")
            print(
                f"  Stopped at {slice_start.isoformat()}. "
                f"Checkpoint NOT advanced past {last_safe_checkpoint.isoformat()}."
            )
            break

        total_fetched += len(raw_events)
        slice_sent = 0
        slice_skipped = 0

        for raw in raw_events:
            event = _normalize(raw)
            event_id = event["event_id"]

            if event_id and event_id_exists(event_id, tenant_id, workspace_id):
                slice_skipped += 1
                continue

            if not event.get("name"):
                slice_skipped += 1
                continue

            try:
                _send(connector, event)
                slice_sent += 1
            except RuntimeError as exc:
                print(f"  [warn] {exc} — skipping event")
                slice_skipped += 1

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

    print(f"\nDone.")
    print(f"  Total fetched : {total_fetched}")
    print(f"  Total sent    : {total_sent}")
    print(f"  Total skipped : {total_skipped}  (duplicates, send errors, or malformed)")
    if lookback_hours is None:
        print(f"  Checkpoint now: {last_safe_checkpoint.isoformat()}")


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
