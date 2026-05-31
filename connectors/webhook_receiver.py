# connectors/webhook_receiver.py
"""
Kaliper Webhook Receiver
------------------------
Accepts real-time event streams from Segment, RudderStack, or any
analytics CDP that can POST to a webhook endpoint.

This solves the freshness problem with the Amplitude pull-based importer
(hourly slices) — events are audited the moment they are fired, not up to
an hour later.

Supported source formats
------------------------
  segment      — Segment Track / Page / Identify calls
  rudderstack  — RudderStack Track / Page / Identify calls (same shape as Segment)
  generic      — Kaliper native envelope (same format as /ingest)

How it works
------------
  POST /webhook/segment?tenant_id=<tid>&workspace_id=<wid>
  POST /webhook/rudderstack?tenant_id=<tid>&workspace_id=<wid>
  POST /webhook/generic?tenant_id=<tid>&workspace_id=<wid>

  Each endpoint:
  1. Validates the shared secret (X-Kaliper-Webhook-Secret header or
     KALIPER_WEBHOOK_SECRET_<TENANT_ID> env var).
  2. Normalises the source-specific payload into a Kaliper IncomingEvent.
  3. Forwards it directly to the /ingest endpoint on the same server
     (localhost:5000) so all existing audit logic, storage and issue
     detection runs exactly as it does for Amplitude events.

Authentication
--------------
Set one env var per tenant:

    KALIPER_WEBHOOK_SECRET_TENANT_DEMO=my_secret_here

Then pass the secret in the webhook source's custom header:

    X-Kaliper-Webhook-Secret: my_secret_here

If no secret env vars are configured the receiver operates in open mode
(development only — always configure secrets in production).

Registering in Segment
----------------------
  Destinations → Custom Webhook
  URL:      https://your-kaliper-host/webhook/segment?tenant_id=X&workspace_id=Y
  Headers:  X-Kaliper-Webhook-Secret: <secret>

Registering in RudderStack
--------------------------
  Destinations → Webhook
  URL:      https://your-kaliper-host/webhook/rudderstack?tenant_id=X&workspace_id=Y
  Headers:  X-Kaliper-Webhook-Secret: <secret>
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

INGEST_URL = "http://127.0.0.1:5000/ingest"

# ---------------------------------------------------------------------------
# Secret validation
# ---------------------------------------------------------------------------

def _load_webhook_secrets() -> dict[str, str]:
    """
    Returns {tenant_id: secret} from KALIPER_WEBHOOK_SECRET_<TENANT_ID> env vars.
    Keys are lowercased for case-insensitive lookup.
    """
    secrets: dict[str, str] = {}
    prefix = "KALIPER_WEBHOOK_SECRET_"
    for env_var, value in os.environ.items():
        if env_var.startswith(prefix) and value:
            tenant_id = env_var[len(prefix):].lower()
            secrets[tenant_id] = value
    return secrets


WEBHOOK_SECRETS: dict[str, str] = _load_webhook_secrets()
WEBHOOK_AUTH_ENABLED: bool = len(WEBHOOK_SECRETS) > 0


def validate_webhook_secret(tenant_id: str, provided_secret: str | None) -> tuple[bool, str]:
    """
    Returns (is_valid, error_message).
    Always returns True when no secrets are configured (dev mode).
    """
    if not WEBHOOK_AUTH_ENABLED:
        return True, ""

    expected = WEBHOOK_SECRETS.get(tenant_id.lower())
    if not expected:
        return False, f"No webhook secret configured for tenant '{tenant_id}'."

    if not provided_secret:
        return False, "Missing X-Kaliper-Webhook-Secret header."

    # Constant-time comparison to prevent timing attacks
    if not hmac.compare_digest(expected.encode(), provided_secret.encode()):
        return False, "Invalid webhook secret."

    return True, ""


# ---------------------------------------------------------------------------
# Segment / RudderStack normalisation
# ---------------------------------------------------------------------------

def _iso_timestamp(raw: Any) -> str:
    """
    Parse a timestamp from Segment/RudderStack into ISO 8601.
    Segment sends: "2024-01-15T10:30:00.000Z"
    Falls back to now() if unparseable.
    """
    if isinstance(raw, str) and raw:
        return raw  # already ISO — pass through
    return datetime.now(timezone.utc).isoformat()


def _derive_event_id(payload: dict[str, Any], source: str) -> str:
    """
    Extract or derive a stable event ID.
    Segment uses messageId; RudderStack uses messageId or rudderId.
    Fall back to a deterministic hash of (type, userId/anonymousId, timestamp).
    """
    msg_id = (
        payload.get("messageId")
        or payload.get("rudderId")
        or payload.get("event_id")
    )
    if msg_id:
        return str(msg_id)

    # Derive a stable ID from key fields so re-delivery is idempotent
    key = "|".join([
        payload.get("type", ""),
        str(payload.get("userId") or payload.get("anonymousId") or ""),
        str(payload.get("timestamp") or payload.get("originalTimestamp") or ""),
    ])
    return f"{source}-" + hashlib.sha256(key.encode()).hexdigest()[:32]


def normalise_segment(payload: dict[str, Any]) -> dict[str, Any] | None:
    """
    Convert a Segment Track / Page / Screen / Identify call into
    a Kaliper IncomingEvent dict.

    Segment payload shape:
      {
        "type": "track",
        "event": "Product Added",          # present on track calls
        "name": "Home",                    # present on page/screen calls
        "userId": "user_123",
        "anonymousId": "anon_abc",
        "timestamp": "2024-01-15T10:30:00.000Z",
        "messageId": "abc-123",
        "properties": { ... }
      }

    Returns None for calls we don't map (e.g. identify-only with no event name).
    """
    call_type = (payload.get("type") or "").lower()

    if call_type == "track":
        event_name = payload.get("event")
    elif call_type in ("page", "screen"):
        # Treat Page Viewed / Screen Viewed as a standard event
        page_name = payload.get("name") or payload.get("category") or "Page"
        event_name = f"{page_name} Viewed"
    elif call_type == "identify":
        # Identify calls carry user traits, not trackable events — skip
        return None
    else:
        return None

    if not event_name:
        return None

    properties = dict(payload.get("properties") or {})

    # Merge context fields that tracking plans often require
    context = payload.get("context") or {}
    page_ctx = context.get("page") or {}
    if page_ctx.get("url") and "page_url" not in properties:
        properties["page_url"] = page_ctx["url"]

    return {
        "name": event_name,
        "user_id": payload.get("userId") or None,
        "anonymous_id": payload.get("anonymousId") or None,
        "timestamp": _iso_timestamp(
            payload.get("timestamp") or payload.get("originalTimestamp")
        ),
        "event_id": _derive_event_id(payload, "segment"),
        "properties": properties,
    }


def normalise_rudderstack(payload: dict[str, Any]) -> dict[str, Any] | None:
    """
    RudderStack payloads are structurally identical to Segment's.
    The only differences are optional fields (rudderId, originalTimestamp).
    We reuse the Segment normaliser with a different source tag for the event_id.
    """
    result = normalise_segment(payload)
    if result is None:
        return None
    # Re-derive the ID with the rudderstack prefix if no messageId was present
    if not (payload.get("messageId") or payload.get("rudderId")):
        result["event_id"] = _derive_event_id(payload, "rudderstack")
    return result


def normalise_generic(payload: dict[str, Any]) -> dict[str, Any] | None:
    """
    Pass-through for Kaliper's native event format.
    Allows any system that can POST JSON to send events directly.

    Expected shape:
      {
        "name": "Order Completed",
        "user_id": "user_1",            # optional if anonymous_id present
        "anonymous_id": "anon_1",       # optional if user_id present
        "timestamp": "2024-01-15T...",
        "event_id": "evt_abc",
        "properties": { ... }
      }
    """
    if not payload.get("name"):
        return None
    if not payload.get("user_id") and not payload.get("anonymous_id"):
        return None

    return {
        "name": payload["name"],
        "user_id": payload.get("user_id") or None,
        "anonymous_id": payload.get("anonymous_id") or None,
        "timestamp": _iso_timestamp(payload.get("timestamp")),
        "event_id": payload.get("event_id") or str(uuid.uuid4()),
        "properties": dict(payload.get("properties") or {}),
    }


# ---------------------------------------------------------------------------
# Forward to /ingest
# ---------------------------------------------------------------------------

def forward_to_ingest(
    event: dict[str, Any],
    tenant_id: str,
    workspace_id: str,
    environment: str,
    source: str,
) -> tuple[bool, str]:
    """
    Wrap the normalised event in a Kaliper envelope and POST it to /ingest.
    Returns (success, error_message).
    """
    envelope = {
        "tenant_id": tenant_id,
        "workspace_id": workspace_id,
        "environment": environment,
        "source": source,
        "event": event,
    }
    try:
        resp = requests.post(INGEST_URL, json=envelope, timeout=10)
        if resp.status_code == 200:
            return True, ""
        body = resp.json() if resp.content else {}
        return False, body.get("message", f"HTTP {resp.status_code}")
    except requests.exceptions.ConnectionError:
        return False, "Ingestion server unreachable."
    except requests.exceptions.Timeout:
        return False, "Ingestion server timed out."


# ---------------------------------------------------------------------------
# Flask route factory — imported by ingestion_server.py
# ---------------------------------------------------------------------------

def register_webhook_routes(app) -> None:
    """
    Attach /webhook/<source> routes to an existing Flask app.
    Called once from ingestion_server.py during startup.

    Routes added:
      POST /webhook/segment
      POST /webhook/rudderstack
      POST /webhook/generic
    """
    from flask import request, jsonify

    NORMALISERS = {
        "segment":     normalise_segment,
        "rudderstack": normalise_rudderstack,
        "generic":     normalise_generic,
    }

    def _handle_webhook(source: str):
        # --- 1. Extract routing params ---
        tenant_id = request.args.get("tenant_id", os.getenv("KALIPER_TENANT_ID", "tenant_demo"))
        workspace_id = request.args.get("workspace_id", os.getenv("KALIPER_WORKSPACE_ID", "ecommerce_workspace"))
        environment = request.args.get("environment", "production")

        # --- 2. Validate secret ---
        provided_secret = (
            request.headers.get("X-Kaliper-Webhook-Secret")
            or request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
        )
        valid, err = validate_webhook_secret(tenant_id, provided_secret)
        if not valid:
            return jsonify({"success": False, "message": err}), 401

        # --- 3. Parse body ---
        payload = request.get_json(silent=True)
        if not payload:
            return jsonify({"success": False, "message": "No JSON payload."}), 400

        # --- 4. Handle Segment/RudderStack batch envelopes ---
        # Both CDPs support batching: {"batch": [...events...]}
        events_raw: list[dict[str, Any]] = []
        if "batch" in payload and isinstance(payload["batch"], list):
            events_raw = payload["batch"]
        else:
            events_raw = [payload]

        # --- 5. Normalise + forward each event ---
        normalise = NORMALISERS[source]
        forwarded = 0
        skipped = 0
        errors: list[str] = []

        for raw_event in events_raw:
            event = normalise(raw_event)
            if event is None:
                skipped += 1
                continue

            ok, err_msg = forward_to_ingest(
                event=event,
                tenant_id=tenant_id,
                workspace_id=workspace_id,
                environment=environment,
                source=source,
            )
            if ok:
                forwarded += 1
            else:
                skipped += 1
                errors.append(err_msg)

        status = 200 if not errors else 207  # 207 Multi-Status: some succeeded
        return jsonify({
            "success": len(errors) == 0,
            "forwarded": forwarded,
            "skipped": skipped,
            "errors": errors[:5],  # cap to avoid huge responses
        }), status

    @app.route("/webhook/segment", methods=["POST"])
    def webhook_segment():
        return _handle_webhook("segment")

    @app.route("/webhook/rudderstack", methods=["POST"])
    def webhook_rudderstack():
        return _handle_webhook("rudderstack")

    @app.route("/webhook/generic", methods=["POST"])
    def webhook_generic():
        return _handle_webhook("generic")

    @app.route("/webhook/info", methods=["GET"])
    def webhook_info():
        """
        Returns webhook endpoint info and auth status.
        Useful for the dashboard setup flow.
        """
        base = request.host_url.rstrip("/")
        return jsonify({
            "auth_enabled": WEBHOOK_AUTH_ENABLED,
            "endpoints": {
                "segment":     f"{base}/webhook/segment",
                "rudderstack": f"{base}/webhook/rudderstack",
                "generic":     f"{base}/webhook/generic",
            },
            "required_header": "X-Kaliper-Webhook-Secret",
            "query_params": {
                "tenant_id":   "Your Kaliper tenant ID",
                "workspace_id": "Target workspace",
                "environment": "production | staging (default: production)",
            },
            "batch_support": True,
            "note": (
                "No secret env vars found — running in open mode. "
                "Set KALIPER_WEBHOOK_SECRET_<TENANT_ID> in .env for production."
                if not WEBHOOK_AUTH_ENABLED else
                "Secrets configured. Pass X-Kaliper-Webhook-Secret in your CDP destination headers."
            ),
        })
