#connectors/event_forwarder
import os
import time
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Callable, Literal

import requests

from simulators.simulators import generate_clean_flow, generate_flow_with_errors
from simulators.simulators_saas import (
    generate_saas_clean_flow,
    generate_saas_flow_with_errors,
)
from simulators.simulators_content import (
    generate_content_clean_flow,
    generate_content_flow_with_errors,
)

try:
    from mixpanel import Mixpanel
except ImportError:
    Mixpanel = None


Mode = Literal["ecommerce", "saas", "content"]
FlowName = Literal["clean", "error", "all"]

from dotenv import load_dotenv
load_dotenv()

LOCAL_INGEST_URL = os.getenv("KALIPER_LOCAL_INGEST_URL", "http://127.0.0.1:5000/ingest")
DESTINATIONS = {
    item.strip().lower()
    for item in os.getenv("KALIPER_DESTINATIONS", "local").split(",")
    if item.strip()
}

MODE = os.getenv("KALIPER_MODE", "ecommerce").strip().lower()
FLOW = os.getenv("KALIPER_FLOW", "error").strip().lower()

TENANT_ID = os.getenv("KALIPER_TENANT_ID", "tenant_demo")
WORKSPACE_ID = os.getenv(
    "KALIPER_WORKSPACE_ID",
    {
        "ecommerce": "ecommerce_workspace",
        "saas": "saas_workspace",
        "content": "content_workspace",
    }.get(MODE, "ecommerce_workspace"),
)
ENVIRONMENT = os.getenv("KALIPER_ENVIRONMENT", "production")

AMPLITUDE_API_KEY = os.getenv("AMPLITUDE_API_KEY", "")
AMPLITUDE_REGION = os.getenv("AMPLITUDE_REGION", "default").strip().lower()

MIXPANEL_PROJECT_TOKEN = os.getenv("MIXPANEL_PROJECT_TOKEN", "")
SLEEP_SECONDS = float(os.getenv("KALIPER_SEND_DELAY_SECONDS", "0.5"))


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _pick_simulator(mode: str, flow: str) -> Callable[[], list]:
    if mode == "saas":
        return generate_saas_clean_flow if flow == "clean" else generate_saas_flow_with_errors
    if mode == "content":
        return generate_content_clean_flow if flow == "clean" else generate_content_flow_with_errors
    return generate_clean_flow if flow == "clean" else generate_flow_with_errors


def _normalize_id(value: str | None, fallback_prefix: str = "kaliper") -> str:
    raw = (value or "").strip()
    if not raw:
        raw = f"{fallback_prefix}_{os.urandom(8).hex()}"
    if len(raw) < 5:
        raw = f"{raw}_{os.urandom(4).hex()}"
    return raw


def _event_to_dict(event: Any) -> dict[str, Any]:
    if hasattr(event, "__dict__"):
        return dict(event.__dict__)
    try:
        return asdict(event)
    except Exception:
        return {
            "name": getattr(event, "name"),
            "user_id": getattr(event, "user_id", None),
            "anonymous_id": getattr(event, "anonymous_id", None),
            "timestamp": getattr(event, "timestamp"),
            "properties": getattr(event, "properties"),
            "event_id": getattr(event, "event_id"),
        }


def build_envelope(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "tenant_id": TENANT_ID,
        "workspace_id": WORKSPACE_ID,
        "environment": ENVIRONMENT,
        "source": "simulator",
        "event": payload,
    }


def send_to_local_ingest(envelope: dict[str, Any]) -> None:
    response = requests.post(LOCAL_INGEST_URL, json=envelope, timeout=10)
    response.raise_for_status()


def send_to_amplitude(payload: dict[str, Any]) -> None:
    if not AMPLITUDE_API_KEY:
        raise RuntimeError("AMPLITUDE_API_KEY is not set.")

    base_url = "https://api.eu.amplitude.com" if AMPLITUDE_REGION == "eu" else "https://api2.amplitude.com"
    url = f"{base_url}/2/httpapi"

    amplitude_event = {
        "event_type": payload["name"],
        "time": _now_ms(),
        "insert_id": payload["event_id"],
        "event_properties": payload.get("properties", {}),
    }

    user_id = payload.get("user_id")
    anonymous_id = payload.get("anonymous_id")

    if user_id:
        amplitude_event["user_id"] = _normalize_id(user_id, "user")
    else:
        amplitude_event["device_id"] = _normalize_id(anonymous_id or payload["event_id"], "device")

    body = {
        "api_key": AMPLITUDE_API_KEY,
        "events": [amplitude_event],
    }

    response = requests.post(url, json=body, timeout=10)
    response.raise_for_status()


class MixpanelForwarder:
    def __init__(self, token: str):
        if Mixpanel is None:
            raise RuntimeError("mixpanel package is not installed. Run: pip install mixpanel")
        self.client = Mixpanel(token)

    def send(self, payload: dict[str, Any]) -> None:
        distinct_id = _normalize_id(
            payload.get("user_id") or payload.get("anonymous_id") or payload["event_id"],
            "distinct",
        )
        event_name = payload["name"]
        properties = dict(payload.get("properties", {}))
        properties["timestamp"] = payload.get("timestamp")
        properties["event_id"] = payload.get("event_id")
        if payload.get("anonymous_id"):
            properties["anonymous_id"] = payload["anonymous_id"]

        self.client.track(distinct_id, event_name, properties)


def get_mixpanel_forwarder() -> MixpanelForwarder | None:
    if not MIXPANEL_PROJECT_TOKEN:
        return None
    return MixpanelForwarder(MIXPANEL_PROJECT_TOKEN)


def main() -> None:
    events = []
    if FLOW in ("clean", "all"):
        events.extend(_pick_simulator(MODE, "clean")())
    if FLOW in ("error", "all"):
        events.extend(_pick_simulator(MODE, "error")())
    
    # Optional: Sort them by timestamp if combining both, though it's simulated data
    events.sort(key=lambda e: e.timestamp)

    mixpanel_forwarder = None
    if "mixpanel" in DESTINATIONS:
        mixpanel_forwarder = get_mixpanel_forwarder()

    print(f"Tenant: {TENANT_ID}")
    print(f"Workspace: {WORKSPACE_ID}")
    print(f"Mode: {MODE}")
    print(f"Flow: {FLOW}")
    print(f"Destinations: {', '.join(sorted(DESTINATIONS)) or 'local'}")
    print(f"Event count: {len(events)}")

    for event in events:
        payload = _event_to_dict(event)

        if "local" in DESTINATIONS:
            send_to_local_ingest(build_envelope(payload))

        if "amplitude" in DESTINATIONS:
            send_to_amplitude(payload)

        if "mixpanel" in DESTINATIONS:
            if mixpanel_forwarder is None:
                raise RuntimeError(
                    "MIXPANEL_PROJECT_TOKEN is not set, but mixpanel is in KALIPER_DESTINATIONS."
                )
            mixpanel_forwarder.send(payload)

        print(f"Sent {payload['name']}")

        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()