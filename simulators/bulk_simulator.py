"""
bulk_simulator.py
-----------------
Generates and sends N realistic user sessions to Amplitude.

Usage:
    python -m simulators.bulk_simulator --vertical ecommerce --sessions 5000
    python -m simulators.bulk_simulator --vertical saas --sessions 5000
    python -m simulators.bulk_simulator --vertical content --sessions 5000

Env vars required:
    AMPLITUDE_API_KEY   — your project API key (read from .env automatically)
    AMPLITUDE_REGION    — "eu" or "default" (default = US)
"""

from __future__ import annotations

import argparse
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

AMPLITUDE_API_KEY = os.getenv("AMPLITUDE_API_KEY", "")
AMPLITUDE_REGION = os.getenv("AMPLITUDE_REGION", "default").strip().lower()
KALIPER_INGEST_URL = os.getenv("KALIPER_INGEST_URL", "http://127.0.0.1:5000/ingest")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BATCH_SIZE = 10          # events per HTTP request (Amplitude allows up to 2000)
BATCH_DELAY = 0.15       # seconds between batches — stay well under rate limits
ERROR_RATE = 0.15        # 15% of sessions will contain injected errors

# Realistic user-pool sizes — sampled from to simulate returning users
USER_POOL = 3000         # distinct users across 5000 sessions

COUNTRIES = ["IN", "US", "GB", "DE", "SG", "AU", "CA", "BR"]
PLATFORMS = ["Web", "iOS", "Android"]
COUNTRY_WEIGHTS = [0.35, 0.25, 0.10, 0.07, 0.05, 0.05, 0.05, 0.08]
PLATFORM_WEIGHTS = [0.55, 0.25, 0.20]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _api_url() -> str:
    if AMPLITUDE_REGION == "eu":
        return "https://api.eu.amplitude.com/2/httpapi"
    return "https://api2.amplitude.com/2/httpapi"


def _random_past_timestamp(days_back: int = 30) -> datetime:
    """Return a random UTC datetime within the last `days_back` days."""
    now = datetime.now(timezone.utc)
    offset = timedelta(
        days=random.randint(0, days_back - 1),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )
    return now - offset


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _user_id(index: int) -> str:
    return f"user_{index:04d}"


def _device_id() -> str:
    return f"device_{uuid.uuid4().hex[:12]}"


def _insert_id() -> str:
    return str(uuid.uuid4())


def _country() -> str:
    return random.choices(COUNTRIES, weights=COUNTRY_WEIGHTS)[0]


def _platform() -> str:
    return random.choices(PLATFORMS, weights=PLATFORM_WEIGHTS)[0]


def _build_amplitude_event(
    event_type: str,
    user_id: str | None,
    device_id: str,
    session_ts: datetime,
    offset_seconds: int,
    properties: dict[str, Any],
    country: str,
    platform: str,
) -> dict[str, Any]:
    ts = session_ts + timedelta(seconds=offset_seconds)
    event: dict[str, Any] = {
        "event_type": event_type,
        "device_id": device_id,
        "time": _ms(ts),
        "insert_id": _insert_id(),
        "event_properties": properties,
        "user_properties": {},
        "country": country,
        "platform": platform,
        "session_id": _ms(session_ts),
    }
    if user_id:
        event["user_id"] = user_id
    return event


# ---------------------------------------------------------------------------
# Per-vertical session generators
# Each returns a list of Amplitude event dicts for one session
# ---------------------------------------------------------------------------

def _ecommerce_session(
    user_id: str, device_id: str, ts: datetime,
    country: str, platform: str, inject_error: bool
) -> list[dict]:
    product_id = f"prod_{random.randint(1, 80):03d}"
    order_id = f"ord_{uuid.uuid4().hex[:8]}"
    revenue = round(random.uniform(199, 4999), 2)
    quantity = random.randint(1, 5)

    steps = [
        ("Product Viewed", 0, {
            "product_id": product_id,
            "title": f"Product {product_id}",
            "category": random.choice(["Clothing", "Electronics", "Books", "Home"]),
            "price": revenue,
        }),
        ("Product Added", random.randint(5, 30), {
            "product_id": product_id,
            "quantity": quantity,
            "price": revenue,
        }),
        ("Checkout Started", random.randint(35, 90), {
            "cart_id": f"cart_{uuid.uuid4().hex[:8]}",
            "cart_value": round(revenue * quantity, 2),
        }),
        ("Order Completed", random.randint(100, 200), {
            "order_id": order_id,
            # inject type error: revenue as string instead of float
            "revenue": str(revenue) if inject_error else revenue,
            "currency": random.choice(["INR", "USD", "GBP"]),
            "quantity": quantity,
        }),
    ]

    # 30% of clean sessions drop off after Product Added
    if not inject_error and random.random() < 0.30:
        steps = steps[:2]

    result = []
    for name, offset, props in steps:
        # inject missing user_id on Product Added for error sessions
        uid = None if (inject_error and name == "Product Added") else user_id
        result.append(_build_amplitude_event(
            name, uid, device_id, ts, offset, props, country, platform
        ))
    return result


def _saas_session(
    user_id: str, device_id: str, ts: datetime,
    country: str, platform: str, inject_error: bool
) -> list[dict]:
    account_id = f"acc_{random.randint(1, 500):04d}"
    plan = random.choice(["Free", "Pro", "Enterprise"])
    feature = random.choice(["dashboard", "reports", "integrations", "api_access", "alerts"])

    steps = [
        ("Login", 0, {
            "method": random.choice(["password", "google_oauth", "sso"]),
            "account_id": account_id,
        }),
        ("Feature Used", random.randint(10, 60), {
            "feature_name": feature,
            "plan": plan,
        }),
        ("Report Generated", random.randint(65, 150), {
            "report_type": random.choice(["funnel", "retention", "segmentation"]),
            # inject type error: rows as string instead of int
            "rows": "many" if inject_error else random.randint(10, 5000),
        }),
    ]

    # 20% of sessions are new signups — prepend signup steps
    if random.random() < 0.20:
        steps = [
            ("Signup", 0, {
                "account_id": account_id,
                "plan": plan,
            }),
            ("Trial Started", random.randint(5, 20), {
                "trial_id": f"trial_{uuid.uuid4().hex[:6]}",
                "plan": "Pro",
            }),
        ] + steps

    result = []
    for name, offset, props in steps:
        # inject missing user_id on Feature Used for error sessions
        uid = None if (inject_error and name == "Feature Used") else user_id
        result.append(_build_amplitude_event(
            name, uid, device_id, ts, offset, props, country, platform
        ))
    return result


def _content_session(
    user_id: str, device_id: str, ts: datetime,
    country: str, platform: str, inject_error: bool
) -> list[dict]:
    article_id = f"art_{random.randint(1, 200):04d}"
    video_id = f"vid_{random.randint(1, 100):04d}"
    session_id = f"sess_{uuid.uuid4().hex[:10]}"
    watch_duration = random.randint(30, 1800)

    steps = [
        ("Page Viewed", 0, {
            "page_url": f"https://example.com/content/{article_id}",
            "session_id": session_id,
            "referrer": random.choice(["google", "direct", "twitter", "newsletter"]),
        }),
        ("Article Read", random.randint(10, 60), {
            "article_id": article_id,
            "duration_seconds": random.randint(30, 600),
            "scroll_depth_pct": random.randint(20, 100),
        }),
    ]

    # 40% of sessions also watch a video
    if random.random() < 0.40:
        steps += [
            ("Video Started", random.randint(65, 120), {
                "video_id": video_id,
                "duration_seconds": watch_duration,
            }),
            ("Video Completed", random.randint(125, 125 + watch_duration), {
                "video_id": video_id,
                # inject type error: duration as string
                "duration_seconds": "long" if inject_error else watch_duration,
                "completion_pct": random.randint(70, 100),
            }),
        ]

    result = []
    for name, offset, props in steps:
        # inject missing user_id on Article Read for error sessions
        uid = None if (inject_error and name == "Article Read") else user_id
        result.append(_build_amplitude_event(
            name, uid, device_id, ts, offset, props, country, platform
        ))
    return result


VERTICAL_GENERATORS = {
    "ecommerce": _ecommerce_session,
    "saas": _saas_session,
    "content": _content_session,
}

# ---------------------------------------------------------------------------
# Amplitude batch sender
# ---------------------------------------------------------------------------

def _send_batch(events: list[dict]) -> None:
    if not AMPLITUDE_API_KEY:
        raise RuntimeError("AMPLITUDE_API_KEY is not set. Check your .env file.")

    body = {
        "api_key": AMPLITUDE_API_KEY,
        "events": events,
        "options": {
            "min_id_length": 1,
        },
    }

    response = requests.post(_api_url(), json=body, timeout=30)

    if response.status_code not in (200, 202):
        raise RuntimeError(
            f"Amplitude batch upload failed: {response.status_code} {response.text}"
        )


# ---------------------------------------------------------------------------
# Kaliper ingestion sender (dual-write)
# Converts Amplitude event format → Kaliper envelope and posts to /ingest
# ---------------------------------------------------------------------------

def _send_session_to_kaliper(
    events: list[dict],
    vertical: str,
    tenant_id: str = "tenant_demo",
) -> None:
    """
    Send each event in a session to the local Kaliper ingestion server.
    Failures are logged as warnings — Kaliper ingestion is best-effort;
    Amplitude is the source of truth.
    """
    workspace_id = f"{vertical}_workspace"

    for amp_event in events:
        kaliper_event = {
            "name": amp_event.get("event_type"),
            "user_id": amp_event.get("user_id"),
            "anonymous_id": amp_event.get("device_id"),
            "timestamp": datetime.fromtimestamp(
                amp_event["time"] / 1000, tz=timezone.utc
            ).isoformat(),
            "event_id": amp_event.get("insert_id", ""),
            "properties": amp_event.get("event_properties", {}),
        }

        envelope = {
            "tenant_id": tenant_id,
            "workspace_id": workspace_id,
            "environment": "production",
            "source": "simulator",
            "event": kaliper_event,
        }

        try:
            resp = requests.post(KALIPER_INGEST_URL, json=envelope, timeout=5)
            if resp.status_code != 200:
                print(f"  [kaliper warn] {amp_event.get('event_type')}: {resp.status_code}")
        except requests.exceptions.ConnectionError:
            # Ingestion server not running — skip silently on first event,
            # but we'll print one warning per session below
            raise
        except requests.exceptions.Timeout:
            print(f"  [kaliper warn] timeout sending {amp_event.get('event_type')} — skipping")


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run(vertical: str, total_sessions: int) -> None:
    if vertical not in VERTICAL_GENERATORS:
        raise ValueError(
            f"Unknown vertical '{vertical}'. Choose from: {list(VERTICAL_GENERATORS)}"
        )

    if not AMPLITUDE_API_KEY:
        raise RuntimeError("AMPLITUDE_API_KEY is not set. Check your .env file.")

    generator = VERTICAL_GENERATORS[vertical]

    print(f"\nKaliper Bulk Simulator")
    print(f"  Vertical   : {vertical}")
    print(f"  Sessions   : {total_sessions:,}")
    print(f"  Error rate : {int(ERROR_RATE * 100)}%")
    print(f"  Batch size : {BATCH_SIZE}")
    print(f"  Amplitude  : {_api_url()}")
    print(f"  Kaliper    : {KALIPER_INGEST_URL}")
    print()

    user_pool = [_user_id(i) for i in range(1, USER_POOL + 1)]

    batch: list[dict] = []
    total_events_sent = 0
    sessions_done = 0
    errors_injected = 0
    kaliper_ok = True   # flip to False if ingestion server is unreachable
    start_time = time.time()

    for i in range(total_sessions):
        user_id = random.choice(user_pool)
        device_id = _device_id()
        session_ts = _random_past_timestamp(days_back=30)
        country = _country()
        platform = _platform()
        inject_error = random.random() < ERROR_RATE

        session_events = generator(
            user_id, device_id, session_ts, country, platform, inject_error
        )
        batch.extend(session_events)
        sessions_done += 1
        if inject_error:
            errors_injected += 1

        # Dual-write: send session to Kaliper for real-time validation
        if kaliper_ok:
            try:
                _send_session_to_kaliper(session_events, vertical)
            except requests.exceptions.ConnectionError:
                print("  [kaliper] ingestion server not reachable — skipping Kaliper writes for this run.")
                kaliper_ok = False

        # Flush to Amplitude when batch is full or this is the last session
        if len(batch) >= BATCH_SIZE or i == total_sessions - 1:
            _send_batch(batch)
            total_events_sent += len(batch)
            batch = []
            time.sleep(BATCH_DELAY)

        # Progress update every 500 sessions
        if sessions_done % 500 == 0 or sessions_done == total_sessions:
            elapsed = time.time() - start_time
            rate = sessions_done / elapsed if elapsed > 0 else 0
            print(
                f"  [{sessions_done:>5,}/{total_sessions:,}]  "
                f"{total_events_sent:,} events sent  |  "
                f"{rate:.0f} sessions/sec  |  "
                f"{errors_injected} error sessions"
            )

    elapsed = time.time() - start_time
    print(f"\nDone in {elapsed:.1f}s")
    print(f"  Sessions sent      : {sessions_done:,}")
    print(f"  Events sent        : {total_events_sent:,}")
    print(f"  Error sessions     : {errors_injected} ({errors_injected / sessions_done * 100:.1f}%)")
    print(f"  Avg events/session : {total_events_sent / sessions_done:.1f}")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Kaliper bulk session simulator → Amplitude"
    )
    parser.add_argument(
        "--vertical",
        choices=["ecommerce", "saas", "content"],
        default="ecommerce",
        help="Which vertical to simulate (default: ecommerce)",
    )
    parser.add_argument(
        "--sessions",
        type=int,
        default=5000,
        help="Number of user sessions to generate (default: 5000)",
    )
    args = parser.parse_args()
    run(args.vertical, args.sessions)
