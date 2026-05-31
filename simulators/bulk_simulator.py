"""
bulk_simulator.py
-----------------
Generates and sends N realistic user sessions to Amplitude and/or Segment.
"""

from __future__ import annotations

import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from dotenv import load_dotenv
from faker import Faker

load_dotenv()

fake = Faker()

AMPLITUDE_API_KEY = os.getenv("AMPLITUDE_API_KEY", "")
AMPLITUDE_REGION = os.getenv("AMPLITUDE_REGION", "default").strip().lower()
SEGMENT_WRITE_KEY = os.getenv("SEGMENT_WRITE_KEY", "gXyJRkV6FC81I9QdJjhKOSvMWPU0OXT0")
KALIPER_INGEST_URL = os.getenv("KALIPER_INGEST_URL", "http://127.0.0.1:5000/ingest")

BATCH_SIZE = 10
BATCH_DELAY = 0.15
USER_POOL = 3000

COUNTRIES = ["IN", "US", "GB", "DE", "SG", "AU", "CA", "BR"]
PLATFORMS = ["Web", "iOS", "Android"]
COUNTRY_WEIGHTS = [0.35, 0.25, 0.10, 0.07, 0.05, 0.05, 0.05, 0.08]
PLATFORM_WEIGHTS = [0.55, 0.25, 0.20]

def _api_url() -> str:
    if AMPLITUDE_REGION == "eu":
        return "https://api.eu.amplitude.com/2/httpapi"
    return "https://api2.amplitude.com/2/httpapi"

def _random_past_timestamp(days_back: int = 30) -> datetime:
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
    event_type: str, user_id: str | None, device_id: str,
    session_ts: datetime, offset_seconds: int, properties: dict[str, Any],
    country: str, platform: str
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

# --- Generators ---

def _ecommerce_session(user_id: str, device_id: str, ts: datetime, country: str, platform: str, inject_error: bool) -> list[dict]:
    product_id = f"prod_{fake.uuid4()[:8]}"
    order_id = f"ord_{fake.uuid4()[:8]}"
    revenue = round(random.uniform(19, 499), 2)
    quantity = random.randint(1, 5)

    steps = [
        ("Product Viewed", 0, {
            "product_id": product_id, "title": fake.catch_phrase(),
            "category": random.choice(["Clothing", "Electronics", "Books", "Home"]),
            "price": revenue
        }),
        ("Product Added", random.randint(5, 30), {
            "product_id": product_id, "quantity": quantity, "price": revenue
        }),
        ("Checkout Started", random.randint(35, 90), {
            "cart_id": f"cart_{fake.uuid4()[:8]}", "cart_value": round(revenue * quantity, 2)
        }),
        ("Order Completed", random.randint(100, 200), {
            "order_id": order_id, "revenue": str(revenue) if inject_error else revenue, # ERROR: string instead of float
            "currency": random.choice(["INR", "USD", "GBP"]), "quantity": quantity
        }),
    ]

    if not inject_error and random.random() < 0.30:
        steps = steps[:2]

    result = []
    for name, offset, props in steps:
        uid = None if (inject_error and name == "Product Added") else user_id # ERROR: missing user_id
        result.append(_build_amplitude_event(name, uid, device_id, ts, offset, props, country, platform))
    return result


def _saas_session(user_id: str, device_id: str, ts: datetime, country: str, platform: str, inject_error: bool) -> list[dict]:
    account_id = f"acc_{fake.company().replace(' ', '_').lower()}"
    plan = random.choice(["Free", "Pro", "Enterprise"])
    feature = random.choice(["dashboard", "reports", "integrations", "api_access", "alerts"])

    steps = [
        ("Login", 0, {"method": random.choice(["password", "google_oauth", "sso"]), "account_id": account_id}),
        ("Feature Used", random.randint(10, 60), {"feature_name": feature, "plan": plan}),
        ("Report Generated", random.randint(65, 150), {
            "report_type": random.choice(["funnel", "retention", "segmentation"]),
            "rows": "many" if inject_error else random.randint(10, 5000), # ERROR: rows as string
        }),
    ]
    if random.random() < 0.20:
        steps = [
            ("Signup", 0, {"account_id": account_id, "plan": plan}),
            ("Trial Started", random.randint(5, 20), {"trial_id": f"trial_{fake.uuid4()[:6]}", "plan": "Pro"}),
        ] + steps

    result = []
    for name, offset, props in steps:
        uid = None if (inject_error and name == "Feature Used") else user_id # ERROR: missing user_id
        result.append(_build_amplitude_event(name, uid, device_id, ts, offset, props, country, platform))
    return result


def _content_session(user_id: str, device_id: str, ts: datetime, country: str, platform: str, inject_error: bool) -> list[dict]:
    article_id = f"art_{fake.word()}"
    video_id = f"vid_{fake.uuid4()[:8]}"
    session_id = f"sess_{fake.uuid4()[:10]}"
    watch_duration = random.randint(30, 1800)

    steps = [
        ("Page Viewed", 0, {
            "page_url": fake.url(), "session_id": session_id,
            "referrer": random.choice(["google", "direct", "twitter", "newsletter"]),
        }),
        ("Article Read", random.randint(10, 60), {
            "article_id": article_id, "duration_seconds": random.randint(30, 600), "scroll_depth_pct": random.randint(20, 100),
        }),
    ]
    if random.random() < 0.40:
        steps += [
            ("Video Started", random.randint(65, 120), {"video_id": video_id, "duration_seconds": watch_duration}),
            ("Video Completed", random.randint(125, 125 + watch_duration), {
                "video_id": video_id, "completion_pct": random.randint(70, 100),
                "duration_seconds": "long" if inject_error else watch_duration, # ERROR: duration as string
            }),
        ]

    result = []
    for name, offset, props in steps:
        uid = None if (inject_error and name == "Article Read") else user_id # ERROR: missing user_id
        result.append(_build_amplitude_event(name, uid, device_id, ts, offset, props, country, platform))
    return result


def _b2b_wholesale_session(user_id: str, device_id: str, ts: datetime, country: str, platform: str, inject_error: bool) -> list[dict]:
    account_id = f"b2b_{fake.company().replace(' ', '_').lower()}"
    quote_id = f"quote_{fake.uuid4()[:8]}"
    value = round(random.uniform(5000, 50000), 2)
    
    steps = [
        ("Account Created", 0, {"account_id": account_id, "company_size": random.randint(10, 5000)}),
        ("Quote Requested", random.randint(10, 100), {"quote_id": quote_id, "items_count": random.randint(100, 1000)}),
    ]
    if not inject_error and random.random() < 0.5:
        # Funnel continues
        steps += [
            ("Quote Approved", random.randint(200, 500), {"quote_id": quote_id, "approver_role": "Manager"}),
            ("Bulk Order Placed", random.randint(600, 1000), {
                "order_id": f"ord_{fake.uuid4()[:8]}", 
                "total_value": str(value) if inject_error else value # ERROR: string value
            }),
            ("Contract Signed", random.randint(1200, 2000), {"contract_id": f"ctr_{fake.uuid4()[:8]}"}),
        ]
        
    result = []
    for name, offset, props in steps:
        uid = None if (inject_error and name == "Quote Approved") else user_id # ERROR: missing user
        result.append(_build_amplitude_event(name, uid, device_id, ts, offset, props, country, platform))
    return result

def _digital_goods_session(user_id: str, device_id: str, ts: datetime, country: str, platform: str, inject_error: bool) -> list[dict]:
    asset_id = f"asset_{fake.uuid4()[:6]}"
    license_id = f"lic_{fake.uuid4()[:8]}"
    
    steps = [
        ("Preview Played", 0, {"asset_id": asset_id, "preview_duration": random.randint(10, 60)}),
    ]
    if random.random() < 0.7:
        steps += [
            ("License Purchased", random.randint(100, 300), {
                "asset_id": asset_id, "license_type": random.choice(["Standard", "Extended", "Commercial"]),
                "price": str(random.randint(10, 99)) if inject_error else random.randint(10, 99) # ERROR
            }),
            ("Download Started", random.randint(310, 350), {"asset_id": asset_id, "file_size_mb": random.randint(5, 500)}),
        ]
        if not inject_error or random.random() < 0.5:
            steps.append(("Download Completed", random.randint(360, 500), {"asset_id": asset_id, "time_taken_sec": random.randint(5, 120)}))
            
    result = []
    for name, offset, props in steps:
        uid = None if (inject_error and name == "Download Started") else user_id
        result.append(_build_amplitude_event(name, uid, device_id, ts, offset, props, country, platform))
    return result

def _subscription_box_session(user_id: str, device_id: str, ts: datetime, country: str, platform: str, inject_error: bool) -> list[dict]:
    sub_id = f"sub_{fake.uuid4()[:8]}"
    box_type = random.choice(["Meals", "Beauty", "Snacks"])
    
    steps = [
        ("Subscription Started", 0, {"subscription_id": sub_id, "box_type": box_type, "tier": "Premium"}),
        ("Box Customization Saved", random.randint(10, 50), {
            "subscription_id": sub_id, 
            "items_selected": "many" if inject_error else random.randint(3, 8) # ERROR
        }),
        ("Box Shipped", random.randint(1000, 5000), {"subscription_id": sub_id, "tracking_number": fake.uuid4()[:10]}),
    ]
    if random.random() < 0.2:
        steps.append(("Subscription Paused", random.randint(6000, 10000), {"subscription_id": sub_id, "reason": "Vacation"}))
    elif random.random() < 0.1:
        steps.append(("Churned", random.randint(6000, 10000), {"subscription_id": sub_id, "reason": "Too Expensive"}))

    result = []
    for name, offset, props in steps:
        uid = None if (inject_error and name == "Box Customization Saved") else user_id
        result.append(_build_amplitude_event(name, uid, device_id, ts, offset, props, country, platform))
    return result


VERTICAL_GENERATORS = {
    "ecommerce": _ecommerce_session,
    "saas": _saas_session,
    "content": _content_session,
    "b2b_wholesale": _b2b_wholesale_session,
    "digital_goods": _digital_goods_session,
    "subscription_box": _subscription_box_session,
}

ERROR_DESCRIPTIONS = {
    "ecommerce": "Missing user_id on 'Product Added', revenue sent as a string on 'Order Completed', truncated funnels.",
    "saas": "Missing user_id on 'Feature Used', 'rows' sent as a string on 'Report Generated'.",
    "content": "Missing user_id on 'Article Read', 'duration_seconds' sent as a string on 'Video Completed'.",
    "b2b_wholesale": "Missing user_id on 'Quote Approved', 'total_value' sent as a string on 'Bulk Order Placed'.",
    "digital_goods": "Missing user_id on 'Download Started', 'price' sent as a string on 'License Purchased'.",
    "subscription_box": "Missing user_id on 'Box Customization Saved', 'items_selected' sent as string."
}


# --- Senders ---

def _send_batch_to_amplitude(events: list[dict]) -> None:
    if not AMPLITUDE_API_KEY:
        return
    body = {"api_key": AMPLITUDE_API_KEY, "events": events, "options": {"min_id_length": 1}}
    response = requests.post(_api_url(), json=body, timeout=30)
    if response.status_code not in (200, 202):
        print(f"  [amplitude err] {response.status_code} {response.text}")

def _send_batch_to_segment(events: list[dict]) -> None:
    if not SEGMENT_WRITE_KEY:
        return
    batch = []
    for e in events:
        # Convert amplitude event to segment format
        seg_event = {
            "type": "track",
            "event": e["event_type"],
            "userId": e.get("user_id"),
            "anonymousId": e.get("device_id"),
            "properties": e.get("event_properties", {}),
            "timestamp": datetime.fromtimestamp(e["time"]/1000, tz=timezone.utc).isoformat()
        }
        batch.append(seg_event)
        
    body = {"batch": batch}
    auth = (SEGMENT_WRITE_KEY, "")
    response = requests.post("https://api.segment.io/v1/batch", json=body, auth=auth, timeout=30)
    if response.status_code not in (200, 202):
        print(f"  [segment err] {response.status_code} {response.text}")

def _send_session_to_kaliper(events: list[dict], vertical: str) -> None:
    workspace_id = f"{vertical}_workspace"
    for amp_event in events:
        kaliper_event = {
            "name": amp_event.get("event_type"),
            "user_id": amp_event.get("user_id"),
            "anonymous_id": amp_event.get("device_id"),
            "timestamp": datetime.fromtimestamp(amp_event["time"] / 1000, tz=timezone.utc).isoformat(),
            "event_id": amp_event.get("insert_id", ""),
            "properties": amp_event.get("event_properties", {}),
        }
        envelope = {
            "tenant_id": "tenant_demo", "workspace_id": workspace_id,
            "environment": "production", "source": "simulator", "event": kaliper_event,
        }
        try:
            resp = requests.post(KALIPER_INGEST_URL, json=envelope, timeout=5)
        except Exception:
            pass

def run(vertical: str, total_sessions: int, error_rate: float, dest_choice: str) -> None:
    generator = VERTICAL_GENERATORS[vertical]

    print("\n" + "="*60)
    print(f"  Kaliper Bulk Simulator ({vertical})")
    print(f"  Sessions   : {total_sessions:,}")
    print(f"  Error rate : {int(error_rate * 100)}%")
    print(f"  Dest       : {dest_choice}")
    print(f"  Errors     : {ERROR_DESCRIPTIONS[vertical]}")
    print("="*60 + "\n")

    user_pool = [_user_id(i) for i in range(1, USER_POOL + 1)]
    batch: list[dict] = []
    total_events_sent = 0
    sessions_done = 0
    errors_injected = 0
    start_time = time.time()

    for i in range(total_sessions):
        user_id = random.choice(user_pool)
        device_id = _device_id()
        session_ts = _random_past_timestamp(days_back=30)
        inject_error = random.random() < error_rate

        session_events = generator(user_id, device_id, session_ts, _country(), _platform(), inject_error)
        batch.extend(session_events)
        sessions_done += 1
        if inject_error:
            errors_injected += 1

        _send_session_to_kaliper(session_events, vertical)

        if len(batch) >= BATCH_SIZE or i == total_sessions - 1:
            if dest_choice in ("amplitude", "both"):
                _send_batch_to_amplitude(batch)
            if dest_choice in ("segment", "both"):
                _send_batch_to_segment(batch)
                
            total_events_sent += len(batch)
            batch = []
            time.sleep(BATCH_DELAY)

        if sessions_done % max(1, total_sessions // 10) == 0 or sessions_done == total_sessions:
            print(f"  [{sessions_done:>5,}/{total_sessions:,}]  {total_events_sent:,} events sent  |  {errors_injected} error sessions")

    print(f"\nDone in {time.time() - start_time:.1f}s")


if __name__ == "__main__":
    print("Welcome to the Kaliper Simulator!")
    
    verts = list(VERTICAL_GENERATORS.keys())
    for idx, v in enumerate(verts, 1):
        print(f"  {idx}. {v}")
        
    v_idx = input(f"Choose a vertical (1-{len(verts)}): ").strip()
    try:
        vertical = verts[int(v_idx) - 1]
    except (ValueError, IndexError):
        vertical = "ecommerce"
        print(f"Invalid input. Defaulting to {vertical}.")

    s_in = input("Number of sessions (0-500): ").strip()
    try:
        sessions = min(max(int(s_in), 0), 500)
    except ValueError:
        sessions = 20
        print(f"Invalid input. Defaulting to {sessions}.")

    e_in = input("Error rate (0%-100%, e.g. 15): ").strip()
    try:
        error_rate = min(max(float(e_in.replace('%','')), 0.0), 100.0) / 100.0
    except ValueError:
        error_rate = 0.15
        print(f"Invalid input. Defaulting to {error_rate*100}%.")
        
    print("\nDestinations:")
    print("  1. Amplitude")
    print("  2. Segment")
    print("  3. Both")
    d_idx = input("Choose a destination (1-3): ").strip()
    
    dest_map = {"1": "amplitude", "2": "segment", "3": "both"}
    dest_choice = dest_map.get(d_idx, "amplitude")
    
    run(vertical, sessions, error_rate, dest_choice)
