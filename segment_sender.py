# segment_sender.py
"""
Sends a realistic mixed stream of ecommerce events to Segment.

Simulates N user sessions where most are clean but ~15% have tracking
issues naturally scattered in — exactly as it would look in production.

Issues injected:
  - revenue sent as a string instead of a number
  - user_id missing on Product Added (identity gap)
  - Checkout Started skipped before Order Completed (funnel violation)
  - duplicate Order Completed (same order_id fired twice)
  - Order Refunded referencing an order that never completed

Usage:
    python segment_sender.py                    # 20 sessions, ecommerce
    python segment_sender.py --sessions 50      # more sessions
    python segment_sender.py --delay 0.2        # faster (0.2s between events)
"""

from __future__ import annotations

import argparse
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SEGMENT_WRITE_KEY = "gXyJRkV6FC81I9QdJjhKOSvMWPU0OXT0"
SEGMENT_TRACK_URL = "https://api.segment.io/v1/track"
_AUTH = (SEGMENT_WRITE_KEY, "")

ERROR_RATE = 0.20   # 1 in 5 sessions has an injected issue

COUNTRIES   = ["IN", "US", "GB", "DE", "SG", "AU"]
PLATFORMS   = ["Web", "iOS", "Android"]
CATEGORIES  = ["Clothing", "Electronics", "Books", "Home", "Beauty"]
CURRENCIES  = ["INR", "USD", "AED", "GBP"]   # must match tracking plan enum
PRODUCTS    = [f"prod_{i:03d}" for i in range(1, 51)]

# ---------------------------------------------------------------------------
# Segment HTTP helper
# ---------------------------------------------------------------------------

def _track(event_name: str, user_id: str | None, anonymous_id: str | None,
           properties: dict, timestamp: str, message_id: str) -> requests.Response:
    payload: dict = {
        "event":      event_name,
        "timestamp":  timestamp,
        "messageId":  message_id,
        "properties": properties,
        "context": {
            "library": {"name": "kaliper-simulator", "version": "1.0.0"}
        },
    }
    if user_id:
        payload["userId"] = user_id
    if anonymous_id:
        payload["anonymousId"] = anonymous_id
    return requests.post(SEGMENT_TRACK_URL, json=payload, auth=_AUTH, timeout=10)


# ---------------------------------------------------------------------------
# Session generators — one realistic user journey per call
# ---------------------------------------------------------------------------

def _ts(base: datetime, offset_s: int) -> str:
    return (base + timedelta(seconds=offset_s)).isoformat()


def _ecommerce_session(user_id: str, anon_id: str, base_ts: datetime,
                       inject_error: bool) -> list[tuple]:
    """
    Returns a list of (event_name, user_id, anon_id, properties, timestamp, message_id).
    inject_error=True scatters one or more tracking issues into this session.
    """
    product_id = random.choice(PRODUCTS)
    order_id   = f"ord_{uuid.uuid4().hex[:8]}"
    revenue    = round(random.uniform(199, 4999), 2)
    quantity   = random.randint(1, 5)
    currency   = random.choice(CURRENCIES)

    events = []

    # -- Page Viewed --
    events.append((
        "Page Viewed",
        user_id, anon_id,
        {
            "page_url":  f"https://shop.example.com/products/{product_id}",
            "session_id": anon_id,
            "referrer":  random.choice(["google", "direct", "instagram", "email"]),
        },
        _ts(base_ts, 0),
        str(uuid.uuid4()),
    ))

    # -- Product Viewed --
    events.append((
        "Product Viewed",
        user_id, anon_id,
        {
            "product_id": product_id,
            "title":      f"Product {product_id}",
            "category":   random.choice(CATEGORIES),
            "price":      revenue,
        },
        _ts(base_ts, random.randint(5, 20)),
        str(uuid.uuid4()),
    ))

    # 30% of clean sessions drop off here (browsed but didn't add to cart)
    if not inject_error and random.random() < 0.30:
        return events

    # -- Product Added --
    # Error type 1: identity gap — user_id stripped on this event
    uid_for_add = None if (inject_error and random.random() < 0.5) else user_id
    events.append((
        "Product Added",
        uid_for_add, anon_id,
        {
            "product_id": product_id,
            "quantity":   quantity,
            "price":      revenue,
        },
        _ts(base_ts, random.randint(25, 50)),
        str(uuid.uuid4()),
    ))

    # Error type 2: funnel violation — skip Checkout Started before Order Completed
    skip_checkout = inject_error and random.random() < 0.4
    if not skip_checkout:
        events.append((
            "Checkout Started",
            user_id, anon_id,
            {
                "cart_id":    f"cart_{uuid.uuid4().hex[:8]}",
                "revenue":    round(revenue * quantity, 2),
                "currency":   currency,
            },
            _ts(base_ts, random.randint(55, 90)),
            str(uuid.uuid4()),
        ))

    # -- Order Completed --
    # Error type 3: revenue sent as a string instead of a float
    revenue_val = str(revenue) if (inject_error and random.random() < 0.5) else revenue
    events.append((
        "Order Completed",
        user_id, anon_id,
        {
            "order_id":  order_id,
            "revenue":   revenue_val,
            "currency":  currency,
            "quantity":  quantity,
        },
        _ts(base_ts, random.randint(100, 180)),
        str(uuid.uuid4()),
    ))

    # Error type 4: duplicate Order Completed (same order_id, same revenue)
    if inject_error and random.random() < 0.3:
        events.append((
            "Order Completed",
            user_id, anon_id,
            {
                "order_id":  order_id,          # same order_id = duplicate
                "revenue":   revenue,
                "currency":  currency,
                "quantity":  quantity,
            },
            _ts(base_ts, random.randint(181, 200)),
            str(uuid.uuid4()),
        ))

    # 20% of completed orders get a refund — 
    # error type 5: wrong order_id on refund (id mismatch)
    if random.random() < 0.20:
        refund_order_id = f"ord_{uuid.uuid4().hex[:8]}" if inject_error else order_id
        events.append((
            "Order Refunded",
            user_id, anon_id,
            {
                "order_id":      refund_order_id,
                "refund_amount": revenue,
            },
            _ts(base_ts, random.randint(300, 600)),
            str(uuid.uuid4()),
        ))

    return events


# ---------------------------------------------------------------------------
# Main sender
# ---------------------------------------------------------------------------

def run(total_sessions: int, delay: float):
    print(f"\n{'='*60}")
    print(f"  Kaliper -> Segment Stream Simulator")
    print(f"  Sessions  : {total_sessions}")
    print(f"  Error rate: {int(ERROR_RATE * 100)}%  (issues scattered across sessions)")
    print(f"  Delay     : {delay}s between events")
    print(f"{'='*60}\n")

    total_events  = 0
    error_sessions = 0
    ok_count      = 0
    fail_count    = 0

    for s in range(1, total_sessions + 1):
        user_id    = f"user_{random.randint(1, 500):04d}"
        anon_id    = f"anon_{uuid.uuid4().hex[:10]}"
        base_ts    = datetime.now(timezone.utc) - timedelta(
            minutes=random.randint(0, 60 * 24 * 7)  # events spread over last week
        )
        inject     = random.random() < ERROR_RATE
        session    = _ecommerce_session(user_id, anon_id, base_ts, inject)

        if inject:
            error_sessions += 1

        tag = " [ISSUES]" if inject else ""
        print(f"  Session {s:>3}/{total_sessions}{tag}  -  user={user_id}  ({len(session)} events)")

        for (name, uid, aid, props, ts, mid) in session:
            resp = _track(name, uid, aid, props, ts, mid)
            symbol = "OK" if resp.status_code == 200 else "FAIL"
            print(f"    {symbol}  {name:<30}  HTTP {resp.status_code}")
            if resp.status_code == 200:
                ok_count += 1
            else:
                fail_count += 1
                print(f"       -> {resp.text[:120]}")
            total_events += 1
            if delay > 0:
                time.sleep(delay)

        print()

    print(f"{'='*60}")
    print(f"  Done.")
    print(f"  Sessions sent  : {total_sessions}  ({error_sessions} with injected issues)")
    print(f"  Events sent    : {total_events}  ({ok_count} ok, {fail_count} failed)")
    print(f"  OK Segment received these events.")
    print(f"  Once your Kaliper webhook destination is configured in Segment,")
    print(f"  all events will flow through automatically.")
    print(f"{'='*60}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Send a realistic mixed event stream to Segment"
    )
    parser.add_argument("--sessions", type=int, default=20,
                        help="Number of user sessions to simulate (default: 20)")
    parser.add_argument("--delay",    type=float, default=0.3,
                        help="Seconds between events (default: 0.3)")
    args = parser.parse_args()
    run(args.sessions, args.delay)
