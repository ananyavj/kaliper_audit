#simulators/simulators.py
from uuid import uuid4
from datetime import datetime, timezone

from core.schemas import IncomingEvent


def _now():
    return datetime.now(timezone.utc).isoformat()


def generate_clean_flow(user_id="user_1"):
    ts = _now()
    anon_id = "anon_1"

    return [
        IncomingEvent(
            name="Page Viewed",
            user_id=user_id,
            anonymous_id=anon_id,
            timestamp=ts,
            properties={
                "page_url": "https://example.com/products/t-shirt",
                "session_id": "sess_1",
            },
            event_id=str(uuid4()),
        ),
        IncomingEvent(
            name="Product Viewed",
            user_id=user_id,
            anonymous_id=anon_id,
            timestamp=ts,
            properties={
                "product_id": "p1",
                "title": "T-Shirt",
                "price": 499.0,
            },
            event_id=str(uuid4()),
        ),
        IncomingEvent(
            name="Product Added",
            user_id=user_id,
            anonymous_id=anon_id,
            timestamp=ts,
            properties={
                "product_id": "p1",
                "quantity": 1,
                "price": 499.0,
            },
            event_id=str(uuid4()),
        ),
        IncomingEvent(
            name="Checkout Started",
            user_id=user_id,
            anonymous_id=anon_id,
            timestamp=ts,
            properties={
                "cart_id": "cart_1",
                "revenue": 499.0,
                "currency": "INR",
            },
            event_id=str(uuid4()),
        ),
        IncomingEvent(
            name="Order Completed",
            user_id=user_id,
            anonymous_id=anon_id,
            timestamp=ts,
            properties={
                "order_id": "ord_1",
                "revenue": 499.0,
                "currency": "INR",
            },
            event_id=str(uuid4()),
        ),
    ]


def generate_flow_with_errors():
    ts = _now()

    return [
        IncomingEvent(
            name="Product Viewed",
            user_id="user_1",
            anonymous_id="anon_1",
            timestamp=ts,
            properties={
                "product_id": "p1",
                "title": "T-Shirt",
            },
            event_id=str(uuid4()),
        ),
        IncomingEvent(
            name="Product Added",
            user_id=None,  # error: missing identity
            anonymous_id="anon_1",
            timestamp=ts,
            properties={
                "product_id": "p1",
                "quantity": 1,
            },
            event_id=str(uuid4()),
        ),
        # skip error: Checkout Started is intentionally omitted here so that
        # Order Completed fires without a preceding Checkout Started.
        # This triggers purchase_without_checkout.
        IncomingEvent(
            name="Order Completed",
            user_id="user_1",
            anonymous_id="anon_1",
            timestamp=ts,
            properties={
                "order_id": "ord_1",
                "revenue": "499",  # error: should be number
                "currency": "INR",
            },
            event_id=str(uuid4()),
        ),
        # id_mismatch error: Order Refunded references a different order_id
        # than the one completed above, so no matching purchase exists.
        IncomingEvent(
            name="Order Refunded",
            user_id="user_1",
            anonymous_id="anon_1",
            timestamp=ts,
            properties={
                "order_id": "ord_WRONG",  # error: id_mismatch — does not match ord_1
                "refund_amount": 499.0,
            },
            event_id=str(uuid4()),
        ),
        IncomingEvent(
            name="Order Completed",
            user_id="user_1",
            anonymous_id="anon_1",
            timestamp=ts,
            properties={
                "order_id": "ord_1",  # duplicate purchase
                "revenue": 499.0,
                "currency": "INR",
            },
            event_id=str(uuid4()),
        ),
    ]