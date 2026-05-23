#simulators/simulators_saas.py
from uuid import uuid4
from datetime import datetime, timezone

from core.schemas import IncomingEvent


def _now():
    return datetime.now(timezone.utc).isoformat()


def generate_saas_clean_flow(user_id="user_1", anonymous_id="anon_1"):
    ts = _now()

    return [
        IncomingEvent(
            name="Signup",
            user_id=user_id,
            anonymous_id=anonymous_id,
            timestamp=ts,
            properties={
                "account_id": "acc_1",
                "plan": "Free",
            },
            event_id=str(uuid4()),
        ),
        IncomingEvent(
            name="Login",
            user_id=user_id,
            anonymous_id=anonymous_id,
            timestamp=ts,
            properties={
                "method": "password",
            },
            event_id=str(uuid4()),
        ),
        IncomingEvent(
            name="Trial Started",
            user_id=user_id,
            anonymous_id=anonymous_id,
            timestamp=ts,
            properties={
                "trial_id": "trial_1",
                "plan": "Pro",
            },
            event_id=str(uuid4()),
        ),
        IncomingEvent(
            name="Subscription Started",
            user_id=user_id,
            anonymous_id=anonymous_id,
            timestamp=ts,
            properties={
                "subscription_id": "sub_1",
                "plan": "Pro",
                "billing_status": "active",
            },
            event_id=str(uuid4()),
        ),
    ]


def generate_saas_flow_with_errors():
    ts = _now()

    return [
        IncomingEvent(
            name="Login",
            user_id="user_1",
            anonymous_id="anon_1",
            timestamp=ts,
            properties={
                "method": "password",
            },
            event_id=str(uuid4()),
        ),
        IncomingEvent(
            name="Trial Started",
            user_id=None,  # error: missing identity
            anonymous_id="anon_1",
            timestamp=ts,
            properties={
                "trial_id": "trial_1",
                "plan": "Pro",
            },
            event_id=str(uuid4()),
        ),
        IncomingEvent(
            name="Signup",
            user_id="user_1",
            anonymous_id="anon_1",
            timestamp=ts,
            properties={
                "account_id": "acc_1",
                "plan": "Free",
            },
            event_id=str(uuid4()),
        ),
        IncomingEvent(
            name="Subscription Started",
            user_id="user_1",
            anonymous_id="anon_1",
            timestamp=ts,
            properties={
                "subscription_id": "sub_1",
                "plan": "Pro",
                "billing_status": "active",
            },
            event_id=str(uuid4()),
        ),
        IncomingEvent(
            name="Subscription Started",
            user_id="user_1",
            anonymous_id="anon_1",
            timestamp=ts,
            properties={
                "subscription_id": "sub_1",
                "plan": "Pro",
                "billing_status": "active",
            },
            event_id=str(uuid4()),
        ),
    ]