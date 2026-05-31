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
                "plan": "free",
                "signup_method": "google",
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
                "plan": "pro",
                "trial_length_days": 14,
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
                "plan": "pro",
                "billing_status": "active",
                "mrr": 2999.0,
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
            anonymous_id="anon_unstitched",  # unique anon_id — never stitched to user_1
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
        # skip error: Subscription Started fires without a trial_length_days
        # property, simulating an incomplete event that skips a required field.
        # This triggers missing_required_property on Trial Started (trial_length_days
        # omitted above) and subscription_without_trial for this event.
        IncomingEvent(
            name="Subscription Started",
            user_id="user_1",
            anonymous_id="anon_1",
            timestamp=ts,
            properties={
                "subscription_id": "sub_1",
                "plan": "Pro",
                "billing_status": "active",
                # skip error: mrr intentionally omitted (required property)
            },
            event_id=str(uuid4()),
        ),
        # id_mismatch error: Subscription Cancelled references a different
        # subscription_id than the one started above.
        IncomingEvent(
            name="Subscription Cancelled",
            user_id="user_1",
            anonymous_id="anon_1",
            timestamp=ts,
            properties={
                "subscription_id": "sub_WRONG",  # error: id_mismatch — does not match sub_1
                "reason": "too_expensive",
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