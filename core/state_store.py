#core/state_store.py
from dataclasses import dataclass, field
from typing import Any
from core.schemas import IncomingEvent

# Substrings that identify a signup event, regardless of casing or surrounding words.
# Must stay in sync with the login_without_signup check in detectors.py.
_SIGNUP_SUBSTRINGS = {"signup", "sign up", "signed up", "register", "onboarding"}


def _is_signup_event(event_name: str) -> bool:
    """Return True if the event name contains any known signup substring."""
    lowered = event_name.lower()
    return any(substring in lowered for substring in _SIGNUP_SUBSTRINGS)


@dataclass
class StreamState:
    seen_event_ids: set[str] = field(default_factory=set)
    seen_event_names_by_identity: dict[str, set[str]] = field(default_factory=dict)
    seen_order_ids: set[str] = field(default_factory=set)
    completed_order_ids: set[str] = field(default_factory=set)
    checkout_steps: dict[str, int] = field(default_factory=dict)

    # anonymous_id -> canonical user_id
    identity_aliases: dict[str, str] = field(default_factory=dict)

    # Identities that have fired a signup event in this run.
    signed_up_identities: set[str] = field(default_factory=set)

    # Identities that have fired a trial start event in this run.
    trial_started_identities: set[str] = field(default_factory=set)

    # Identities that have fired a subscription event in this run.
    subscription_started_identities: set[str] = field(default_factory=set)

    # identity -> revenue value captured at checkout start, for cross-event consistency.
    checkout_revenue: dict[str, float] = field(default_factory=dict)
    # property name used to read revenue (kept in sync with detectors' property_map).
    revenue_prop: str = "revenue"

    signup_event_name: str | None = None
    purchase_event_name: str | None = None
    order_id_prop: str = "order_id"

    def learn_identity(self, event: IncomingEvent) -> None:
        """
        If we ever see both anonymous_id and user_id together, stitch them.
        """
        if event.user_id and event.anonymous_id:
            self.identity_aliases[event.anonymous_id] = event.user_id

    def resolve_identity(self, event: IncomingEvent) -> str:
        """
        Return the best identity key we can use for this event.
        """
        if event.user_id:
            return event.user_id

        if event.anonymous_id and event.anonymous_id in self.identity_aliases:
            return self.identity_aliases[event.anonymous_id]

        if event.anonymous_id:
            return event.anonymous_id

        return "unknown"

    def mark_event(self, event: IncomingEvent, purchase_roles: set[str] | None = None, order_id_prop: str | None = None) -> None:
        from core.plan_normalizer import normalize_incoming_event_name
        norm_name = normalize_incoming_event_name(event.name)
        identity = self.resolve_identity(event)

        if identity not in self.seen_event_names_by_identity:
            self.seen_event_names_by_identity[identity] = set()

        self.seen_event_names_by_identity[identity].add(norm_name)

        if _is_signup_event(event.name):
            self.signed_up_identities.add(identity)

        if any(s in event.name.lower() for s in (
            "trial started", "trial_started", "trial start", "trial_start",
            "free trial started", "free_trial_started", "free trial start",
        )):
            self.trial_started_identities.add(identity)

        if any(s in event.name.lower() for s in (
            "subscription started", "subscription_started",
            "subscription start", "subscription_start",
            "subscribed", "plan activated", "plan_activated",
        )):
            self.subscription_started_identities.add(identity)

        # Track completed order IDs.
        # Primary path: caller passes purchase_roles so we know exactly when a
        # purchase event fires, and order_id_prop so we read the right property.
        # Fallback: legacy keyword matching for callers that don't pass roles.
        is_purchase = False
        if purchase_roles is not None:
            is_purchase = bool(purchase_roles)  # non-empty means this event IS the purchase
        else:
            event_name_lower = event.name.lower().replace("_", " ").replace(" ", "")
            is_purchase = (
                "purchase" in event_name_lower
                or "ordercompleted" in event_name_lower
                or "orderplaced" in event_name_lower
                or "paymentsuccess" in event_name_lower
                or "paymentcompleted" in event_name_lower
                or "paymentconfirmed" in event_name_lower
                or "transactioncompleted" in event_name_lower
            )

        if is_purchase:
            # Use caller-supplied prop name, then the state's configured default,
            # then fall back through a list of common aliases.
            _oid_candidates = (
                ([order_id_prop] if order_id_prop else [])
                + [self.order_id_prop]
                + ["order_id", "transaction_id", "basket_id", "cart_id", "purchase_id"]
            )
            for _prop in _oid_candidates:
                oid = event.properties.get(_prop)
                if oid:
                    self.completed_order_ids.add(str(oid))
                    break

    def store_checkout_revenue(self, identity: str, revenue: float) -> None:
        """Record the revenue value at checkout start for later consistency check."""
        self.checkout_revenue[identity] = revenue

    def get_checkout_revenue(self, identity: str) -> float | None:
        """Return the stored checkout revenue for this identity, or None."""
        return self.checkout_revenue.get(identity)

    def has_seen_event_id(self, event_id: str) -> bool:
        return event_id in self.seen_event_ids

    def has_seen_event(self, event: IncomingEvent, event_name: str) -> bool:
        from core.plan_normalizer import normalize_incoming_event_name
        norm_event_name = normalize_incoming_event_name(event_name)
        identity = self.resolve_identity(event)
        return norm_event_name in self.seen_event_names_by_identity.get(identity, set())

    def mark_order(self, order_id: str | None) -> bool:
        if not order_id:
            return False

        already_seen = str(order_id) in self.seen_order_ids
        self.seen_order_ids.add(str(order_id))
        return already_seen

    def has_completed_order(self, order_id: str) -> bool:
        return str(order_id) in self.completed_order_ids

    def get_checkout_step(self, identity: str) -> int | None:
        return self.checkout_steps.get(identity)

    def set_checkout_step(self, identity: str, step: int) -> None:
        self.checkout_steps[identity] = step

    def reset(self) -> None:
        self.seen_event_ids.clear()
        self.seen_event_names_by_identity.clear()
        self.seen_order_ids.clear()
        self.completed_order_ids.clear()
        self.checkout_steps.clear()
        self.identity_aliases.clear()
        self.signed_up_identities.clear()
        self.trial_started_identities.clear()
        self.subscription_started_identities.clear()
        self.checkout_revenue.clear()


def make_state_from_profile(profile) -> StreamState:
    fm = getattr(profile, "funnel_map", {})
    pm = getattr(profile, "property_map", {})
    return StreamState(
        signup_event_name=fm.get("signup_event"),
        purchase_event_name=fm.get("purchase_event"),
        order_id_prop=pm.get("order_id_prop", "order_id"),
    )
