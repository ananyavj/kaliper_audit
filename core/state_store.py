#core/state_store.py
from dataclasses import dataclass, field
from typing import Any

from core.schemas import IncomingEvent

# Substrings that identify a signup event, regardless of casing or surrounding words.
# Must stay in sync with the login_without_signup check in detectors.py.
_SIGNUP_SUBSTRINGS = {"signup", "sign up", "signed up", "register"}


def _is_signup_event(event_name: str) -> bool:
    """Return True if the event name contains any known signup substring."""
    lowered = event_name.lower()
    return any(substring in lowered for substring in _SIGNUP_SUBSTRINGS)


@dataclass
class StreamState:
    seen_event_ids: set[str] = field(default_factory=set)
    seen_event_names_by_identity: dict[str, set[str]] = field(default_factory=dict)
    seen_order_ids: set[str] = field(default_factory=set)

    # anonymous_id -> canonical user_id
    identity_aliases: dict[str, str] = field(default_factory=dict)

    # Identities that have fired a signup event in this run.
    # Used to suppress login_without_signup false positives for returning
    # users who signed up in a previous run (not in this session window).
    # Detection uses substring matching (via _is_signup_event) so that
    # "Signed Up", "User Signed Up", "sign up completed" etc. all count.
    signed_up_identities: set[str] = field(default_factory=set)

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

    def mark_event(self, event: IncomingEvent) -> None:
        identity = self.resolve_identity(event)

        # Note: seen_event_ids is managed directly by the detector
        # to ensure correct duplicate detection ordering.
        # Only track per-identity event names here.

        if identity not in self.seen_event_names_by_identity:
            self.seen_event_names_by_identity[identity] = set()

        self.seen_event_names_by_identity[identity].add(event.name)

        # Bug 3 fix: use substring matching (same logic as detectors.py _name_matches)
        # so that "Signed Up", "User Signed Up", "sign up completed" etc. all suppress
        # the login_without_signup false positive for returning users in this session.
        if _is_signup_event(event.name):
            self.signed_up_identities.add(identity)

    def has_seen_event_id(self, event_id: str) -> bool:
        return event_id in self.seen_event_ids

    def has_seen_event(self, event: IncomingEvent, event_name: str) -> bool:
        identity = self.resolve_identity(event)
        return event_name in self.seen_event_names_by_identity.get(identity, set())

    def mark_order(self, order_id: str | None) -> bool:
        if not order_id:
            return False

        already_seen = order_id in self.seen_order_ids
        self.seen_order_ids.add(order_id)
        return already_seen

    def reset(self) -> None:
        self.seen_event_ids.clear()
        self.seen_event_names_by_identity.clear()
        self.seen_order_ids.clear()
        self.identity_aliases.clear()
        self.signed_up_identities.clear()
