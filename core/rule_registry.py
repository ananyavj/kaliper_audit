#core/rule_registry.py
from dataclasses import dataclass, field
from typing import FrozenSet, Literal

Severity = Literal["low", "medium", "high", "critical"]
Domain = Literal["generic", "ecommerce", "saas", "content"]


@dataclass(frozen=True)
class RuleSpec:
    key: str
    description: str
    severity: Severity
    domains: FrozenSet[str] = field(default_factory=frozenset)
    needs_state: bool = False


RULES: dict[str, RuleSpec] = {
    "duplicate_event_id": RuleSpec(
        key="duplicate_event_id",
        description="Detect repeated event IDs, retries, or replay bugs.",
        severity="high",
        domains=frozenset({"generic", "ecommerce", "saas", "content"}),
        needs_state=True,
    ),
    "unknown_event": RuleSpec(
        key="unknown_event",
        description="Detect events that are not present in the tracking plan.",
        severity="medium",
        domains=frozenset({"generic", "ecommerce", "saas", "content"}),
    ),
    "missing_required_property": RuleSpec(
        key="missing_required_property",
        description="Detect missing required properties.",
        severity="high",
        domains=frozenset({"generic", "ecommerce", "saas", "content"}),
    ),
    "wrong_property_type": RuleSpec(
        key="wrong_property_type",
        description="Detect properties with the wrong data type.",
        severity="high",
        domains=frozenset({"generic", "ecommerce", "saas", "content"}),
    ),
    "missing_identity": RuleSpec(
        key="missing_identity",
        description="Detect events that require identity but do not have it.",
        severity="high",
        domains=frozenset({"generic", "ecommerce", "saas", "content"}),
        needs_state=True,
    ),
    "sequence_validation": RuleSpec(
        key="sequence_validation",
        description="Detect invalid event ordering based on the tracking plan.",
        severity="medium",
        domains=frozenset({"generic", "ecommerce", "saas", "content"}),
        needs_state=True,
    ),
    "duplicate_purchase": RuleSpec(
        key="duplicate_purchase",
        description="Detect repeated purchases with the same order_id.",
        severity="critical",
        domains=frozenset({"ecommerce"}),
        needs_state=True,
    ),
    "purchase_without_checkout": RuleSpec(
        key="purchase_without_checkout",
        description="Detect purchase events that happen without checkout.",
        severity="critical",
        domains=frozenset({"ecommerce"}),
        needs_state=True,
    ),
    "revenue_currency_validation": RuleSpec(
        key="revenue_currency_validation",
        description="Validate revenue and currency fields on commerce events.",
        severity="high",
        domains=frozenset({"ecommerce"}),
        needs_state=False,
    ),
    "login_without_signup": RuleSpec(
        key="login_without_signup",
        description="Detect login before signup.",
        severity="high",
        domains=frozenset({"saas"}),
        needs_state=True,
    ),
    "trial_without_signup": RuleSpec(
        key="trial_without_signup",
        description="Detect trial start before signup.",
        severity="high",
        domains=frozenset({"saas"}),
        needs_state=True,
    ),
    "subscription_without_trial": RuleSpec(
        key="subscription_without_trial",
        description="Detect subscription start before trial or signup.",
        severity="high",
        domains=frozenset({"saas"}),
        needs_state=True,
    ),
    "article_read_without_page_view": RuleSpec(
        key="article_read_without_page_view",
        description="Detect content reads before any page view.",
        severity="medium",
        domains=frozenset({"content"}),
        needs_state=True,
    ),
    "video_complete_without_video_start": RuleSpec(
        key="video_complete_without_video_start",
        description="Detect video completion before video start.",
        severity="medium",
        domains=frozenset({"content"}),
        needs_state=True,
    ),
    "content_duration_validation": RuleSpec(
        key="content_duration_validation",
        description="Validate duration fields on content events.",
        severity="medium",
        domains=frozenset({"content"}),
        needs_state=False,
    ),
}


DOMAIN_PACKS: dict[str, list[str]] = {
    "generic": [
        "duplicate_event_id",
        "unknown_event",
        "missing_required_property",
        "wrong_property_type",
        "missing_identity",
        "sequence_validation",
    ],
    "ecommerce": [
        "duplicate_event_id",
        "unknown_event",
        "missing_required_property",
        "wrong_property_type",
        "missing_identity",
        "sequence_validation",
        "duplicate_purchase",
        "purchase_without_checkout",
        "revenue_currency_validation",
    ],
    "saas": [
        "duplicate_event_id",
        "unknown_event",
        "missing_required_property",
        "wrong_property_type",
        "missing_identity",
        "sequence_validation",
        "login_without_signup",
        "trial_without_signup",
        "subscription_without_trial",
    ],
    "content": [
        "duplicate_event_id",
        "unknown_event",
        "missing_required_property",
        "wrong_property_type",
        "missing_identity",
        "sequence_validation",
        "article_read_without_page_view",
        "video_complete_without_video_start",
        "content_duration_validation",
    ],
}


def get_rule_spec(key: str) -> RuleSpec | None:
    return RULES.get(key)


def get_rules_for_domain(domain: str) -> list[RuleSpec]:
    keys = DOMAIN_PACKS.get(domain, DOMAIN_PACKS["generic"])
    return [RULES[key] for key in keys if key in RULES]


def get_rule_keys_for_domain(domain: str) -> list[str]:
    return list(DOMAIN_PACKS.get(domain, DOMAIN_PACKS["generic"]))