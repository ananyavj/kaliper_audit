#core/plan_analyzer.py
from dataclasses import dataclass, field
from typing import List

from core.schemas import TrackingEventSpec

ECOMMERCE_KEYWORDS = {
    "product",
    "cart",
    "checkout",
    "order",
    "purchase",
    "payment",
    "revenue",
    "sku",
}

SAAS_KEYWORDS = {
    "signup",
    "sign up",
    "login",
    "log in",
    "identify",
    "trial",
    "subscription",
    "billing",
    "onboarding",
    "activate",
}

CONTENT_KEYWORDS = {
    "page",
    "screen",
    "article",
    "video",
    "watch",
    "read",
    "scroll",
    "engagement",
    "session",
}

COMMERCE_PROPERTY_KEYS = {
    "product_id",
    "cart_id",
    "order_id",
    "quantity",
    "revenue",
    "currency",
    "sku",
}

SAAS_PROPERTY_KEYS = {
    "user_id",
    "account_id",
    "workspace_id",
    "trial_id",
    "plan",
    "subscription_id",
    "billing_status",
}

CONTENT_PROPERTY_KEYS = {
    "page_url",
    "content_id",
    "article_id",
    "video_id",
    "duration",
    "duration_seconds",
    "session_id",
}


@dataclass
class CheckItem:
    key: str
    enabled: bool
    reason: str
    severity: str = "medium"


@dataclass
class PlanProfile:
    domain: str
    confidence: float
    signals: List[str] = field(default_factory=list)
    checks: List[CheckItem] = field(default_factory=list)
    enabled_checks: set[str] = field(default_factory=set)
    disabled_checks: set[str] = field(default_factory=set)
    check_weights: dict[str, str] = field(default_factory=dict)


def analyze_tracking_plan(specs: list[TrackingEventSpec]) -> PlanProfile:
    signals: list[str] = []
    ecommerce_score = 0
    saas_score = 0
    content_score = 0

    for spec in specs:
        name_lower = spec.name.lower()
        prop_keys = {prop.lower() for prop in spec.required_properties}

        if any(keyword in name_lower for keyword in ECOMMERCE_KEYWORDS):
            ecommerce_score += 2
            signals.append(f"ecommerce event: {spec.name}")

        if any(keyword in name_lower for keyword in SAAS_KEYWORDS):
            saas_score += 2
            signals.append(f"saas event: {spec.name}")

        if any(keyword in name_lower for keyword in CONTENT_KEYWORDS):
            content_score += 2
            signals.append(f"content event: {spec.name}")

        if prop_keys & COMMERCE_PROPERTY_KEYS:
            ecommerce_score += 1

        if prop_keys & SAAS_PROPERTY_KEYS:
            saas_score += 1

        if prop_keys & CONTENT_PROPERTY_KEYS:
            content_score += 1

    scores = {
        "ecommerce": ecommerce_score,
        "saas": saas_score,
        "content": content_score,
    }

    top_domain = max(scores, key=scores.get)
    top_score = scores[top_domain]
    second_score = sorted(scores.values(), reverse=True)[1]

    if top_score == 0:
        domain = "generic"
        confidence = 0.35
    else:
        domain = top_domain

        # Bug 7 fix: the old formula was top_score / (top_score + second_score),
        # which only measures dominance within the plan — not how clearly domain
        # signals appear relative to the plan's size. A 1-event ecommerce plan
        # with score=2 and second=0 got confidence=1.0, identical to a 50-event
        # plan. We now blend two signals:
        #   - dominance_ratio: how dominant the top domain is vs the runner-up
        #     (unchanged from before, reflects signal clarity)
        #   - density_ratio: what fraction of events carry a domain signal at all
        #     (new, penalises tiny or mostly-untagged plans)
        # Final confidence = harmonic-ish blend, capped at 0.95.
        n_specs = max(1, len(specs))
        # Max possible score per event is 2 (name match) + 1 (property match) = 3
        max_possible = n_specs * 3
        dominance_ratio = top_score / max(1, top_score + second_score)
        density_ratio = top_score / max_possible
        confidence = min(0.95, (dominance_ratio + density_ratio) / 2)

    has_identity_required = any(spec.identity_required for spec in specs)
    has_sequence_rules = any(spec.allowed_previous_events for spec in specs)

    checks: list[CheckItem] = [
        CheckItem("duplicate_event_id", True, "Always detect replay/idempotency bugs.", "high"),
        CheckItem("unknown_event", True, "Always compare incoming events against the plan.", "medium"),
        CheckItem("missing_required_property", True, "Always validate required properties.", "high"),
        CheckItem("wrong_property_type", True, "Always validate property types.", "high"),
    ]

    if has_identity_required:
        checks.append(CheckItem("missing_identity", True, "Some plan events require identity.", "high"))

    if has_sequence_rules:
        checks.append(CheckItem("sequence_validation", True, "Plan defines allowed previous events.", "medium"))

    if domain == "ecommerce":
        checks.extend([
            CheckItem("duplicate_purchase", True, "Commerce should reject repeated purchases.", "critical"),
            CheckItem("purchase_without_checkout", True, "Purchase must follow checkout.", "critical"),
            CheckItem("revenue_currency_validation", True, "Revenue and currency should be sane.", "high"),
        ])

    elif domain == "saas":
        # Bug 6 note: login_without_signup, trial_without_signup, and
        # subscription_without_trial all rely on signed_up_identities, which is
        # held in-memory in StreamState. This means they only suppress false
        # positives for identities that signed up within the current server run.
        # Returning users who signed up in a previous run (or before the server
        # restarted) will still trigger these checks. This is a known design
        # limitation: the checks are session-scoped, not lifetime-scoped.
        # To eliminate false positives for returning users entirely, persist
        # signed_up_identities to SQLite and load it on runtime initialisation.
        checks.extend([
            CheckItem("login_without_signup", True, "Login should usually follow signup (session-scoped).", "high"),
            CheckItem("trial_without_signup", True, "Trial start should usually follow signup (session-scoped).", "high"),
            CheckItem("subscription_without_trial", True, "Subscription events should usually follow trial or signup (session-scoped).", "high"),
        ])

    elif domain == "content":
        checks.extend([
            CheckItem("article_read_without_page_view", True, "Content reads should usually follow a page view.", "medium"),
            CheckItem("video_complete_without_video_start", True, "Video completion should usually follow video start.", "medium"),
            CheckItem("content_duration_validation", True, "Engagement duration should be valid.", "medium"),
        ])

    enabled_checks = {check.key for check in checks if check.enabled}
    disabled_checks = {check.key for check in checks if not check.enabled}
    check_weights = {check.key: check.severity for check in checks}

    return PlanProfile(
        domain=domain,
        confidence=confidence,
        signals=signals,
        checks=checks,
        enabled_checks=enabled_checks,
        disabled_checks=disabled_checks,
        check_weights=check_weights,
    )
