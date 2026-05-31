#core/plan_analyzer.py
from dataclasses import dataclass, field
from typing import List

from core.schemas import TrackingEventSpec

ECOMMERCE_KEYWORDS = {
    "product", "cart", "checkout", "order", "purchase", "payment", "revenue", "sku",
}

SAAS_KEYWORDS = {
    "signup", "sign up", "login", "log in", "identify", "trial",
    "subscription", "billing", "onboarding", "activate",
}

CONTENT_KEYWORDS = {
    "page", "screen", "article", "video", "watch", "read",
    "scroll", "engagement", "session",
}

COMMERCE_PROPERTY_KEYS = {
    "product_id", "cart_id", "order_id", "quantity", "revenue", "currency", "sku",
}

SAAS_PROPERTY_KEYS = {
    "user_id", "account_id", "workspace_id", "trial_id",
    "plan", "subscription_id", "billing_status",
}

CONTENT_PROPERTY_KEYS = {
    "page_url", "content_id", "article_id", "video_id",
    "duration", "duration_seconds", "session_id",
}

FUNNEL_ROLE_PATTERNS = {
    "signup_event": [
        "sign up completed", "signup completed", "signed up", "sign up", "signup",
        "registered", "account created", "user created", "onboarding completed",
        "registration completed", "user registered",
    ],
    "purchase_event": [
        "order completed", "purchase completed", "order placed", "transaction completed",
        "payment success", "payment completed", "payment confirmed", "purchase",
        "txn success", "txn_success", "order_completed", "payment_success",
        "checkout completed", "sale completed",
    ],
    "checkout_start_event": [
        "checkout started", "checkout start", "checkout initiated", "begin checkout",
        "checkout_started", "begin_checkout", "start checkout",
    ],
    "product_view_event": [
        "product viewed", "product detail viewed", "view product",
        "product_viewed", "detail_view", "item viewed",
    ],
    "add_to_cart_event": [
        "add to cart", "product added", "cart added", "added to cart",
        "product_added", "add_to_cart",
    ],
    "checkout_step_event": [
        "checkout step viewed", "checkout step", "checkout_step_viewed", "checkout_step",
    ],
    "coupon_event": [
        "coupon applied", "promo applied", "coupon_applied", "promo_applied",
        "discount applied", "voucher applied",
    ],
    "return_event": [
        "return initiated", "return requested", "refund initiated", "refund completed",
        "return completed", "order returned", "order refunded", "refunded",
        "order_refunded", "refund_completed",
    ],
    "wishlist_add_event": [
        "wishlist added", "add to wishlist", "wishlist_added", "saved for later",
    ],
    "wishlist_remove_event": [
        "wishlist removed", "remove from wishlist", "wishlist_removed",
    ],
    "page_view_event": [
        "page viewed", "page_view", "screen viewed", "page_viewed", "screen_viewed",
        "pageview",
    ],
    "login_event": [
        "login", "sign in", "signin", "log in", "logged in",
        "login_completed", "sign_in",
    ],
    "trial_start_event": [
        "trial started", "trial start", "trial_started", "trial_start",
        "free trial started",
    ],
    "subscription_event": [
        "subscription started", "subscription start", "subscription_started",
        "subscription_start", "subscribed", "plan activated",
    ],
    "article_read_event": [
        "article read", "read article", "article_read", "content read",
    ],
    "video_start_event": [
        "video started", "video start", "video_started", "video_start",
        "playback started", "play started",
    ],
    "video_complete_event": [
        "video completed", "video complete", "video_completed", "video_complete",
        "playback completed", "watch completed",
    ],
}

# All roles we consider "critical" for their domain — missing ones are flagged
# as warnings rather than errors, because not every plan has every event type.
_ECOMMERCE_CRITICAL_ROLES = {
    "purchase_event", "checkout_start_event",
}
_SAAS_CRITICAL_ROLES = {
    "signup_event", "trial_start_event", "subscription_event",
}
_CONTENT_CRITICAL_ROLES = {
    "page_view_event", "article_read_event",
}

# B1 FIX: expanded candidate lists covering common non-standard property names.
# Previously these only covered the most common names; clients using aliases
# like "amt", "total_price", "iso_code", "basket_id" would get silent misses
# on revenue/currency/order-id checks because the property map would fall back
# to defaults that didn't exist on those events.
_REVENUE_CANDIDATES = [
    "revenue", "value", "amount", "total", "price", "subtotal",
    "amt", "total_price", "order_value", "grand_total", "net_amount",
    "transaction_value", "sale_amount", "gmv", "total_amount",
]
_CURRENCY_CANDIDATES = [
    "currency", "currency_code", "iso_currency",
    "iso_code", "currency_iso", "payment_currency",
]
_ORDER_ID_CANDIDATES = [
    "order_id", "transaction_id", "basket_id", "cart_id", "purchase_id",
    "txn_id", "order_number", "receipt_id", "confirmation_id",
    "checkout_id", "payment_id",
]


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
    enabled_checks: set = field(default_factory=set)
    disabled_checks: set = field(default_factory=set)
    check_weights: dict = field(default_factory=dict)
    funnel_map: dict = field(default_factory=dict)
    property_map: dict = field(default_factory=dict)
    # A2: roles that pattern-matching could not resolve from this plan's event names.
    # Non-empty means some checks will silently not fire — caller should surface this.
    unresolved_roles: List[str] = field(default_factory=list)


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

    scores = {"ecommerce": ecommerce_score, "saas": saas_score, "content": content_score}

    # ── Funnel map resolution ────────────────────────────────────────────────
    # For each role, try every pattern against every event name.
    # We normalize both sides to lowercase_with_underscores so that
    # "Order Completed", "order_completed", and "OrderCompleted" all match
    # the pattern "order completed".
    event_names = [spec.name for spec in specs]
    funnel_map: dict[str, str] = {}
    for role, patterns in FUNNEL_ROLE_PATTERNS.items():
        for pattern in patterns:
            p_norm = pattern.lower().replace(" ", "_")
            for name in event_names:
                n_norm = name.lower().replace(" ", "_")
                if p_norm in n_norm or n_norm in p_norm:
                    funnel_map[role] = name
                    break
            if role in funnel_map:
                break

    # A2: record which roles could not be resolved so callers can warn the user.
    top_domain_for_warning = max(scores, key=scores.get)
    if scores[top_domain_for_warning] == 0:
        top_domain_for_warning = "generic"

    _critical_roles_by_domain = {
        "ecommerce": _ECOMMERCE_CRITICAL_ROLES,
        "saas": _SAAS_CRITICAL_ROLES,
        "content": _CONTENT_CRITICAL_ROLES,
    }
    _critical_roles = _critical_roles_by_domain.get(top_domain_for_warning, set())
    unresolved_roles = sorted(
        role for role in _critical_roles if role not in funnel_map
    )

    # ── Property map inference ───────────────────────────────────────────────
    # B1: scan ALL properties from the purchase + checkout specs using the
    # expanded candidate lists.  Previously only required_properties was
    # scanned; now property_types keys and allowed_values keys are included
    # so aliases used only as typed/constrained props (not "required") are
    # also picked up.
    purchase_event_name  = funnel_map.get("purchase_event")
    checkout_event_name  = funnel_map.get("checkout_start_event")

    _commerce_props: set[str] = set()
    for _evt_name in (purchase_event_name, checkout_event_name):
        if not _evt_name:
            continue
        _target = _evt_name.lower().replace(" ", "_")
        for _s in specs:
            if _s.name.lower().replace(" ", "_") == _target:
                _commerce_props.update(_s.required_properties)
                _commerce_props.update(_s.property_types.keys())
                if _s.allowed_values:
                    _commerce_props.update(_s.allowed_values.keys())
                break

    # B1: also scan ALL specs for currency-like property names — a client might
    # define currency only on a non-purchase event (e.g. a "Price Displayed"
    # impression event) and we still want to pick up the right column name.
    _all_props: set[str] = set()
    for _s in specs:
        _all_props.update(_s.required_properties)
        _all_props.update(_s.property_types.keys())

    def _pick(candidates: list[str], *prop_sets: set[str]) -> str:
        """Return the first candidate found in any of the supplied prop sets."""
        for c in candidates:
            for ps in prop_sets:
                if c in ps:
                    return c
        return candidates[0]  # fall back to the most common name

    property_map = {
        "order_id_prop":      _pick(_ORDER_ID_CANDIDATES,  _commerce_props, _all_props),
        "revenue_prop":       _pick(_REVENUE_CANDIDATES,   _commerce_props, _all_props),
        "currency_prop":      _pick(_CURRENCY_CANDIDATES,  _commerce_props, _all_props),
        "checkout_step_prop": "step",
    }

    # ── Domain + confidence ──────────────────────────────────────────────────
    top_domain = max(scores, key=scores.get)
    top_score  = scores[top_domain]
    second_score = sorted(scores.values(), reverse=True)[1]

    if top_score == 0:
        domain = "generic"
        confidence = 0.35
    else:
        domain = top_domain
        n_specs = max(1, len(specs))
        max_possible = n_specs * 3
        dominance_ratio = top_score / max(1, top_score + second_score)
        density_ratio   = top_score / max_possible
        confidence = min(0.95, (dominance_ratio + density_ratio) / 2)

    has_identity_required = any(spec.identity_required for spec in specs)

    # ── Checks ───────────────────────────────────────────────────────────────
    checks: list[CheckItem] = [
        CheckItem("duplicate_event_id",       True, "Always detect replay/idempotency bugs.",              "high"),
        CheckItem("unknown_event",             True, "Always compare incoming events against the plan.",    "medium"),
        CheckItem("missing_required_property", True, "Always validate required properties.",               "high"),
        CheckItem("wrong_property_type",       True, "Always validate property types.",                    "high"),
        CheckItem("sequence_validation",       True, "Plan defines allowed previous events.",              "medium"),
        CheckItem("enum_value_violation",      True, "Plan specifies allowed values for some properties.", "high"),
    ]

    if has_identity_required:
        checks.append(CheckItem(
            "missing_identity", True, "Some plan events require identity.", "high"
        ))

    # D1: enable conditional_property check if any spec defines conditional_rules
    has_conditional_rules = any(
        getattr(spec, "conditional_rules", [])
        for spec in specs
    )
    if has_conditional_rules:
        checks.append(CheckItem(
            "conditional_property", True,
            "Plan encodes conditional rules (e.g. if payment_method==emi then emi_bank required).",
            "high",
        ))

    if domain == "ecommerce":
        checks.extend([
            CheckItem("duplicate_purchase",        True, "Commerce should reject repeated purchases.",        "critical"),
            CheckItem("purchase_without_checkout", True, "Purchase must follow checkout.",                    "critical"),
            CheckItem("revenue_currency_validation",True,"Revenue and currency should be sane.",              "high"),
        ])

    # Role-gated checks — only enabled when the role was actually resolved,
    # so we never enable a check that can never fire.
    if "product_view_event" in funnel_map:
        checks.append(CheckItem(
            "product_viewed_without_page_view", True,
            "Product viewed should follow a page view.", "medium"
        ))
    if "add_to_cart_event" in funnel_map:
        checks.append(CheckItem(
            "cart_add_without_product_view", True,
            "Add to cart should follow a product view.", "medium"
        ))
    if "checkout_step_event" in funnel_map:
        checks.append(CheckItem(
            "checkout_step_regression", True,
            "Checkout step numbers must increase monotonically.", "high"
        ))
    if "coupon_event" in funnel_map and "checkout_start_event" in funnel_map:
        checks.append(CheckItem(
            "coupon_without_checkout", True,
            "Coupon must occur inside an active checkout.", "high"
        ))
    if "return_event" in funnel_map and "purchase_event" in funnel_map:
        checks.append(CheckItem(
            "return_without_purchase", True,
            "Return must reference a completed order.", "high"
        ))
    if "wishlist_add_event" in funnel_map or "wishlist_remove_event" in funnel_map:
        checks.append(CheckItem(
            "wishlist_without_identity", True,
            "Wishlist actions require an identified user.", "medium"
        ))
    if "login_event" in funnel_map and "signup_event" in funnel_map:
        checks.append(CheckItem(
            "login_without_signup", True,
            "Login should follow signup (session-scoped).", "high"
        ))

    if domain == "saas":
        # login_without_signup may have already been added above via the
        # funnel_map role-gate (when both login_event and signup_event resolved).
        # Build from a dict keyed on check key to deduplicate cleanly.
        _existing_keys = {c.key for c in checks}
        _saas_checks = [
            CheckItem("login_without_signup",      True, "Login should usually follow signup.",           "high"),
            CheckItem("trial_without_signup",      True, "Trial start should usually follow signup.",     "high"),
            CheckItem("subscription_without_trial",True, "Subscription should usually follow trial.",     "high"),
        ]
        for _c in _saas_checks:
            if _c.key not in _existing_keys:
                checks.append(_c)
                _existing_keys.add(_c.key)

    elif domain == "content":
        checks.extend([
            CheckItem("article_read_without_page_view",      True, "Content reads should follow a page view.",    "medium"),
            CheckItem("video_complete_without_video_start",  True, "Video completion should follow video start.", "medium"),
            CheckItem("content_duration_validation",         True, "Engagement duration should be valid.",        "medium"),
        ])

    enabled_checks  = {c.key for c in checks if c.enabled}
    disabled_checks = {c.key for c in checks if not c.enabled}
    check_weights   = {c.key: c.severity for c in checks}

    return PlanProfile(
        domain=domain,
        confidence=confidence,
        signals=signals,
        checks=checks,
        enabled_checks=enabled_checks,
        disabled_checks=disabled_checks,
        check_weights=check_weights,
        funnel_map=funnel_map,
        property_map=property_map,
        unresolved_roles=unresolved_roles,
    )
