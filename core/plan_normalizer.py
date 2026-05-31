#core/plan_normalizer.py
from core.schemas import ConditionalRule, TrackingEventSpec

TYPE_ALIASES = {
    "str": "string",
    "string": "string",
    "int": "number",
    "float": "number",
    "number": "number",
    "bool": "boolean",
    "boolean": "boolean",
    "dict": "object",
    "object": "object",
    "list": "array",
    "array": "array",
}

ANONYMOUS_EVENTS = {
    "app_install", "app_open", "app_closed", "sign_up_started", "sign_up_completed", "login", "logout",
    "password_reset_requested", "home_screen_viewed", "search_performed", "search_no_results", "category_viewed",
    "filter_applied", "product_list_viewed", "banner_clicked", "product_viewed", "image_swiped", "size_chart_viewed",
    "size_selected", "review_read", "product_shared", "page_viewed", "page_view", "landing_page_viewed",
    "referral_link_clicked",
}

OPTIONAL_PROPERTIES = {
    "utm_source", "utm_campaign", "utm_medium", "campaign_id", "campaign", "medium", "source", "discount_pct",
    "discount_code", "discount", "coupon", "coupon_code", "rating", "review_count", "avg_rating", "login_success",
    "referral_code", "push_enabled", "install_referrer", "user_segment", "experiment_variant", "emi_bank",
    "upi_app", "is_new_address", "pincode", "city", "estimated_delivery_date", "current_status", "points_earned",
    "total_points", "skipped", "time_to_complete_sec", "result_count", "corrected_query", "filters_applied",
    "list_id", "image_index", "total_images", "share_channel", "device_id", "user_id", "anonymous_id",
}


def normalize_event_name(name: str) -> str:
    return name.strip().lower().replace(" ", "_")


def normalize_incoming_event_name(name: str) -> str:
    return name.strip().lower().replace(" ", "_")


def normalize_property_name(name: str) -> str:
    return "_".join(name.strip().lower().split())


def normalize_type(type_name: str) -> str:
    cleaned = type_name.strip().lower()
    return TYPE_ALIASES.get(cleaned, cleaned)


def _normalize_conditional_rule(rule: ConditionalRule) -> ConditionalRule:
    """Normalize property names inside a ConditionalRule to snake_case."""
    return ConditionalRule(
        when_property=normalize_property_name(rule.when_property),
        when_value=rule.when_value,   # values are not normalized — keep as-is
        then_property=normalize_property_name(rule.then_property),
        then_allowed_values=rule.then_allowed_values,
    )


def normalize_spec(spec: TrackingEventSpec) -> TrackingEventSpec:
    name = normalize_event_name(spec.name)

    required_properties = [
        normalize_property_name(p) for p in spec.required_properties
        if normalize_property_name(p) not in OPTIONAL_PROPERTIES
    ]

    identity_required = spec.identity_required
    if name in ANONYMOUS_EVENTS:
        identity_required = False
    elif "user_id" in required_properties or "user_id" in [
        normalize_property_name(p) for p in spec.required_properties
    ]:
        identity_required = True

    property_types = {
        normalize_property_name(k): normalize_type(v)
        for k, v in spec.property_types.items()
    }

    # A1 FIX: do not inject a default currency allowlist.
    # If the plan author didn't specify allowed currencies we don't constrain
    # them — format is validated by _is_valid_currency_code in detectors.py.
    allowed_values: dict = {}
    if hasattr(spec, "allowed_values") and spec.allowed_values:
        allowed_values = {
            normalize_property_name(k): v
            for k, v in spec.allowed_values.items()
        }

    # D1: normalize property names inside conditional rules
    conditional_rules = [
        _normalize_conditional_rule(r)
        for r in getattr(spec, "conditional_rules", [])
    ]

    return TrackingEventSpec(
        name=name,
        required_properties=required_properties,
        property_types=property_types,
        identity_required=identity_required,
        allowed_previous_events=[normalize_event_name(e) for e in spec.allowed_previous_events],
        allowed_values=allowed_values,
        conditional_rules=conditional_rules,
    )


def normalize_specs(specs: list[TrackingEventSpec]) -> list[TrackingEventSpec]:
    return [normalize_spec(spec) for spec in specs]
