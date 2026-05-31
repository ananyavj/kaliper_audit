#core/detectors.py
from core.schemas import Issue, TrackingEventSpec, IncomingEvent
from core.state_store import StreamState, _is_signup_event   # FIX 1: import _is_signup_event
from core.plan_normalizer import normalize_incoming_event_name

DEFAULT_ENABLED_CHECKS = {
    "duplicate_event_id",
    "unknown_event",
    "missing_required_property",
    "wrong_property_type",
    "missing_identity",
    "sequence_validation",
    "duplicate_purchase",
    "purchase_without_checkout",
    "revenue_currency_validation",
    "login_without_signup",
    "trial_without_signup",
    "subscription_without_trial",
    "article_read_without_page_view",
    "video_complete_without_video_start",
    "content_duration_validation",
    # D1: conditional logic check — always on when the plan encodes conditional_rules
    "conditional_property",
}


def _expected_python_type(type_name: str):
    if type_name == "string":
        return str
    if type_name == "number":
        return (int, float)
    if type_name == "boolean":
        return bool
    if type_name == "object":
        return dict
    if type_name == "array":
        return list
    return None


def _name_matches(name: str, patterns: list[str]) -> bool:
    lowered = name.lower()
    return any(pattern in lowered for pattern in patterns)


_PROPERTY_ALIASES = {
    "method": ["signup_method", "login_method", "payment_method"],
    "signup_method": ["method"],
    "login_method": ["method"],
    "payment_method": ["method"],
}


def _has_property(properties, name):
    if name in properties:
        return True
    for alias in _PROPERTY_ALIASES.get(name, []):
        if alias in properties:
            return True
    return False


def _get_property(properties, name):
    if name in properties:
        return properties[name]
    for alias in _PROPERTY_ALIASES.get(name, []):
        if alias in properties:
            return properties[alias]
    return None


def _is_positive_number(val) -> bool:
    if isinstance(val, bool):
        return False
    if isinstance(val, (int, float)):
        return val > 0
    return False


def _is_valid_currency_code(val) -> bool:
    if not isinstance(val, str):
        return False
    return len(val.strip()) == 3


def detect_issues(
    events: list[IncomingEvent],
    plan_specs: list[TrackingEventSpec],
    enabled_checks: set[str] | None = None,
    state: StreamState | None = None,
    funnel_map: dict[str, str] | None = None,
    property_map: dict[str, str] | None = None,
) -> list[Issue]:
    enabled = set(enabled_checks) if enabled_checks is not None else set(DEFAULT_ENABLED_CHECKS)
    state = state or StreamState()
    fm = funnel_map or {}
    pm = property_map or {}

    event_roles = {}
    for role, event_name in fm.items():
        # Store in event_roles under the normalized spec event name
        event_roles.setdefault(normalize_incoming_event_name(event_name), set()).add(role)

    issues: list[Issue] = []
    # Tracks event_ids that have already received a semantic ordering issue
    # (login_without_signup, trial_without_signup, article_read_without_page_view, etc.).
    # sequence_validation is suppressed for those events to avoid double-reporting
    # the same root cause at both the structural and semantic layer.
    _semantic_ordering_issued: set[str] = set()
    # Tracks event_ids for which wrong_property_type already fired for the revenue
    # property, so invalid_revenue is suppressed for the same event.
    _revenue_type_issued: set[str] = set()
    spec_map = {spec.name: spec for spec in plan_specs}

    order_id_prop   = pm.get("order_id_prop", "order_id")
    revenue_prop    = pm.get("revenue_prop", "revenue")
    currency_prop   = pm.get("currency_prop", "currency")
    step_prop       = pm.get("checkout_step_prop", "step")

    # FIX 2: Pass 1 does ONLY identity stitching — nothing else.
    # Previously, Pass 1 also pre-populated signed_up_identities /
    # trial_started_identities / subscription_started_identities, which
    # meant that a Signup appearing AFTER a Login was still counted as
    # "signed up before login", destroying all ordering-dependent checks
    # (login_without_signup, trial_without_signup, subscription_without_trial).
    # Those sets are now populated in Pass 2 in event-arrival order, so
    # a Login that arrives before any Signup correctly triggers the check.
    seen_in_batch = set()
    for event in events:
        if event.event_id not in state.seen_event_ids and event.event_id not in seen_in_batch:
            state.learn_identity(event)
        seen_in_batch.add(event.event_id)

    # Pass 2 — per-event checks
    for event in events:
        norm_name = normalize_incoming_event_name(event.name)
        spec = spec_map.get(norm_name)
        identity = state.resolve_identity(event)
        roles = event_roles.get(norm_name, set())

        # If funnel roles are not resolved dynamically, infer them via keywords
        if not fm:
            event_name_lower = event.name.lower().replace("_", " ").replace(" ", "")
            if "purchase" in event_name_lower or "ordercompleted" in event_name_lower or "orderplaced" in event_name_lower:
                roles.add("purchase_event")
            if "checkoutstart" in event_name_lower or "begincheckout" in event_name_lower:
                roles.add("checkout_start_event")
            if "productview" in event_name_lower or "detailview" in event_name_lower:
                roles.add("product_view_event")
            if "addtocart" in event_name_lower or "productadd" in event_name_lower:
                roles.add("add_to_cart_event")
            if "checkoutstep" in event_name_lower:
                roles.add("checkout_step_event")
            if "coupon" in event_name_lower or "promo" in event_name_lower:
                roles.add("coupon_event")
            if "return" in event_name_lower or "refund" in event_name_lower:
                roles.add("return_event")
            if "wishlist" in event_name_lower:
                roles.add("wishlist_add_event")
            if "pageview" in event_name_lower or "screenview" in event_name_lower:
                roles.add("page_view_event")
            if "login" in event_name_lower or "signin" in event_name_lower:
                roles.add("login_event")
            if "signup" in event_name_lower or "register" in event_name_lower or "onboarding" in event_name_lower:
                roles.add("signup_event")
            if "trial" in event_name_lower:
                roles.add("trial_start_event")
            if "subscription" in event_name_lower:
                roles.add("subscription_event")
            if "article" in event_name_lower:
                roles.add("article_read_event")
            if "videostart" in event_name_lower:
                roles.add("video_start_event")
            if "videocomplete" in event_name_lower:
                roles.add("video_complete_event")

        already_seen_id = state.has_seen_event_id(event.event_id)
        state.seen_event_ids.add(event.event_id)

        # --- 1. Duplicate event ID ---
        if "duplicate_event_id" in enabled and already_seen_id:
            issues.append(Issue(
                issue_type="duplicate_event_id", severity="high",
                message=f"Duplicate event_id '{event.event_id}' for '{event.name}'.",
                event_id=event.event_id, event_name=event.name,
            ))

        # --- 2. Unknown event ---
        if spec is None:
            if "unknown_event" in enabled:
                issues.append(Issue(
                    issue_type="unknown_event", severity="medium",
                    message=f"Event '{event.name}' is not in the tracking plan.",
                    event_id=event.event_id, event_name=event.name,
                ))
            state.mark_event(
                event,
                purchase_roles=roles if "purchase_event" in roles else None,
                order_id_prop=order_id_prop,
            )
            continue

        # --- 3. Missing required properties ---
        if "missing_required_property" in enabled:
            for prop in spec.required_properties:
                if prop == "user_id":
                    if not event.user_id and not _has_property(event.properties, prop):
                        issues.append(Issue(
                            issue_type="missing_required_property", severity="high",
                            message=f"Missing required property '{prop}' in '{event.name}'.",
                            event_id=event.event_id, event_name=event.name,
                        ))
                elif prop == "anonymous_id":
                    if not event.anonymous_id and not _has_property(event.properties, prop):
                        issues.append(Issue(
                            issue_type="missing_required_property", severity="high",
                            message=f"Missing required property '{prop}' in '{event.name}'.",
                            event_id=event.event_id, event_name=event.name,
                        ))
                else:
                    if not _has_property(event.properties, prop):
                        issues.append(Issue(
                            issue_type="missing_required_property", severity="high",
                            message=f"Missing required property '{prop}' in '{event.name}' (event_id: '{event.event_id}').",
                            event_id=event.event_id, event_name=event.name,
                        ))

        # --- 4. Wrong property type ---
        if "wrong_property_type" in enabled:
            for prop_name, expected_type_name in spec.property_types.items():
                if not _has_property(event.properties, prop_name):
                    continue
                value = _get_property(event.properties, prop_name)
                expected_type = _expected_python_type(expected_type_name)
                if expected_type is None:
                    continue

                if expected_type_name == "number" and isinstance(value, bool):
                    issues.append(Issue(
                        issue_type="wrong_property_type", severity="high",
                        message=f"Property '{prop_name}' in '{event.name}' should be number, got boolean.",
                        event_id=event.event_id, event_name=event.name,
                    ))
                    continue

                if expected_type_name == "number" and isinstance(value, str):
                    try:
                        float(value)
                        msg = (f"Property '{prop_name}' in '{event.name}' is a numeric string "
                               f"('{value}'). Send as a number, not a string.")
                    except ValueError:
                        msg = (f"Property '{prop_name}' in '{event.name}' should be "
                               f"{expected_type_name}, got {type(value).__name__}.")
                    issues.append(Issue(
                        issue_type="wrong_property_type", severity="high",
                        message=msg, event_id=event.event_id, event_name=event.name,
                    ))
                    # Mark so invalid_revenue / content_duration_validation won't
                    # double-report the same string-typed numeric property.
                    _revenue_type_issued.add(event.event_id)
                    continue

                if expected_type_name == "boolean" and not isinstance(value, bool):
                    issues.append(Issue(
                        issue_type="wrong_property_type", severity="high",
                        message=(f"Property '{prop_name}' in '{event.name}' should be a boolean "
                                 f"primitive, got {type(value).__name__} '{value}'. "
                                 "Never use 1/0 or string booleans."),
                        event_id=event.event_id, event_name=event.name,
                    ))
                    continue

                if not isinstance(value, expected_type):
                    issues.append(Issue(
                        issue_type="wrong_property_type", severity="high",
                        message=(f"Property '{prop_name}' in '{event.name}' should be "
                                 f"{expected_type_name}, got {type(value).__name__}."),
                        event_id=event.event_id, event_name=event.name,
                    ))

        # --- 4a. Revenue/currency validation (purchase event) ---
        if "purchase_event" in roles:
            order_id = _get_property(event.properties, order_id_prop)
            revenue  = _get_property(event.properties, revenue_prop)
            currency = _get_property(event.properties, currency_prop)
            checkout_start = fm.get("checkout_start_event", "checkout_started")

            if "purchase_without_checkout" in enabled:
                if not state.has_seen_event(event, checkout_start):
                    issues.append(Issue(
                        issue_type="purchase_without_checkout", severity="critical",
                        message=(f"'{event.name}' (purchase) occurred without a prior "
                                 f"'{checkout_start}'."),
                        event_id=event.event_id, event_name=event.name,
                    ))

            if "duplicate_purchase" in enabled:
                if state.mark_order(order_id):
                    issues.append(Issue(
                        issue_type="duplicate_purchase", severity="critical",
                        message=f"Duplicate purchase detected for {order_id_prop} '{order_id}'.",
                        event_id=event.event_id, event_name=event.name,
                    ))

            if "revenue_currency_validation" in enabled:
                stored_checkout_rev = state.get_checkout_revenue(identity)
                if stored_checkout_rev is not None and _is_positive_number(revenue):
                    if abs(float(revenue) - stored_checkout_rev) > 0.01:
                        issues.append(Issue(
                            issue_type="revenue_inconsistency", severity="high",
                            message=(
                                f"'{event.name}' '{revenue_prop}' ({revenue}) does not match "
                                f"the revenue recorded at checkout start ({stored_checkout_rev}). "
                                f"These values must be identical — read the amount from the same "
                                f"source field ('{revenue_prop}') on both events."
                            ),
                            event_id=event.event_id, event_name=event.name,
                        ))

            if "revenue_currency_validation" in enabled:
                # Suppress invalid_revenue when wrong_property_type already fired for
                # this revenue property on this event — same root cause, one signal.
                if revenue is not None and not _is_positive_number(revenue) \
                        and event.event_id not in _revenue_type_issued:
                    issues.append(Issue(
                        issue_type="invalid_revenue", severity="high",
                        message=(f"'{revenue_prop}' in '{event.name}' must be a positive number "
                                 f"(got {type(revenue).__name__} '{revenue}'). "
                                 "Never send as string or zero/negative."),
                        event_id=event.event_id, event_name=event.name,
                    ))
                if currency is not None and not _is_valid_currency_code(currency):
                    issues.append(Issue(
                        issue_type="invalid_currency", severity="high",
                        message=(f"'{currency_prop}' in '{event.name}' should be a 3-letter ISO "
                                 f"code (got '{currency}')."),
                        event_id=event.event_id, event_name=event.name,
                    ))

        # --- 4b. Checkout start revenue/currency validation ---
        if "checkout_start_event" in roles and "revenue_currency_validation" in enabled:
            revenue  = _get_property(event.properties, revenue_prop)
            currency = _get_property(event.properties, currency_prop)
            if revenue is not None and not _is_positive_number(revenue):
                issues.append(Issue(
                    issue_type="invalid_revenue", severity="high",
                    message=(f"'{revenue_prop}' in '{event.name}' (checkout start) must be "
                             f"a positive number (got '{revenue}')."),
                    event_id=event.event_id, event_name=event.name,
                ))
            if currency is not None and not _is_valid_currency_code(currency):
                issues.append(Issue(
                    issue_type="invalid_currency", severity="high",
                    message=(f"'{currency_prop}' in '{event.name}' should be a 3-letter ISO "
                             f"code (got '{currency}')."),
                    event_id=event.event_id, event_name=event.name,
                ))
            # Store checkout revenue against this identity for later cross-event check.
            if _is_positive_number(revenue):
                state.store_checkout_revenue(identity, float(revenue))

        # --- 5. Enum / allowed-values violation ---
        _currency_already_flagged = any(
            i.event_id == event.event_id and i.issue_type == "invalid_currency"
            for i in issues
        )
        if "enum_value_violation" in enabled and spec.allowed_values:
            for prop_name, allowed in spec.allowed_values.items():
                if not _has_property(event.properties, prop_name) or not allowed:
                    continue
                if prop_name == currency_prop and _currency_already_flagged:
                    continue
                value = _get_property(event.properties, prop_name)
                if isinstance(value, str) and value not in allowed:
                    issues.append(Issue(
                        issue_type="enum_value_violation", severity="high",
                        message=(f"Property '{prop_name}' in '{event.name}' has unexpected value "
                                 f"'{value}'. Allowed: {allowed}."),
                        event_id=event.event_id, event_name=event.name,
                    ))

        # --- 6. Missing identity ---
        if "missing_identity" in enabled and spec.identity_required and not event.user_id:
            issues.append(Issue(
                issue_type="missing_identity", severity="high",
                message=f"'{event.name}' requires user_id but it is missing.",
                event_id=event.event_id, event_name=event.name,
            ))

        # --- 7. Sequence validation ---
        # NOTE: this block is evaluated AFTER all semantic ordering checks (10–20)
        # so that _semantic_ordering_issued is fully populated before we decide
        # whether to suppress the generic sequence_error for this event.
        # (Block deliberately left here as a placeholder — see end of loop below.)

        # --- 10. Product Viewed without Page Viewed ---
        if "product_view_event" in roles and "product_viewed_without_page_view" in enabled:
            page_view = fm.get("page_view_event", "page_view")
            if not state.has_seen_event(event, page_view):
                _semantic_ordering_issued.add(event.event_id)
                issues.append(Issue(
                    issue_type="product_viewed_without_page_view", severity="medium",
                    message=(f"'{event.name}' fired before any '{page_view}' in this session "
                             f"(identity: '{identity}'). Ensure the product page route fires the page view first."),
                    event_id=event.event_id, event_name=event.name,
                ))

        # --- 11. Add to cart without product view ---
        if "add_to_cart_event" in roles and "cart_add_without_product_view" in enabled:
            product_view = fm.get("product_view_event", "product_viewed")
            if not state.has_seen_event(event, product_view):
                _semantic_ordering_issued.add(event.event_id)
                issues.append(Issue(
                    issue_type="cart_add_without_product_view", severity="medium",
                    message=(f"'{event.name}' fired before any '{product_view}' in this session. "
                             "Quick-add paths should set source='quick_add'."),
                    event_id=event.event_id, event_name=event.name,
                ))

        # --- 12. Checkout step regression ---
        if "checkout_step_event" in roles and "checkout_step_regression" in enabled:
            step = _get_property(event.properties, step_prop) or _get_property(event.properties, "checkout_step")
            if isinstance(step, (int, float)) and not isinstance(step, bool):
                step_int = int(step)
                last_step = state.get_checkout_step(identity)
                if last_step is not None and step_int < last_step:
                    issues.append(Issue(
                        issue_type="checkout_step_regression", severity="high",
                        message=(f"Checkout step regression for identity '{identity}': "
                                 f"received step {step_int} after step {last_step}."),
                        event_id=event.event_id, event_name=event.name,
                    ))
                state.set_checkout_step(identity, step_int)

        # --- 13. Coupon without checkout ---
        if "coupon_event" in roles and "coupon_without_checkout" in enabled:
            checkout_start = fm.get("checkout_start_event", "checkout_started")
            if not state.has_seen_event(event, checkout_start):
                _semantic_ordering_issued.add(event.event_id)
                issues.append(Issue(
                    issue_type="coupon_without_checkout", severity="high",
                    message=(f"'{event.name}' fired before '{checkout_start}'. "
                             "Coupons should only be applied during an active checkout."),
                    event_id=event.event_id, event_name=event.name,
                ))

        # --- 14. Return without purchase ---
        if "return_event" in roles and "return_without_purchase" in enabled:
            order_id = _get_property(event.properties, order_id_prop)
            if order_id and not state.has_completed_order(str(order_id)):
                _semantic_ordering_issued.add(event.event_id)
                issues.append(Issue(
                    issue_type="return_without_purchase", severity="high",
                    message=(f"'{event.name}' for {order_id_prop} '{order_id}' but no matching "
                             f"purchase event was seen in this session."),
                    event_id=event.event_id, event_name=event.name,
                ))

        # --- 15. Wishlist without identity ---
        if ("wishlist_add_event" in roles or "wishlist_remove_event" in roles) \
                and "wishlist_without_identity" in enabled:
            if not event.user_id:
                issues.append(Issue(
                    issue_type="wishlist_without_identity", severity="medium",
                    message=(f"'{event.name}' fired without a user_id. "
                             "Wishlist features should be gated behind login."),
                    event_id=event.event_id, event_name=event.name,
                ))

        # --- 16. Login without signup ---
        if "login_event" in roles and "login_without_signup" in enabled:
            if identity not in state.signed_up_identities:
                _semantic_ordering_issued.add(event.event_id)
                signup_event = fm.get("signup_event", "onboarding_completed")
                issues.append(Issue(
                    issue_type="login_without_signup", severity="high",
                    message=(f"'{event.name}' (login) occurred before '{signup_event}' "
                             "in this session."),
                    event_id=event.event_id, event_name=event.name,
                ))

        # --- 17. Trial without signup ---
        if "trial_start_event" in roles and "trial_without_signup" in enabled:
            if identity not in state.signed_up_identities:
                _semantic_ordering_issued.add(event.event_id)
                signup_event = fm.get("signup_event", "signup")
                issues.append(Issue(
                    issue_type="trial_without_signup", severity="high",
                    message=(f"'{event.name}' (trial start) occurred before '{signup_event}' "
                             "in this session. Users should sign up before starting a trial."),
                    event_id=event.event_id, event_name=event.name,
                ))

        # --- 18. Subscription without trial ---
        if "subscription_event" in roles and "subscription_without_trial" in enabled:
            trial_event = fm.get("trial_start_event", "trial_started")
            if identity not in state.trial_started_identities:
                _semantic_ordering_issued.add(event.event_id)
                issues.append(Issue(
                    issue_type="subscription_without_trial", severity="high",
                    message=(f"'{event.name}' (subscription) occurred without a prior "
                             f"'{trial_event}' in this session."),
                    event_id=event.event_id, event_name=event.name,
                ))

        # --- 19. Article read without page view ---
        if "article_read_event" in roles and "article_read_without_page_view" in enabled:
            page_view = fm.get("page_view_event", "page_viewed")
            if not state.has_seen_event(event, page_view):
                _semantic_ordering_issued.add(event.event_id)
                issues.append(Issue(
                    issue_type="article_read_without_page_view", severity="medium",
                    message=(f"'{event.name}' fired before any '{page_view}' in this session. "
                             "Ensure the page view fires before content engagement events."),
                    event_id=event.event_id, event_name=event.name,
                ))

        # --- 20. Video complete without video start ---
        if "video_complete_event" in roles and "video_complete_without_video_start" in enabled:
            video_start = fm.get("video_start_event", "video_started")
            if not state.has_seen_event(event, video_start):
                _semantic_ordering_issued.add(event.event_id)
                issues.append(Issue(
                    issue_type="video_complete_without_video_start", severity="medium",
                    message=(f"'{event.name}' fired before any '{video_start}' in this session. "
                             "Video Completed should always be preceded by Video Started."),
                    event_id=event.event_id, event_name=event.name,
                ))

        # --- 21. Content duration validation ---
        if "content_duration_validation" in enabled:
            duration = _get_property(event.properties, "duration_seconds") \
                or _get_property(event.properties, "duration")
            if duration is not None:
                if isinstance(duration, bool) or not isinstance(duration, (int, float)):
                    # Suppress when wrong_property_type already fired for duration on
                    # this event — same root cause, one signal is enough.
                    if event.event_id not in _revenue_type_issued:
                        issues.append(Issue(
                            issue_type="content_duration_validation", severity="medium",
                            message=(f"Duration in '{event.name}' must be a number, "
                                     f"got {type(duration).__name__} '{duration}'."),
                            event_id=event.event_id, event_name=event.name,
                        ))
                elif duration < 0:
                    issues.append(Issue(
                        issue_type="content_duration_validation", severity="medium",
                        message=(f"Duration in '{event.name}' is negative ({duration}s). "
                                 "Duration must be 0 or greater."),
                        event_id=event.event_id, event_name=event.name,
                    ))

        # --- 22. Conditional property checks (D1) ---
        # Evaluate each ConditionalRule on the spec: if when_property equals
        # when_value then then_property must be present (and, if then_allowed_values
        # is non-empty, its value must be one of those values).
        if "conditional_property" in enabled and spec.conditional_rules:
            for rule in spec.conditional_rules:
                trigger_val = _get_property(event.properties, rule.when_property)
                # Only fire when the trigger property is present AND matches
                if trigger_val is None:
                    continue
                # Loose equality so int 1 matches when_value "1" only if types match;
                # keep it strict — plan authors should use the exact value type.
                if trigger_val != rule.when_value:
                    continue
                # Condition is met — check the consequent property
                if not _has_property(event.properties, rule.then_property):
                    issues.append(Issue(
                        issue_type="conditional_property_missing",
                        severity="high",
                        message=(
                            f"'{event.name}': when '{rule.when_property}' is "
                            f"'{rule.when_value}', '{rule.then_property}' is required "
                            f"but was not sent."
                        ),
                        event_id=event.event_id,
                        event_name=event.name,
                    ))
                elif rule.then_allowed_values:
                    actual = _get_property(event.properties, rule.then_property)
                    if actual not in rule.then_allowed_values:
                        issues.append(Issue(
                            issue_type="conditional_enum_violation",
                            severity="high",
                            message=(
                                f"'{event.name}': when '{rule.when_property}' is "
                                f"'{rule.when_value}', '{rule.then_property}' must be "
                                f"one of {rule.then_allowed_values} (got '{actual}')."
                            ),
                            event_id=event.event_id,
                            event_name=event.name,
                        ))

        state.mark_event(
            event,
            purchase_roles=roles if "purchase_event" in roles else None,
            order_id_prop=order_id_prop,
        )

        # --- 7. Sequence validation (evaluated last so semantic checks run first) ---
        # Suppressed when a semantic ordering check (10/11/13/14/16/17/18/19/20)
        # already fired for this event — same root cause, one signal is enough.
        if "sequence_validation" in enabled and spec.allowed_previous_events \
                and event.event_id not in _semantic_ordering_issued:
            seen_for_identity = state.seen_event_names_by_identity.get(identity, set())
            if not any(prev in seen_for_identity for prev in spec.allowed_previous_events):
                issues.append(Issue(
                    issue_type="funnel_order_violation", severity="medium",
                    message=(f"'{event.name}' appeared before its allowed predecessor. "
                             f"Expected one of {spec.allowed_previous_events} earlier in the flow."),
                    event_id=event.event_id, event_name=event.name,
                ))

    return issues
