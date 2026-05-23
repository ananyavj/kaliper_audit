#core/detectors.py
from core.schemas import Issue, TrackingEventSpec, IncomingEvent
from core.state_store import StreamState

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
    # Substring match on the lowered event name. This is intentionally
    # permissive so detectors catch variants like "User Logged In" matching
    # "login", but it also means composite event names like "Catalog Login
    # Flow" or "Relogin Attempt" will match the "login" pattern.
    # If false positives appear in production, switch to word-boundary checks
    # (e.g. re.search(r'\b' + re.escape(pattern) + r'\b', lowered)) per
    # pattern, or maintain an explicit allowlist of canonical event names.
    lowered = name.lower()
    return any(pattern in lowered for pattern in patterns)


def detect_issues(
    events: list[IncomingEvent],
    plan_specs: list[TrackingEventSpec],
    enabled_checks: set[str] | None = None,
    state: StreamState | None = None,
) -> list[Issue]:
    enabled = set(enabled_checks) if enabled_checks is not None else set(DEFAULT_ENABLED_CHECKS)
    state = state or StreamState()

    issues: list[Issue] = []
    spec_map = {spec.name: spec for spec in plan_specs}

    # Bug 5 fix: only learn identity from events that are NOT duplicates.
    # Learning from duplicate events can stitch an anonymous_id to a user_id
    # that the duplicate event shouldn't have had access to, which misattributes
    # subsequent issues to the wrong identity.
    seen_in_batch: set[str] = set()
    for event in events:
        if event.event_id not in state.seen_event_ids and event.event_id not in seen_in_batch:
            state.learn_identity(event)
        seen_in_batch.add(event.event_id)

    # Second pass: run detections using stitched identity.
    for event in events:
        spec = spec_map.get(event.name)

        # Bug 1 fix: resolve identity unconditionally at the top of the loop.
        # Previously identity was only set inside the sequence_validation block,
        # so subscription_without_trial (and any future check) would throw
        # NameError if sequence_validation was disabled or had no allowed_previous_events.
        identity = state.resolve_identity(event)

        # Mark the event_id FIRST so duplicate detection is correct:
        # if this id was already seen in a prior call (or earlier in this batch),
        # has_seen_event_id returns True before we add it again.
        already_seen_id = state.has_seen_event_id(event.event_id)
        state.seen_event_ids.add(event.event_id)

        if "duplicate_event_id" in enabled and already_seen_id:
            issues.append(
                Issue(
                    issue_type="duplicate_event",
                    severity="high",
                    message=f"Duplicate event_id detected for '{event.name}'.",
                    event_id=event.event_id,
                    event_name=event.name,
                )
            )

        if spec is None:
            if "unknown_event" in enabled:
                issues.append(
                    Issue(
                        issue_type="unknown_event",
                        severity="medium",
                        message=f"Event '{event.name}' is not in the tracking plan.",
                        event_id=event.event_id,
                        event_name=event.name,
                    )
                )
            state.mark_event(event)
            continue

        if "missing_required_property" in enabled:
            for prop in spec.required_properties:
                if prop not in event.properties:
                    issues.append(
                        Issue(
                            issue_type="missing_property",
                            severity="high",
                            message=f"Missing required property '{prop}' in '{event.name}'.",
                            event_id=event.event_id,
                            event_name=event.name,
                        )
                    )

        if "wrong_property_type" in enabled:
            for prop_name, expected_type_name in spec.property_types.items():
                if prop_name in event.properties:
                    expected_type = _expected_python_type(expected_type_name)
                    value = event.properties[prop_name]

                    if expected_type is not None and not isinstance(value, expected_type):
                        issues.append(
                            Issue(
                                issue_type="wrong_property_type",
                                severity="high",
                                message=(
                                    f"Property '{prop_name}' in '{event.name}' should be "
                                    f"{expected_type_name}, got {type(value).__name__}."
                                ),
                                event_id=event.event_id,
                                event_name=event.name,
                            )
                        )

        if "missing_identity" in enabled and spec.identity_required and not event.user_id:
            issues.append(
                Issue(
                    issue_type="missing_identity",
                    severity="high",
                    message=f"'{event.name}' requires user_id but it is missing.",
                    event_id=event.event_id,
                    event_name=event.name,
                )
            )

        if "sequence_validation" in enabled and spec.allowed_previous_events:
            # identity already resolved above — no re-assignment needed here.
            seen_for_identity = state.seen_event_names_by_identity.get(identity, set())

            if not any(prev in seen_for_identity for prev in spec.allowed_previous_events):
                issues.append(
                    Issue(
                        issue_type="sequence_error",
                        severity="medium",
                        message=(
                            f"'{event.name}' appeared before its allowed predecessor. "
                            f"Expected one of {spec.allowed_previous_events} earlier in the flow."
                        ),
                        event_id=event.event_id,
                        event_name=event.name,
                    )
                )

        if event.name == "Order Completed":
            order_id = event.properties.get("order_id")
            revenue = event.properties.get("revenue")
            currency = event.properties.get("currency")

            if "purchase_without_checkout" in enabled:
                if not state.has_seen_event(event, "Checkout Started"):
                    issues.append(
                        Issue(
                            issue_type="purchase_without_checkout",
                            severity="critical",
                            message="Order Completed occurred without a prior Checkout Started.",
                            event_id=event.event_id,
                            event_name=event.name,
                        )
                    )

            if "duplicate_purchase" in enabled:
                if state.mark_order(order_id):
                    issues.append(
                        Issue(
                            issue_type="duplicate_purchase",
                            severity="critical",
                            message=f"Duplicate purchase detected for order_id '{order_id}'.",
                            event_id=event.event_id,
                            event_name=event.name,
                        )
                    )

            if "revenue_currency_validation" in enabled:
                if isinstance(revenue, (int, float)) and revenue <= 0:
                    issues.append(
                        Issue(
                            issue_type="invalid_revenue",
                            severity="high",
                            message=f"Revenue in '{event.name}' must be positive.",
                            event_id=event.event_id,
                            event_name=event.name,
                        )
                    )

                if isinstance(currency, str) and len(currency.strip()) != 3:
                    issues.append(
                        Issue(
                            issue_type="invalid_currency",
                            severity="high",
                            message=f"Currency in '{event.name}' should look like a 3-letter code.",
                            event_id=event.event_id,
                            event_name=event.name,
                        )
                    )

        if "login_without_signup" in enabled and _name_matches(event.name, ["login", "log in", "signed in"]):
            if identity not in state.signed_up_identities:
                issues.append(
                    Issue(
                        issue_type="login_without_signup",
                        severity="high",
                        message="Login occurred before any signup event in this session.",
                        event_id=event.event_id,
                        event_name=event.name,
                    )
                )

        if "trial_without_signup" in enabled and _name_matches(event.name, ["trial started", "started trial", "trial begin"]):
            if identity not in state.signed_up_identities:
                issues.append(
                    Issue(
                        issue_type="trial_without_signup",
                        severity="high",
                        message="Trial start occurred before any signup event in this session.",
                        event_id=event.event_id,
                        event_name=event.name,
                    )
                )

        if "subscription_without_trial" in enabled and _name_matches(event.name, ["subscription started", "subscription created", "plan subscribed"]):
            # Bug 1 fix: `identity` is now guaranteed to exist here (resolved at top of loop).
            if not state.has_seen_event(event, "Trial Started") and identity not in state.signed_up_identities:
                issues.append(
                    Issue(
                        issue_type="subscription_without_trial",
                        severity="high",
                        message="Subscription event occurred before trial or signup in this session.",
                        event_id=event.event_id,
                        event_name=event.name,
                    )
                )

        if "article_read_without_page_view" in enabled and _name_matches(event.name, ["article read", "content read", "read article"]):
            if not state.has_seen_event(event, "Page Viewed") and not state.has_seen_event(event, "Page View"):
                issues.append(
                    Issue(
                        issue_type="article_read_without_page_view",
                        severity="medium",
                        message="Article read occurred before any page view.",
                        event_id=event.event_id,
                        event_name=event.name,
                    )
                )

        if "video_complete_without_video_start" in enabled and _name_matches(event.name, ["video completed", "video complete", "video watched"]):
            if not state.has_seen_event(event, "Video Started") and not state.has_seen_event(event, "Video Start"):
                issues.append(
                    Issue(
                        issue_type="video_complete_without_video_start",
                        severity="medium",
                        message="Video completion occurred before any video start.",
                        event_id=event.event_id,
                        event_name=event.name,
                    )
                )

        if "content_duration_validation" in enabled:
            if "duration" in event.properties or "duration_seconds" in event.properties:
                duration = event.properties.get("duration_seconds", event.properties.get("duration"))
                if not isinstance(duration, (int, float)) or duration <= 0:
                    issues.append(
                        Issue(
                            issue_type="content_duration_validation",
                            severity="medium",
                            message="Content duration should be a positive number.",
                            event_id=event.event_id,
                            event_name=event.name,
                        )
                    )

        state.mark_event(event)

    return issues
