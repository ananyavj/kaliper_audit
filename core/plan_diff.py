#core/plan_diff.py
from dataclasses import dataclass, field
from typing import Dict, List, Set

from core.schemas import TrackingEventSpec


@dataclass
class PlanChange:
    change_type: str
    severity: str
    message: str


@dataclass
class PlanDiffResult:
    added_events: List[str] = field(default_factory=list)
    removed_events: List[str] = field(default_factory=list)

    modified_events: List[PlanChange] = field(default_factory=list)

    breaking_changes: List[PlanChange] = field(default_factory=list)
    warnings: List[PlanChange] = field(default_factory=list)

    compatibility_score: float = 1.0


def _spec_map(specs: List[TrackingEventSpec]) -> Dict[str, TrackingEventSpec]:
    return {spec.name: spec for spec in specs}


def _added_properties(
    old_props: Set[str],
    new_props: Set[str],
) -> Set[str]:
    return new_props - old_props


def _removed_properties(
    old_props: Set[str],
    new_props: Set[str],
) -> Set[str]:
    return old_props - new_props


def compare_tracking_plans(
    old_specs: List[TrackingEventSpec],
    new_specs: List[TrackingEventSpec],
) -> PlanDiffResult:

    result = PlanDiffResult()

    old_map = _spec_map(old_specs)
    new_map = _spec_map(new_specs)

    old_events = set(old_map.keys())
    new_events = set(new_map.keys())

    added_events = new_events - old_events
    removed_events = old_events - new_events

    result.added_events.extend(sorted(added_events))
    result.removed_events.extend(sorted(removed_events))

    for event_name in sorted(removed_events):
        result.breaking_changes.append(
            PlanChange(
                change_type="removed_event",
                severity="critical",
                message=f"Event '{event_name}' was removed from the tracking plan.",
            )
        )

    shared_events = old_events & new_events

    for event_name in sorted(shared_events):
        old_spec = old_map[event_name]
        new_spec = new_map[event_name]

        old_required = set(old_spec.required_properties)
        new_required = set(new_spec.required_properties)

        added_required = _added_properties(old_required, new_required)
        removed_required = _removed_properties(old_required, new_required)

        for prop in sorted(added_required):
            result.breaking_changes.append(
                PlanChange(
                    change_type="required_property_added",
                    severity="high",
                    message=(
                        f"Event '{event_name}' added required property '{prop}'."
                    ),
                )
            )

        for prop in sorted(removed_required):
            result.warnings.append(
                PlanChange(
                    change_type="required_property_removed",
                    severity="medium",
                    message=(
                        f"Event '{event_name}' removed required property '{prop}'."
                    ),
                )
            )

        old_types = old_spec.property_types
        new_types = new_spec.property_types

        shared_props = set(old_types.keys()) & set(new_types.keys())

        for prop in sorted(shared_props):
            old_type = old_types[prop]
            new_type = new_types[prop]

            if old_type != new_type:
                result.breaking_changes.append(
                    PlanChange(
                        change_type="property_type_changed",
                        severity="critical",
                        message=(
                            f"Event '{event_name}' changed property "
                            f"'{prop}' type from '{old_type}' to '{new_type}'."
                        ),
                    )
                )

        if old_spec.identity_required != new_spec.identity_required:
            result.modified_events.append(
                PlanChange(
                    change_type="identity_requirement_changed",
                    severity="high",
                    message=(
                        f"Event '{event_name}' changed identity requirement "
                        f"from {old_spec.identity_required} "
                        f"to {new_spec.identity_required}."
                    ),
                )
            )

        old_prev = set(old_spec.allowed_previous_events)
        new_prev = set(new_spec.allowed_previous_events)

        if old_prev != new_prev:
            result.modified_events.append(
                PlanChange(
                    change_type="sequence_rules_changed",
                    severity="medium",
                    message=(
                        f"Event '{event_name}' changed allowed previous events."
                    ),
                )
            )

    # Score each change category as a fraction of the 1.0 budget so the
    # total deduction is always in [0.0, 1.0] and the final score is always
    # in [0.0, 1.0] — no clamping needed, no lossy negative intermediate.
    #
    # Weights (share of the 1.0 budget consumed per item):
    #   breaking change : 0.15  → 7 breaking changes exhaust the full budget
    #   modified event  : 0.05  → meaningful but not fatal
    #   warning         : 0.02  → informational
    #
    # min(..., 1.0) caps total deduction at 1.0 so score never goes negative.
    total_deduction = min(
        1.0,
        len(result.breaking_changes) * 0.15
        + len(result.modified_events) * 0.05
        + len(result.warnings) * 0.02,
    )

    result.compatibility_score = round(1.0 - total_deduction, 2)

    return result