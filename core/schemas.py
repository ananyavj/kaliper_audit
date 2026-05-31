#core/schemas.py
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ConditionalRule:
    """
    Encodes: "if <when_property> == <when_value> then <then_property> must be present
              (and optionally must equal one of <then_allowed_values>)."

    Examples from a real plan:
      - if payment_method == "emi"  → emi_bank must be present
      - if payment_method == "upi"  → upi_app must be present
      - if order_type == "gift"     → gift_message must be present
      - if content_type == "video"  → duration_seconds must be present
                                       AND in range (handled by value check)

    Fields
    ------
    when_property       : the trigger property name (e.g. "payment_method")
    when_value          : the trigger value (e.g. "emi")
    then_property       : the property that must exist when the condition is met
    then_allowed_values : optional — if non-empty, then_property's value must be
                          one of these (acts as a context-dependent enum check)
    """
    when_property: str
    when_value: Any
    then_property: str
    then_allowed_values: List[Any] = field(default_factory=list)


@dataclass
class TrackingEventSpec:
    name: str
    required_properties: List[str] = field(default_factory=list)
    property_types: Dict[str, str] = field(default_factory=dict)
    identity_required: bool = False
    allowed_previous_events: List[str] = field(default_factory=list)
    allowed_values: Dict[str, List[Any]] = field(default_factory=dict)
    # D1: conditional rules for this event (evaluated in detectors.py check #22)
    conditional_rules: List[ConditionalRule] = field(default_factory=list)


@dataclass
class IncomingEvent:
    name: str
    timestamp: str
    properties: Dict[str, Any]
    event_id: str
    user_id: Optional[str] = None
    anonymous_id: Optional[str] = None


@dataclass
class Issue:
    issue_type: str
    severity: str
    message: str
    event_id: str
    event_name: str
