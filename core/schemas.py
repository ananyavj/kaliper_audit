#core/schemas.py
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

@dataclass
class TrackingEventSpec:
    name: str
    required_properties: List[str] = field(default_factory=list)
    property_types: Dict[str, str] = field(default_factory=dict)
    identity_required: bool = False
    allowed_previous_events: List[str] = field(default_factory=list)

@dataclass
class IncomingEvent:
    name: str
    timestamp: str
    properties: Dict[str, Any]
    event_id: str
    # Bug 4 fix: Optional fields must have = None defaults so the dataclass
    # can be constructed with keyword args that omit either field.
    # Previously these were declared Optional[str] without a default, which
    # forced callers to pass them positionally or always supply both — any
    # keyword-only construction that omitted one raised TypeError.
    user_id: Optional[str] = None
    anonymous_id: Optional[str] = None

@dataclass
class Issue:
    issue_type: str
    severity: str
    message: str
    event_id: str
    event_name: str
