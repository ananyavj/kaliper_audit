#core/plan_loader.py
import json
from core.schemas import TrackingEventSpec

def load_tracking_plan(path: str) -> list[TrackingEventSpec]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    specs = []
    for event in data.get("events", []):
        specs.append(
            TrackingEventSpec(
                name = event["name"],
                required_properties = event.get("required_properties", []),
                property_types = event.get("property_types", {}),
                identity_required = event.get("identity_required", False),
                allowed_previous_events = event.get("allowed_previous_events", []),
            )
        )
    return specs 
