#core/plan_normalizer.py
from core.schemas import TrackingEventSpec

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


def normalize_event_name(name: str) -> str:
    return " ".join(name.strip().split())


def normalize_property_name(name: str) -> str:
    return "_".join(name.strip().lower().split())


def normalize_type(type_name: str) -> str:
    cleaned = type_name.strip().lower()
    return TYPE_ALIASES.get(cleaned, cleaned)


def normalize_spec(spec: TrackingEventSpec) -> TrackingEventSpec:
    return TrackingEventSpec(
        name=normalize_event_name(spec.name),
        required_properties=[normalize_property_name(p) for p in spec.required_properties],
        property_types={
            normalize_property_name(k): normalize_type(v)
            for k, v in spec.property_types.items()
        },
        identity_required=spec.identity_required,
        allowed_previous_events=[normalize_event_name(e) for e in spec.allowed_previous_events],
    )


def normalize_specs(specs: list[TrackingEventSpec]) -> list[TrackingEventSpec]:
    return [normalize_spec(spec) for spec in specs]
