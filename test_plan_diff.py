#test_plan_diff.py
from core.plan_loader import load_tracking_plan
from core.plan_normalizer import normalize_specs
from core.plan_diff import compare_tracking_plans


old_specs = normalize_specs(
    load_tracking_plan("sample_data/tracking_plan_ecommerce.json")
)

new_specs = normalize_specs(
    load_tracking_plan("sample_data/tracking_plan_ecommerce_v2.json")
)

diff = compare_tracking_plans(old_specs, new_specs)

print("\nADDED EVENTS")
for event in diff.added_events:
    print("-", event)

print("\nREMOVED EVENTS")
for event in diff.removed_events:
    print("-", event)

print("\nMODIFIED EVENTS")
for change in diff.modified_events:
    print(f"- [{change.severity}] {change.message}")

print("\nBREAKING CHANGES")
for change in diff.breaking_changes:
    print(f"- [{change.severity}] {change.message}")

print("\nWARNINGS")
for change in diff.warnings:
    print(f"- [{change.severity}] {change.message}")

print("\nCOMPATIBILITY SCORE")
print(diff.compatibility_score)