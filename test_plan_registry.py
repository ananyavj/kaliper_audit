#test_plan_registry.py
from core.plan_registry import (
    register_and_compare,
    get_latest_plan_version,
    get_active_plan_version,
    list_plan_versions,
)

TENANT_ID = "tenant_demo"
WORKSPACE_ID = "ecommerce_workspace"


print("\nREGISTERING V1 PLAN")

plan_v1, diff_v1 = register_and_compare(
    tenant_id=TENANT_ID,
    workspace_id=WORKSPACE_ID,
    plan_path="sample_data/tracking_plan_ecommerce.json",
    version_prefix="ecommerce",
    make_active=True,
)

print("Stored V1:")
print(plan_v1)

print("\nV1 DIFF")
print(diff_v1)


print("\nREGISTERING V2 PLAN")

plan_v2, diff_v2 = register_and_compare(
    tenant_id=TENANT_ID,
    workspace_id=WORKSPACE_ID,
    plan_path="sample_data/tracking_plan_ecommerce_v2.json",
    version_prefix="ecommerce",
    make_active=True,
)

print("Stored V2:")
print(plan_v2)


print("\nDIFF AGAINST PREVIOUS VERSION")

print("\nADDED EVENTS")
for event in diff_v2.added_events:
    print("-", event)

print("\nREMOVED EVENTS")
for event in diff_v2.removed_events:
    print("-", event)

print("\nMODIFIED EVENTS")
for change in diff_v2.modified_events:
    print(f"- [{change.severity}] {change.message}")

print("\nBREAKING CHANGES")
for change in diff_v2.breaking_changes:
    print(f"- [{change.severity}] {change.message}")

print("\nWARNINGS")
for change in diff_v2.warnings:
    print(f"- [{change.severity}] {change.message}")

print("\nCOMPATIBILITY SCORE")
print(diff_v2.compatibility_score)


print("\nLATEST PLAN VERSION")
latest = get_latest_plan_version(
    tenant_id=TENANT_ID,
    workspace_id=WORKSPACE_ID,
)
print(latest)


print("\nACTIVE PLAN VERSION")
active = get_active_plan_version(
    tenant_id=TENANT_ID,
    workspace_id=WORKSPACE_ID,
)
print(active)


print("\nALL PLAN VERSIONS")
versions = list_plan_versions(
    tenant_id=TENANT_ID,
    workspace_id=WORKSPACE_ID,
)

for version in versions:
    print(version)