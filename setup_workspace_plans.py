#setup_workspace_plans.py
from core.plan_registry import register_plan_from_file

TENANT_ID = "tenant_demo"

WORKSPACES = [
    ("ecommerce_workspace", "sample_data/tracking_plan_ecommerce.json", "ecommerce"),
    ("saas_workspace", "sample_data/tracking_plan_saas.json", "saas"),
    ("content_workspace", "sample_data/tracking_plan_content.json", "content"),
]

for workspace_id, plan_path, prefix in WORKSPACES:
    plan = register_plan_from_file(
        tenant_id=TENANT_ID,
        workspace_id=workspace_id,
        plan_path=plan_path,
        version_prefix=prefix,
        make_active=True,
    )
    print(plan)