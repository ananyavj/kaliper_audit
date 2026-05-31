#setup_workspace_plans.py
from core.plan_registry import register_plan_from_file, get_active_plan_version
from core.storage import initialize_db

TENANT_ID = "tenant_demo"

WORKSPACES = [
    ("ecommerce_workspace", "sample_data/tracking_plan_ecommerce.json", "ecommerce"),
    ("saas_workspace",      "sample_data/tracking_plan_saas.json",      "saas"),
    ("content_workspace",   "sample_data/tracking_plan_content.json",   "content"),
]

initialize_db()

for workspace_id, plan_path, prefix in WORKSPACES:
    # Idempotency guard: skip registration if an active plan already exists for
    # this workspace.  Re-running this script was previously safe only by accident
    # (last writer wins) but stacked duplicate plan_versions rows in the DB.
    # Now it only registers when there is genuinely no active plan, preventing
    # the version table from growing unboundedly on repeated runs.
    existing_active = get_active_plan_version(
        tenant_id=TENANT_ID,
        workspace_id=workspace_id,
    )
    if existing_active is not None:
        print(
            f"[skip] {workspace_id}: active plan already registered "
            f"(version_id={existing_active['plan_version_id']}, "
            f"domain={existing_active['domain']}).  "
            f"Upload a new plan via /upload-plan to change it."
        )
        continue

    plan = register_plan_from_file(
        tenant_id=TENANT_ID,
        workspace_id=workspace_id,
        plan_path=plan_path,
        version_prefix=prefix,
        make_active=True,
    )
    print(f"[registered] {workspace_id}: plan_version_id={plan.plan_version_id} domain={plan.domain}")