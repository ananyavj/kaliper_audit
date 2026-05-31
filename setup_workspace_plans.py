# setup_workspace_plans.py
# ============================================================
# Creates workspace records in the DB without registering any
# default tracking plan.
#
# Design rationale
# ----------------
# In real analytics work, a tracking plan is a document you
# request from the product/engineering team. Kaliper should
# never auto-load one — the user uploads it explicitly through
# the dashboard "Upload Plan" step.
#
# This script only ensures the tenant + workspace rows exist
# so the dashboard workspace switcher has something to show.
# Plans come in only via /upload-plan (dashboard UI).
#
# Future-scope workspaces (b2b_wholesale, digital_goods, etc.)
# are listed here as slots — they exist in the DB with no plan
# attached until someone actually uploads one.
#
# Usage
# -----
#   python setup_workspace_plans.py
#
# Safe to re-run — uses INSERT OR IGNORE so no duplicates.
# ============================================================

from core.storage import initialize_db, ensure_tenant, ensure_workspace

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TENANT_ID   = "tenant_demo"
TENANT_NAME = "Demo Tenant"

# Each tuple: (workspace_id, human-readable name)
# Add new workspaces here as future scope — no plan file needed.
WORKSPACES = [
    ("ecommerce_workspace", "Ecommerce"),
    ("saas_workspace",      "SaaS"),
    ("content_workspace",   "Content Platform"),
    # Future scope — uncomment when ready to use:
    # ("b2b_wholesale_workspace",   "B2B Wholesale"),
    # ("digital_goods_workspace",   "Digital Goods"),
    # ("subscription_box_workspace","Subscription Box"),
]


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

def setup() -> None:
    initialize_db()
    ensure_tenant(TENANT_ID, TENANT_NAME)

    for workspace_id, workspace_name in WORKSPACES:
        ensure_workspace(workspace_id, TENANT_ID, workspace_name)
        print(f"[ok] workspace '{workspace_id}' ({workspace_name}) — ready, no plan attached")

    print()
    print("Done. Upload a tracking plan for each workspace via the dashboard.")


if __name__ == "__main__":
    setup()
