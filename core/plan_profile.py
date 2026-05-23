#core/plan_profile.py
#
# This file is intentionally kept as a hard redirect so that any stale import
# fails loudly with a clear message instead of silently working via re-export.
#
# DO NOT import from this file. Import PlanProfile from core.plan_analyzer:
#
#   from core.plan_analyzer import PlanProfile
#
# This file will be deleted once we are certain nothing in the codebase imports
# from it directly (confirmed: no file in the project does as of this audit).
#
raise ImportError(
    "core.plan_profile is a deleted stub. "
    "Import PlanProfile from core.plan_analyzer instead:\n"
    "    from core.plan_analyzer import PlanProfile"
)
