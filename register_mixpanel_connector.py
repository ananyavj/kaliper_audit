"""
register_mixpanel_connector.py
-------------------------------
Register one Mixpanel connector per workspace.

Mixpanel credentials required:
  MIXPANEL_API_SECRET   — Project Secret (found in Project Settings → Access Keys)
  MIXPANEL_PROJECT_ID   — (optional) used for display/logging only

Usage
-----
    python register_mixpanel_connector.py                           # ecommerce_workspace (default)
    python register_mixpanel_connector.py --workspace saas_workspace
    python register_mixpanel_connector.py --workspace content_workspace
    python register_mixpanel_connector.py --all-workspaces          # register all three at once
    python register_mixpanel_connector.py --list                    # show all registered connectors
"""

from __future__ import annotations

import argparse
import os

from dotenv import load_dotenv

from core.connector_registry import (
    initialize_connector_tables,
    register_connector,
    list_connectors,
    list_all_workspaces,
)

load_dotenv()

TENANT_ID = "tenant_demo"

WORKSPACE_CONFIGS = {
    "ecommerce_workspace": {
        "connector_name": "Mixpanel — Ecommerce",
        "environment": "production",
    },
    "saas_workspace": {
        "connector_name": "Mixpanel — SaaS",
        "environment": "production",
    },
    "content_workspace": {
        "connector_name": "Mixpanel — Content",
        "environment": "production",
    },
}


def _get_credentials() -> tuple[str, str]:
    api_secret = os.getenv("MIXPANEL_API_SECRET", "").strip()
    project_id = os.getenv("MIXPANEL_PROJECT_ID", "").strip()
    if not api_secret:
        raise RuntimeError(
            "MIXPANEL_API_SECRET must be set in .env\n"
            "Find it in Mixpanel → Project Settings → Access Keys → Project Secret"
        )
    return api_secret, project_id


def register_for_workspace(workspace_id: str) -> int:
    api_secret, project_id = _get_credentials()
    cfg = WORKSPACE_CONFIGS.get(workspace_id)

    if cfg is None:
        raise ValueError(
            f"Unknown workspace '{workspace_id}'. "
            f"Known: {list(WORKSPACE_CONFIGS)}"
        )

    connector_id = register_connector(
        tenant_id=TENANT_ID,
        workspace_id=workspace_id,
        connector_name=cfg["connector_name"],
        connector_type="mixpanel",
        credentials={
            "api_secret": api_secret,
            "project_id": project_id,
        },
        config={
            "poll_interval_minutes": 1440,   # Mixpanel export has daily granularity
            "environment": cfg["environment"],
        },
        is_active=True,
    )

    print(f"Registered connector {connector_id} for workspace '{workspace_id}'")
    return connector_id


def print_all_connectors() -> None:
    pairs = list_all_workspaces()
    if not pairs:
        print("No connectors registered yet.")
        return

    print(f"\n{'ID':<6} {'Type':<12} {'Workspace':<24} {'Name':<30} {'Active':<8} {'Last sync'}")
    print("-" * 92)
    for pair in pairs:
        connectors = list_connectors(
            tenant_id=pair["tenant_id"],
            workspace_id=pair["workspace_id"],
        )
        for c in connectors:
            last_sync = (c.get("last_sync_at") or "—")[:19]
            print(
                f"{c['id']:<6} {c['connector_type']:<12} {c['workspace_id']:<24} "
                f"{c['connector_name']:<30} {'yes' if c['is_active'] else 'no':<8} {last_sync}"
            )


def main() -> None:
    initialize_connector_tables()

    parser = argparse.ArgumentParser(
        description="Register Mixpanel connectors for Kaliper workspaces"
    )
    parser.add_argument(
        "--workspace",
        default="ecommerce_workspace",
        choices=list(WORKSPACE_CONFIGS),
        help="Which workspace to register a connector for (default: ecommerce_workspace)",
    )
    parser.add_argument(
        "--all-workspaces",
        action="store_true",
        help="Register connectors for all three workspaces at once.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all registered connectors and exit.",
    )
    args = parser.parse_args()

    if args.list:
        print_all_connectors()
        return

    if args.all_workspaces:
        for workspace_id in WORKSPACE_CONFIGS:
            register_for_workspace(workspace_id)
    else:
        register_for_workspace(args.workspace)

    print()
    print_all_connectors()


if __name__ == "__main__":
    main()
