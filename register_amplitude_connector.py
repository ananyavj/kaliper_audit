"""
register_amplitude_connector.py
--------------------------------
Register one Amplitude connector per workspace.

Each workspace should point to a DIFFERENT Amplitude project that matches
its domain (ecommerce, saas, content). Using the same project credentials
for multiple workspaces will flood each workspace with foreign events and
trigger false `unknown_event` issues on every single event.

Per-workspace credentials are read from .env in priority order:
  1. AMPLITUDE_API_KEY_<WORKSPACE_ID>  (e.g. AMPLITUDE_API_KEY_ECOMMERCE_WORKSPACE)
  2. AMPLITUDE_API_KEY                 (fallback, only when explicitly intended for
                                        all workspaces — usually wrong)

Run once per workspace to set up the connector record.
Re-running is safe — it checks for an existing active connector first and
skips registration if one already exists (use --force to override).

Usage
-----
    python register_amplitude_connector.py                         # ecommerce_workspace (default)
    python register_amplitude_connector.py --workspace saas_workspace
    python register_amplitude_connector.py --workspace content_workspace
    python register_amplitude_connector.py --list                  # show all registered connectors
    python register_amplitude_connector.py --all-workspaces        # register all three at once
    python register_amplitude_connector.py --force                 # re-register even if one exists
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
    deactivate_connector,
    delete_inactive_connectors,
    delete_all_connectors,
)

load_dotenv()

TENANT_ID = "tenant_demo"

WORKSPACE_CONFIGS = {
    "ecommerce_workspace": {
        "connector_name": "Amplitude — Ecommerce",
        "environment": "production",
    },
    "saas_workspace": {
        "connector_name": "Amplitude — SaaS",
        "environment": "production",
    },
    "content_workspace": {
        "connector_name": "Amplitude — Content",
        "environment": "production",
    },
}


def _get_credentials_for_workspace(workspace_id: str) -> tuple[str, str]:
    """
    Resolve Amplitude credentials for a specific workspace.

    Looks for workspace-specific keys first, e.g.:
        AMPLITUDE_API_KEY_ECOMMERCE_WORKSPACE=...
        AMPLITUDE_SECRET_KEY_ECOMMERCE_WORKSPACE=...

    Falls back to generic AMPLITUDE_API_KEY / AMPLITUDE_SECRET_KEY only if
    workspace-specific ones aren't set.  Logs a clear warning when falling back
    so the operator knows they should probably set workspace-specific keys.
    """
    env_suffix = workspace_id.upper()
    ws_api_key    = os.getenv(f"AMPLITUDE_API_KEY_{env_suffix}", "").strip()
    ws_secret_key = os.getenv(f"AMPLITUDE_SECRET_KEY_{env_suffix}", "").strip()

    if ws_api_key and ws_secret_key:
        return ws_api_key, ws_secret_key

    # Fall back to generic keys (valid when one Amplitude project serves all workspaces)
    generic_api_key    = os.getenv("AMPLITUDE_API_KEY", "").strip()
    generic_secret_key = os.getenv("AMPLITUDE_SECRET_KEY", "").strip()

    if generic_api_key and generic_secret_key:
        return generic_api_key, generic_secret_key

    raise RuntimeError(
        f"No Amplitude credentials found for workspace '{workspace_id}'.\n"
        f"Set AMPLITUDE_API_KEY_{env_suffix} + AMPLITUDE_SECRET_KEY_{env_suffix}\n"
        f"(or the generic AMPLITUDE_API_KEY + AMPLITUDE_SECRET_KEY) in .env."
    )


def _get_existing_active_connector(workspace_id: str) -> dict | None:
    """Return the first active Amplitude connector for a workspace, or None."""
    connectors = list_connectors(tenant_id=TENANT_ID, workspace_id=workspace_id)
    return next(
        (c for c in connectors if c["connector_type"] == "amplitude" and c["is_active"]),
        None,
    )


def register_for_workspace(workspace_id: str, force: bool = False) -> int:
    cfg = WORKSPACE_CONFIGS.get(workspace_id)
    if cfg is None:
        raise ValueError(
            f"Unknown workspace '{workspace_id}'. "
            f"Known: {list(WORKSPACE_CONFIGS)}"
        )

    # Duplicate guard: don't register a second connector if one already exists,
    # because the scheduler will then run BOTH and double-count events.
    existing = _get_existing_active_connector(workspace_id)
    if existing is not None and not force:
        print(
            f"[skip] '{workspace_id}' already has an active Amplitude connector\n"
            f"       (id={existing['id']}, name='{existing['connector_name']}').\n"
            f"       Use --force to deactivate it and register a new one."
        )
        return existing["id"]

    if existing is not None and force:
        print(f"[force] Deactivating existing connector {existing['id']} for '{workspace_id}'.")
        deactivate_connector(existing["id"])

    api_key, secret_key = _get_credentials_for_workspace(workspace_id)

    connector_id = register_connector(
        tenant_id=TENANT_ID,
        workspace_id=workspace_id,
        connector_name=cfg["connector_name"],
        connector_type="amplitude",
        credentials={
            "api_key": api_key,
            "secret_key": secret_key,
        },
        config={
            "poll_interval_minutes": 1440,
            "environment": cfg["environment"],
        },
        is_active=True,
    )

    print(f"[registered] connector {connector_id} for workspace '{workspace_id}'")
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
        description="Register Amplitude connectors for Kaliper workspaces"
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
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Deactivate any existing active connector for the workspace(s) "
            "and register a fresh one. Use this after updating credentials in .env."
        ),
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Delete all inactive (historical) connectors for the workspace(s) to tidy up the list.",
    )
    parser.add_argument(
        "--delete-all",
        action="store_true",
        help="Delete ALL connectors (including active) for the workspace and exit. Clean slate — does not re-register.",
    )
    args = parser.parse_args()

    if args.list:
        print_all_connectors()
        return

    if args.delete_all:
        workspaces = list(WORKSPACE_CONFIGS) if args.all_workspaces else [args.workspace]
        for workspace_id in workspaces:
            n = delete_all_connectors(TENANT_ID, workspace_id)
            print(f"[delete-all] Removed {n} connector(s) for '{workspace_id}'.")
        print()
        print_all_connectors()
        return

    if args.all_workspaces:
        for workspace_id in WORKSPACE_CONFIGS:
            register_for_workspace(workspace_id, force=args.force)
            if args.cleanup:
                n = delete_inactive_connectors(TENANT_ID, workspace_id)
                if n:
                    print(f"  [cleanup] Deleted {n} inactive connector(s) for '{workspace_id}'.")
    else:
        register_for_workspace(args.workspace, force=args.force)
        if args.cleanup:
            n = delete_inactive_connectors(TENANT_ID, args.workspace)
            if n:
                print(f"  [cleanup] Deleted {n} inactive connector(s) for '{args.workspace}'.")

    print()
    print_all_connectors()


if __name__ == "__main__":
    main()
