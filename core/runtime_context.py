#core/runtime_context.py
from dataclasses import dataclass


@dataclass
class RuntimeContext:
    tenant_id: str
    workspace_id: str
    environment: str
    source: str
    tenant_name: str = "Demo Tenant"
    workspace_name: str = "Demo Workspace"