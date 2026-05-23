#core/tenant_models.py
from dataclasses import dataclass


@dataclass
class Tenant:
    tenant_id: str
    tenant_name: str


@dataclass
class Workspace:
    workspace_id: str
    tenant_id: str
    workspace_name: str


@dataclass
class Environment:
    environment: str