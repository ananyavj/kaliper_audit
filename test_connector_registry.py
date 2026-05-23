#test_connector_registry.py
from dotenv import load_dotenv
import os

from core.connector_registry import (
    initialize_connector_tables,
    register_connector,
    list_connectors,
)

load_dotenv()

initialize_connector_tables()

connector_id = register_connector(
    tenant_id="tenant_demo",
    workspace_id="ecommerce_workspace",
    connector_name="Amplitude Production",
    connector_type="amplitude",
    credentials={
        "api_key": os.getenv("AMPLITUDE_API_KEY"),
        "secret_key": os.getenv("AMPLITUDE_SECRET_KEY"),
    },
    config={
        "poll_interval_minutes": 15,
        "environment": "production",
        "flow_type": "clean",
    },
)

print("REGISTERED CONNECTOR:", connector_id)

connectors = list_connectors(
    tenant_id="tenant_demo",
    workspace_id="ecommerce_workspace",
)

print("\nCONNECTORS")
for connector in connectors:
    print(connector)