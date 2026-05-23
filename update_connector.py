#update_connector.property
from core.storage import get_connection
import json

conn = get_connection()
cur = conn.cursor()

config = {
    "poll_interval_minutes": 15,
    "environment": "production",
    "flow_type": "clean"
}

cur.execute(
    """
    UPDATE connectors
    SET config_json = ?
    WHERE id = 1
    """,
    (json.dumps(config),)
)

conn.commit()
conn.close()

print("Connector updated.")