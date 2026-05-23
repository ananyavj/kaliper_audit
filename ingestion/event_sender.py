#ingestion/event_sender.py
import time
import requests

from simulators.simulators import generate_flow_with_errors


INGEST_URL = "http://127.0.0.1:5000/ingest"


def send_events():
    events = generate_flow_with_errors()

    for event in events:
        payload = {
            "name": event.name,
            "user_id": event.user_id,
            "anonymous_id": event.anonymous_id,
            "timestamp": event.timestamp,
            "properties": event.properties,
            "event_id": event.event_id,
        }

        response = requests.post(INGEST_URL, json=payload)
        print(f"Sent {event.name} -> {response.status_code}")
        time.sleep(1)


if __name__ == "__main__":
    send_events()