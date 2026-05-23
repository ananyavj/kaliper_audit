#simulators/simulators_content.py
from uuid import uuid4
from datetime import datetime, timezone

from core.schemas import IncomingEvent


def _now():
    return datetime.now(timezone.utc).isoformat()


def generate_content_clean_flow(user_id="user_1", anonymous_id="anon_1"):
    ts = _now()

    return [
        IncomingEvent(
            name="Page Viewed",
            user_id=user_id,
            anonymous_id=anonymous_id,
            timestamp=ts,
            properties={
                "page_url": "https://example.com/blog/analytics",
                "session_id": "sess_1",
            },
            event_id=str(uuid4()),
        ),
        IncomingEvent(
            name="Article Read",
            user_id=user_id,
            anonymous_id=anonymous_id,
            timestamp=ts,
            properties={
                "article_id": "art_1",
                "duration_seconds": 120,
            },
            event_id=str(uuid4()),
        ),
        IncomingEvent(
            name="Video Started",
            user_id=user_id,
            anonymous_id=anonymous_id,
            timestamp=ts,
            properties={
                "video_id": "vid_1",
                "duration_seconds": 300,
            },
            event_id=str(uuid4()),
        ),
        IncomingEvent(
            name="Video Completed",
            user_id=user_id,
            anonymous_id=anonymous_id,
            timestamp=ts,
            properties={
                "video_id": "vid_1",
                "duration_seconds": 300,
            },
            event_id=str(uuid4()),
        ),
    ]


def generate_content_flow_with_errors():
    ts = _now()

    return [
        IncomingEvent(
            name="Article Read",
            user_id="user_1",
            anonymous_id="anon_1",
            timestamp=ts,
            properties={
                "article_id": "art_1",
                "duration_seconds": 120,
            },
            event_id=str(uuid4()),
        ),
        IncomingEvent(
            name="Page Viewed",
            user_id="user_1",
            anonymous_id="anon_1",
            timestamp=ts,
            properties={
                "page_url": "https://example.com/blog/analytics",
                "session_id": "sess_1",
            },
            event_id=str(uuid4()),
        ),
        IncomingEvent(
            name="Video Completed",
            user_id="user_1",
            anonymous_id="anon_1",
            timestamp=ts,
            properties={
                "video_id": "vid_1",
                "duration_seconds": 300,
            },
            event_id=str(uuid4()),
        ),
        IncomingEvent(
            name="Video Started",
            user_id="user_1",
            anonymous_id="anon_1",
            timestamp=ts,
            properties={
                "video_id": "vid_1",
                "duration_seconds": "300",  # error: should be number
            },
            event_id=str(uuid4()),
        ),
        IncomingEvent(
            name="Article Read",
            user_id=None,  # error: missing identity
            anonymous_id="anon_1",
            timestamp=ts,
            properties={
                "article_id": "art_2",
                "duration_seconds": -10,  # error: invalid duration
            },
            event_id=str(uuid4()),
        ),
    ]