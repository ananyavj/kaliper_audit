# core/progress_store.py
from typing import Dict, Any

PROGRESS_STATE: Dict[str, Dict[str, Any]] = {}

def update_progress(workspace_id: str, status: str, current: int, total: int):
    PROGRESS_STATE[workspace_id] = {
        "status": status,
        "current": current,
        "total": total
    }

def get_progress(workspace_id: str):
    return PROGRESS_STATE.get(workspace_id, {"status": "idle", "current": 0, "total": 0})
