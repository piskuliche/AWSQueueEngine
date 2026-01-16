# Queue management helpers
import json
from pathlib import Path
from .config import QUEUE_FILE

def load_queue():
    if QUEUE_FILE.exists():
        try:
            data = json.loads(QUEUE_FILE.read_text())
            return data if isinstance(data, list) else []
        except Exception:
            return []
    return []

def save_queue(q):
    QUEUE_FILE.write_text(json.dumps(q, indent=2))

def enqueue_item(item):
    q = load_queue()
    q.append(item)
    save_queue(q)

def dequeue():
    q = load_queue()
    if not q:
        return None
    item = q.pop(0)
    save_queue(q)
    if isinstance(item, str):
        return {"cmd": item, "payload": None}
    return item
