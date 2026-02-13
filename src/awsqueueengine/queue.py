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
    """ Dequeue the next job item from the queue.

    This function removes and returns the next job item from the queue, prioritizing high-priority jobs.
    """
    q = load_queue()
    if not q:
        return None
    high_idx = None
    for i, queued_item in enumerate(q):
        if isinstance(queued_item, dict) and queued_item.get("priority") == "high":
            high_idx = i
            break
    if high_idx is None:
        item = q.pop(0)
    else:
        item = q.pop(high_idx)
    save_queue(q)
    if isinstance(item, str):
        return {"cmd": item, "payload": None, "priority": "normal"}
    if isinstance(item, dict) and "priority" not in item:
        item["priority"] = "normal"
    return item
