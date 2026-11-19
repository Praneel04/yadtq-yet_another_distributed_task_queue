"""Submit a batch of demo tasks and print each result as it finishes.

    python examples/demo_client.py
"""

import time

from yadtq import YADTQClient
from yadtq.store import TERMINAL

JOBS = [
    ("divide", 1, 0),  # fails, is retried, then fails permanently
    ("add", 1, 2),
    ("sub", 5, 3),
    ("multiply", 2, 4),
    ("add", 10, 20),
    ("sub", 15, 7),
    ("multiply", 3, 5),
]


def main():
    client = YADTQClient()
    pending = {client.submit(name, *args): f"{name}{args}" for name, *args in JOBS}
    print(f"submitted {len(pending)} tasks")

    while pending:
        for task_id, call in list(pending.items()):
            info = client.status(task_id)
            if info["status"] in TERMINAL:
                outcome = info.get("result") if info["status"] == "success" else info.get("error")
                print(f"{call:<20} {info['status']:<8} {outcome}  (attempts: {info.get('attempts', 0)})")
                del pending[task_id]
        time.sleep(1)


if __name__ == "__main__":
    main()
