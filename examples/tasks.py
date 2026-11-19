"""Demo tasks. Each sleeps to simulate long-running work (``YADTQ_DEMO_DELAY`` seconds)."""

import os
import time

DELAY = float(os.environ.get("YADTQ_DEMO_DELAY", "3"))


def add(a, b):
    time.sleep(DELAY)
    return a + b


def sub(a, b):
    time.sleep(DELAY)
    return a - b


def multiply(a, b):
    time.sleep(DELAY)
    return a * b


def divide(a, b):
    time.sleep(DELAY)
    return a / b  # divide(1, 0) demonstrates retries and permanent failure


TASKS = {"add": add, "sub": sub, "multiply": multiply, "divide": divide}
