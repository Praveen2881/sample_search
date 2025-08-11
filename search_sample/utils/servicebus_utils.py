# utils/servicebus_utils.py
"""
Service Bus utilities — simulates Azure Service Bus messaging locally.
Replace with azure-servicebus SDK in production.
"""

import json
from pathlib import Path
from config import AZURE_SERVICE_BUS_CONNECTION_STRING

QUEUE_DIR = Path("./local_servicebus_queues")
QUEUE_DIR.mkdir(exist_ok=True)

def send_message(queue_name: str, message: dict):
    """Simulate sending a message to a Service Bus queue."""
    queue_path = QUEUE_DIR / f"{queue_name}.queue"
    with open(queue_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(message) + "\n")
    print(f"[ServiceBus] Sent to {queue_name}: {message}")

def receive_messages(queue_name: str):
    """Simulate receiving all messages from a queue."""
    queue_path = QUEUE_DIR / f"{queue_name}.queue"
    if not queue_path.exists():
        return []
    messages = []
    with open(queue_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                messages.append(json.loads(line.strip()))
            except json.JSONDecodeError:
                pass
    # Clear the queue
    queue_path.unlink()
    print(f"[ServiceBus] Received {len(messages)} messages from {queue_name}")
    return messages
