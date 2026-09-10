"""Central configuration for the Kafka order pipeline.

Every value can be overridden with an environment variable, so the same code
runs against the local docker-compose stack or any other cluster.
"""
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_DIR = PROJECT_ROOT / "schemas"

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

ORDERS_TOPIC = os.getenv("ORDERS_TOPIC", "orders")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "orders.DLQ")

CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "order-processor")
DLQ_CONSUMER_GROUP = os.getenv("DLQ_CONSUMER_GROUP", "dlq-inspector")

TOPIC_PARTITIONS = int(os.getenv("TOPIC_PARTITIONS", "3"))
TOPIC_REPLICATION = int(os.getenv("TOPIC_REPLICATION", "1"))

# --- Retry policy -----------------------------------------------------------
# A record is attempted MAX_ATTEMPTS times in total. Waits grow exponentially
# (RETRY_BASE_DELAY * 2**n) and are capped at RETRY_MAX_DELAY.
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "4"))
RETRY_BASE_DELAY = float(os.getenv("RETRY_BASE_DELAY", "0.5"))
RETRY_MAX_DELAY = float(os.getenv("RETRY_MAX_DELAY", "8.0"))
RETRY_JITTER = float(os.getenv("RETRY_JITTER", "0.3"))  # +/- fraction of the delay

# --- Failure injection (demo only) ------------------------------------------
# Probability that processing a record raises a *transient* error that a retry
# will most likely recover from.
TRANSIENT_FAILURE_RATE = float(os.getenv("TRANSIENT_FAILURE_RATE", "0.25"))
# Probability that the producer emits a structurally invalid order, which can
# never be processed and therefore goes straight to the DLQ.
POISON_MESSAGE_RATE = float(os.getenv("POISON_MESSAGE_RATE", "0.08"))


def read_schema(filename: str) -> str:
    """Return the raw text of an .avsc file from the schemas/ directory."""
    return (SCHEMA_DIR / filename).read_text(encoding="utf-8")
