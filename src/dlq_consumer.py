"""Dead Letter Queue inspector.

Reads the DLQ topic and prints each dead-lettered record with the failure
context that the consumer attached: why it failed, how many attempts were made
and exactly where the original record lives (topic/partition/offset) so it can
be replayed once the underlying problem is fixed.

Run it in a third terminal during the demo:

    python src/dlq_consumer.py --from-beginning
"""
import argparse
import signal
import sys
from collections import Counter
from datetime import datetime

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

import config

_running = True


def _stop(_signum, _frame):
    global _running
    _running = False
    print("\n[dlq] stop requested...")


def main():
    parser = argparse.ArgumentParser(description="Inspect the Dead Letter Queue.")
    parser.add_argument("--from-beginning", action="store_true",
                        help="read every dead letter ever written")
    parser.add_argument("--group", default=config.DLQ_CONSUMER_GROUP,
                        help="consumer group id")
    parser.add_argument("--show-payload", action="store_true",
                        help="also print the raw bytes of the original record")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _stop)

    schema_registry = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
    deserializer = AvroDeserializer(schema_registry,
                                    config.read_schema("dead_letter.avsc"))
    ctx = SerializationContext(config.DLQ_TOPIC, MessageField.VALUE)

    consumer = Consumer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "group.id": args.group,
        "auto.offset.reset": "earliest" if args.from_beginning else "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([config.DLQ_TOPIC])

    reasons = Counter()
    total = 0
    print(f"[dlq] watching {config.DLQ_TOPIC} (Ctrl+C to stop)...\n")

    try:
        while _running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[dlq] kafka error: {msg.error()}")
                continue

            record = deserializer(msg.value(), ctx)
            if record is None:
                continue

            total += 1
            reasons[record["errorType"]] += 1
            failed_at = record["failedAt"]
            if isinstance(failed_at, datetime):
                failed_at = failed_at.strftime("%Y-%m-%d %H:%M:%S")

            print(f"[dlq] #{total} orderId={record['orderId']} "
                  f"attempts={record['attempts']} at {failed_at}")
            print(f"      reason : {record['errorType']}: {record['errorMessage']}")
            print(f"      origin : {record['sourceTopic']}"
                  f"[{record['sourcePartition']}]@{record['sourceOffset']}")
            if args.show_payload:
                print(f"      payload: {record['payload']!r}")
            print()
    finally:
        print(f"\n[dlq] total dead letters seen: {total}")
        for reason, count in reasons.most_common():
            print(f"      {reason:<20} {count}")
        consumer.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
