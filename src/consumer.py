"""Avro order consumer with retry logic, a Dead Letter Queue and live aggregation.

Per record the flow is:

    deserialize (Avro / Schema Registry)
        |- fails ------------------------> PermanentError -> DLQ
        v
    validate business rules
        |- fails ------------------------> PermanentError -> DLQ
        v
    process()  (simulated downstream call)
        |- TransientError -> retry with exponential backoff + jitter
        |     |- still failing after MAX_ATTEMPTS -> DLQ
        v
    aggregate  (running average of prices)
        v
    commit offset

Offsets are committed manually and only *after* the record has either been
processed or safely handed to the DLQ, so nothing is silently lost.
"""
import argparse
import random
import signal
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import (MessageField, SerializationContext,
                                           StringSerializer)

import config
from aggregator import OrderAggregator
from errors import PermanentError, TransientError

_running = True


def _stop(_signum, _frame):
    global _running
    _running = False
    print("\n[consumer] stop requested, finishing current record...")


# --------------------------------------------------------------------------- #
# Business logic
# --------------------------------------------------------------------------- #
def validate(order):
    """Reject records that are structurally fine but semantically impossible."""
    if not order.get("orderId"):
        raise PermanentError("orderId is empty")
    if not order.get("product"):
        raise PermanentError("product name is empty")
    price = order.get("price")
    if price is None:
        raise PermanentError("price is missing")
    if price <= 0:
        raise PermanentError(f"price must be positive, got {price:.2f}")


def process(order, transient_rate):
    """Stand-in for the real downstream work (DB write, API call, ...).

    Fails with a TransientError `transient_rate` of the time so the retry path
    is exercised during the demo.
    """
    if random.random() < transient_rate:
        raise TransientError("downstream order service temporarily unavailable")
    time.sleep(0.01)  # pretend the work costs something


def backoff_delay(attempt):
    """Exponential backoff with jitter: base * 2**(attempt-1), capped."""
    delay = min(config.RETRY_BASE_DELAY * (2 ** (attempt - 1)), config.RETRY_MAX_DELAY)
    jitter = delay * config.RETRY_JITTER
    return max(0.0, delay + random.uniform(-jitter, jitter))


# --------------------------------------------------------------------------- #
# Dead Letter Queue
# --------------------------------------------------------------------------- #
class DeadLetterQueue:
    """Publishes failed records - payload plus failure context - to the DLQ topic."""

    def __init__(self, schema_registry):
        self._producer = Producer({
            "bootstrap.servers": config.BOOTSTRAP_SERVERS,
            "acks": "all",
            "enable.idempotence": True,
        })
        self._serializer = AvroSerializer(schema_registry,
                                          config.read_schema("dead_letter.avsc"),
                                          lambda obj, _ctx: obj)
        self._key_serializer = StringSerializer("utf_8")
        self._ctx = SerializationContext(config.DLQ_TOPIC, MessageField.VALUE)
        self.count = 0

    def send(self, msg, order, error, attempts):
        order_id = (order or {}).get("orderId")
        envelope = {
            "orderId": order_id,
            "payload": msg.value() or b"",
            "errorType": type(error).__name__,
            "errorMessage": str(error),
            "attempts": attempts,
            "sourceTopic": msg.topic(),
            "sourcePartition": msg.partition(),
            "sourceOffset": msg.offset(),
            "failedAt": datetime.now(timezone.utc),
        }
        key = order_id or (msg.key().decode("utf-8", "replace") if msg.key() else "unknown")
        self._producer.produce(
            topic=config.DLQ_TOPIC,
            key=self._key_serializer(key),
            value=self._serializer(envelope, self._ctx),
            headers={
                "errorType": type(error).__name__,
                "sourceTopic": msg.topic(),
                "sourceOffset": str(msg.offset()),
                "attempts": str(attempts),
            },
        )
        # Block until the DLQ write is acknowledged: the offset of the failed
        # record must not be committed before its copy is durable.
        self._producer.flush(10)
        self.count += 1
        print(f"[consumer] -> DLQ  key={key} attempts={attempts} "
              f"{type(error).__name__}: {error}")

    def close(self):
        self._producer.flush(10)


# --------------------------------------------------------------------------- #
# Main loop
# --------------------------------------------------------------------------- #
def main():
    parser = argparse.ArgumentParser(
        description="Consume Avro orders with retry, DLQ and running aggregation.")
    parser.add_argument("--group", default=config.CONSUMER_GROUP,
                        help="consumer group id")
    parser.add_argument("--from-beginning", action="store_true",
                        help="read the topic from offset 0 on first run")
    parser.add_argument("--max-attempts", type=int, default=config.MAX_ATTEMPTS,
                        help="total attempts per record before it is dead-lettered")
    parser.add_argument("--transient-rate", type=float,
                        default=config.TRANSIENT_FAILURE_RATE,
                        help="simulated transient failure probability (0 disables)")
    parser.add_argument("--report-every", type=int, default=20,
                        help="print the full aggregation table every N orders")
    parser.add_argument("--max-records", type=int, default=0,
                        help="exit cleanly after this many records are settled "
                             "(0 = run until Ctrl+C); handy for scripted runs")
    parser.add_argument("--idle-timeout", type=float, default=0.0,
                        help="exit after this many seconds with no new records "
                             "(0 = wait forever)")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _stop)

    schema_registry = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
    deserializer = AvroDeserializer(schema_registry,
                                    config.read_schema("order.avsc"))
    ctx = SerializationContext(config.ORDERS_TOPIC, MessageField.VALUE)

    consumer = Consumer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "group.id": args.group,
        "auto.offset.reset": "earliest" if args.from_beginning else "latest",
        "enable.auto.commit": False,      # commit only after a record is settled
        "max.poll.interval.ms": 600000,   # room for blocking retry backoffs
        "session.timeout.ms": 45000,
    })
    consumer.subscribe([config.ORDERS_TOPIC])

    dlq = DeadLetterQueue(schema_registry)
    agg = OrderAggregator()
    processed = 0
    retried = 0

    print(f"[consumer] group={args.group} topic={config.ORDERS_TOPIC} "
          f"dlq={config.DLQ_TOPIC}")
    print(f"[consumer] max-attempts={args.max_attempts} "
          f"transient-rate={args.transient_rate}")
    print("[consumer] waiting for messages (Ctrl+C to stop)...\n")

    def to_dlq(msg, order, error, attempts):
        """Dead-letter a record. Returns False if the DLQ write itself failed.

        A failed DLQ write is the one case where we must not commit: the record
        would be lost entirely. We stop instead, leaving the offset uncommitted
        so the record is redelivered when the consumer restarts.
        """
        try:
            dlq.send(msg, order, error, attempts)
            return True
        except Exception as dlq_exc:
            print(f"[consumer] FATAL: could not write to DLQ: {dlq_exc}")
            print("[consumer] offset NOT committed - record will be redelivered "
                  "on restart. Stopping.")
            return False

    try:
        settled = 0
        last_record_at = time.monotonic()
        while _running:
            msg = consumer.poll(1.0)
            if msg is None:
                if (args.idle_timeout
                        and time.monotonic() - last_record_at > args.idle_timeout):
                    print(f"[consumer] idle for {args.idle_timeout:.0f}s, stopping.")
                    break
                continue
            last_record_at = time.monotonic()
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[consumer] kafka error: {msg.error()}")
                continue

            order = None
            attempts = 0
            try:
                # 1. Decode. Anything undecodable is permanently broken.
                try:
                    order = deserializer(msg.value(), ctx)
                except Exception as exc:
                    raise PermanentError(f"Avro deserialization failed: {exc}") from exc
                if order is None:
                    raise PermanentError("record value was null (tombstone)")

                # 2. Validate business rules.
                validate(order)

                # 3. Process, retrying transient failures.
                while True:
                    attempts += 1
                    try:
                        process(order, args.transient_rate)
                        break
                    except TransientError as exc:
                        if attempts >= args.max_attempts:
                            raise PermanentError(
                                f"still failing after {attempts} attempts: {exc}"
                            ) from exc
                        delay = backoff_delay(attempts)
                        retried += 1
                        print(f"[consumer] RETRY order={order['orderId']} "
                              f"attempt {attempts}/{args.max_attempts} "
                              f"in {delay:.2f}s ({exc})")
                        time.sleep(delay)

                # 4. Aggregate: running average of prices.
                running_avg = agg.add(order["product"], float(order["price"]))
                processed += 1
                print(f"[consumer] OK    order={order['orderId']:<6} "
                      f"product={order['product']:<6} price={order['price']:8.2f} "
                      f"| running_avg={running_avg:8.2f} "
                      f"| {agg.snapshot_line()}")

                if args.report_every and processed % args.report_every == 0:
                    print("\n[consumer] === running aggregation ===")
                    print(agg.report(), "\n")

            except PermanentError as exc:
                if not to_dlq(msg, order, exc, max(attempts, 1)):
                    break
            except Exception as exc:  # never let one bad record kill the loop
                if not to_dlq(msg, order, exc, max(attempts, 1)):
                    break

            # 5. Only now is it safe to advance the offset.
            consumer.commit(message=msg, asynchronous=False)

            settled += 1
            if args.max_records and settled >= args.max_records:
                print(f"[consumer] settled {settled} records, stopping.")
                break
    finally:
        print("\n[consumer] === final aggregation ===")
        print(agg.report())
        print(f"[consumer] processed={processed} retries={retried} "
              f"dead_lettered={dlq.count}")
        dlq.close()
        consumer.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
