"""Avro order producer.

Generates randomised order messages, serialises them with the Confluent Avro
serializer (schema registered in Schema Registry, 5-byte wire format) and
publishes them to the orders topic.

To make the consumer's retry / DLQ behaviour observable during the live demo the
producer also injects two kinds of bad traffic:

  * poison orders  - valid Avro, invalid business data (negative price, blank
                     product). These can never be processed, so the consumer
                     routes them to the DLQ on the first attempt.
  * corrupt bytes  - not Avro at all. These fail deserialisation and also land
                     in the DLQ, proving the consumer survives garbage input.
"""
import argparse
import random
import signal
import sys
import time

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (MessageField, SerializationContext,
                                           StringSerializer)

import config

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]

_running = True


def _stop(_signum, _frame):
    global _running
    _running = False
    print("\n[producer] stop requested, flushing...")


def order_to_dict(order, _ctx):
    """AvroSerializer callback: the message is already a plain dict."""
    return order


def make_order(order_id: int, poison_rate: float) -> tuple[dict, str]:
    """Build one order. Returns (order, kind) where kind is 'good' or 'poison'."""
    order = {
        "orderId": str(order_id),
        "product": random.choice(PRODUCTS),
        "price": round(random.uniform(5.0, 500.0), 2),
    }
    if random.random() < poison_rate:
        # Two flavours of unprocessable-but-schema-valid data.
        if random.random() < 0.5:
            order["price"] = round(random.uniform(-500.0, -5.0), 2)
        else:
            order["product"] = ""
        return order, "poison"
    return order, "good"


def delivery_report(err, msg):
    if err is not None:
        print(f"[producer] DELIVERY FAILED: {err}")
    else:
        print(f"[producer] delivered key={msg.key().decode()} "
              f"-> {msg.topic()}[{msg.partition()}]@{msg.offset()}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Produce Avro order messages.")
    parser.add_argument("-n", "--count", type=int, default=0,
                        help="number of orders to send (0 = run until Ctrl+C)")
    parser.add_argument("-i", "--interval", type=float, default=0.5,
                        help="seconds to wait between messages")
    parser.add_argument("--start-id", type=int, default=1001,
                        help="first orderId to use")
    parser.add_argument("--poison-rate", type=float, default=config.POISON_MESSAGE_RATE,
                        help="fraction of orders that carry invalid business data")
    parser.add_argument("--corrupt-rate", type=float, default=0.0,
                        help="fraction of records emitted as non-Avro garbage bytes")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _stop)

    schema_registry = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(schema_registry,
                                     config.read_schema("order.avsc"),
                                     order_to_dict)
    key_serializer = StringSerializer("utf_8")

    producer = Producer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "acks": "all",              # wait for the full ISR before acknowledging
        "enable.idempotence": True,  # no duplicates on internal broker retries
        "retries": 5,               # transient *broker* errors are retried here
        "linger.ms": 5,
        "compression.type": "snappy",
    })

    ctx = SerializationContext(config.ORDERS_TOPIC, MessageField.VALUE)
    sent = good = poison = corrupt = 0
    order_id = args.start_id

    print(f"[producer] bootstrap={config.BOOTSTRAP_SERVERS} "
          f"registry={config.SCHEMA_REGISTRY_URL} topic={config.ORDERS_TOPIC}")
    print(f"[producer] poison-rate={args.poison_rate} corrupt-rate={args.corrupt_rate}")

    try:
        while _running and (args.count == 0 or sent < args.count):
            key = str(order_id)

            if random.random() < args.corrupt_rate:
                value = b"\x00not-an-avro-record"   # deliberately undecodable
                kind = "corrupt"
                corrupt += 1
            else:
                order, kind = make_order(order_id, args.poison_rate)
                value = avro_serializer(order, ctx)
                if kind == "poison":
                    poison += 1
                    print(f"[producer] INJECTED POISON  {order}")
                else:
                    good += 1
                    print(f"[producer] order {order}")

            producer.produce(topic=config.ORDERS_TOPIC,
                             key=key_serializer(key),
                             value=value,
                             headers={"messageKind": kind},
                             on_delivery=delivery_report)
            producer.poll(0)        # serve delivery callbacks

            sent += 1
            order_id += 1
            if args.interval:
                time.sleep(args.interval)
    finally:
        remaining = producer.flush(15)
        if remaining:
            print(f"[producer] WARNING: {remaining} message(s) not delivered")
        print(f"[producer] done: sent={sent} good={good} "
              f"poison={poison} corrupt={corrupt}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
