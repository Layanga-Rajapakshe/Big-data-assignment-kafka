"""Create the orders and DLQ topics.

Auto-topic-creation is switched off on the broker so that partition counts are
explicit and a typo in a topic name fails loudly instead of silently creating a
new topic.
"""
import sys

from confluent_kafka.admin import AdminClient, NewTopic

import config


def main() -> int:
    admin = AdminClient({"bootstrap.servers": config.BOOTSTRAP_SERVERS})

    existing = set(admin.list_topics(timeout=15).topics)
    wanted = [config.ORDERS_TOPIC, config.DLQ_TOPIC]
    to_create = [
        NewTopic(name, num_partitions=config.TOPIC_PARTITIONS,
                 replication_factor=config.TOPIC_REPLICATION)
        for name in wanted if name not in existing
    ]

    for name in wanted:
        if name in existing:
            print(f"  = {name} already exists")

    if not to_create:
        return 0

    failed = False
    for name, future in admin.create_topics(to_create).items():
        try:
            future.result()
            print(f"  + created {name} "
                  f"({config.TOPIC_PARTITIONS} partitions, rf={config.TOPIC_REPLICATION})")
        except Exception as exc:  # noqa: BLE001 - report and keep going
            print(f"  ! failed to create {name}: {exc}")
            failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
