"""Stage-1 smoke check: build one Smoke message and produce it to smoke.v1.

Replaced in stage 3 by the seeded synthetic-event generator.
"""

import os

from confluent_kafka import Producer

import smoke_pb2

TOPIC = "smoke.v1"


def main() -> None:
    msg = smoke_pb2.Smoke(
        vin="TST00000000000001",
        ts=1790000100000,
        note="stage1 smoke from python",
    )
    data = msg.SerializeToString()

    broker = os.environ.get("KAFKA_BROKER", "localhost:9092")
    producer = Producer({"bootstrap.servers": broker})
    producer.produce(TOPIC, key=msg.vin.encode(), value=data)
    remaining = producer.flush(30)
    if remaining:
        raise SystemExit(f"{remaining} message(s) not delivered to {broker}")

    print(f"produced {len(data)} bytes to {TOPIC} on {broker}")
    print(msg)


if __name__ == "__main__":
    main()
