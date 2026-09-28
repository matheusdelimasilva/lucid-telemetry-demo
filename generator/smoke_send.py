"""Smoke check: build one ChargingEvent and produce it to vehicle.charging.v1.

Replaced in stage 3 by the seeded synthetic-event generator.
"""

import os

from confluent_kafka import Producer

import vehicle_charging_v1_pb2

TOPIC = "vehicle.charging.v1"


def main() -> None:
    msg = vehicle_charging_v1_pb2.ChargingEvent(
        event_id="00000000-0000-4000-8000-000000000001",
        vin="TST00000000000001",
        ts=1790000100000,
        event=vehicle_charging_v1_pb2.PLUG_IN,
        energy_wh=0,
        lat=37.4,
        lon=-122.1,
        charger_type="dc_fast",
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
