# producer.py
import time
import json
import uuid
import random
from datetime import datetime, timezone, timedelta

from kafka import KafkaProducer
from faker import Faker

fake = Faker()

TOPIC_NAME = "rides"          # Kafka topic
BOOTSTRAP_SERVERS = "localhost:9092"


def create_fake_ride() -> dict:
    """
    Create one synthetic ride-sharing trip event.
    """
    now = datetime.now(timezone.utc)
    duration_min = random.randint(5, 40)
    dropoff_time = now + timedelta(minutes=duration_min)

    distance_km = round(random.uniform(1.0, 25.0), 2)
    surge_multiplier = random.choice([1.0, 1.0, 1.0, 1.25, 1.5, 2.0])

    base_fare = 3.0
    per_km = 1.75
    fare = round((base_fare + per_km * distance_km) * surge_multiplier, 2)

    pickup_area = fake.city()
    dropoff_area = fake.city()

    ride = {
        "trip_id": str(uuid.uuid4()),
        "driver_id": f"driver_{random.randint(1, 50)}",
        "rider_id": f"rider_{random.randint(1, 200)}",
        "pickup_area": pickup_area,
        "dropoff_area": dropoff_area,
        "pickup_time": now.isoformat(),
        "dropoff_time": dropoff_time.isoformat(),
        "distance_km": distance_km,
        "fare": float(fare),
        "surge_multiplier": surge_multiplier,
        "status": random.choice(["completed", "cancelled", "no_show"]),
        "payment_method": random.choice(["card", "cash", "voucher"]),
    }
    return ride


def get_producer() -> KafkaProducer:
    """
    Create a KafkaProducer that serializes values as JSON.
    """
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    return producer


def run_producer():
    """
    Continuously generate synthetic ride events and send them to Kafka.
    """
    producer = get_producer()
    print(f"[Producer] Sending ride events to topic '{TOPIC_NAME}' on {BOOTSTRAP_SERVERS}")
    count = 0

    try:
        while True:
            ride = create_fake_ride()
            producer.send(TOPIC_NAME, value=ride)
            producer.flush()

            count += 1
            print(
                f"[Producer] Sent ride #{count} | "
                f"trip_id={ride['trip_id']} | "
                f"fare=${ride['fare']} | "
                f"{ride['pickup_area']} -> {ride['dropoff_area']}"
            )

            # Random delay to make the stream look realistic
            time.sleep(random.uniform(0.5, 2.0))
    except KeyboardInterrupt:
        print("\n[Producer] Stopping producer...")
    except Exception as e:
        print(f"[Producer ERROR] {e}")
        raise
    finally:
        producer.close()


if __name__ == "__main__":
    run_producer()
