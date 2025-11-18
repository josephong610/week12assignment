# consumer.py
import json
import psycopg2
from kafka import KafkaConsumer

TOPIC_NAME = "rides"
BOOTSTRAP_SERVERS = "localhost:9092"

PG_HOST = "localhost"
PG_DB = "kafka_db"
PG_USER = "kafka_user"
PG_PASSWORD = "kafka_password"
PG_PORT = 5432


def get_pg_connection():
    conn = psycopg2.connect(
        host=PG_HOST,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT,
    )
    conn.autocommit = False
    return conn


def ensure_table(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS rides (
            id SERIAL PRIMARY KEY,
            trip_id TEXT UNIQUE,
            driver_id TEXT,
            rider_id TEXT,
            pickup_area TEXT,
            dropoff_area TEXT,
            pickup_time TIMESTAMPTZ,
            dropoff_time TIMESTAMPTZ,
            distance_km DOUBLE PRECISION,
            fare DOUBLE PRECISION,
            surge_multiplier DOUBLE PRECISION,
            status TEXT,
            payment_method TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
    )


def get_consumer() -> KafkaConsumer:
    # Explicit group_id + earliest so we definitely read messages
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="rides-consumer-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    return consumer


def run_consumer():
    print(f"[Consumer] Connecting to Postgres at {PG_HOST}:{PG_PORT}/{PG_DB}")
    conn = get_pg_connection()
    cur = conn.cursor()
    ensure_table(cur)
    conn.commit()

    consumer = get_consumer()
    print(f"[Consumer] Subscribed to topic '{TOPIC_NAME}' on {BOOTSTRAP_SERVERS}")

    try:
        for msg in consumer:
            ride = msg.value
            print(f"[Consumer] Received from Kafka: trip_id={ride.get('trip_id')}")  # debug

            try:
                cur.execute(
                    """
                    INSERT INTO rides (
                        trip_id,
                        driver_id,
                        rider_id,
                        pickup_area,
                        dropoff_area,
                        pickup_time,
                        dropoff_time,
                        distance_km,
                        fare,
                        surge_multiplier,
                        status,
                        payment_method
                    )
                    VALUES (
                        %(trip_id)s,
                        %(driver_id)s,
                        %(rider_id)s,
                        %(pickup_area)s,
                        %(dropoff_area)s,
                        %(pickup_time)s,
                        %(dropoff_time)s,
                        %(distance_km)s,
                        %(fare)s,
                        %(surge_multiplier)s,
                        %(status)s,
                        %(payment_method)s
                    )
                    ON CONFLICT (trip_id) DO NOTHING;
                    """,
                    ride,
                )
                conn.commit()

                print(
                    f"[Consumer] Stored trip_id={ride['trip_id']} | "
                    f"fare=${ride['fare']} | "
                    f"{ride['pickup_area']} -> {ride['dropoff_area']}"
                )
            except Exception as e:
                conn.rollback()
                print(f"[Consumer ERROR] Failed to insert row: {e}")
    except KeyboardInterrupt:
        print("\n[Consumer] Stopping consumer...")
    finally:
        consumer.close()
        cur.close()
        conn.close()


if __name__ == "__main__":
    run_consumer()
