import json
import os
import sys
import time
import duckdb
from datetime import datetime
from kafka import KafkaConsumer

# Add parent directory to path so we can import checks
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from checks.detectors import run_all_checks, update_last_message_time
from checks.alerting import handle_alarms

# Kafka configuration
KAFKA_BROKER = "localhost:9092"
TOPIC = "flight-positions"
GROUP_ID = "flight-monitor-consumer"

# Bronze table location
BRONZE_PATH = "./bronze/flights"
BRONZE_DB = "./bronze/flights.duckdb"


def setup_bronze_table(conn):
    """Create Bronze table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_flights (
            icao24 VARCHAR,
            callsign VARCHAR,
            latitude DOUBLE,
            longitude DOUBLE,
            altitude DOUBLE,
            velocity DOUBLE,
            heading DOUBLE,
            on_ground BOOLEAN,
            timestamp VARCHAR,
            source VARCHAR,
            ingestion_time VARCHAR
        )
    """)
    print("✅ Bronze table ready")


def write_to_bronze(conn, flights):
    """Write valid flights to Bronze DuckDB table."""
    if not flights:
        return 0

    ingestion_time = datetime.utcnow().isoformat()

    for flight in flights:
        conn.execute("""
            INSERT INTO bronze_flights VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            flight.get("icao24"),
            flight.get("callsign"),
            flight.get("latitude"),
            flight.get("longitude"),
            float(0 if flight.get("altitude") == "ground" else (flight.get("altitude") or 0)),
            flight.get("velocity"),
            flight.get("heading"),
            bool(flight.get("on_ground") == True),
            flight.get("timestamp"),
            flight.get("source"),
            ingestion_time
        ])

    return len(flights)


def run_consumer():
    """
    Main consumer loop:
    1. Read messages from Kafka
    2. Run all 4 alarms
    3. Block invalid records
    4. Write valid records to Bronze table
    """
    print("🚀 Starting Flight Monitor Consumer")
    print(f"   Topic: {TOPIC}")
    print(f"   Broker: {KAFKA_BROKER}")
    print(f"   Bronze table: {BRONZE_DB}")
    print("   Press Ctrl+C to stop\n")

    # Setup Bronze table
    os.makedirs("./bronze", exist_ok=True)
    conn = duckdb.connect(BRONZE_DB)
    setup_bronze_table(conn)

    # Setup Kafka consumer
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        consumer_timeout_ms=15000  # wait 15 seconds for messages
    )

    cycle = 0
    total_processed = 0
    total_blocked = 0

    print("👂 Listening for messages from Kafka...\n")

    while True:
        cycle += 1
        batch = []

        # Collect messages for 15 seconds
        try:
            for message in consumer:
                update_last_message_time()
                batch.append(message.value)

                # Process in batches of 1000
                if len(batch) >= 1000:
                    break
        except Exception:
            pass  # timeout — process what we have

        if batch:
            print(f"--- Cycle {cycle} — {len(batch)} messages received ---")

            # Run all 4 alarms
            valid_flights, alarm_results, summary = run_all_checks(batch)

            # Handle any alarms that fired
            if alarm_results:
                handle_alarms(alarm_results)

            # Write valid flights to Bronze
            written = write_to_bronze(conn, valid_flights)

            total_processed += summary["total"]
            total_blocked += summary["blocked"]

            print(f"✅ Processed: {summary['total']} | Valid: {summary['valid']} | Blocked: {summary['blocked']} | Written to Bronze: {written}")
            print(f"📊 Total so far — Processed: {total_processed} | Blocked: {total_blocked}\n")

        else:
            print(f"--- Cycle {cycle} — No messages (waiting for producer) ---")

            # Check freshness alarm
            from checks.detectors import check_freshness
            freshness = check_freshness()
            if freshness["alarm"]:
                handle_alarms([freshness])

        time.sleep(1)


if __name__ == "__main__":
    try:
        run_consumer()
    except KeyboardInterrupt:
        print("\n🛑 Consumer stopped by user")