import json
import time
from kafka import KafkaProducer
from fetch_flights import fetch_flights

# Kafka configuration
KAFKA_BROKER = "localhost:9092"
TOPIC = "flight-positions"
POLL_INTERVAL = 15  # seconds


def create_producer():
    """Create and return a Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",  # wait for all replicas to confirm
        retries=3
    )


def run_producer():
    """
    Main producer loop.
    Fetches flights every 15 seconds and sends each one as a Kafka message.
    Runs forever until stopped with Ctrl+C.
    """
    print("🚀 Starting Flight Monitor Producer")
    print(f"   Topic: {TOPIC}")
    print(f"   Broker: {KAFKA_BROKER}")
    print(f"   Poll interval: {POLL_INTERVAL}s")
    print("   Press Ctrl+C to stop\n")

    producer = create_producer()
    cycle = 0

    while True:
        cycle += 1
        print(f"--- Cycle {cycle} ---")

        # Fetch live flights
        flights = fetch_flights()

        if flights:
            # Send each flight as a separate Kafka message
            for flight in flights:
                producer.send(TOPIC, value=flight)

            # Make sure all messages are sent
            producer.flush()
            print(f"📨 Sent {len(flights)} flights to Kafka topic '{TOPIC}'")
        else:
            print("⚠️  No flights to send this cycle")

        print(f"⏳ Waiting {POLL_INTERVAL} seconds...\n")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    try:
        run_producer()
    except KeyboardInterrupt:
        print("\n🛑 Producer stopped by user")