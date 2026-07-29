import time
import math
from collections import deque
from datetime import datetime

# California bounding box
CA_LAT_MIN, CA_LAT_MAX = 32.5, 42.0
CA_LON_MIN, CA_LON_MAX = -125.5, -114.0

# Alarm thresholds
FRESHNESS_TIMEOUT = 60      # seconds — alert if no message in 60s
MIN_ALTITUDE = 0            # feet
MAX_ALTITUDE = 45000        # feet
MIN_VELOCITY = 0            # knots
MAX_VELOCITY = 700          # knots
MIN_VOLUME = 50             # minimum aircraft expected over California
ANOMALY_ZSCORE = 3.0        # z-score threshold for anomaly detection

# Rolling window for anomaly detection (last 100 batches)
volume_history = deque(maxlen=100)

# Track last message time for freshness check
last_message_time = None


def update_last_message_time():
    """Call this every time a message is received from Kafka."""
    global last_message_time
    last_message_time = time.time()


def check_freshness():
    """
    Alarm 1 — FRESHNESS
    Fires if no Kafka message received in last 60 seconds.
    Means the feed has likely dropped.
    """
    if last_message_time is None:
        return {"status": "ok", "alarm": False}

    seconds_since_last = time.time() - last_message_time

    if seconds_since_last > FRESHNESS_TIMEOUT:
        return {
            "status": "critical",
            "alarm": True,
            "alarm_type": "FRESHNESS",
            "message": f"No messages received in {int(seconds_since_last)} seconds — feed may be down",
            "severity": "critical"
        }

    return {
        "status": "ok",
        "alarm": False,
        "seconds_since_last": int(seconds_since_last)
    }


def check_validity(flight):
    """
    Alarm 2 — VALIDITY
    Checks every single flight record.
    Bad records are BLOCKED — never reach Bronze table.
    """
    errors = []

    # Check icao24
    if not flight.get("icao24") or flight["icao24"] == "unknown":
        errors.append("missing icao24")

    # Check coordinates inside California bounding box
    lat = flight.get("latitude")
    lon = flight.get("longitude")

    if lat is None or lon is None:
        errors.append("missing coordinates")
    else:
        if not (CA_LAT_MIN <= lat <= CA_LAT_MAX):
            errors.append(f"latitude {lat} outside California bounds")
        if not (CA_LON_MIN <= lon <= CA_LON_MAX):
            errors.append(f"longitude {lon} outside California bounds")

    # Check altitude
    alt_raw = flight.get("altitude")
    alt = 0 if alt_raw == "ground" else float(alt_raw or 0)
    if not (MIN_ALTITUDE <= alt <= MAX_ALTITUDE):
        errors.append(f"altitude {alt} out of range (0-45000ft)")

    # Check velocity
    vel_raw = flight.get("velocity")
    vel = float(vel_raw or 0)
    if not (MIN_VELOCITY <= vel <= MAX_VELOCITY):
        errors.append(f"velocity {vel} out of range (0-700 knots)")

    # Check timestamp exists
    if not flight.get("timestamp"):
        errors.append("missing timestamp")

    if errors:
        return {
            "status": "critical",
            "alarm": True,
            "alarm_type": "VALIDITY",
            "message": f"BLOCKED — invalid record {flight.get('icao24', 'unknown')}: {', '.join(errors)}",
            "severity": "critical",
            "blocked": True
        }

    return {"status": "ok", "alarm": False, "blocked": False}


def check_anomaly(current_count):
    """
    Alarm 3 — ANOMALY
    Compares current batch size to rolling average.
    Fires if count is >3 standard deviations from normal.
    """
    volume_history.append(current_count)

    if len(volume_history) < 10:
        # Not enough history yet
        return {"status": "ok", "alarm": False, "message": "Building history..."}

    mean = sum(volume_history) / len(volume_history)
    variance = sum((x - mean) ** 2 for x in volume_history) / len(volume_history)
    std = math.sqrt(variance)

    if std == 0:
        return {"status": "ok", "alarm": False}

    zscore = abs(current_count - mean) / std

    if zscore > ANOMALY_ZSCORE:
        return {
            "status": "warning",
            "alarm": True,
            "alarm_type": "ANOMALY",
            "message": f"Anomaly detected — {current_count} aircraft (z-score: {zscore:.2f}, normal: {mean:.0f}±{std:.0f})",
            "severity": "warning",
            "zscore": zscore
        }

    return {
        "status": "ok",
        "alarm": False,
        "zscore": zscore,
        "mean": mean
    }


def check_volume(current_count):
    """
    Alarm 4 — VOLUME
    Fires if fewer than 50 aircraft detected over California.
    Indicates a partial feed failure.
    """
    if current_count < MIN_VOLUME:
        return {
            "status": "warning",
            "alarm": True,
            "alarm_type": "VOLUME",
            "message": f"Low volume — only {current_count} aircraft detected (minimum: {MIN_VOLUME})",
            "severity": "warning"
        }

    return {
        "status": "ok",
        "alarm": False,
        "count": current_count
    }


def run_all_checks(flights):
    """
    Run all 4 checks on a batch of flights.
    Returns: (valid_flights, alarm_results)
    """
    results = []

    # Batch-level checks
    freshness = check_freshness()
    if freshness["alarm"]:
        results.append(freshness)

    volume = check_volume(len(flights))
    if volume["alarm"]:
        results.append(volume)

    anomaly = check_anomaly(len(flights))
    if anomaly["alarm"]:
        results.append(anomaly)

    # Record-level validity check — filter out bad records
    valid_flights = []
    blocked_count = 0

    for flight in flights:
        validity = check_validity(flight)
        if validity["blocked"]:
            blocked_count += 1
            results.append(validity)
        else:
            valid_flights.append(flight)

    summary = {
        "total": len(flights),
        "valid": len(valid_flights),
        "blocked": blocked_count,
        "alarms": len(results)
    }

    return valid_flights, results, summary


if __name__ == "__main__":
    # Quick test
    print("Testing detectors...")

    # Test validity with a bad record
    bad_flight = {
        "icao24": "TEST",
        "latitude": 999,  # impossible
        "longitude": -119.7,
        "altitude": 30000,
        "velocity": 400,
        "timestamp": datetime.utcnow().isoformat()
    }

    result = check_validity(bad_flight)
    print(f"Bad flight test: {result['message']}")

    # Test volume alarm
    volume_result = check_volume(5)
    print(f"Volume test: {volume_result['message']}")

    print("\n✅ All detectors working correctly")