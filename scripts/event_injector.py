"""
Event Injector — injects anomaly events into Kafka topics for testing.

Supports three anomaly types:
  1. demand_spike  — multiplies trip request rate in a specific zone
  2. gps_blackout  — suppresses GPS events for specific vehicles
  3. rain_event    — increases global trip request rate

Usage:
    python event_injector.py demand_spike --zone 3 --factor 3.0 --duration 300
    python event_injector.py gps_blackout --taxis 20000589,20000596 --duration 120
    python event_injector.py rain_event --factor 1.4 --duration 600
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

# Casablanca zone IDs
ZONE_IDS = list(range(1, 17))
ZONE_NAMES = {
    1: "Anfa", 2: "Maarif", 3: "Sidi Belyout", 4: "El Fida",
    5: "Mers Sultan", 6: "Ain Sebaa", 7: "Hay Mohammadi",
    8: "Roches Noires", 9: "Hay Hassani", 10: "Ain Chock",
    11: "Sidi Bernoussi", 12: "Sidi Moumen", 13: "Ben M'Sick",
    14: "Sbata", 15: "Moulay Rachid", 16: "Sidi Othmane",
}

CALL_TYPES = ["A", "B", "C"]
CALL_WEIGHTS = [0.30, 0.25, 0.45]


def create_producer(kafka_servers):
    return KafkaProducer(
        bootstrap_servers=kafka_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


# -------------------------------------------------------------------------
# Anomaly 1: Demand Spike
# -------------------------------------------------------------------------
def inject_demand_spike(producer, zone_id, factor, duration, base_rate, topic):
    """Flood a specific zone with trip requests at [factor]× the base rate."""
    print(f"[DEMAND SPIKE] zone={zone_id} ({ZONE_NAMES.get(zone_id, '?')}), "
          f"factor={factor}x, duration={duration}s")

    effective_rate = base_rate * factor
    sleep_interval = 1.0 / effective_rate if effective_rate > 0 else 1.0

    start = time.time()
    sent = 0

    while time.time() - start < duration:
        destination = random.choice([z for z in ZONE_IDS if z != zone_id] or ZONE_IDS)
        trip = {
            "trip_id": str(uuid.uuid4()),
            "rider_id": f"rider_{random.randint(1000, 99999)}",
            "origin_zone": zone_id,
            "origin_zone_name": ZONE_NAMES.get(zone_id, "Unknown"),
            "destination_zone": destination,
            "destination_zone_name": ZONE_NAMES.get(destination, "Unknown"),
            "requested_at": int(time.time()),
            "request_time_iso": datetime.now(timezone.utc).isoformat(),
            "call_type": random.choices(CALL_TYPES, weights=CALL_WEIGHTS, k=1)[0],
            "anomaly": "demand_spike",
            "anomaly_zone": zone_id,
        }
        producer.send(topic, key=trip["trip_id"], value=trip)
        sent += 1

        if sent % 50 == 0:
            elapsed = time.time() - start
            print(f"  ... {sent} spike events sent ({elapsed:.0f}s elapsed)")

        time.sleep(sleep_interval)

    print(f"[DEMAND SPIKE] done — {sent} events injected in {duration}s.")
    return sent


# -------------------------------------------------------------------------
# Anomaly 2: GPS Blackout
# -------------------------------------------------------------------------
def inject_gps_blackout(producer, taxi_ids, duration, topic):
    """Publish blackout-start and blackout-end control events for specified taxis."""
    print(f"[GPS BLACKOUT] taxis={taxi_ids}, duration={duration}s")

    now_ts = int(time.time())

    # Publish blackout-start events
    for taxi_id in taxi_ids:
        event = {
            "taxi_id": taxi_id,
            "timestamp": now_ts,
            "event_time": datetime.fromtimestamp(now_ts, tz=timezone.utc).isoformat(),
            "lat": 0.0,
            "lon": 0.0,
            "speed": 0.0,
            "status": "BLACKOUT_START",
            "anomaly": "gps_blackout",
            "blackout_duration_s": duration,
        }
        producer.send(topic, key=taxi_id, value=event)
        print(f"  BLACKOUT_START sent for taxi {taxi_id}")

    print(f"  Waiting {duration}s for blackout period...")
    time.sleep(duration)

    # Publish blackout-end events
    end_ts = int(time.time())
    for taxi_id in taxi_ids:
        event = {
            "taxi_id": taxi_id,
            "timestamp": end_ts,
            "event_time": datetime.fromtimestamp(end_ts, tz=timezone.utc).isoformat(),
            "lat": 0.0,
            "lon": 0.0,
            "speed": 0.0,
            "status": "BLACKOUT_END",
            "anomaly": "gps_blackout",
        }
        producer.send(topic, key=taxi_id, value=event)
        print(f"  BLACKOUT_END sent for taxi {taxi_id}")

    print(f"[GPS BLACKOUT] done — {len(taxi_ids)} taxis blacked out for {duration}s.")
    return len(taxi_ids) * 2


# -------------------------------------------------------------------------
# Anomaly 3: Rain Event
# -------------------------------------------------------------------------
def inject_rain_event(producer, factor, duration, base_rate, topic):
    """Increase global trip requests by [factor]× to simulate rain."""
    print(f"[RAIN EVENT] factor={factor}x, duration={duration}s")

    effective_rate = base_rate * factor
    sleep_interval = 1.0 / effective_rate if effective_rate > 0 else 1.0

    start = time.time()
    sent = 0

    while time.time() - start < duration:
        origin = random.choice(ZONE_IDS)
        destination = random.choice([z for z in ZONE_IDS if z != origin] or ZONE_IDS)
        trip = {
            "trip_id": str(uuid.uuid4()),
            "rider_id": f"rider_{random.randint(1000, 99999)}",
            "origin_zone": origin,
            "origin_zone_name": ZONE_NAMES.get(origin, "Unknown"),
            "destination_zone": destination,
            "destination_zone_name": ZONE_NAMES.get(destination, "Unknown"),
            "requested_at": int(time.time()),
            "request_time_iso": datetime.now(timezone.utc).isoformat(),
            "call_type": random.choices(CALL_TYPES, weights=CALL_WEIGHTS, k=1)[0],
            "anomaly": "rain_event",
        }
        producer.send(topic, key=trip["trip_id"], value=trip)
        sent += 1

        if sent % 50 == 0:
            elapsed = time.time() - start
            print(f"  ... {sent} rain events sent ({elapsed:.0f}s elapsed)")

        time.sleep(sleep_interval)

    print(f"[RAIN EVENT] done — {sent} events injected in {duration}s.")
    return sent


# -------------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Event Injector — inject anomalies into Kafka streams"
    )
    parser.add_argument("--kafka", type=str, default="localhost:9092",
                        help="Kafka bootstrap servers")
    parser.add_argument("--base-rate", type=float, default=2.0,
                        help="Base events per second")

    sub = parser.add_subparsers(dest="command", required=True)

    # demand_spike
    sp_spike = sub.add_parser("demand_spike", help="Inject a zone demand spike")
    sp_spike.add_argument("--zone", type=int, required=True,
                          help="Target zone ID (1-16)")
    sp_spike.add_argument("--factor", type=float, default=3.0,
                          help="Multiplier for demand (default 3.0)")
    sp_spike.add_argument("--duration", type=int, default=300,
                          help="Duration in seconds (default 300)")
    sp_spike.add_argument("--topic", type=str, default="raw.trips")

    # gps_blackout
    sp_black = sub.add_parser("gps_blackout", help="Inject a GPS blackout")
    sp_black.add_argument("--taxis", type=str, required=True,
                          help="Comma-separated taxi IDs")
    sp_black.add_argument("--duration", type=int, default=120,
                          help="Duration in seconds (default 120)")
    sp_black.add_argument("--topic", type=str, default="raw.gps")

    # rain_event
    sp_rain = sub.add_parser("rain_event", help="Inject a rain event")
    sp_rain.add_argument("--factor", type=float, default=1.4,
                         help="Global demand multiplier (default 1.4)")
    sp_rain.add_argument("--duration", type=int, default=600,
                         help="Duration in seconds (default 600)")
    sp_rain.add_argument("--topic", type=str, default="raw.trips")

    args = parser.parse_args()
    producer = create_producer(args.kafka)

    try:
        if args.command == "demand_spike":
            inject_demand_spike(producer, args.zone, args.factor,
                                args.duration, args.base_rate, args.topic)
        elif args.command == "gps_blackout":
            taxi_list = [t.strip() for t in args.taxis.split(",")]
            inject_gps_blackout(producer, taxi_list, args.duration, args.topic)
        elif args.command == "rain_event":
            inject_rain_event(producer, args.factor, args.duration,
                              args.base_rate, args.topic)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
