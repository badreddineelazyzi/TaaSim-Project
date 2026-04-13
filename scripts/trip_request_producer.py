import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

# ---------------------------------------------------------------------------
# Casablanca zone IDs (from zone_mapping.csv)
# ---------------------------------------------------------------------------
ZONE_IDS = list(range(1, 17))  # 1..16
ZONE_NAMES = {
    1: "Anfa", 2: "Maarif", 3: "Sidi Belyout", 4: "El Fida",
    5: "Mers Sultan", 6: "Ain Sebaa", 7: "Hay Mohammadi",
    8: "Roches Noires", 9: "Hay Hassani", 10: "Ain Chock",
    11: "Sidi Bernoussi", 12: "Sidi Moumen", 13: "Ben M'Sick",
    14: "Sbata", 15: "Moulay Rachid", 16: "Sidi Othmane",
}

CALL_TYPES = ["A", "B", "C"]          # dispatched, taxi-stand, street-hail
CALL_WEIGHTS = [0.30, 0.25, 0.45]     # distribution (approx from Porto)

# ---------------------------------------------------------------------------
# Demand curve — multiplier by hour-of-day  (index = hour 0..23)
# Based on typical Porto taxi demand patterns
# ---------------------------------------------------------------------------
DEMAND_CURVE = [
    0.3, 0.2, 0.15, 0.10, 0.10, 0.15,   # 0-5   (night, very low)
    0.5, 1.5, 2.0,  1.2,  1.0,  1.0,     # 6-11  (morning peak 7-8)
    1.0, 1.0, 0.9,  0.9,  1.2,  2.0,     # 12-17 (afternoon → evening peak)
    2.5, 2.0, 1.5,  1.2,  0.8,  0.5,     # 18-23 (evening peak → wind-down)
]

# Friday modifier: reduce 12-14h rate
FRIDAY_MODIFIER = {12: 0.6, 13: 0.6}


def get_demand_multiplier(sim_hour, sim_weekday):
    """Return the demand multiplier for the given simulated hour & weekday.
       weekday: 0=Mon .. 4=Fri, 5=Sat, 6=Sun
    """
    base = DEMAND_CURVE[sim_hour]
    if sim_weekday == 4:  # Friday
        base *= FRIDAY_MODIFIER.get(sim_hour, 1.0)
    return base


def generate_trip(sim_timestamp):
    """Create a single trip request event."""
    origin = random.choice(ZONE_IDS)
    # Destination: prefer different zone, small chance of same-zone
    destination = random.choice([z for z in ZONE_IDS if z != origin] or ZONE_IDS)
    call_type = random.choices(CALL_TYPES, weights=CALL_WEIGHTS, k=1)[0]

    return {
        "trip_id": str(uuid.uuid4()),
        "rider_id": f"rider_{random.randint(1000, 99999)}",
        "origin_zone": origin,
        "origin_zone_name": ZONE_NAMES.get(origin, "Unknown"),
        "destination_zone": destination,
        "destination_zone_name": ZONE_NAMES.get(destination, "Unknown"),
        "requested_at": sim_timestamp,
        "request_time_iso": datetime.fromtimestamp(sim_timestamp, tz=timezone.utc).isoformat(),
        "call_type": call_type,
    }


def main():
    parser = argparse.ArgumentParser(description="Trip Request Producer")
    parser.add_argument("--rate", type=float, default=2.0,
                        help="Base events per second at multiplier=1.0")
    parser.add_argument("--duration", type=int, default=60,
                        help="Duration in seconds to run the producer")
    parser.add_argument("--kafka", type=str, default="localhost:9092",
                        help="Kafka bootstrap servers")
    parser.add_argument("--topic", type=str, default="raw.trips",
                        help="Kafka topic to publish to")
    parser.add_argument("--sim-hour", type=int, default=-1,
                        help="Simulated hour of day (0-23). -1 = use real clock")
    parser.add_argument("--sim-weekday", type=int, default=-1,
                        help="Simulated weekday (0=Mon..6=Sun). -1 = use real clock")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.kafka,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    print(f"Trip Request Producer started — base rate={args.rate} evt/s, "
          f"duration={args.duration}s")

    start = time.time()
    sent = 0

    while time.time() - start < args.duration:
        now_dt = datetime.now(timezone.utc)
        sim_hour = args.sim_hour if args.sim_hour >= 0 else now_dt.hour
        sim_weekday = args.sim_weekday if args.sim_weekday >= 0 else now_dt.weekday()

        multiplier = get_demand_multiplier(sim_hour, sim_weekday)
        effective_rate = args.rate * multiplier

        if effective_rate <= 0:
            time.sleep(0.5)
            continue

        sleep_interval = 1.0 / effective_rate
        sim_ts = int(time.time())

        trip = generate_trip(sim_ts)
        producer.send(args.topic, key=trip["trip_id"], value=trip)
        sent += 1

        if sent % 20 == 0:
            print(f"  [{now_dt.strftime('%H:%M:%S')}] sent={sent}  "
                  f"hour={sim_hour} weekday={sim_weekday} "
                  f"multiplier={multiplier:.2f} rate={effective_rate:.1f}/s")

        # Deliberately generate ~3% out-of-order events (3 min delay)
        if random.random() < 0.03:
            late_trip = generate_trip(sim_ts - 180)
            late_trip["late_arrival"] = True
            producer.send(args.topic, key=late_trip["trip_id"], value=late_trip)
            sent += 1
            print(f"  [OUT-OF-ORDER] trip {late_trip['trip_id'][:8]}... "
                  f"sent with 3-min-old timestamp")

        time.sleep(sleep_interval)

    producer.flush()
    producer.close()
    print(f"\nDone — {sent} trip requests published to '{args.topic}' "
          f"in {args.duration}s.")


if __name__ == "__main__":
    main()
