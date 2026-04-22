"""
Vehicle GPS Producer — streams simulated taxi GPS pings to Kafka topic `raw.gps`.

Reads the Porto dataset, applies the Porto→Casablanca coordinate transformation,
adds GPS jitter and blackout simulation, then publishes events at configurable speed.

Usage:
    python vehicle_gps_producer.py [--speed 10] [--csv data/porto-trips/train.csv]
"""

import argparse
import csv
import json
import math
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
import requests

# ---------------------------------------------------------------------------
# Porto → Casablanca coordinate transformation (same as issue4)
# ---------------------------------------------------------------------------
PORTO_MIN_LAT = 41.13
PORTO_MIN_LON = -8.68
PORTO_LAT_RANGE = 41.19 - 41.13   # 0.06
PORTO_LON_RANGE = -8.55 - (-8.68) # 0.13

CASA_MIN_LAT = 33.48
CASA_MIN_LON = -7.68
CASA_LAT_RANGE = 33.63 - 33.48    # 0.15
CASA_LON_RANGE = -7.53 - (-7.68)  # 0.15

SCALE_LAT = CASA_LAT_RANGE / PORTO_LAT_RANGE
SCALE_LON = CASA_LON_RANGE / PORTO_LON_RANGE

GPS_NOISE_SIGMA = 0.0002   # ~20 m jitter
BLACKOUT_PROB   = 0.05     # 5 % chance per vehicle per event
BLACKOUT_MIN_S  = 60
BLACKOUT_MAX_S  = 180
REAL_INTERVAL_S = 15       # Porto dataset: 1 point every 15 seconds
OSRM_NEAREST_URL = "http://127.0.0.1:5000/nearest/v1/driving"


def transform_point(lon, lat):
    """Linear transform from Porto to Casablanca coords."""
    new_lat = CASA_MIN_LAT + (lat - PORTO_MIN_LAT) * SCALE_LAT
    new_lon = CASA_MIN_LON + (lon - PORTO_MIN_LON) * SCALE_LON
    return new_lon, new_lat


def add_noise(lon, lat):
    """Add Gaussian jitter to GPS coordinates."""
    return (
        lon + random.gauss(0, GPS_NOISE_SIGMA),
        lat + random.gauss(0, GPS_NOISE_SIGMA),
    )


def compute_speed(prev_lon, prev_lat, cur_lon, cur_lat, dt_seconds):
    """Approximate speed in km/h using haversine."""
    if dt_seconds == 0:
        return 0.0
    R = 6371.0
    dLat = math.radians(cur_lat - prev_lat)
    dLon = math.radians(cur_lon - prev_lon)
    a = (math.sin(dLat / 2) ** 2
         + math.cos(math.radians(prev_lat))
         * math.cos(math.radians(cur_lat))
         * math.sin(dLon / 2) ** 2)
    c = 2 * math.asin(math.sqrt(a))
    dist_km = R * c
    return (dist_km / dt_seconds) * 3600


def snap_to_road(lon, lat):
    """
    Calls the local OSRM Nearest API to snap coordinates to the road network.
    """
    url = f"{OSRM_NEAREST_URL}/{lon},{lat}?number=1"
    try:
        response = requests.get(url, timeout=2)
        if response.status_code == 200:
            data = response.json()
            if data.get("code") == "Ok" and data.get("waypoints"):
                snapped_lon = data["waypoints"][0]["location"][0]
                snapped_lat = data["waypoints"][0]["location"][1]
                return snapped_lon, snapped_lat
    except Exception as e:
        print(f"[OSRM Warning] Failed to snap coordinates: {e}")

    # Fallback to raw coordinates if OSRM fails or road not found
    return lon, lat


def main():
    parser = argparse.ArgumentParser(description="Vehicle GPS Producer")
    parser.add_argument("--speed", type=float, default=10,
                        help="Replay speed multiplier (default: 10×)")
    parser.add_argument("--csv", type=str,
                        default="data/downloads/porto/train.csv",
                        help="Path to Porto CSV file")

    parser.add_argument("--kafka", type=str, default="localhost:9092",
                        help="Kafka bootstrap servers")
    parser.add_argument("--topic", type=str, default="raw.gps",
                        help="Kafka topic to publish to")
    parser.add_argument("--sample-ratio", type=float, default=1.0,
                        help="Fraction of trips to replay in (0,1]. Default=1.0")
    parser.add_argument("--max-trips", type=int, default=0,
                        help="Max trips to replay (0 = all)")
    parser.add_argument("--seed", type=int, default=None,
                        help="Optional random seed for reproducible sampling/shuffle")
    args = parser.parse_args()

    if not (0 < args.sample_ratio <= 1.0):
        raise ValueError("--sample-ratio must be > 0 and <= 1.0")

    if args.seed is not None:
        random.seed(args.seed)

    producer = KafkaProducer(
        bootstrap_servers=args.kafka,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    sleep_between_points = REAL_INTERVAL_S / args.speed
    sent = 0
    delayed_msgs = []  # (send_at_epoch, key, payload)

    # Load all rows first, then shuffle for random trip selection
    with open(args.csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        all_rows = list(reader)

    random.shuffle(all_rows)

    if args.sample_ratio < 1.0 and all_rows:
        sample_size = max(1, int(len(all_rows) * args.sample_ratio))
        all_rows = random.sample(all_rows, sample_size)
        random.shuffle(all_rows)

    print(
        f"Vehicle GPS Producer started - speed={args.speed}x, "
        f"rows_selected={len(all_rows)}, sample_ratio={args.sample_ratio}"
        + (f", seed={args.seed}" if args.seed is not None else "")
    )

    trip_count = 0
    for row in all_rows:
        if args.max_trips and trip_count >= args.max_trips:
            break

        taxi_id = row.get("TAXI_ID", "unknown")
        trip_id = row.get("TRIP_ID", str(uuid.uuid4()))
        base_ts = int(row.get("TIMESTAMP", int(time.time())))
        polyline_str = row.get("POLYLINE", "[]")

        try:
            points = json.loads(polyline_str)
        except json.JSONDecodeError:
            continue

        if not points:
            continue

        trip_count += 1
        prev_lon, prev_lat = None, None

        for idx, (lon, lat) in enumerate(points):
            # Transform to Casablanca
            c_lon, c_lat = transform_point(lon, lat)
            # Add noise
            c_lon, c_lat = add_noise(c_lon, c_lat)
            # Snap to nearest drivable road segment via local OSRM
            c_lon, c_lat = snap_to_road(c_lon, c_lat)

            event_ts = base_ts + idx * REAL_INTERVAL_S
            speed = 0.0
            if prev_lon is not None:
                speed = compute_speed(prev_lon, prev_lat, c_lon, c_lat,
                                      REAL_INTERVAL_S)

            payload = {
                "taxi_id": taxi_id,
                "trip_id": trip_id,
                "timestamp": event_ts,
                "event_time": datetime.fromtimestamp(event_ts, tz=timezone.utc).isoformat(),
                "lat": round(c_lat, 6),
                "lon": round(c_lon, 6),
                "speed": round(speed, 2),
                "status": "MOVING" if speed > 2 else "IDLE",
            }

            # Blackout simulation — delay with 5 % probability
            if random.random() < BLACKOUT_PROB:
                delay = random.randint(BLACKOUT_MIN_S, BLACKOUT_MAX_S)
                delayed_msgs.append(
                    (time.time() + delay / args.speed, taxi_id, payload)
                )
                print(f"  [BLACKOUT] taxi={taxi_id} point #{idx} "
                      f"delayed {delay}s (sim)")
            else:
                producer.send(args.topic, key=taxi_id, value=payload)
                sent += 1

            prev_lon, prev_lat = c_lon, c_lat

            # Flush any delayed messages whose time has come
            now = time.time()
            still_delayed = []
            for send_at, key, msg in delayed_msgs:
                if now >= send_at:
                    # Mark it as a late-arriving event
                    msg["late_arrival"] = True
                    producer.send(args.topic, key=key, value=msg)
                    sent += 1
                    print(f"  [LATE] taxi={key} delivered late")
                else:
                    still_delayed.append((send_at, key, msg))
            delayed_msgs = still_delayed

            time.sleep(sleep_between_points)

        print(f"Trip {trip_count} (taxi={taxi_id}): "
              f"{len(points)} points streamed")

    # Drain remaining delayed messages
    for _, key, msg in delayed_msgs:
        msg["late_arrival"] = True
        producer.send(args.topic, key=key, value=msg)
        sent += 1

    producer.flush()
    producer.close()
    print(f"\nDone — {sent} GPS events published to '{args.topic}'.")


if __name__ == "__main__":
    main()
