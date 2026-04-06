# Issue #9 — Flink Job 1: GPS Normalizer

**Milestone**: 3 — Stream Processing I — GPS Pipeline  
**Labels**: `streaming` `flink` `gps` `priority-critical`  
**Assignees**: TBD  
**Estimate**: 4–5 hours

## Description

Implement the GPS Normalizer Flink job — the first and most foundational real-time processing job in TaaSim. This job transforms raw GPS pings into clean, zone-assigned vehicle positions.

## Processing Pipeline

```
raw.gps (Kafka) → Validate → Deduplicate → Watermark → Zone-Map → Anonymize → Cassandra + processed.gps
```

## Implementation Steps

### 1. Kafka Source
- [ ] Configure `FlinkKafkaConsumer` on topic `raw.gps`
- [ ] Use JSON deserializer for GPS events
- [ ] Set consumer group name for offset tracking

### 2. Event-Time & Watermarks
- [ ] Assign `BoundedOutOfOrdernessWatermarks` with **3-minute max lateness**
- [ ] Extract event time from the `timestamp` field in GPS payload
- [ ] This is critical — processing-time approach will produce incorrect aggregations

### 3. Coordinate Validation
- [ ] Filter out coordinates outside Casablanca bounding box:
  - Latitude: 33.4° – 33.7° N
  - Longitude: 7.4° – 7.8° W
- [ ] Discard events with `speed > 150 km/h` (physically impossible)
- [ ] Drop events with null/invalid `taxi_id`

### 4. Stateful Deduplication
- [ ] Vehicles ping every ~4 seconds — near-duplicate events are common
- [ ] Use keyed state (by `taxi_id`) to detect and drop duplicates within a time window
- [ ] Consider dedup window of 2 seconds

### 5. Zone Mapping (Geospatial Join)
- [ ] Load `zone_mapping.csv` as **broadcast state**
- [ ] For each GPS event, determine arrondissement using bounding box lookup
- [ ] Assign `zone_id` to each event

### 6. GPS Anonymization
- [ ] Replace raw `lat`/`lon` with **zone centroid coordinates** before writing to Cassandra
- [ ] Raw coordinates must **never be persisted** — privacy requirement

### 7. Sinks
- [ ] **Cassandra sink**: Write to `vehicle_positions` table
- [ ] **Kafka sink**: Forward normalized events to `processed.gps` topic (for Job 2)

## Acceptance Criteria

- [ ] Job consumes from `raw.gps` and writes to Cassandra `vehicle_positions`
- [ ] Invalid coordinates and high-speed anomalies filtered out
- [ ] Events correctly assigned to Casablanca zones
- [ ] Anonymized coordinates (zone centroids) persisted, not raw GPS
- [ ] `processed.gps` topic receiving normalized events
