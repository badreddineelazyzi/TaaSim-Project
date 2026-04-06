# Issue #14 — Flink Job 3: Trip Matcher

**Milestone**: 4 — Stream Processing II — Demand & Trip Matching  
**Labels**: `streaming` `flink` `matching` `priority-critical`  
**Assignees**: TBD  
**Estimate**: 4–5 hours

## Description

Implement the Trip Matcher Flink job — the core real-time matching engine of TaaSim. This job receives trip requests and matches each rider to the nearest available vehicle, computing a simple ETA.

## Processing Pipeline

```
raw.trips (Kafka) → Find Vehicle in Zone → Match → Compute ETA → Cassandra trips + processed.matches
```

## Implementation Steps

### 1. Kafka Source & State
- [ ] Consume `raw.trips` topic for new trip requests
- [ ] Maintain keyed state (by `zone_id`) with available vehicle positions from `processed.gps`
- [ ] State backend: **RocksDB** (handles large state volumes)

### 2. Matching Logic
- [ ] On each trip request:
  1. Look up available vehicles in the **same zone** (from Flink keyed state)
  2. Select vehicle with `status=available` and **oldest `last_seen` timestamp**
  3. If match found → proceed to ETA computation
  4. If **no vehicle in zone** → expand search to **adjacent zones** (adjacency list from `zone_mapping.csv`)
  5. **SLA**: If no match within **5 seconds** of event time → emit `unmatched` event to monitoring topic

### 3. ETA Computation
- [ ] Simple formula: `ETA_seconds = distance_km / avg_speed_kmh × 3600`
- [ ] Use Haversine distance between vehicle and pickup zone centroid
- [ ] Average speed: configurable parameter (default: 25 km/h for urban Casablanca)

### 4. State Update
- [ ] On successful match: update vehicle status to `assigned` in Flink state
- [ ] Emit match event with: `trip_id`, `taxi_id`, `estimated_arrival_seconds`

### 5. Sinks
- [ ] **Cassandra**: Write trip record to `trips` table with match status and ETA
- [ ] **Kafka**: Publish match event to `processed.matches` topic
- [ ] **Kafka** (monitoring): Publish unmatched events to `processed.unmatched`

## Acceptance Criteria

- [ ] Trip requests are matched to vehicles within 5 seconds (P95)
- [ ] Adjacent zone fallback works when origin zone has no vehicles
- [ ] ETA is computed and included in trip record
- [ ] Vehicle status updated to `assigned` after matching
- [ ] Unmatched events emitted for SLA monitoring
- [ ] End-to-end flow verified: `POST /api/trips` → match event in < 5s
