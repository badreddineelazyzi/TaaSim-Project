# Issue #13 — Flink Job 2: Demand Aggregator

**Milestone**: 4 — Stream Processing II — Demand & Trip Matching  
**Labels**: `streaming` `flink` `demand` `priority-critical`  
**Assignees**: TBD  
**Estimate**: 3–4 hours

## Description

Implement the Demand Aggregator Flink job that computes real-time supply/demand ratios per Casablanca zone using 30-second tumbling windows.

## Processing Pipeline

```
processed.gps + raw.trips (Kafka) → Key by zone → 30s Tumbling Window → Aggregate → Cassandra + processed.demand
```

## Implementation Steps

### 1. Dual Kafka Source
- [ ] Consume `processed.gps` topic (vehicle positions from Job 1)
- [ ] Consume `raw.trips` topic (pending trip requests)
- [ ] Use event-time processing on **both** streams

### 2. Stream Keying
- [ ] Key both streams by `zone_id`
- [ ] This enables parallel processing per zone

### 3. Tumbling Window Aggregation
- [ ] **Window size**: 30 seconds (event time)
- [ ] Per window, per zone, compute:
  - `active_vehicles`: count of distinct `taxi_id` with status=available
  - `pending_requests`: count of trip request events
  - `ratio`: `pending_requests / max(active_vehicles, 1)`

### 4. Sinks
- [ ] **Cassandra**: Upsert to `demand_zones` table using `(zone_id, window_start)` as composite key
- [ ] **Kafka**: Publish to `processed.demand` topic

## Kafka Topics

| Topic | Access |
|---|---|
| `processed.gps` | Input (from Job 1) |
| `raw.trips` | Input (from producer) |
| `processed.demand` | Output |

## Acceptance Criteria

- [ ] Job consumes from both topics and produces demand aggregates
- [ ] `demand_zones` table updated every 30 seconds with accurate counts
- [ ] Supply/demand ratio correctly computed
- [ ] `processed.demand` topic receiving aggregation events
- [ ] Verify by running `SELECT * FROM demand_zones WHERE zone_id = X LIMIT 10`
