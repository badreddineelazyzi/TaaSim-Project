# Issue #5 — Kafka Data Producers (Streaming Simulation)

**Milestone**: 1 — Infrastructure Setup & Data Exploration  
**Labels**: `streaming` `kafka` `simulation` `priority-high`  
**Assignees**: TBD  
**Estimate**: 3–4 hours

## Description

Build the Python streaming simulators that replay Porto data through Kafka topics, simulating real-time GPS feeds and citizen trip reservations.

## Scripts to Implement

### `vehicle_gps_producer.py`
- [ ] Read Porto POLYLINE field (JSON array of [lon, lat] pairs)
- [ ] Iterate coordinates at configurable speed (default: 10× real time)
- [ ] Apply Casablanca coordinate transformation
- [ ] Add GPS noise: Gaussian jitter (σ ≈ 0.0002 degrees ≈ 20m)
- [ ] Add blackout simulation: 5% probability per vehicle per event — delay send by 60–180 seconds
- [ ] Kafka message key = `taxi_id`
- [ ] Payload fields: `taxi_id`, `timestamp` (event time), `lat`, `lon`, `speed`, `status`
- [ ] Publish to Kafka topic `raw.gps`

### `trip_request_producer.py`
- [ ] Compute demand multiplier per hour from Porto dataset (aggregate trips by hour → normalize)
- [ ] Apply demand curve to control event emission rate
- [ ] Peak hours (7–9h, 17–19h): 3–5× off-peak rate
- [ ] Friday 12–14h: reduced rate
- [ ] Event fields: `trip_id` (UUID), `rider_id`, `origin_zone`, `destination_zone`, `requested_at` (event time), `call_type` (A/B/C)
- [ ] Publish to Kafka topic `raw.trips`

### `event_injector.py`
- [ ] **Demand spike**: multiply emission rate by configurable factor (e.g., 3.0) for 5 minutes in a chosen zone
- [ ] **GPS blackout**: suppress all GPS events from a set of vehicles for configurable duration
- [ ] **Rain event**: increase trip request rate globally by 1.4× for configurable period
- [ ] Design as standalone script publishing to the same Kafka topics

## Kafka Topics to Create

| Topic | Partitions | Retention |
|---|---|---|
| `raw.gps` | 3 | 7 days |
| `raw.trips` | 3 | 7 days |

## Acceptance Criteria

- [ ] `vehicle_gps_producer.py` emitting GPS events to `raw.gps` at configurable rate
- [ ] `trip_request_producer.py` emitting trip requests following demand curve
- [ ] `event_injector.py` can inject all 3 anomaly types
- [ ] Events visible in Kafka console consumer
- [ ] Out-of-order events (3-min delay) deliberately generated for watermark testing
