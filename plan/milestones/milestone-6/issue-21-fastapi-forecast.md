# Issue #21 — FastAPI Demand Forecast Endpoint

**Milestone**: 6 — ML Pipeline — Demand Forecasting  
**Labels**: `api` `fastapi` `ml` `priority-high`  
**Assignees**: TBD  
**Estimate**: 2–3 hours

## Description

Implement the FastAPI REST service with the demand forecast endpoint that loads the trained GBT model from MinIO and serves predictions.

## Endpoints

### `POST /api/v1/trips`
- **Body**: `{origin_zone, destination_zone, rider_id}`
- **Action**: Publishes event to `raw.trips` Kafka topic
- **Response**: `{trip_id, status: "pending"}`

### `GET /api/v1/trips/{trip_id}`
- **Action**: Reads from Cassandra `trips` table
- **Response**: Match status and ETA (once Job 3 has processed)

### `GET /api/v1/vehicles/zone/{zone_id}`
- **Action**: Queries Cassandra `vehicle_positions` for vehicles with `event_time > now - 30s`
- **Response**: List of active vehicles in zone

### `POST /api/v1/demand/forecast`
- **Body**: `{zone_id, datetime}`
- **Action**: Loads PipelineModel from MinIO at startup (cached in memory)
- **Response**: `{predicted_demand, zone_id}`
- **SLA**: Response time < 500ms at 20 req/s

## Implementation Notes

- [ ] Load model at FastAPI startup — cache in memory
- [ ] Use Cassandra async driver for trip lookups
- [ ] Kafka producer for trip reservation events
- [ ] Run with `uvicorn` in Docker container
- [ ] JWT auth placeholder (full implementation in Milestone 7)

## Performance Validation

- [ ] Use the provided `locust` load test script
- [ ] Target: `/demand/forecast` < 500ms at 20 req/s

## Acceptance Criteria

- [ ] All 4 endpoints implemented and responding
- [ ] `/demand/forecast` returns valid predictions
- [ ] Trip reservation publishes to Kafka correctly
- [ ] Response time < 500ms under load
