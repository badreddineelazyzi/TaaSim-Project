# Issue #7 — Cassandra Schema Design & Deployment

**Milestone**: 2 — Storage Design & Data Architecture  
**Labels**: `storage` `cassandra` `data-model` `priority-critical`  
**Assignees**: TBD  
**Estimate**: 2–3 hours

## Description

Design and deploy the Cassandra keyspace and tables for TaaSim. Tables must be designed around **query patterns**, not normalization — this is the fundamental NoSQL design principle.

## Keyspace

```cql
CREATE KEYSPACE taasim
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
```

## Required Tables

### `vehicle_positions`
```cql
CREATE TABLE taasim.vehicle_positions (
    city TEXT,
    zone_id INT,
    event_time TIMESTAMP,
    taxi_id TEXT,
    lat DOUBLE,
    lon DOUBLE,
    speed DOUBLE,
    status TEXT,
    PRIMARY KEY ((city, zone_id), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC);
```
**Primary query**: All available vehicles in a given zone  
**Partition key justification**: API queries "all vehicles in zone X", not "all trips by taxi Y"

### `trips`
```cql
CREATE TABLE taasim.trips (
    city TEXT,
    date_bucket TEXT,
    created_at TIMESTAMP,
    trip_id UUID,
    rider_id TEXT,
    taxi_id TEXT,
    origin_zone INT,
    dest_zone INT,
    status TEXT,
    fare DOUBLE,
    eta_seconds INT,
    PRIMARY KEY ((city, date_bucket), created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);
```
**Primary query**: Trip history by day  
**Partition key justification**: Without date bucketing, partitions grow unboundedly → hotspots

### `demand_zones`
```cql
CREATE TABLE taasim.demand_zones (
    city TEXT,
    zone_id INT,
    window_start TIMESTAMP,
    active_vehicles INT,
    pending_requests INT,
    ratio DOUBLE,
    forecasted_demand DOUBLE,
    PRIMARY KEY ((city, zone_id), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);
```
**Primary query**: Live demand heatmap per zone

## Tasks

- [ ] Create CQL script with keyspace and all 3 tables
- [ ] Deploy to Cassandra instance via `cqlsh`
- [ ] Verify schema with `DESCRIBE KEYSPACE taasim`
- [ ] Insert sample test rows into each table
- [ ] Query each table to verify partition key access pattern works
- [ ] Document partition key design decisions

## Acceptance Criteria

- [ ] All 3 tables created successfully
- [ ] Sample data inserted and queryable
- [ ] Partition key choices documented with justification
- [ ] CQL script committed to repository
