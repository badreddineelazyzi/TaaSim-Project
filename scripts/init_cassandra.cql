-- Keyspace
CREATE KEYSPACE taasim
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

-- Required Tables

    --- Table for storing vehicle positions
CREATE TABLE taasim.vehicle_positions (
    city text,
    zone_id int,
    event_time timestamp,
    taxi_id text,
    lat double,
    lon double,
    speed double,
    status text,
    PRIMARY KEY ((city, zone_id), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC);

    -- table for storing trip information

CREATE TABLE taasim.trips (
    city text,
    date_bucket text,
    created_at timestamp,
    trip_id uuid,
    rider_id text,
    taxi_id text,
    origin_zone int,
    dest_zone int,
    status text,
    fare double,
    eta_seconds int,
    PRIMARY KEY ((city, date_bucket), created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);

    -- table for storing demand zone information

CREATE TABLE taasim.demand_zones (
    city text,
    zone_id int,
    window_start timestamp,
    active_vehicles int,
    pending_requests int,
    ratio double,
    forecasted_demand double,
    PRIMARY KEY ((city, zone_id), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

