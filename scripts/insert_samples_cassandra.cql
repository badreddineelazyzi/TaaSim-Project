-- Sample data insertion for Cassandra tables in the Taasim application
    -- sample records into the `vehicle_positions` table
INSERT INTO taasim.vehicle_positions (city, zone_id, event_time, taxi_id, lat, lon, speed, status) 
VALUES ('Casablanca', 1, toTimestamp(now()), 'TAXI-001', 33.5898, -7.6038, 45.5, 'FREE');

INSERT INTO taasim.vehicle_positions (city, zone_id, event_time, taxi_id, lat, lon, speed, status) 
VALUES ('Casablanca', 1, toTimestamp(now()), 'TAXI-002', 33.5900, -7.6040, 0.0, 'WAITING');

    -- Sample records for the `trips` table
INSERT INTO taasim.trips (city, date_bucket, created_at, trip_id, rider_id, taxi_id, origin_zone, dest_zone, status, fare, eta_seconds) 
VALUES ('Casablanca', '2026-04-19', toTimestamp(now()), uuid(), 'USER-123', 'TAXI-001', 1, 5, 'COMPLETED', 25.50, 600);

INSERT INTO taasim.trips (city, date_bucket, created_at, trip_id, rider_id, taxi_id, origin_zone, dest_zone, status, fare, eta_seconds) 
VALUES ('Casablanca', '2026-04-19', toTimestamp(now()), uuid(), 'USER-999', 'TAXI-002', 2, 8, 'IN_PROGRESS', 0.0, 300);

    -- Sample records for the `demand_zones` table
INSERT INTO taasim.demand_zones (city, zone_id, window_start, active_vehicles, pending_requests, ratio, forecasted_demand) 
VALUES ('Casablanca', 1, toTimestamp(now()), 50, 120, 2.4, 150.0);

INSERT INTO taasim.demand_zones (city, zone_id, window_start, active_vehicles, pending_requests, ratio, forecasted_demand) 
VALUES ('Casablanca', 2, toTimestamp(now()), 30, 80, 2.7, 100.0);