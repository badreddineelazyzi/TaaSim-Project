# Issue #11 — Grafana Live Vehicle Position Map

**Milestone**: 3 — Stream Processing I — GPS Pipeline  
**Labels**: `dashboard` `grafana` `visualization` `priority-high`  
**Assignees**: TBD  
**Estimate**: 1–2 hours

## Description

Connect Grafana to Cassandra and create the first live dashboard panel: a geomap showing real-time vehicle positions across Casablanca zones.

## Requirements

### Datasource Setup
- [ ] Install **HadesArchitect-Cassandra-datasource** Grafana plugin
- [ ] Configure datasource pointing to Cassandra keyspace `taasim`
- [ ] Verify connection with a test query

### Vehicle Position Map Panel
- [ ] Use **Geomap** panel type
- [ ] Query: `SELECT * FROM vehicle_positions WHERE event_time > now - 30s`
- [ ] Map `taxi_id` to point marker
- [ ] Color by `status`:
  - 🟢 `available` = green
  - 🟠 `assigned` = orange
- [ ] Set appropriate zoom level for Casablanca geography
- [ ] Configure auto-refresh every **10 seconds**

### Dashboard Setup
- [ ] Create TaaSim dashboard
- [ ] Add vehicle position map as Panel 1
- [ ] Set dashboard-level auto-refresh to 10 seconds
- [ ] Save dashboard and export JSON for version control

## Acceptance Criteria

- [ ] Grafana connects to Cassandra successfully
- [ ] Vehicle positions appear on the map and update in real-time
- [ ] Colors distinguish available vs assigned vehicles
- [ ] Dashboard auto-refreshes every 10 seconds
