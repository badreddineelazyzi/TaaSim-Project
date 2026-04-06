# Issue #15 — Grafana Demand Heatmap Panel

**Milestone**: 4 — Stream Processing II — Demand & Trip Matching  
**Labels**: `dashboard` `grafana` `visualization` `priority-high`  
**Assignees**: TBD  
**Estimate**: 1–2 hours

## Description

Add a demand heatmap panel to the Grafana dashboard that visualizes real-time supply/demand ratio across Casablanca zones.

## Requirements

### Demand Heatmap Panel
- [ ] Use **Geomap** panel with **heatmap layer**
- [ ] Query: `SELECT * FROM demand_zones WHERE window_start > now - 2min`
- [ ] Color intensity = `pending_requests / active_vehicles` (the `ratio` field)
- [ ] Higher ratio (more demand, fewer vehicles) = hotter color (red)
- [ ] Lower ratio = cooler color (green/blue)

### Configuration
- [ ] Set appropriate zoom level for Casablanca
- [ ] Auto-refresh every 10 seconds
- [ ] Legend showing ratio scale
- [ ] Zone labels if supported

## Acceptance Criteria

- [ ] Heatmap updates every 30 seconds (matching Job 2 output frequency)
- [ ] High-demand zones clearly visible in hot colors
- [ ] Dashboard saved and exported
