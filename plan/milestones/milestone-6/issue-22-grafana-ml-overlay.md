# Issue #22 — Grafana ML Forecast Overlay Panel

**Milestone**: 6 — ML Pipeline — Demand Forecasting  
**Labels**: `dashboard` `grafana` `ml` `priority-medium`  
**Assignees**: TBD  
**Estimate**: 1–2 hours

## Description

Add a Grafana panel that overlays ML demand forecasts against actual observed demand, providing visual validation of the model's predictions.

## Requirements

### Panel 4 — ML Forecast vs Actual
- [ ] **Chart type**: Bar chart per zone
- [ ] **Actual demand**: Query `demand_zones.pending_requests` from Cassandra
- [ ] **Predicted demand**: Query `demand_zones.forecast_demand` (populated by Flink Job 2 enrichment or batch load)
- [ ] **Side-by-side** comparison per zone
- [ ] Color differentiation: actual (blue) vs predicted (orange)

### Enrichment Options
- [ ] **Option A**: Flink Job 2 enriches `demand_zones` rows with the ML forecast at write time
- [ ] **Option B**: Batch Spark job periodically writes forecast values to `demand_zones`
- [ ] Choose one approach and document the decision

## Acceptance Criteria

- [ ] Panel shows actual vs predicted demand per zone
- [ ] Visual comparison is clear and intuitive
- [ ] Panel refreshes with dashboard auto-refresh (10s)
- [ ] Approach documented (Job 2 enrichment vs batch)
