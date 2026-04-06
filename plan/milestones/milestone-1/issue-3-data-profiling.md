# Issue #3 — Porto Dataset Profiling in Jupyter

**Milestone**: 1 — Infrastructure Setup & Data Exploration  
**Labels**: `data-exploration` `analytics` `priority-high`  
**Assignees**: TBD  
**Estimate**: 2–3 hours

## Description

Create a comprehensive Jupyter notebook that profiles the Porto Taxi Trajectories dataset. This notebook serves as the foundation for understanding the data before building the streaming and batch pipelines.

## Analysis Required

### Schema Exploration
- [ ] Document all columns: `TRIP_ID`, `CALL_TYPE`, `ORIGIN_CALL`, `ORIGIN_STAND`, `TAXI_ID`, `TIMESTAMP`, `DAY_TYPE`, `MISSING_DATA`, `POLYLINE`
- [ ] Check data types, null counts, and unique value counts

### Statistical Profiling
- [ ] Trip duration distribution (compute from POLYLINE length × 15 seconds)
- [ ] CALL_TYPE breakdown: A (dispatched), B (taxi stand), C (street hail) — percentages
- [ ] Fleet analysis: trips per taxi, active taxis per hour
- [ ] Missing data rate (`MISSING_DATA = True` percentage)

### Temporal Analysis
- [ ] Trips per hour of day (demand curve)
- [ ] Trips per day of week
- [ ] Weekend vs weekday patterns
- [ ] Friday-specific patterns (12–14h reduced rate)
- [ ] Peak hours identification (7–9am, 5–7pm)

### Spatial Analysis
- [ ] GPS polyline parsing (JSON array of [lon, lat])
- [ ] Trip start/end point distribution
- [ ] Average trip length (in km)
- [ ] Most popular origin/destination areas

### Visualizations
- [ ] Demand curve (trips/hour) — bar chart
- [ ] Trip duration histogram
- [ ] Call type pie chart
- [ ] Heatmap of trip origins on Porto map

## Acceptance Criteria

- [ ] Jupyter notebook is self-contained and reproducible
- [ ] All visualizations render correctly
- [ ] Key findings documented in markdown cells
- [ ] Notebook saved to repository
