# Issue #26 — Polish Grafana Dashboard for Demo

**Milestone**: 8 — Demo Day + Investor Pitch  
**Labels**: `dashboard` `grafana` `demo` `priority-critical`  
**Assignees**: TBD  
**Estimate**: 2–3 hours

## Description

Polish and finalize the Grafana dashboard for the live demo. All panels must be visually compelling, auto-refreshing, and tell a clear data story.

## Required Panels

### Panel 1 — Vehicle Map
- [ ] Geomap showing live vehicle positions
- [ ] Color by status (green=available, orange=assigned)
- [ ] Smooth updates, no flicker
- [ ] Appropriate zoom for Casablanca

### Panel 2 — Demand Heatmap
- [ ] Geomap with heatmap overlay
- [ ] Color intensity = supply/demand ratio
- [ ] Updates every 30 seconds
- [ ] Visually responds to anomaly injection within 60s

### Panel 3 — KPI Table
- [ ] Total trips in last 24h
- [ ] Average ETA
- [ ] % matched within 5 seconds
- [ ] Top 3 demand zones

### Panel 4 — ML Forecast vs Actual
- [ ] Bar chart per zone
- [ ] Actual demand (blue) vs predicted demand (orange)
- [ ] Clear legend

### Dashboard-Level Settings
- [ ] Auto-refresh: **10 seconds**
- [ ] Clean layout, no overlapping panels
- [ ] Professional color scheme
- [ ] TaaSim branding/logo if possible

## Acceptance Criteria

- [ ] All 4 panels render correctly
- [ ] Dashboard tells a coherent data story
- [ ] Auto-refresh works smoothly during demo
- [ ] Dashboard exported as JSON for backup
