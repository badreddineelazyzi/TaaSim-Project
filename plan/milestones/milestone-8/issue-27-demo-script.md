# Issue #27 — Live Demo Script & Anomaly Injection Rehearsal

**Milestone**: 8 — Demo Day + Investor Pitch  
**Labels**: `demo` `testing` `priority-critical`  
**Assignees**: All team members  
**Estimate**: 2–3 hours

## Description

Create and rehearse the live demo script. The demo must show the full platform operating in real-time, including a live anomaly injection that visually triggers a response in the dashboard.

## Demo Script (20 minutes)

### Act 1 — Steady State (5 min)
- [ ] Start all services, producers, and Flink jobs
- [ ] Show Grafana: vehicles moving on map, demand heatmap calm
- [ ] Explain architecture while data flows

### Act 2 — Trip Reservation (5 min)
- [ ] Live `curl POST /api/v1/trips` with JWT token
- [ ] Show trip appearing in Cassandra within 5 seconds
- [ ] Show vehicle status changing to "assigned" on map
- [ ] Show ETA returned by API

### Act 3 — Morning Rush + Anomaly (5 min)
- [ ] Run `event_injector.py` — inject demand spike in one zone
- [ ] Watch Grafana heatmap turn red in the surge zone within 60 seconds
- [ ] Show ML forecast overlay reacting
- [ ] Explain how the platform handles the anomaly

### Act 4 — KPIs & ML (5 min)
- [ ] Show KPI panel: how many trips, average ETA, match rate
- [ ] Show ML forecast vs actual — explain model accuracy
- [ ] Show feature importance — explain what drives demand

## Rehearsal Checklist

- [ ] Run the full demo script at least **2 times** before Demo Day
- [ ] Time each section — stay within 20 minutes
- [ ] Practice Q&A: each team member must be able to explain every component
- [ ] Prepare backup plan if one component fails during demo

## Acceptance Criteria

- [ ] Demo script documented and rehearsed
- [ ] All 4 acts flow smoothly
- [ ] Anomaly injection visually demonstrates platform responsiveness
- [ ] Every team member can answer technical questions about any component
