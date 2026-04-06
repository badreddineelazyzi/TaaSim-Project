# 🎤 Milestone 8 — Demo Day + Investor Pitch

## Overview

**Duration**: Week 8  
**Theme**: Showtime — Polish, rehearse, deliver  
**Goal**: Polish the Grafana dashboard, rehearse the live demo script with anomaly injection, prepare the 10-slide pitch deck, finalize the technical report, and deliver a compelling 20-minute live demo + 10-minute Q&A.

## Objectives

- [ ] Grafana dashboard polished: vehicle map, demand heatmap, trip funnel, ML forecast
- [ ] Live demo rehearsed: morning rush → demand spike → trip matches → heatmap response
- [ ] 10-slide pitch deck prepared
- [ ] Technical report finalized (12–15 pages)
- [ ] 20-minute live demo + 10-minute Q&A delivered

## Acceptance Criteria

✅ Live 20-minute demo + 10-min Q&A completed  
✅ Technical report submitted (12–15 pages)  
✅ Pitch deck submitted (10 slides)  
✅ All 5 Demo Day checklist items working live  

## Demo Day Checklist (Must Work Live)

1. ✅ **GPS events flowing**: Kafka → Flink Job 1 → Cassandra → Grafana vehicle map (updating live)
2. ✅ **Trip reservation**: POST → Flink Job 3 match → trip record in Cassandra with ETA < 5s
3. ✅ **Demand heatmap**: Flink Job 2 → Cassandra → Grafana heatmap updating every 30s
4. ✅ **ML forecast**: Spark-trained model → FastAPI `/demand/forecast` responding < 500ms
5. ✅ **Anomaly visible**: `event_injector.py` demand spike → heatmap shows surge within 60s

## Dependencies

- All milestones 1–7 completed

## Labels

`demo` `pitch` `report` `final` `week-8`
