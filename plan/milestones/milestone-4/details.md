# 🗂️ Milestone 4 — Stream Processing II — Demand & Trip Matching

## Overview

**Duration**: Week 4  
**Theme**: Flink Jobs 2 & 3 — Complete the real-time pipeline  
**Goal**: Implement the Demand Aggregator (30-second tumbling windows per zone) and Trip Matcher (nearest vehicle assignment with ETA). Achieve the end-to-end real-time flow: trip reservation → match → ETA in < 5 seconds.

## Objectives

- [ ] Flink Job 2 (Demand Aggregator) running with 30s tumbling windows
- [ ] Flink Job 3 (Trip Matcher) matching riders to nearest vehicles
- [ ] End-to-end trip flow: request → match → ETA < 5 seconds
- [ ] Adjacent zone fallback (5-second timeout)
- [ ] Grafana demand heatmap updating every 30 seconds
- [ ] RocksDB state backend configured for Job 3

## Acceptance Criteria

✅ End-to-end trip flow: request → match → ETA < 5s  
✅ Grafana demand heatmap updating every 30 seconds  
✅ Flink Job 3 state backend configured (RocksDB)  
✅ Adjacent zone fallback working when no vehicle in requested zone  

## Dependencies

- Milestone 3 (Flink Job 1 running, GPS data flowing to Cassandra)

## Labels

`streaming` `flink` `matching` `demand` `week-4`
