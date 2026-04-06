# ⚡ Milestone 3 — Stream Processing I — GPS Pipeline

## Overview

**Duration**: Week 3  
**Theme**: Flink Job 1 — GPS Normalizer  
**Goal**: Implement the first Flink streaming job that ingests raw GPS events from Kafka, validates coordinates, assigns event-time watermarks, maps GPS positions to Casablanca zones, and sinks normalized data to Cassandra. Connect Grafana to display live vehicle positions.

## Objectives

- [ ] Flink Job 1 (GPS Normalizer) fully implemented and running
- [ ] Event-time processing with 3-minute watermark configured
- [ ] Flink checkpointing to MinIO every 60 seconds
- [ ] Grafana live vehicle position map connected to Cassandra
- [ ] Late event handling tested and documented

## Acceptance Criteria

✅ Flink Job 1 running with checkpointing  
✅ Grafana shows live vehicle positions updating  
✅ Watermark test: 3-minute late GPS event handled correctly (documented with evidence)  
✅ GPS events with coordinates outside Casablanca bounding box filtered out  

## Dependencies

- Milestone 1 (Kafka producers running)
- Milestone 2 (Cassandra schema deployed, MinIO configured)

## Labels

`streaming` `flink` `gps` `real-time` `week-3`
