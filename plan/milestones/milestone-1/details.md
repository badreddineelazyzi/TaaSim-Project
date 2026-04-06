# 🗂️ Milestone 1 — Infrastructure Setup & Data Exploration

## Overview

**Duration**: Week 1  
**Theme**: Foundation — Get the platform running and understand the data  
**Goal**: Provision the entire Docker Compose stack, ingest both datasets into MinIO, explore the Porto dataset, apply the Casablanca zone remapping, and verify Kafka producers are emitting events.

## Objectives

- [ ] All services running via Docker Compose (Kafka, MinIO, Cassandra, Flink, Spark, Grafana)
- [ ] Porto and NYC TLC datasets downloaded and uploaded to MinIO `raw/`
- [ ] Porto dataset profiled in Jupyter (schema, distributions, temporal patterns)
- [ ] Zone remapping (Porto → Casablanca) implemented in PySpark and validated on OSM map
- [ ] Kafka GPS + trip request producers running and emitting events
- [ ] Team identity established (name + 1-slide startup concept)

## Acceptance Criteria

✅ Docker stack running (screenshot evidence)  
✅ Jupyter data profiling notebook completed  
✅ Zone-remapped trips visualized on Casablanca map  
✅ Kafka console consumer shows GPS and trip events  
✅ Team name + 1-slide startup concept submitted  

## Dependencies

- Kaggle account for Porto dataset download
- NYC TLC data (public download)
- Docker & Docker Compose installed on workstation (8 GB RAM minimum)

## Labels

`infrastructure` `data-exploration` `setup` `week-1`
