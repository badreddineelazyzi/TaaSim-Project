# Issue #17 — Spark ETL: NYC TLC Demand Aggregation

**Milestone**: 5 — Batch ETL + Spark Analytics  
**Labels**: `batch` `spark` `etl` `priority-high`  
**Assignees**: TBD  
**Estimate**: 2–3 hours

## Description

Build the PySpark ETL job that processes NYC TLC trip records to compute per-zone-per-hour demand aggregates. This provides scale-testing experience (30M+ rows) and enriches the analytical layer.

## Processing Steps

### 1. Read Raw Data
- [ ] Read NYC TLC Parquet from `s3a://raw/nyc-tlc/` (3 months)
- [ ] Fields: pickup/dropoff datetime, location IDs, trip distance, fare, passenger count

### 2. Demand Aggregation
- [ ] Group by zone (location ID) and hour
- [ ] Compute per-zone-per-hour:
  - Trip count
  - Average fare
  - Average distance
  - Average passenger count

### 3. Write Output
- [ ] Write aggregates to `s3a://curated/demand-by-zone/` in Parquet
- [ ] Partition by month

## Spark Optimization
- [ ] Use proper partitioning strategy for `groupBy`
- [ ] Leverage broadcast joins where applicable
- [ ] Monitor and tune Spark shuffle partitions
- [ ] Aim for efficient execution on 30M+ rows

## Acceptance Criteria

- [ ] NYC TLC data (3 months, ~30M rows) processes successfully
- [ ] Demand aggregates written to curated zone in Parquet
- [ ] Spark UI shows reasonable execution plan (no excessive shuffles)
