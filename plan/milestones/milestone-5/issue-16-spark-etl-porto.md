# Issue #16 — Spark ETL: Porto Dataset Cleaning & Enrichment

**Milestone**: 5 — Batch ETL + Spark Analytics  
**Labels**: `batch` `spark` `etl` `priority-critical`  
**Assignees**: TBD  
**Estimate**: 3–4 hours

## Description

Build the PySpark ETL job that reads the raw Porto taxi dataset from MinIO, cleans it, applies zone remapping, and writes enriched Parquet files to the curated zone.

## Processing Steps

### 1. Read Raw Data
- [ ] Read Porto CSV from `s3a://raw/porto-trips/`
- [ ] Parse POLYLINE JSON column using `from_json` + `explode` to produce one row per GPS point

### 2. Data Cleaning
- [ ] Drop rows where `MISSING_DATA = True`
- [ ] Deduplicate on `TRIP_ID`
- [ ] Filter out trips with empty POLYLINE (no GPS data)
- [ ] Validate coordinate ranges

### 3. Zone Remapping
- [ ] Broadcast `zone_mapping` DataFrame
- [ ] Join to add `arrondissement_id` and `zone_type` to each trip record
- [ ] Apply linear coordinate transformation (Porto → Casablanca)

### 4. Feature Computation
- [ ] Compute trip duration from POLYLINE length × 15 seconds
- [ ] Compute trip distance from GPS points (Haversine)
- [ ] Extract temporal features: hour_of_day, day_of_week, year_month

### 5. Write Output
- [ ] Write to `s3a://curated/porto-trips/` in **Parquet** with **snappy** compression
- [ ] Partition by `year_month` for efficient ML reads

## Performance Target

- [ ] Full Porto dataset (1.7M rows) processes in **< 5 minutes**
- [ ] Verify via Spark UI job duration

## Acceptance Criteria

- [ ] Curated Parquet files in MinIO with correct schema
- [ ] No duplicate TRIP_IDs in output
- [ ] Zone IDs correctly assigned
- [ ] Processing time < 5 minutes
- [ ] Partitioned by year_month
