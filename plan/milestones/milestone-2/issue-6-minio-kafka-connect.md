# Issue #6 — MinIO Bucket Structure & Kafka Connect S3 Sink

**Milestone**: 2 — Storage Design & Data Architecture  
**Labels**: `storage` `minio` `kafka-connect` `priority-high`  
**Assignees**: TBD  
**Estimate**: 2–3 hours

## Description

Finalize the MinIO data lake structure and configure Kafka Connect to automatically archive raw Kafka events to MinIO for historical replay and batch processing.

## MinIO Bucket Structure

```
raw/
  porto-trips/          # Raw Porto CSV files (uploaded in M1)
  nyc-tlc/              # NYC TLC Parquet files (uploaded in M1)
  kafka-archive/        # Kafka S3 Sink mirror of all topics
    raw.gps/
    raw.trips/
curated/
  porto-trips/          # Cleaned + geo-enriched trips (Parquet)
  demand-by-zone/       # NYC aggregated demand
ml-data/
  features/             # Feature matrix for ML training
  models/
    demand_v1/          # Trained GBT model artifact
```

## Tasks

- [ ] Create all MinIO buckets and prefix structure
- [ ] Install and configure Kafka Connect with S3 Sink connector
- [ ] Configure S3 Sink to mirror `raw.gps` → `raw/kafka-archive/raw.gps/`
- [ ] Configure S3 Sink to mirror `raw.trips` → `raw/kafka-archive/raw.trips/`
- [ ] Set file rotation policy (by size or time)
- [ ] Verify archived files appear in MinIO after running producers

## Acceptance Criteria

- [ ] All bucket prefixes exist in MinIO
- [ ] Kafka Connect S3 Sink is running and healthy
- [ ] After 5 minutes of producer activity, Kafka archive files visible in MinIO
- [ ] Files are in a queryable format (JSON or Avro with proper partitioning)

## Note

- MinIO bucket names must be at least 3 characters, so this project uses `ml-data/` instead of `ml/`.
