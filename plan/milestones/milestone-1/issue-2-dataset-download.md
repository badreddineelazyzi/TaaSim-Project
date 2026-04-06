# Issue #2 — Download & Upload Datasets to MinIO

**Milestone**: 1 — Infrastructure Setup & Data Exploration  
**Labels**: `data` `setup` `priority-high`  
**Assignees**: TBD  
**Estimate**: 1–2 hours

## Description

Download both required datasets and upload them to the MinIO object store in the correct bucket structure.

## Requirements

### Porto Taxi Trajectories (ECML/PKDD 2015)
- **Source**: https://www.kaggle.com/c/pkdd-15-predict-taxi-service-trajectory-i
- **Format**: CSV (~1.5 GB compressed)
- **Upload to**: `s3a://raw/porto-trips/`

### NYC TLC Trip Records
- **Source**: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- **Format**: Parquet (~500 MB per month)
- **Volume**: Download 3 months of Yellow Taxi data
- **Upload to**: `s3a://raw/nyc-tlc/`

## Tasks

- [ ] Create Kaggle account (if needed) and download Porto dataset
- [ ] Download 3 months of NYC TLC Parquet files
- [ ] Create MinIO buckets: `raw`, `curated`, `ml`, `kafka-archive`
- [ ] Upload Porto CSV to `raw/porto-trips/`
- [ ] Upload NYC TLC Parquet to `raw/nyc-tlc/`
- [ ] Verify uploads with `mc ls` commands

## Acceptance Criteria

- [ ] `mc ls raw/porto-trips/` shows Porto CSV files
- [ ] `mc ls raw/nyc-tlc/` shows 3 months of Parquet files
- [ ] All 4 top-level buckets exist in MinIO
