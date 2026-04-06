# 🗂️ Milestone 5 — Batch ETL + Spark Analytics

## Overview

**Duration**: Week 5  
**Theme**: Spark — Batch processing, ETL, and analytical KPIs  
**Goal**: Build Spark ETL jobs to process Porto and NYC datasets from MinIO, compute business KPIs, and load analytical results to Cassandra for Grafana visualization. This milestone bridges the gap between raw data and the ML pipeline.

## Objectives

- [ ] Spark ETL on Porto CSV: zone remapping, deduplication, Parquet output
- [ ] Spark ETL on NYC TLC: demand aggregates by zone/hour
- [ ] KPI computation: trips/zone, avg duration, peak hours, coverage gaps
- [ ] KPI aggregates loaded to Cassandra for Grafana
- [ ] Grafana KPI panel showing corridor demand and peak hours

## Acceptance Criteria

✅ Spark ETL processes Porto 1.7M rows in < 5 minutes  
✅ Spark ETL processes NYC 10M rows/month  
✅ Grafana KPI panel: corridor demand, peak hours  
✅ Curated Parquet files in MinIO `curated/`  

## Dependencies

- Milestone 2 (MinIO and Cassandra configured)
- Milestone 1 (Raw datasets uploaded to MinIO)

## Labels

`batch` `spark` `etl` `analytics` `week-5`
