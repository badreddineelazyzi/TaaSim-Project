# Issue #19 — Spark Feature Engineering for Demand Forecasting

**Milestone**: 6 — ML Pipeline — Demand Forecasting  
**Labels**: `ml` `spark` `feature-engineering` `priority-critical`  
**Assignees**: TBD  
**Estimate**: 3–4 hours

## Description

Build the PySpark feature engineering job that reads curated trip data and produces the feature matrix for GBT demand forecasting. Each row represents a (zone_id, 30-min time slot) with all predictive features.

## Feature Groups

### Temporal Features
- [ ] `hour_of_day` (0–23) — extracted from trip TIMESTAMP
- [ ] `day_of_week` (0–6)
- [ ] `is_weekend` (binary)
- [ ] `is_friday` (binary — Friday has unique demand patterns)

### Spatial Features
- [ ] `zone_id` (1–16) — Casablanca arrondissement
- [ ] `zone_population_density` — joined from zone reference table
- [ ] `zone_type` — residential / commercial / transit_hub (one-hot encoded)

### Weather Features
- [ ] `is_raining` (binary)
- [ ] `temperature_bucket` — cold (< 15°C) / mild / hot (> 28°C)
- [ ] Source: Open-Meteo historical weather API for Porto dates

### Lag Features (Window Functions)
- [ ] `demand_lag_1d` — demand same slot yesterday
- [ ] `demand_lag_7d` — demand same slot 7 days ago
- [ ] `rolling_7d_mean` — rolling 7-day mean for the slot

## Implementation

- [ ] Read curated trips from `s3a://curated/porto-trips/`
- [ ] Aggregate trips into 30-minute time slots per zone
- [ ] Compute all features using Spark SQL and Window functions
- [ ] Join with Open-Meteo historical weather data
- [ ] Join with zone reference table
- [ ] Write feature matrix to `s3a://ml/features/`

## Target Variable

- `demand_count` — number of trip requests per zone per 30-min slot

## Acceptance Criteria

- [ ] Feature matrix includes all 4 feature groups
- [ ] One row per (zone_id, time_slot_30min)
- [ ] No data leakage (lag features computed correctly)
- [ ] Feature matrix saved to MinIO `ml/features/`
